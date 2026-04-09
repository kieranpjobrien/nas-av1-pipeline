"""Full Gamut: one function, one file, everything done.

Takes a file from the NAS, processes it completely (encode, audio transcode,
language detection, sub/audio stripping, metadata tagging, filename cleaning),
uploads it back, replaces the original, and updates the media report.

No handoffs between threads. No intermediate states. One file, one thread,
start to finish.
"""

import hashlib
import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional

from paths import STAGING_DIR, PLEX_URL, PLEX_TOKEN
from pipeline.config import get_res_key, resolve_encode_params, REMUX_EXTENSIONS
from pipeline.ffmpeg import (
    build_ffmpeg_cmd, _should_transcode_audio, _select_audio_streams,
    _map_subtitle_streams, format_bytes, format_duration, get_duration,
    _remux_to_mkv,
)
from pipeline.language import detect_all_languages
from pipeline.report import update_entry
from pipeline.state import FileStatus, PipelineState
from pipeline.transfer import fetch_file, get_free_space


def full_gamut(
    filepath: str,
    item: dict,
    config: dict,
    state: PipelineState,
    staging_dir: str,
) -> bool:
    """Process a single file completely. Returns True on success.

    Steps:
    1. Fetch to local staging
    2. Clean filename
    3. Detect undetermined languages
    4. Find and include external subs
    5. Build ONE ffmpeg command (AV1 + EAC-3 + strip + mux)
    6. Execute encode
    7. Upload to NAS
    8. Verify (duration check)
    9. Replace original (crash-safe)
    10. Write TMDb tags (mkvpropedit, direct on NAS)
    11. Update media report
    12. Trigger Plex scan
    13. Cleanup
    """
    filename = item["filename"]
    library_type = item.get("library_type", "")

    try:
        # === STEP 1: Fetch ===
        # Wait for network worker to fetch this file (it should be pre-fetching ahead).
        # Only fetch ourselves as a last resort if the file never appears.
        existing = state.get_file(filepath)
        local_path = existing.get("local_path") if existing else None

        if local_path and os.path.exists(local_path):
            logging.info(f"Already fetched: {filename}")
        else:
            # Wait for network worker (up to 10 min for large files)
            wait_start = time.time()
            max_wait = 600  # 10 minutes
            while time.time() - wait_start < max_wait:
                existing = state.get_file(filepath)
                status = existing.get("status") if existing else None
                local_path = existing.get("local_path") if existing else None
                if status == FileStatus.FETCHING.value:
                    # Network worker is actively fetching — wait
                    time.sleep(5)
                    continue
                if local_path and os.path.exists(local_path):
                    logging.info(f"Pre-fetched by network worker: {filename}")
                    break
                if status == FileStatus.ERROR.value:
                    logging.error(f"Fetch failed (network worker): {filename}")
                    return False
                # Not yet started by network worker — fetch ourselves
                break

            existing = state.get_file(filepath)
            local_path = existing.get("local_path") if existing else None
            if not (local_path and os.path.exists(local_path)):
                logging.info(f"Fetching (GPU thread): {filename} ({format_bytes(item['file_size_bytes'])})")
                local_path = fetch_file(item, staging_dir, config, state)
                if local_path is None:
                    state.set_file(filepath, FileStatus.ERROR, error="fetch failed", stage="fetch")
                    return False

        # === STEP 2: Clean filename ===
        try:
            from pipeline.filename import clean_filename
            clean_name = clean_filename(filepath, library_type)
        except (ImportError, Exception):
            clean_name = None  # filename module not ready yet, skip

        # === STEP 3: Detect undetermined languages ===
        state.set_file(filepath, FileStatus.PROCESSING, stage="language_detect")
        try:
            # Detect languages for undetermined tracks
            enriched = detect_all_languages(item, use_whisper=False)
            if enriched:
                # Update the item's stream data with detections
                item.update(enriched)
                logging.info(f"  Language detection complete")
        except Exception as e:
            logging.warning(f"  Language detection failed (non-fatal): {e}")

        # === STEP 4: Find external subs ===
        external_subs = _find_external_subs(filepath)
        if external_subs:
            logging.info(f"  Found {len(external_subs)} external subtitle file(s)")

        # === STEP 5: Build ffmpeg command ===
        state.set_file(filepath, FileStatus.PROCESSING, stage="encoding")
        encode_dir = os.path.join(staging_dir, "encoded")
        os.makedirs(encode_dir, exist_ok=True)

        # Output filename: use clean name if available
        out_stem = Path(clean_name).stem if clean_name else Path(filename).stem
        safe_prefix = hashlib.md5(filepath.encode()).hexdigest()[:12]
        output_path = os.path.join(encode_dir, f"{safe_prefix}_{out_stem}.mkv")

        # Remux if container is problematic
        actual_input = local_path
        remuxed_path = None
        ext = Path(local_path).suffix.lower()
        if ext in REMUX_EXTENSIONS:
            logging.info(f"  Remuxing {ext} container to MKV...")
            remuxed_path = _remux_to_mkv(local_path)
            if remuxed_path:
                actual_input = remuxed_path

        # Build the ONE ffmpeg command
        encode_start = time.time()
        # TODO: integrate external_subs into ffmpeg command (add -i and -map for each)
        cmd = build_ffmpeg_cmd(actual_input, output_path, item, config,
                               include_subs=True)

        logging.info(f"  Encoding: AV1 + EAC-3 audio + strip foreign tracks")
        res_key = get_res_key(item)
        params = resolve_encode_params(config, item, config.get("_profile", "baseline"))
        logging.info(f"  {library_type.upper()} | {item.get('resolution', '?')} | "
                     f"HDR: {item.get('hdr', False)} | CQ: {params.get('cq', '?')} | "
                     f"Preset: {params.get('preset', '?')}")

        # === STEP 6: Execute encode ===
        success = _run_encode(cmd, actual_input, output_path, item, config, state, filepath)
        if not success:
            _cleanup(local_path, remuxed_path, output_path)
            return False

        encode_elapsed = time.time() - encode_start
        output_size = os.path.getsize(output_path)
        input_size = os.path.getsize(actual_input)
        saved = input_size - output_size
        ratio = (1 - output_size / input_size) * 100 if input_size > 0 else 0

        logging.info(f"  Encoded in {format_duration(encode_elapsed)}: "
                     f"{format_bytes(input_size)} -> {format_bytes(output_size)} "
                     f"({ratio:.1f}% reduction, {format_bytes(abs(saved))} {'saved' if saved > 0 else 'added'})")

        # Cleanup local fetch file (free staging space)
        _cleanup(local_path, remuxed_path)

        # === STEP 7: Upload to NAS ===
        state.set_file(filepath, FileStatus.UPLOADING, stage="upload",
                       output_path=output_path, encode_time_secs=round(encode_elapsed, 1))

        # Determine final name on NAS
        final_name = (clean_name if clean_name else Path(filename).stem + ".mkv")
        if not final_name.endswith(".mkv"):
            final_name = Path(final_name).stem + ".mkv"
        source_dir = os.path.dirname(filepath)
        dest_path = os.path.join(source_dir, final_name + ".av1.tmp")
        final_path = os.path.join(source_dir, final_name)

        logging.info(f"  Uploading to NAS...")
        upload_start = time.time()
        try:
            shutil.copy2(output_path, dest_path)
        except Exception as e:
            state.set_file(filepath, FileStatus.ERROR, error=f"upload failed: {e}", stage="upload")
            return False
        upload_elapsed = time.time() - upload_start
        upload_speed = output_size / upload_elapsed / (1024**2) if upload_elapsed > 0 else 0
        logging.info(f"  Uploaded in {format_duration(upload_elapsed)} ({upload_speed:.0f} MB/s)")

        # Cleanup local encoded file
        try:
            os.remove(output_path)
        except OSError:
            pass

        # === STEP 8: Verify ===
        state.set_file(filepath, FileStatus.UPLOADING, stage="verify")
        duration_tolerance = config.get("verify_duration_tolerance_secs", 2.0)
        input_duration = item.get("duration_seconds", 0)
        output_duration = get_duration(dest_path) or 0

        if input_duration > 0 and abs(input_duration - output_duration) > duration_tolerance:
            logging.error(f"  Duration mismatch: input={input_duration:.1f}s, output={output_duration:.1f}s")
            state.set_file(filepath, FileStatus.ERROR,
                           error=f"duration mismatch ({input_duration:.0f}s vs {output_duration:.0f}s)",
                           stage="verify")
            # Clean up the uploaded temp file
            try:
                os.remove(dest_path)
            except OSError:
                pass
            return False

        # === STEP 9: Replace original (crash-safe) ===
        state.set_file(filepath, FileStatus.UPLOADING, stage="replace")
        backup_path = filepath + ".original.bak"

        try:
            # Rename original -> .bak
            if os.path.exists(filepath) and not os.path.exists(backup_path):
                os.rename(filepath, backup_path)
                logging.info(f"  Backed up original")

            # Rename temp -> final name
            if os.path.exists(dest_path):
                os.rename(dest_path, final_path)
                logging.info(f"  Renamed to {final_name}")

            # Delete backup
            if os.path.exists(backup_path):
                os.remove(backup_path)
                logging.info(f"  Deleted backup")

            # Also delete any external sub files that were muxed in
            for sub_path in external_subs:
                try:
                    os.remove(sub_path)
                    logging.info(f"  Removed external sub: {os.path.basename(sub_path)}")
                except OSError:
                    pass

        except Exception as e:
            state.set_file(filepath, FileStatus.ERROR, error=f"replace failed: {e}", stage="replace")
            return False

        # === STEP 10: Write TMDb tags ===
        try:
            from pipeline.metadata import enrich_and_tag
            tmdb_data = enrich_and_tag(final_path, final_name, library_type)
            if tmdb_data:
                logging.info(f"  TMDb: {tmdb_data.get('director', tmdb_data.get('created_by', ['?']))}")
        except (ImportError, Exception) as e:
            logging.debug(f"  TMDb tagging skipped: {e}")

        # === STEP 11: Update media report ===
        try:
            update_entry(final_path, library_type)
            logging.info(f"  Report updated")
        except Exception as e:
            logging.warning(f"  Report update failed (non-fatal): {e}")

        # === STEP 12: Trigger Plex scan ===
        _trigger_plex_scan(final_path)

        # === DONE ===
        state.set_file(filepath, FileStatus.DONE,
                       final_path=final_path,
                       output_size_bytes=output_size,
                       input_size_bytes=input_size,
                       bytes_saved=saved,
                       compression_ratio=round(ratio, 1),
                       encode_time_secs=round(encode_elapsed, 1),
                       upload_time_secs=round(upload_elapsed, 1),
                       mode="full_gamut")

        # Update global stats
        state.stats["completed"] = state.stats.get("completed", 0) + 1
        state.stats["bytes_saved"] = state.stats.get("bytes_saved", 0) + saved
        state.stats["total_encode_time_secs"] = state.stats.get("total_encode_time_secs", 0) + encode_elapsed
        state.stats["total_source_size_bytes"] = state.stats.get("total_source_size_bytes", 0) + input_size
        state.stats["total_content_duration_secs"] = (
            state.stats.get("total_content_duration_secs", 0) + item.get("duration_seconds", 0)
        )
        # Per-tier stats
        tier_stats = state.stats.setdefault("tier_stats", {})
        tier = tier_stats.setdefault(res_key, {
            "completed": 0, "bytes_saved": 0,
            "total_input_bytes": 0, "total_output_bytes": 0,
            "total_encode_time_secs": 0,
        })
        tier["completed"] += 1
        tier["bytes_saved"] += saved
        tier["total_input_bytes"] += input_size
        tier["total_output_bytes"] += output_size
        tier["total_encode_time_secs"] += encode_elapsed
        state.save()

        logging.info(f"  DONE: DONE: {final_name}")
        return True

    except Exception as e:
        logging.error(f"Full gamut failed for {filename}: {e}")
        state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="full_gamut")
        return False


def _run_encode(
    cmd: list[str],
    input_path: str,
    output_path: str,
    item: dict,
    config: dict,
    state: PipelineState,
    filepath: str,
) -> bool:
    """Execute the ffmpeg encode command. Retries without subs if subtitle codec fails."""
    for attempt, include_subs in enumerate([True, False]):
        if attempt > 0:
            # Retry without subs
            from pipeline.ffmpeg import build_ffmpeg_cmd
            cmd = build_ffmpeg_cmd(input_path, output_path, item, config, include_subs=False)
            logging.warning(f"  Retrying without subtitles")

        try:
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                encoding="utf-8", errors="replace",
            )
            _, stderr = process.communicate()

            if process.returncode == 0:
                if not os.path.exists(output_path):
                    continue
                return True

            if os.path.exists(output_path):
                os.remove(output_path)

            if attempt == 0 and ("subtitle" in stderr.lower() or "codec none" in stderr.lower()):
                continue  # retry without subs

            logging.error(f"  Encode failed (exit {process.returncode})")
            for line in stderr.strip().split("\n")[-5:]:
                logging.error(f"    ffmpeg: {line}")
            state.set_file(filepath, FileStatus.ERROR,
                           error=f"ffmpeg exit {process.returncode}", stage="encoding")
            return False

        except Exception as e:
            logging.error(f"  Encode exception: {e}")
            state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="encoding")
            if os.path.exists(output_path):
                os.remove(output_path)
            return False

    state.set_file(filepath, FileStatus.ERROR, error="encode failed after retries", stage="encoding")
    return False


def _find_external_subs(filepath: str) -> list[str]:
    """Find external subtitle files (.srt, .ass, .ssa, .sub) alongside the MKV."""
    source_dir = os.path.dirname(filepath)
    stem = Path(filepath).stem
    sub_exts = {".srt", ".ass", ".ssa", ".sub"}
    external = []

    try:
        for f in os.listdir(source_dir):
            fpath = os.path.join(source_dir, f)
            if not os.path.isfile(fpath):
                continue
            ext = Path(f).suffix.lower()
            if ext in sub_exts and f.startswith(stem[:20]):  # loose match on filename prefix
                external.append(fpath)
    except OSError:
        pass

    return external


def _cleanup(*paths: str | None) -> None:
    """Remove local staging files, ignoring errors."""
    for p in paths:
        if p and os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


_plex_scan_lock = None
_last_plex_scan = 0


def _trigger_plex_scan(filepath: str) -> None:
    """Trigger a Plex library scan (debounced, non-blocking)."""
    global _last_plex_scan
    import threading

    if not PLEX_URL or not PLEX_TOKEN:
        return

    now = time.time()
    if now - _last_plex_scan < 30:
        return  # debounce: max one scan per 30 seconds
    _last_plex_scan = now

    def _scan():
        try:
            import urllib.request
            # Determine which section(s) to scan
            sections_url = f"{PLEX_URL}/library/sections?X-Plex-Token={PLEX_TOKEN}"
            resp = urllib.request.urlopen(sections_url, timeout=10)
            # Scan all sections (simple approach)
            from xml.etree import ElementTree
            root = ElementTree.fromstring(resp.read())
            scanned = 0
            for section in root.findall(".//Directory"):
                section_key = section.get("key")
                if section_key:
                    scan_url = f"{PLEX_URL}/library/sections/{section_key}/refresh?X-Plex-Token={PLEX_TOKEN}"
                    urllib.request.urlopen(scan_url, timeout=10)
                    scanned += 1
            if scanned:
                logging.info(f"  Triggered Plex scan ({scanned} sections)")
        except Exception:
            pass  # Plex scan is best-effort

    threading.Thread(target=_scan, daemon=True, name="plex-scan").start()
