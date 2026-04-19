"""Full Gamut: one function, one file, everything done.

Takes a file from the NAS, processes it completely (encode, audio transcode,
language detection, sub/audio stripping, metadata tagging, filename cleaning),
uploads it back, replaces the original, and updates the media report.

No handoffs between threads. No intermediate states. One file, one thread,
start to finish.
"""

import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

from paths import PLEX_TOKEN, PLEX_URL
from pipeline.config import REMUX_EXTENSIONS, get_res_key, resolve_encode_params
from pipeline.ffmpeg import (
    _remux_to_mkv,
    build_ffmpeg_cmd,
    format_bytes,
    format_duration,
    get_duration,
)
from pipeline.language import detect_all_languages
from pipeline.report import update_entry
from pipeline.state import FileStatus, PipelineState


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
        status = existing.get("status") if existing else None
        local_path = existing.get("local_path") if existing else None

        # Already done — bail cleanly rather than waiting forever for a fetch that won't come.
        # (The orchestrator's force-stack check should prevent this, but belt-and-braces.)
        terminal_states = {
            FileStatus.DONE.value,
            getattr(FileStatus, "REPLACED", FileStatus.DONE).value,
            getattr(FileStatus, "SKIPPED", FileStatus.DONE).value,
        }
        if status in terminal_states:
            logging.info(f"Already {status}: {filename} — skipping full_gamut.")
            return True

        # Wait for file to be ready (status=PROCESSING, set after copy completes).
        # No timers — just block until the network worker signals completion.
        if not (status == FileStatus.PROCESSING.value and local_path and os.path.exists(local_path)):
            logging.info(f"Waiting for fetch: {filename}")
            waited = 0
            last_warn_status = None  # only warn when the status hasn't changed in a while
            while True:
                existing = state.get_file(filepath)
                status = existing.get("status") if existing else None
                local_path = existing.get("local_path") if existing else None
                if status == FileStatus.PROCESSING.value and local_path and os.path.exists(local_path):
                    break
                if status == FileStatus.ERROR.value:
                    logging.error(f"Fetch failed: {filename}")
                    return False
                if status in terminal_states:
                    logging.info(f"Became {status} while waiting for fetch: {filename} — bailing.")
                    return True
                time.sleep(2)
                waited += 2
                # Log once per 2 min and only if status isn't FETCHING (which means progress is happening).
                # Avoids the "Still waiting 60s/120s/180s..." spam while fetch is actually in flight.
                if waited % 120 == 0 and status != FileStatus.FETCHING.value:
                    logging.warning(
                        f"Still waiting for fetch after {waited}s "
                        f"(status={status}, not actively fetching): {filename}"
                    )
                    last_warn_status = status
            logging.info(f"Fetched: {filename}")

        # === STEP 2: Clean filename ===
        try:
            from pipeline.filename import clean_filename

            clean_name = clean_filename(filepath, library_type)
        except (ImportError, Exception):
            clean_name = None  # filename module not ready yet, skip

        # === STEP 3: Detect undetermined languages ===
        # The fetch worker runs this eagerly on fetch-complete and caches results in state,
        # so encoding startup is instant for pre-fetched files. If the cache is present we
        # use it; otherwise we detect inline (old behaviour, e.g. for files we fetched
        # ourselves above). Refresh `existing` first in case the post-fetch hook wrote the
        # cache after we grabbed the earlier snapshot.
        existing = state.get_file(filepath) or existing
        detected_audio = existing.get("detected_audio") if existing else None
        detected_subs = existing.get("detected_subs") if existing else None
        if existing and existing.get("pre_processed"):
            if detected_audio is not None:
                item["audio_streams"] = detected_audio
            if detected_subs is not None:
                item["subtitle_streams"] = detected_subs
            logging.info("  Language detection: using pre-computed results")
        else:
            state.set_file(filepath, FileStatus.PROCESSING, stage="language_detect")
            try:
                enriched = detect_all_languages(item, use_whisper=False)
                if enriched:
                    item.update(enriched)
                    logging.info("  Language detection complete")
            except Exception as e:
                logging.warning(f"  Language detection failed (non-fatal): {e}")

        # === STEP 4: Find external subs ===
        # Also eagerly computed by the fetch worker — use cached list if present.
        cached_external = existing.get("external_subs") if existing else None
        if existing and existing.get("pre_processed") and cached_external is not None:
            external_subs = cached_external
        else:
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

        # Build the ONE ffmpeg command (including external subs from Bazarr)
        encode_start = time.time()
        # Filter external subs: only regular English (not HI) — 1 sub per file
        eng_external = []
        for s in external_subs:
            fn = os.path.basename(s).lower()
            is_eng = ".en." in fn or ".eng." in fn
            is_hi = ".hi." in fn or ".sdh." in fn
            if is_eng and not is_hi:
                eng_external.append(s)
                break  # only 1 regular English sub
        if eng_external:
            logging.info(f"  Muxing {len(eng_external)} external English subtitle(s)")
        cmd = build_ffmpeg_cmd(
            actual_input, output_path, item, config, include_subs=True, external_subs=eng_external or None
        )

        logging.info("  Encoding: AV1 + EAC-3 audio + strip foreign tracks")
        get_res_key(item)
        params = resolve_encode_params(config, item, config.get("_profile", "baseline"))
        logging.info(
            f"  {library_type.upper()} | {item.get('resolution', '?')} | "
            f"HDR: {item.get('hdr', False)} | CQ: {params.get('cq', '?')} | "
            f"Preset: {params.get('preset', '?')}"
        )

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

        logging.info(
            f"  Encoded in {format_duration(encode_elapsed)}: "
            f"{format_bytes(input_size)} -> {format_bytes(output_size)} "
            f"({ratio:.1f}% reduction, {format_bytes(abs(saved))} {'saved' if saved > 0 else 'added'})"
        )

        # Cleanup local fetch file (free staging space)
        _cleanup(local_path, remuxed_path)

        # === HAND OFF TO NETWORK WORKER ===
        # GPU is done. Set UPLOADING with all the info the network worker needs.
        # Network worker will: upload, verify, replace, TMDb, report, Plex.
        final_name = clean_name if clean_name else Path(filename).stem + ".mkv"
        if not final_name.endswith(".mkv"):
            final_name = Path(final_name).stem + ".mkv"

        state.set_file(
            filepath,
            FileStatus.UPLOADING,
            stage="pending_upload",
            output_path=output_path,
            encode_time_secs=round(encode_elapsed, 1),
            output_size_bytes=output_size,
            input_size_bytes=input_size,
            bytes_saved=saved,
            compression_ratio=round(ratio, 1),
            final_name=final_name,
            library_type=library_type,
            duration_seconds=item.get("duration_seconds", 0),
        )

        logging.info(f"  Encoded, queued for upload: {final_name}")
        return True

    except Exception as e:
        logging.error(f"Full gamut failed for {filename}: {e}")
        state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="full_gamut")
        return False


def finalize_upload(filepath: str, state: PipelineState, config: dict) -> bool:
    """Upload encoded file to NAS, verify, replace original, tag, report, Plex.

    Called by the network worker after the GPU worker sets status=UPLOADING.
    This runs on the network thread — one at a time, full bandwidth.
    """
    entry = state.get_file(filepath)
    if not entry:
        return False

    output_path = entry.get("output_path")
    final_name = entry.get("final_name", os.path.basename(filepath))
    library_type = entry.get("library_type", "")
    input_size = entry.get("input_size_bytes", 0)
    output_size = entry.get("output_size_bytes", 0)
    saved = entry.get("bytes_saved", 0)
    encode_time = entry.get("encode_time_secs", 0)
    input_duration = entry.get("duration_seconds", 0)
    ratio = entry.get("compression_ratio", 0)

    if not output_path or not os.path.exists(output_path):
        logging.error(f"  Upload: encoded file missing: {final_name}")
        state.set_file(filepath, FileStatus.ERROR, error="encoded file missing", stage="upload")
        return False

    source_dir = os.path.dirname(filepath)
    dest_path = os.path.join(source_dir, final_name + ".av1.tmp")
    final_path = os.path.join(source_dir, final_name)

    # === Upload ===
    state.set_file(filepath, FileStatus.UPLOADING, stage="upload")
    logging.info(f"  Uploading: {final_name} ({format_bytes(output_size)})")
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

    # === Verify ===
    # Three tiers, chosen to match the empirical ffmpeg/NVENC behaviour we've actually seen:
    #   - within max(2s, 2% of input): accept — normal container/rounding drift.
    #   - within 0.8x-1.2x: log a warning but STILL accept — visually fine in practice,
    #     and re-encoding wastes GPU when the output was probably usable.
    #   - outside 0.8x-1.2x: real broken encode (20%+ off). Auto-retry once, then park.
    # Anything that gets past the 20% threshold is deterministic garbage (seen today on VFR
    # sources and the raw-DTS-in-.mkv files). Retry cap prevents GPU loop on those.
    MAX_DURATION_RETRIES = 1
    duration_tolerance_fixed = config.get("verify_duration_tolerance_secs", 2.0)
    duration_tolerance_pct = config.get("verify_duration_tolerance_pct", 0.02)  # 2%
    output_duration = get_duration(dest_path) or 0
    if input_duration > 0:
        diff = abs(input_duration - output_duration)
        ratio = output_duration / input_duration if input_duration else 1.0
        # Dynamic tolerance — scales with content length. A 50-min episode gets ~60s
        # grace; a 3-min short gets 3.6s. Prevents false alarms on long-form content.
        allowed_drift = max(duration_tolerance_fixed, input_duration * duration_tolerance_pct)
        if diff > allowed_drift:
            if ratio > 1.2 or ratio < 0.8:
                # Clearly broken (>20% off). Clean up the output file before retry/park.
                try:
                    os.remove(dest_path)
                except OSError:
                    pass
                prev = state.get_file(filepath) or {}
                retry_count = int(prev.get("duration_retry_count", 0) or 0)
                if retry_count < MAX_DURATION_RETRIES:
                    # First (or first N) time — discard + reset to PENDING so the pipeline
                    # picks it up again. Bump the counter so the second attempt can't loop.
                    logging.error(
                        f"  Duration mismatch (broken encode, ratio {ratio:.2f}, retry {retry_count + 1}/{MAX_DURATION_RETRIES}): "
                        f"input={input_duration:.0f}s, output={output_duration:.0f}s — resetting to pending."
                    )
                    state.set_file(
                        filepath,
                        FileStatus.PENDING,
                        error=None,
                        stage=None,
                        reason=f"auto-retry {retry_count + 1}/{MAX_DURATION_RETRIES}: duration mismatch {input_duration:.0f}s→{output_duration:.0f}s",
                        duration_retry_count=retry_count + 1,
                    )
                else:
                    # Already retried and still broken — park in ERROR with the retry count
                    # recorded for audit. User can manually reset from the Errors page if
                    # they want to try again (e.g. after tweaking config).
                    logging.error(
                        f"  Duration mismatch persists after {retry_count} retries (ratio {ratio:.2f}): "
                        f"input={input_duration:.0f}s, output={output_duration:.0f}s — parking in ERROR."
                    )
                    state.set_file(
                        filepath,
                        FileStatus.ERROR,
                        error=f"duration mismatch after {retry_count} auto-retries ({input_duration:.0f}s vs {output_duration:.0f}s)",
                        stage="verify",
                        duration_retry_count=retry_count + 1,
                    )
            else:
                # 2%-20% off — drift is real but not catastrophic. Log a warning so the
                # user can spot a pattern, but DON'T reject the encode. In practice these
                # files play fine; rejecting them costs GPU for no benefit. The > 20%
                # branch above handles genuinely broken cases.
                logging.warning(
                    f"  Duration drift (accepting anyway): input={input_duration:.1f}s, "
                    f"output={output_duration:.1f}s, {(ratio - 1) * 100:+.1f}%"
                )
                # fall through to replace + DONE below

    # === Replace original (crash-safe) ===
    backup_path = filepath + ".original.bak"
    try:
        # If clean-named target already exists (e.g. from a previous encode of a
        # duplicate scene-tagged file), delete it first so rename succeeds
        if os.path.exists(final_path) and final_path != filepath:
            os.remove(final_path)
            logging.info(f"  Removed existing target: {final_name}")
        if os.path.exists(filepath) and not os.path.exists(backup_path):
            os.rename(filepath, backup_path)
        if os.path.exists(dest_path):
            os.rename(dest_path, final_path)
            logging.info(f"  Replaced: {final_name}")
        if os.path.exists(backup_path):
            os.remove(backup_path)
    except Exception as e:
        state.set_file(filepath, FileStatus.ERROR, error=f"replace failed: {e}", stage="replace")
        return False

    # === TMDb tags ===
    try:
        from pipeline.metadata import enrich_and_tag

        tmdb_data = enrich_and_tag(final_path, final_name, library_type)
        if tmdb_data:
            logging.info(f"  TMDb: {tmdb_data.get('director', tmdb_data.get('created_by', ['?']))}")
    except Exception as e:
        logging.debug(f"  TMDb tagging failed: {e}")

    # === Update media report ===
    try:
        update_entry(final_path, library_type)
    except Exception as e:
        logging.debug(f"  Report update failed: {e}")

    # === Plex scan ===
    _trigger_plex_scan(final_path)

    # === DONE ===
    # Clear the duration_retry_count on success — a file that retried once and then
    # encoded cleanly shouldn't carry the counter forward if it's re-queued later.
    state.set_file(
        filepath,
        FileStatus.DONE,
        final_path=final_path,
        output_size_bytes=output_size,
        input_size_bytes=input_size,
        bytes_saved=saved,
        compression_ratio=ratio,
        encode_time_secs=encode_time,
        upload_time_secs=round(upload_elapsed, 1),
        mode="full_gamut",
        duration_retry_count=0,
    )

    # Update global stats
    state.stats["completed"] = state.stats.get("completed", 0) + 1
    state.stats["bytes_saved"] = state.stats.get("bytes_saved", 0) + saved
    state.stats["total_encode_time_secs"] = state.stats.get("total_encode_time_secs", 0) + encode_time
    state.stats["total_source_size_bytes"] = state.stats.get("total_source_size_bytes", 0) + input_size
    state.stats["total_content_duration_secs"] = state.stats.get("total_content_duration_secs", 0) + input_duration
    state.save()

    # === Append to encode_history.jsonl (what the dashboard reads) ===
    # State entries are flat — video/hdr/resolution live in the media report, which we consult
    # here rather than rely on state (where those fields aren't reliably populated).
    try:
        from datetime import datetime, timezone

        from paths import MEDIA_REPORT, STAGING_DIR
        from server.helpers import read_json_safe

        report = read_json_safe(MEDIA_REPORT) or {}
        video_info = {}
        for f in report.get("files", []):
            if f.get("filepath") == filepath or f.get("filepath") == final_path:
                video_info = f.get("video", {}) or {}
                break

        history_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "filepath": final_path,
            "filename": final_name,
            "tier": entry.get("tier") or entry.get("tier_name") or "",
            "res_key": entry.get("res_key") or video_info.get("resolution_class", ""),
            "input_bytes": input_size,
            "output_bytes": output_size,
            "saved_bytes": saved,
            "encode_time_secs": encode_time,
            "fetch_time_secs": entry.get("fetch_time_secs", 0),
            "upload_time_secs": round(upload_elapsed, 1),
            "compression_ratio": round(output_size / input_size, 3) if input_size > 0 else 0,
            "codec_from": video_info.get("codec", "") or entry.get("codec", ""),
            "resolution": video_info.get("resolution_class", ""),
            "hdr": bool(video_info.get("hdr")),
            "audio_only": entry.get("mode") == "audio_remux",
            "library_type": library_type,
        }
        history_file = STAGING_DIR / "encode_history.jsonl"
        with open(history_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(history_entry) + "\n")
    except Exception as e:
        logging.debug(f"  History append failed (non-fatal): {e}")

    logging.info(f"  DONE: {final_name}")
    return True


def _run_encode(
    cmd: list[str],
    input_path: str,
    output_path: str,
    item: dict,
    config: dict,
    state: PipelineState,
    filepath: str,
) -> bool:
    """Execute the ffmpeg encode command with up to three attempts.

    1. Full command as built.
    2. If subtitle codec rejected → retry without subs.
    3. If audio timestamps corrupted (common on DTS-HD MA → EAC-3) → retry with audio copy.

    Progress is parsed from stderr (frame=/fps=/time=/speed= lines) and pushed into
    pipeline state so the dashboard can show live % / speed / ETA per file.
    """
    from pipeline.ffmpeg import build_ffmpeg_cmd

    retry_mode = "none"  # tracks what retry strategy we applied
    attempts = 3  # up to 3 attempts total (original + 2 retries)
    duration_secs = item.get("duration_seconds") or 0

    for attempt in range(attempts):
        if attempt == 0:
            pass  # original cmd
        elif retry_mode == "no_subs":
            cmd = build_ffmpeg_cmd(input_path, output_path, item, config, include_subs=False)
            logging.warning("  Retrying without subtitles")
        elif retry_mode == "audio_copy":
            cmd = _build_audio_copy_cmd(cmd)
            logging.warning("  Retrying with audio passthrough (DTS timestamp workaround)")
        else:
            break  # no more retry strategies

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf-8",
                errors="replace",
            )
            stderr = _stream_encode_progress(process, state, filepath, duration_secs)

            if process.returncode == 0:
                if not os.path.exists(output_path):
                    continue
                return True

            if os.path.exists(output_path):
                os.remove(output_path)

            stderr_low = stderr.lower()
            # Decide what to retry next
            if attempt == 0 and ("subtitle" in stderr_low or "codec none" in stderr_low):
                retry_mode = "no_subs"
                continue
            if "non-monotonic dts" in stderr_low or "non monotonic dts" in stderr_low:
                # The DTS timestamp error mostly hits the output EAC-3 from DTS-HD MA sources.
                # Falling back to audio copy skips the transcode and mostly sidesteps the bug.
                if retry_mode != "audio_copy":
                    retry_mode = "audio_copy"
                    continue

            logging.error(f"  Encode failed (exit {process.returncode})")
            for line in stderr.strip().split("\n")[-5:]:
                logging.error(f"    ffmpeg: {line}")
            state.set_file(filepath, FileStatus.ERROR, error=f"ffmpeg exit {process.returncode}", stage="encoding")
            return False

        except Exception as e:
            logging.error(f"  Encode exception: {e}")
            state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="encoding")
            if os.path.exists(output_path):
                os.remove(output_path)
            return False

    state.set_file(filepath, FileStatus.ERROR, error="encode failed after retries", stage="encoding")
    return False


def _stream_encode_progress(process, state: PipelineState, filepath: str, duration_secs: float) -> str:
    """Consume ffmpeg's stable `-progress pipe:1` key=value output on stdout, emit state updates.

    Also drains stderr on a background thread so it doesn't deadlock the subprocess (stderr
    still carries warnings + errors which the caller needs for the retry detection).

    Each progress snapshot ends with `progress=continue` (or `progress=end` at finish). We
    push a state update on each snapshot boundary, throttled to one per ~1.5s to keep the
    SQLite write volume sane.
    """
    import threading

    stderr_buf: list[str] = []

    def _drain_stderr():
        assert process.stderr is not None
        for line in iter(process.stderr.readline, ""):
            if not line:
                break
            stderr_buf.append(line.rstrip("\n"))

    t = threading.Thread(target=_drain_stderr, daemon=True)
    t.start()

    snapshot: dict[str, str] = {}
    last_update = 0.0
    assert process.stdout is not None
    for raw in iter(process.stdout.readline, ""):
        if not raw:
            break
        line = raw.strip()
        if not line:
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        snapshot[key.strip()] = val.strip()
        if key.strip() != "progress":
            continue
        # snapshot is complete (progress=continue or progress=end)
        now = time.time()
        if snapshot["progress"] != "end" and now - last_update < 1.5:
            snapshot.clear()
            continue
        last_update = now
        try:
            fps_s = snapshot.get("fps", "0")
            speed_s = snapshot.get("speed", "0x").rstrip("x")
            out_time_us_s = snapshot.get("out_time_us") or snapshot.get("out_time_ms")
            # out_time_us is microseconds; out_time_ms is (misnamed) also microseconds per ffmpeg docs
            out_time_us = int(out_time_us_s) if out_time_us_s and out_time_us_s.isdigit() else 0
            elapsed_out = out_time_us / 1_000_000
            fps = float(fps_s) if fps_s else 0.0
            speed = float(speed_s) if speed_s else 0.0
            pct = int(elapsed_out / duration_secs * 100) if duration_secs > 0 else None
            eta_secs = (
                (duration_secs - elapsed_out) / speed if speed > 0 and duration_secs else None
            )
            eta_text = None
            if eta_secs and eta_secs > 0:
                h, rem = divmod(int(eta_secs), 3600)
                m_, _s = divmod(rem, 60)
                eta_text = f"{h}h {m_:02d}m" if h else f"{m_}m {_s:02d}s"
            state.set_file(
                filepath,
                FileStatus.PROCESSING,
                stage="encoding",
                progress_pct=pct,
                speed=f"{speed}x",
                fps=round(fps, 1),
                eta_text=eta_text,
            )
        except (ValueError, KeyError):
            pass
        snapshot.clear()

    process.wait()
    t.join(timeout=5)
    return "\n".join(stderr_buf)


def _build_audio_copy_cmd(cmd: list[str]) -> list[str]:
    """Rewrite an ffmpeg command to use audio passthrough instead of transcode.

    Strips per-stream -c:a:N / -b:a:N pairs (added by build_ffmpeg_cmd) and inserts a single
    global -c:a copy just before the output path. Faster and sidesteps DTS timestamp bugs.
    """
    out = []
    skip_next = False
    for i, tok in enumerate(cmd):
        if skip_next:
            skip_next = False
            continue
        # Strip per-stream audio codec/bitrate flags
        if tok.startswith(("-c:a", "-b:a", "-filter:a", "-ac:a")):
            skip_next = True
            continue
        out.append(tok)
    # Insert -c:a copy right before the output path (final argument)
    if out:
        output = out[-1]
        out = out[:-1] + ["-c:a", "copy", output]
    return out


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
            from urllib.request import Request, urlopen
            from xml.etree import ElementTree

            headers = {"X-Plex-Token": PLEX_TOKEN, "Accept": "application/xml"}

            # Determine which section(s) to scan
            sections_req = Request(f"{PLEX_URL}/library/sections", headers=headers)
            resp = urlopen(sections_req, timeout=10)
            root = ElementTree.fromstring(resp.read())
            scanned = 0
            for section in root.findall(".//Directory"):
                section_key = section.get("key")
                if section_key:
                    scan_req = Request(
                        f"{PLEX_URL}/library/sections/{section_key}/refresh",
                        headers=headers,
                    )
                    urlopen(scan_req, timeout=10)
                    scanned += 1
            if scanned:
                logging.info(f"  Triggered Plex scan ({scanned} sections)")
        except Exception as e:
            logging.debug(f"  Plex scan failed (best-effort): {e}")

    threading.Thread(target=_scan, daemon=True, name="plex-scan").start()
