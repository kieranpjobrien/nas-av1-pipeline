"""Gap Filler: CPU-only cleanup for already-AV1 files.

Intelligently chooses the cheapest method:
- mkvmerge on NAS for track removal (no fetch, ~30s)
- mkvpropedit on NAS for metadata (no fetch, ~1s)
- os.rename on NAS for filename cleaning (no fetch, instant)
- Fetch + ffmpeg for audio codec change (~5 min)

Most operations need NO FETCH — they work directly on the NAS file.
"""

import json
import logging
import os
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from paths import STAGING_DIR
from pipeline.ffmpeg import (
    _should_transcode_audio, format_bytes, format_duration,
)
from pipeline.report import update_entry
from pipeline.state import FileStatus, PipelineState


_KEEP_LANGS = {"eng", "en", "english", "und", ""}

_MKVMERGE_SEARCH = [
    r"C:\Program Files\MKVToolNix\mkvmerge.exe",
    r"C:\Program Files (x86)\MKVToolNix\mkvmerge.exe",
]

_MKVPROPEDIT_SEARCH = [
    r"C:\Program Files\MKVToolNix\mkvpropedit.exe",
    r"C:\Program Files (x86)\MKVToolNix\mkvpropedit.exe",
]


def _find_tool(name: str, search_paths: list[str]) -> Optional[str]:
    found = shutil.which(name)
    if found:
        return found
    for path in search_paths:
        if os.path.isfile(path):
            return path
    return None


# Drive letter mapping for mkvmerge/mkvpropedit (they don't support UNC paths)
_drive_map_lock = threading.Lock()
_mapped_drive: Optional[str] = None
_mapped_share: Optional[str] = None


def _unc_to_mapped(filepath: str) -> tuple[str, bool]:
    """Convert a UNC path to a mapped drive path for mkvmerge.

    Returns (mapped_path, was_mapped). If already a local path, returns as-is.
    The mapping is shared across all workers and persists for the session.
    """
    global _mapped_drive, _mapped_share

    if not filepath.startswith("\\\\"):
        return filepath, False

    # Extract share: \\server\share
    parts = filepath.split(os.sep)
    if len(parts) < 5:
        return filepath, False
    share = os.sep + os.sep + parts[2] + os.sep + parts[3]
    rest = os.sep.join(parts[4:])

    with _drive_map_lock:
        if _mapped_share == share and _mapped_drive:
            return _mapped_drive + os.sep + rest, True

        # Check if an existing mapping already covers this share
        try:
            net_result = subprocess.run(
                ["net", "use"], capture_output=True, text=True, timeout=10,
            )
            for line in net_result.stdout.splitlines():
                parts = line.split()
                if len(parts) >= 3 and parts[1].endswith(":") and parts[2].lower() == share.lower():
                    _mapped_drive = parts[1]
                    _mapped_share = share
                    return _mapped_drive + os.sep + rest, True
        except Exception:
            pass

        # Find a free drive letter and map it
        for letter in "MNOPQRSTUVWXYZ":
            if not os.path.exists(letter + ":" + os.sep):
                drive = letter + ":"
                result = subprocess.run(
                    ["net", "use", drive, share, "/persistent:no"],
                    capture_output=True, text=True, timeout=10,
                )
                if result.returncode == 0:
                    _mapped_drive = drive
                    _mapped_share = share
                    logging.info(f"  Mapped {share} -> {drive}")
                    return drive + os.sep + rest, True

    return filepath, False


@dataclass
class GapAnalysis:
    """What a file needs to be fully 'done'."""
    needs_track_removal: bool = False
    needs_audio_transcode: bool = False
    needs_metadata: bool = False
    needs_filename_clean: bool = False
    needs_language_detect: bool = False
    needs_sub_mux: bool = False
    needs_foreign_sub_cleanup: bool = False
    audio_keep_indices: list[int] = field(default_factory=list)
    sub_keep_indices: list[int] = field(default_factory=list)
    audio_transcode_indices: list[int] = field(default_factory=list)
    external_subs: list[str] = field(default_factory=list)
    foreign_external_subs: list[str] = field(default_factory=list)
    clean_name: Optional[str] = None

    @property
    def needs_fetch(self) -> bool:
        """Only audio codec transcoding requires fetching to local."""
        return self.needs_audio_transcode

    @property
    def needs_anything(self) -> bool:
        return (self.needs_track_removal or self.needs_audio_transcode or
                self.needs_metadata or self.needs_filename_clean or
                self.needs_language_detect or self.needs_sub_mux or
                self.needs_foreign_sub_cleanup)

    def describe(self) -> str:
        parts = []
        if self.needs_track_removal:
            parts.append("strip tracks")
        if self.needs_sub_mux:
            parts.append(f"mux {len(self.external_subs)} subs")
        if self.needs_foreign_sub_cleanup:
            parts.append(f"delete {len(self.foreign_external_subs)} foreign subs")
        if self.needs_audio_transcode:
            parts.append("transcode audio")
        if self.needs_metadata:
            parts.append("write metadata")
        if self.needs_filename_clean:
            parts.append("clean filename")
        if self.needs_language_detect:
            parts.append("detect languages")
        return " + ".join(parts) if parts else "nothing"


def _scan_external_subs(filepath: str, gaps: GapAnalysis) -> None:
    """Scan for external subtitle files next to the MKV. Called lazily during gap_fill."""
    source_dir = os.path.dirname(filepath)
    stem = Path(filepath).stem
    sub_exts = {".srt", ".ass", ".ssa", ".sub"}
    eng_tokens = {".en.", ".eng.", ".en-", ".eng-"}
    hi_tokens = {".hi.", ".sdh."}
    found_regular_eng = False
    try:
        for f in os.listdir(source_dir):
            ext = Path(f).suffix.lower()
            if ext in sub_exts and f.startswith(stem[:20]):
                fl = f.lower()
                is_eng = any(t in fl for t in eng_tokens)
                is_hi = any(t in fl for t in hi_tokens)
                if is_eng and not is_hi and not found_regular_eng:
                    gaps.external_subs.append(os.path.join(source_dir, f))
                    found_regular_eng = True
                else:
                    # HI, foreign, or duplicate English — all go to cleanup
                    gaps.foreign_external_subs.append(os.path.join(source_dir, f))
    except OSError:
        pass
    if gaps.external_subs:
        gaps.needs_sub_mux = True
        gaps.needs_track_removal = True
    if gaps.foreign_external_subs:
        gaps.needs_foreign_sub_cleanup = True


def analyse_gaps(file_entry: dict, config: dict) -> GapAnalysis:
    """Analyse what an already-AV1 file needs to be fully done."""
    gaps = GapAnalysis()
    audio_streams = file_entry.get("audio_streams", [])
    sub_streams = file_entry.get("subtitle_streams", [])

    # Audio codec check
    for i, a in enumerate(audio_streams):
        codec = (a.get("codec_raw") or a.get("codec", "")).lower()
        if codec in ("eac3", "e-ac-3"):
            gaps.audio_keep_indices.append(i)
        else:
            if _should_transcode_audio(a, config):
                gaps.needs_audio_transcode = True
                gaps.audio_transcode_indices.append(i)
            gaps.audio_keep_indices.append(i)

    # Foreign audio check (keep track 0 + English/und)
    if len(audio_streams) > 1:
        clean_audio_keep = [0]  # always keep original
        for i, a in enumerate(audio_streams):
            if i == 0:
                continue
            lang = (a.get("language") or a.get("detected_language") or "und").lower().strip()
            if lang in _KEEP_LANGS:
                clean_audio_keep.append(i)
        if len(clean_audio_keep) < len(audio_streams):
            gaps.needs_track_removal = True
            gaps.audio_keep_indices = clean_audio_keep

    # Subtitle selection: keep exactly 1 regular English sub + forced/foreign parts
    # Strip: HI subs, duplicate English subs, all non-English subs
    eng_sub_langs = {"eng", "en", "english"}
    sub_keep = []
    found_regular_eng = False
    for i, s in enumerate(sub_streams):
        lang = (s.get("language") or s.get("detected_language") or "und").lower().strip()
        title = (s.get("title") or "").lower()
        is_forced = "forced" in title or "foreign" in title
        is_hi = "hearing" in title or "sdh" in title or ".hi" in title

        if is_forced:
            sub_keep.append(i)  # always keep forced/foreign parts subs
        elif lang in eng_sub_langs and not is_hi and not found_regular_eng:
            sub_keep.append(i)  # keep first regular English sub
            found_regular_eng = True
        # Everything else (HI, non-English, duplicate English, und) gets stripped

    if len(sub_keep) < len(sub_streams):
        gaps.needs_track_removal = True
    gaps.sub_keep_indices = sub_keep

    # TMDb metadata check
    if not file_entry.get("tmdb"):
        gaps.needs_metadata = True

    # Filename check
    try:
        from pipeline.filename import clean_filename
        clean = clean_filename(file_entry["filepath"], file_entry.get("library_type", ""))
        if clean and clean != file_entry["filename"]:
            gaps.needs_filename_clean = True
            gaps.clean_name = clean
    except (ImportError, Exception):
        pass

    # Undetermined language check
    for a in audio_streams:
        lang = (a.get("language") or "und").lower().strip()
        if lang in ("und", "unk", "") and not a.get("detected_language"):
            gaps.needs_language_detect = True
            break
    for s in sub_streams:
        lang = (s.get("language") or "und").lower().strip()
        if lang in ("und", "unk", "") and not s.get("detected_language"):
            gaps.needs_language_detect = True
            break

    # External subtitle check — deferred to gap_fill() to avoid slow NAS scans
    # during queue building. Set a flag so gap_fill knows to check.
    gaps._check_external_subs = True
    if gaps.external_subs:
        gaps.needs_sub_mux = True

    return gaps


def gap_fill(
    filepath: str,
    file_entry: dict,
    gaps: GapAnalysis,
    config: dict,
    state: PipelineState,
) -> bool:
    """Fill gaps for an already-AV1 file. Returns True on success.

    Intelligently chooses the cheapest method per operation.
    Most operations work directly on the NAS — no fetch needed.
    """
    filename = file_entry["filename"]
    library_type = file_entry.get("library_type", "")

    # Verify file still exists (may have been renamed by another worker)
    if not os.path.exists(filepath):
        # Try the clean name in the same directory
        if gaps.clean_name:
            alt_path = os.path.join(os.path.dirname(filepath), gaps.clean_name)
            if os.path.exists(alt_path):
                filepath = alt_path
                filename = gaps.clean_name
                gaps.needs_filename_clean = False  # already clean
            else:
                logging.warning(f"  File not found (renamed?): {filename}")
                return True  # not an error, just already handled
        else:
            logging.warning(f"  File not found: {filename}")
            return True

    # Deferred external sub check (avoids slow NAS scans during queue building)
    if getattr(gaps, '_check_external_subs', False):
        _scan_external_subs(filepath, gaps)

    if not gaps.needs_anything:
        state.set_file(filepath, FileStatus.DONE, mode="gap_filler", reason="nothing to do")
        return True

    logging.info(f"Gap fill: {filename} ({gaps.describe()})")
    state.set_file(filepath, FileStatus.PROCESSING, stage="gap_fill", mode="gap_filler")

    try:
        # Language detection first (informs track selection)
        if gaps.needs_language_detect:
            try:
                from pipeline.language import detect_all_languages
                enriched = detect_all_languages(file_entry, use_whisper=False)
                if enriched:
                    file_entry.update(enriched)
                    # Re-analyse with detected languages
                    gaps = analyse_gaps(file_entry, config)
                    logging.info(f"  Languages detected, re-analysed: {gaps.describe()}")
            except Exception as e:
                logging.warning(f"  Language detection failed: {e}")

        # Track removal and/or sub muxing (mkvmerge on NAS — no fetch)
        if (gaps.needs_track_removal or gaps.needs_sub_mux) and not gaps.needs_audio_transcode:
            success = _strip_tracks_on_nas(filepath, gaps)
            if not success:
                state.set_file(filepath, FileStatus.ERROR, error="track strip failed", stage="gap_fill")
                return False
            # Delete external sub files after successful mux
            if gaps.needs_sub_mux:
                for sub_path in gaps.external_subs:
                    try:
                        os.remove(sub_path)
                        logging.info(f"  Muxed and removed: {os.path.basename(sub_path)}")
                    except OSError:
                        pass

        # Delete foreign external subs (not muxed, just cleaned up)
        if gaps.needs_foreign_sub_cleanup:
            for sub_path in gaps.foreign_external_subs:
                try:
                    os.remove(sub_path)
                    logging.info(f"  Deleted foreign sub: {os.path.basename(sub_path)}")
                except OSError:
                    pass

        # Audio transcode (needs fetch + ffmpeg)
        elif gaps.needs_audio_transcode:
            success = _audio_transcode(filepath, file_entry, gaps, config, state)
            if not success:
                return False  # state already set by _audio_transcode

        # Filename clean (os.rename on NAS)
        if gaps.needs_filename_clean and gaps.clean_name:
            new_path = _rename_file(filepath, gaps.clean_name)
            if new_path:
                filepath = new_path  # update path for subsequent operations

        # TMDb metadata (mkvpropedit on NAS)
        if gaps.needs_metadata:
            try:
                from pipeline.metadata import enrich_and_tag
                tmdb_data = enrich_and_tag(filepath, os.path.basename(filepath), library_type)
                if tmdb_data:
                    logging.info(f"  TMDb: written")
            except (ImportError, Exception) as e:
                logging.debug(f"  TMDb skipped: {e}")

        # Update media report
        try:
            update_entry(filepath, library_type)
        except Exception as e:
            logging.warning(f"  Report update failed: {e}")

        state.set_file(filepath, FileStatus.DONE, mode="gap_filler")
        state.stats["gap_filled"] = state.stats.get("gap_filled", 0) + 1
        state.save()
        logging.info(f"  DONE: Gap filled: {filename}")
        return True

    except Exception as e:
        logging.error(f"Gap fill failed for {filename}: {e}")
        state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="gap_fill")
        return False


def _strip_tracks_on_nas(filepath: str, gaps: GapAnalysis) -> bool:
    """Remove foreign audio/subtitle tracks using mkvmerge directly on NAS.

    No fetch needed — mkvmerge reads and writes on the NAS path.
    """
    if not os.path.exists(filepath):
        logging.error(f"  Track strip failed: file not found: {os.path.basename(filepath)}")
        return False

    mkvmerge = _find_tool("mkvmerge", _MKVMERGE_SEARCH)
    if not mkvmerge:
        logging.error("mkvmerge not found — cannot strip tracks")
        return False

    # Map UNC paths — mkvmerge doesn't support UNC
    mapped_fp, was_mapped = _unc_to_mapped(filepath)
    tmp_path = mapped_fp + ".gapfill_tmp.mkv"
    cmd = [mkvmerge, "-o", tmp_path]

    # Get track IDs from mkvmerge (they differ from ffprobe stream indices)
    audio_track_ids = []
    sub_track_ids = []
    try:
        id_result = subprocess.run(
            [mkvmerge, "--identify", "--identification-format", "json", mapped_fp],
            capture_output=True, text=True, timeout=60, encoding="utf-8", errors="replace",
        )
        if id_result.returncode <= 1 and id_result.stdout:
            id_data = json.loads(id_result.stdout)
            for track in id_data.get("tracks", []):
                if track["type"] == "audio":
                    audio_track_ids.append(track["id"])
                elif track["type"] == "subtitles":
                    sub_track_ids.append(track["id"])
    except (subprocess.TimeoutExpired, json.JSONDecodeError, KeyError, TypeError) as e:
        logging.warning(f"  mkvmerge identify failed, using ffprobe indices: {e}")
        # Fallback: calculate track IDs from stream order (video=0, then audio, then subs)
        # This works for standard MKV layouts
        audio_track_ids = list(range(1, 1 + len(gaps.audio_keep_indices) + 5))  # generous range
        sub_track_ids = []  # can't determine, skip sub stripping for safety
        if gaps.needs_track_removal and not gaps.audio_keep_indices:
            return False  # can't safely strip without track IDs

    # Audio track selection (convert relative indices to absolute track IDs)
    if gaps.audio_keep_indices and audio_track_ids:
        keep_ids = [audio_track_ids[i] for i in gaps.audio_keep_indices if i < len(audio_track_ids)]
        if keep_ids:
            cmd.extend(["--audio-tracks", ",".join(str(tid) for tid in keep_ids)])
    elif gaps.audio_keep_indices:
        cmd.extend(["--audio-tracks", ",".join(str(i) for i in gaps.audio_keep_indices)])

    # Subtitle track selection (convert relative indices to absolute track IDs)
    # When muxing external subs: strip ALL internal English subs (external replaces them)
    # Only keep forced/foreign-parts internal subs
    if gaps.external_subs and sub_track_ids:
        # Keep only forced subs — the external sub replaces all internal English
        forced_keep = []
        for track in id_data.get("tracks", []):
            if track["type"] == "subtitles":
                props = track.get("properties", {})
                name = (props.get("track_name") or "").lower()
                if "forced" in name or "foreign" in name or props.get("forced_track"):
                    forced_keep.append(track["id"])
        if forced_keep:
            cmd.extend(["--subtitle-tracks", ",".join(str(tid) for tid in forced_keep)])
        else:
            cmd.extend(["--no-subtitles"])  # strip all internal, external will be added
    elif gaps.sub_keep_indices and sub_track_ids:
        keep_ids = [sub_track_ids[i] for i in gaps.sub_keep_indices if i < len(sub_track_ids)]
        if keep_ids:
            cmd.extend(["--subtitle-tracks", ",".join(str(tid) for tid in keep_ids)])
    elif gaps.sub_keep_indices:
        cmd.extend(["--subtitle-tracks", ",".join(str(i) for i in gaps.sub_keep_indices)])
    elif not gaps.sub_keep_indices and gaps.needs_track_removal:
        cmd.extend(["--no-subtitles"])

    cmd.append(mapped_fp)

    # Add external subtitle files as additional inputs
    for sub_path in gaps.external_subs:
        mapped_sub, _ = _unc_to_mapped(sub_path)
        lang = "eng"
        fl = os.path.basename(sub_path).lower()
        if ".en." in fl or ".eng." in fl:
            lang = "eng"
        cmd.extend(["--language", f"0:{lang}", mapped_sub])

    try:
        src_size = os.path.getsize(mapped_fp)
        timeout = max(900, int(src_size / (1024 * 1024)))  # 15 min minimum, 1s per MB (NAS shared with 3 workers + network)

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode >= 2:
            err = result.stderr.strip() or result.stdout.strip()
            logging.error(f"  mkvmerge failed: {err[:500]}")
            return False

        if not os.path.exists(tmp_path):
            return False

        dst_size = os.path.getsize(tmp_path)
        if dst_size < src_size * 0.3:
            logging.error(f"  Output too small ({format_bytes(dst_size)} vs {format_bytes(src_size)})")
            os.remove(tmp_path)
            return False

        os.replace(tmp_path, mapped_fp)
        saved = src_size - dst_size
        logging.info(f"  Stripped tracks: {format_bytes(src_size)} -> {format_bytes(dst_size)} "
                     f"({format_bytes(abs(saved))} {'saved' if saved > 0 else 'added'})")
        return True

    except subprocess.TimeoutExpired:
        logging.error(f"  mkvmerge timed out")
        return False
    except Exception as e:
        logging.error(f"  Track strip failed: {e}")
        return False
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def _audio_transcode(
    filepath: str,
    file_entry: dict,
    gaps: GapAnalysis,
    config: dict,
    state: PipelineState,
) -> bool:
    """Transcode audio codecs to EAC-3. Requires fetch to local + upload back."""
    from pipeline.ffmpeg import build_audio_remux_cmd
    import hashlib

    staging_dir = str(STAGING_DIR)
    fetch_dir = os.path.join(staging_dir, "fetch")
    encode_dir = os.path.join(staging_dir, "encoded")
    os.makedirs(fetch_dir, exist_ok=True)
    os.makedirs(encode_dir, exist_ok=True)

    safe_name = hashlib.md5(filepath.encode()).hexdigest()[:12] + "_" + file_entry["filename"]
    local_path = os.path.join(fetch_dir, safe_name)
    output_path = os.path.join(encode_dir, safe_name)

    try:
        # Fetch
        state.set_file(filepath, FileStatus.FETCHING, stage="fetch")
        logging.info(f"  Fetching for audio transcode...")
        shutil.copy2(filepath, local_path)

        # Build remux command (copy video, transcode audio, strip foreign tracks)
        cmd = build_audio_remux_cmd(local_path, output_path, file_entry, config, include_subs=True)

        # Execute
        state.set_file(filepath, FileStatus.PROCESSING, stage="audio_transcode")
        start = time.time()
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   encoding="utf-8", errors="replace")
        _, stderr = process.communicate()
        elapsed = time.time() - start

        if process.returncode != 0:
            logging.error(f"  Audio transcode failed (exit {process.returncode})")
            state.set_file(filepath, FileStatus.ERROR, error=f"ffmpeg exit {process.returncode}",
                           stage="audio_transcode")
            return False

        if not os.path.exists(output_path):
            state.set_file(filepath, FileStatus.ERROR, error="output not created", stage="audio_transcode")
            return False

        output_size = os.path.getsize(output_path)
        input_size = os.path.getsize(local_path)
        logging.info(f"  Audio transcode in {format_duration(elapsed)}: "
                     f"{format_bytes(input_size)} -> {format_bytes(output_size)}")

        # Upload back to NAS (replace original)
        state.set_file(filepath, FileStatus.UPLOADING, stage="upload")
        tmp_path = filepath + ".audiotrans_tmp.mkv"
        shutil.copy2(output_path, tmp_path)
        os.replace(tmp_path, filepath)
        logging.info(f"  Uploaded and replaced")

        return True

    except Exception as e:
        logging.error(f"  Audio transcode failed: {e}")
        state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="audio_transcode")
        return False
    finally:
        for p in (local_path, output_path):
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


def _rename_file(filepath: str, clean_name: str) -> Optional[str]:
    """Rename a file on the NAS to a cleaned name. Returns new path or None."""
    source_dir = os.path.dirname(filepath)
    new_path = os.path.join(source_dir, clean_name)

    if new_path == filepath:
        return None

    try:
        os.rename(filepath, new_path)
        logging.info(f"  Renamed: {os.path.basename(filepath)} -> {clean_name}")
        return new_path
    except Exception as e:
        logging.warning(f"  Rename failed: {e}")
        return None
