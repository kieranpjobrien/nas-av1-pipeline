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


@dataclass
class GapAnalysis:
    """What a file needs to be fully 'done'."""
    needs_track_removal: bool = False
    needs_audio_transcode: bool = False
    needs_metadata: bool = False
    needs_filename_clean: bool = False
    needs_language_detect: bool = False
    needs_sub_mux: bool = False
    audio_keep_indices: list[int] = field(default_factory=list)
    sub_keep_indices: list[int] = field(default_factory=list)
    audio_transcode_indices: list[int] = field(default_factory=list)
    external_subs: list[str] = field(default_factory=list)
    clean_name: Optional[str] = None

    @property
    def needs_fetch(self) -> bool:
        """Only audio codec transcoding requires fetching to local."""
        return self.needs_audio_transcode

    @property
    def needs_anything(self) -> bool:
        return (self.needs_track_removal or self.needs_audio_transcode or
                self.needs_metadata or self.needs_filename_clean or
                self.needs_language_detect or self.needs_sub_mux)

    def describe(self) -> str:
        parts = []
        if self.needs_track_removal:
            parts.append("strip tracks")
        if self.needs_sub_mux:
            parts.append(f"mux {len(self.external_subs)} subs")
        if self.needs_audio_transcode:
            parts.append("transcode audio")
        if self.needs_metadata:
            parts.append("write metadata")
        if self.needs_filename_clean:
            parts.append("clean filename")
        if self.needs_language_detect:
            parts.append("detect languages")
        return " + ".join(parts) if parts else "nothing"


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

    # Foreign subtitle check
    sub_keep = []
    for i, s in enumerate(sub_streams):
        lang = (s.get("language") or s.get("detected_language") or "und").lower().strip()
        title = (s.get("title") or "").lower()
        is_forced = "forced" in title or "foreign" in title
        if lang in _KEEP_LANGS or is_forced:
            sub_keep.append(i)
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

    # External subtitle check — Bazarr downloads .srt/.ass next to the file
    filepath = file_entry.get("filepath", "")
    source_dir = os.path.dirname(filepath)
    stem = Path(filepath).stem
    sub_exts = {".srt", ".ass", ".ssa", ".sub"}
    try:
        for f in os.listdir(source_dir):
            ext = Path(f).suffix.lower()
            if ext in sub_exts and f.startswith(stem[:20]):
                # Only English subs
                fl = f.lower()
                if ".en." in fl or ".eng." in fl:
                    gaps.external_subs.append(os.path.join(source_dir, f))
    except OSError:
        pass
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
                        logging.info(f"  Removed external sub: {os.path.basename(sub_path)}")
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
    mkvmerge = _find_tool("mkvmerge", _MKVMERGE_SEARCH)
    if not mkvmerge:
        logging.error("mkvmerge not found — cannot strip tracks")
        return False

    tmp_path = filepath + ".gapfill_tmp.mkv"
    cmd = [mkvmerge, "-o", tmp_path]

    # Audio track selection
    if gaps.audio_keep_indices:
        cmd.extend(["--audio-tracks", ",".join(str(i) for i in gaps.audio_keep_indices)])

    # Subtitle track selection
    if gaps.sub_keep_indices:
        cmd.extend(["--subtitle-tracks", ",".join(str(i) for i in gaps.sub_keep_indices)])
    elif not gaps.sub_keep_indices and gaps.needs_track_removal:
        cmd.extend(["--no-subtitles"])

    cmd.append(filepath)

    # Add external subtitle files as additional inputs
    for sub_path in gaps.external_subs:
        # Parse language from filename (e.g. Movie.en.srt -> eng)
        lang = "eng"
        fl = os.path.basename(sub_path).lower()
        if ".en." in fl or ".eng." in fl:
            lang = "eng"
        cmd.extend(["--language", f"0:{lang}", sub_path])

    try:
        src_size = os.path.getsize(filepath)
        timeout = max(300, int(src_size / (5 * 1024 * 1024)))  # 5 min minimum, 1s per 5MB

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode >= 2:
            logging.error(f"  mkvmerge failed: {result.stderr[:200]}")
            return False

        if not os.path.exists(tmp_path):
            return False

        dst_size = os.path.getsize(tmp_path)
        if dst_size < src_size * 0.3:
            logging.error(f"  Output too small ({format_bytes(dst_size)} vs {format_bytes(src_size)})")
            os.remove(tmp_path)
            return False

        os.replace(tmp_path, filepath)
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
