"""Atomic media report updates.

Provides functions to update individual file entries in media_report.json
after pipeline processing completes. Uses file-based locking for safe
concurrent access from multiple processes/threads.
"""

import json
import logging
import os
import subprocess
import threading
from pathlib import Path
from typing import Optional

from paths import MEDIA_REPORT

_update_lock = threading.Lock()


def probe_file(filepath: str) -> Optional[dict]:
    """Run ffprobe on a file and return parsed JSON, or None on failure."""
    cmd = [
        "ffprobe", "-v", "quiet",
        "-print_format", "json",
        "-show_format", "-show_streams",
        filepath,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return None


def build_file_entry(filepath: str, probe_data: dict, library_type: str) -> dict:
    """Build a media report entry from ffprobe data.

    Extracts video codec, resolution, HDR, audio streams, subtitle streams,
    file size, duration, and all metadata needed for the pipeline.
    """
    # Import the existing extract_info from scanner — it already does this perfectly
    from tools.scanner import extract_info
    return extract_info(filepath, probe_data, library_type)


def update_entry(filepath: str, library_type: str = "") -> bool:
    """Re-probe a file and update its entry in media_report.json atomically.

    Called after pipeline processing completes. Thread-safe via lock.
    Preserves TMDb and language detection data from the old entry.

    Returns True on success, False if probe or I/O fails.
    """
    probe_data = probe_file(filepath)
    if probe_data is None:
        logging.warning(f"Report update: probe failed for {os.path.basename(filepath)}")
        return False

    new_entry = build_file_entry(filepath, probe_data, library_type)

    with _update_lock:
        from tools.report_lock import patch_report

        def _patch(report: dict) -> None:
            files = report.get("files", [])

            # Find and replace existing entry
            updated = False
            for i, e in enumerate(files):
                if e.get("filepath") == filepath:
                    old = files[i]
                    # Preserve TMDb data
                    if old.get("tmdb") and not new_entry.get("tmdb"):
                        new_entry["tmdb"] = old["tmdb"]
                    # Preserve language detection data
                    for stream_key in ("audio_streams", "subtitle_streams"):
                        for j, s in enumerate(new_entry.get(stream_key, [])):
                            if j < len(old.get(stream_key, [])):
                                old_s = old[stream_key][j]
                                for field in ("detected_language", "detection_confidence",
                                              "detection_method", "whisper_attempted"):
                                    if old_s.get(field) and not s.get(field):
                                        s[field] = old_s[field]
                    files[i] = new_entry
                    updated = True
                    break
            if not updated:
                files.append(new_entry)

            # Update summary counts
            _update_summary(report)

        try:
            patch_report(_patch)
            return True
        except Exception as e:
            logging.warning(f"Report update failed: {e}")
            return False


def remove_entry(filepath: str) -> bool:
    """Remove a file entry from media_report.json (e.g. after deletion)."""
    with _update_lock:
        from tools.report_lock import patch_report

        def _patch(report: dict) -> None:
            files = report.get("files", [])
            report["files"] = [f for f in files if f.get("filepath") != filepath]
            _update_summary(report)

        try:
            patch_report(_patch)
            return True
        except Exception:
            return False


def _update_summary(report: dict) -> None:
    """Recalculate summary statistics from the files list."""
    files = report.get("files", [])
    total_size = sum(f.get("file_size_bytes", 0) for f in files)
    movies = [f for f in files if f.get("library_type") == "movie"]
    series = [f for f in files if f.get("library_type") == "series"]

    summary = report.setdefault("summary", {})
    summary["total_files"] = len(files)
    summary["total_size_gb"] = round(total_size / 1024**3, 2)
    summary["total_size_tb"] = round(total_size / 1024**4, 3)
    summary.setdefault("movies", {})["count"] = len(movies)
    summary["movies"]["size_gb"] = round(sum(f.get("file_size_bytes", 0) for f in movies) / 1024**3, 2)
    summary.setdefault("series", {})["count"] = len(series)
    summary["series"]["size_gb"] = round(sum(f.get("file_size_bytes", 0) for f in series) / 1024**3, 2)
