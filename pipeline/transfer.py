"""File transfer operations: fetch from NAS + staging-space helpers.

The upload/verify/replace functions that used to live here were dead code —
they called `FileStatus.UPLOADED`, `FileStatus.VERIFIED`, `FileStatus.REPLACING`,
`FileStatus.REPLACED`, none of which exist in the current enum. Any caller
that tried to use them would `AttributeError`. Full_gamut.finalize_upload now
owns the upload/verify/replace responsibility inline. Those ~200 lines were
removed on the post-incident cleanup; keep this file small and living.
"""

import hashlib
import logging
import os
import shutil
import time
from typing import Optional

from pipeline.ffmpeg import format_bytes, format_duration
from pipeline.state import FileStatus, PipelineState


def get_staging_usage(staging_dir: str) -> int:
    """Get total bytes used in staging directory."""
    total = 0
    for dirpath, _, filenames in os.walk(staging_dir):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total


def get_free_space(path: str) -> int:
    """Get free space on the drive containing path."""
    return shutil.disk_usage(path).free


def fetch_file(item: dict, staging_dir: str, config: dict, state: PipelineState, force: bool = False) -> Optional[str]:
    """Copy file from NAS to local staging. Returns local path or None on failure."""
    source = item["filepath"]
    # Mirror directory structure under staging/fetch/
    fetch_dir = os.path.join(staging_dir, "fetch")
    # Use a flat structure with hash to avoid path length issues on Windows
    safe_name = hashlib.md5(source.encode()).hexdigest()[:12] + "_" + item["filename"]
    local_path = os.path.join(fetch_dir, safe_name)

    os.makedirs(fetch_dir, exist_ok=True)

    # Check staging space
    current_usage = get_staging_usage(staging_dir)
    file_size = item["file_size_bytes"]
    if current_usage + file_size > config["max_staging_bytes"]:
        logging.warning(f"Staging full ({format_bytes(current_usage)} used). Waiting...")
        return None

    free = get_free_space(staging_dir)
    if free < config["min_free_space_bytes"] + file_size:
        logging.warning(f"Insufficient free space ({format_bytes(free)}). Waiting...")
        return None

    # Check fetch buffer specifically
    fetch_usage = 0
    if os.path.exists(fetch_dir):
        for f in os.listdir(fetch_dir):
            try:
                fetch_usage += os.path.getsize(os.path.join(fetch_dir, f))
            except OSError:
                pass
    if fetch_usage + file_size > config["max_fetch_buffer_bytes"] and not force:
        return None  # buffer full — caller handles the wait

    # Check source still exists on NAS (may have been renamed/deleted since scan)
    if not os.path.exists(source):
        logging.warning(f"Source file not found, skipping: {item['filename']}")
        state.set_file(source, FileStatus.DONE, reason="source file not found")
        return None

    # Atomically claim this file for fetching — prevents the prefetch thread
    # and main loop from copying the same file concurrently (WinError 32).
    with state._lock:
        existing = state.get_file(source)
        current = existing["status"] if existing else None
        if current == FileStatus.FETCHING.value:
            return None  # Another thread is already fetching this file
        state.set_file(source, FileStatus.FETCHING, local_path=local_path)

    logging.info(f"Fetching: {item['filename']} ({format_bytes(file_size)})")

    # Clean up stale local file from a previous failed run (avoids WinError 32)
    if os.path.exists(local_path):
        try:
            os.remove(local_path)
        except OSError:
            logging.warning(f"Cannot remove stale fetch file (locked): {os.path.basename(local_path)}")
            state.set_file(source, FileStatus.ERROR, error="stale fetch file locked", stage="fetch")
            return None

    try:
        start = time.time()
        shutil.copy2(source, local_path)
        elapsed = time.time() - start
        # Verify copy is complete (catch truncated fetches)
        local_size = os.path.getsize(local_path)
        if file_size > 0 and local_size < file_size * 0.99:
            logging.error(f"Fetch incomplete: {format_bytes(local_size)} vs expected {format_bytes(file_size)}")
            os.remove(local_path)
            state.set_file(source, FileStatus.ERROR, error="fetch incomplete", stage="fetch")
            return None
        speed = file_size / elapsed / (1024**2) if elapsed > 0 else 0
        logging.info(f"Fetched in {format_duration(elapsed)} ({speed:.0f} MB/s)")
        state.set_file(
            source,
            FileStatus.PROCESSING,
            local_path=local_path,
            input_size_bytes=file_size,
            fetch_start=start,
            fetch_end=time.time(),
            fetch_time_secs=round(elapsed, 1),
        )
        return local_path
    except Exception as e:
        logging.error(f"Fetch failed: {e}")
        state.set_file(source, FileStatus.ERROR, error=str(e), stage="fetch")
        # Clean up partial — may fail if another process holds a lock
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
        except OSError:
            pass
        return None


# NOTE: upload_file / verify_file / replace_original used to live here.
# They referenced FileStatus enum members that no longer exist (UPLOADED, VERIFIED,
# REPLACING, REPLACED). Any caller would have raised AttributeError at import time.
# The live upload/verify/replace path is `full_gamut.finalize_upload`; this dead
# code was removed during the post-incident cleanup.
