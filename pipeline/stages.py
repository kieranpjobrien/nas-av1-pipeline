"""Pipeline stages: fetch, upload, verify, replace original."""

import hashlib
import logging
import os
import shutil
import time
from pathlib import Path
from typing import Optional

from pipeline.config import get_res_key
from pipeline.encoding import format_bytes, format_duration, get_duration
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


def stage_fetch(item: dict, staging_dir: str, config: dict, state: PipelineState) -> Optional[str]:
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
    if fetch_usage + file_size > config["max_fetch_buffer_bytes"]:
        logging.info(f"Fetch buffer full ({format_bytes(fetch_usage)}). Waiting for encodes to complete...")
        return None

    # Check source still exists on NAS (may have been renamed/deleted since scan)
    if not os.path.exists(source):
        logging.warning(f"Source file not found, skipping: {item['filename']}")
        state.set_file(source, FileStatus.SKIPPED, reason="source file not found")
        return None

    # Atomically claim this file for fetching — prevents the prefetch thread
    # and main loop from copying the same file concurrently (WinError 32).
    with state._lock:
        existing = state.data["files"].get(source)
        current = existing["status"] if existing else None
        if current == FileStatus.FETCHING.value:
            return None  # Another thread is already fetching this file
        state.set_file(source, FileStatus.FETCHING, local_path=local_path)

    logging.info(f"Fetching: {item['filename']} ({format_bytes(file_size)})")

    try:
        start = time.time()
        shutil.copy2(source, local_path)
        elapsed = time.time() - start
        speed = file_size / elapsed / (1024**2) if elapsed > 0 else 0
        logging.info(f"Fetched in {format_duration(elapsed)} ({speed:.0f} MB/s)")
        state.set_file(source, FileStatus.FETCHED, local_path=local_path)
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


def stage_upload(source_filepath: str, item: dict, staging_dir: str,
                 config: dict, state: PipelineState) -> bool:
    """Copy encoded file back to NAS alongside the original."""
    file_info = state.get_file(source_filepath)
    if not file_info:
        return False

    output_path = file_info.get("output_path")
    if not output_path or not os.path.exists(output_path):
        logging.error(f"Encoded file missing: {output_path}")
        state.set_file(source_filepath, FileStatus.ERROR, error="encoded file missing", stage="upload")
        return False

    # Destination: same directory as original, with .av1.mkv suffix
    source_dir = os.path.dirname(source_filepath)
    original_stem = Path(item["filename"]).stem
    dest_filename = original_stem + ".av1.mkv"
    dest_path = os.path.join(source_dir, dest_filename)

    if os.path.exists(dest_path) and not config["overwrite_existing"]:
        logging.warning(f"Destination exists, skipping: {dest_path}")
        state.set_file(source_filepath, FileStatus.SKIPPED,
                       reason="destination exists", dest_path=dest_path)
        # Clean up local encoded file
        if os.path.exists(output_path):
            os.remove(output_path)
        return True

    state.set_file(source_filepath, FileStatus.UPLOADING, dest_path=dest_path)
    logging.info(f"Uploading: {dest_filename} -> {source_dir}")

    try:
        start = time.time()
        shutil.copy2(output_path, dest_path)
        elapsed = time.time() - start
        output_size = os.path.getsize(output_path)
        speed = output_size / elapsed / (1024**2) if elapsed > 0 else 0
        logging.info(f"Uploaded in {format_duration(elapsed)} ({speed:.0f} MB/s)")

        state.set_file(source_filepath, FileStatus.UPLOADED, dest_path=dest_path)

        # Clean up local encoded file
        os.remove(output_path)
        logging.info(f"Cleaned up local encoded file")

        return True

    except Exception as e:
        logging.error(f"Upload failed: {e}")
        state.set_file(source_filepath, FileStatus.ERROR, error=str(e), stage="upload")
        return False


def stage_verify(source_filepath: str, item: dict, config: dict, state: PipelineState) -> bool:
    """Verify the uploaded file on NAS."""
    file_info = state.get_file(source_filepath)
    if not file_info:
        return False

    dest_path = file_info.get("dest_path")
    if not dest_path or not os.path.exists(dest_path):
        logging.error(f"Destination file missing: {dest_path}")
        state.set_file(source_filepath, FileStatus.ERROR,
                       error="dest file missing after upload", stage="verify")
        return False

    # Check duration
    dest_duration = get_duration(dest_path) or 0
    source_duration = item.get("duration_seconds", 0)
    tolerance = config["verify_duration_tolerance_secs"]

    if source_duration > 0 and abs(source_duration - dest_duration) > tolerance:
        logging.error(f"Verification failed: duration mismatch "
                      f"(source={source_duration:.1f}s, dest={dest_duration:.1f}s)")
        state.set_file(source_filepath, FileStatus.ERROR,
                       error="duration mismatch", stage="verify")
        return False

    dest_size = os.path.getsize(dest_path)
    source_size = item["file_size_bytes"]
    saved = source_size - dest_size

    state.set_file(source_filepath, FileStatus.VERIFIED,
                   dest_path=dest_path,
                   dest_size_bytes=dest_size,
                   bytes_saved=saved)

    state.stats["completed"] += 1
    state.stats["bytes_saved"] += saved

    # Per-tier stats
    res_key = get_res_key(item)
    tier_stats = state.stats.setdefault("tier_stats", {})
    tier = tier_stats.setdefault(res_key, {
        "completed": 0, "bytes_saved": 0,
        "total_input_bytes": 0, "total_output_bytes": 0,
        "total_encode_time_secs": 0,
    })
    tier["completed"] += 1
    tier["bytes_saved"] += saved
    tier["total_input_bytes"] += source_size
    tier["total_output_bytes"] += dest_size

    # Pull encode_time from file info (set during encode stage)
    file_info_updated = state.get_file(source_filepath)
    encode_time = file_info_updated.get("encode_time_secs", 0) if file_info_updated else 0
    tier["total_encode_time_secs"] += encode_time

    state.save()

    logging.info(f"Verified: {item['filename']} -> saved {format_bytes(saved)}")

    return True


def stage_replace(source_filepath: str, item: dict, config: dict, state: PipelineState) -> bool:
    """Replace original file on NAS with the AV1 version. Crash-safe via rename sequence.

    Sequence: original → .original.bak → rename .av1.mkv → original name (.mkv) → delete .bak
    On crash during REPLACING, resume detects and completes the sequence.
    """
    file_info = state.get_file(source_filepath)
    if not file_info:
        return False

    dest_path = file_info.get("dest_path")  # the .av1.mkv on NAS
    if not dest_path or not os.path.exists(dest_path):
        logging.error(f"AV1 file missing for replace: {dest_path}")
        state.set_file(source_filepath, FileStatus.ERROR,
                       error="av1 file missing for replace", stage="replace")
        return False

    # Target: original filename but with .mkv extension
    source_dir = os.path.dirname(source_filepath)
    final_name = Path(item["filename"]).stem + ".mkv"
    final_path = os.path.join(source_dir, final_name)
    backup_path = source_filepath + ".original.bak"

    state.set_file(source_filepath, FileStatus.REPLACING,
                   dest_path=dest_path, final_path=final_path, backup_path=backup_path)

    try:
        # Step 1: Rename original → .original.bak (if original still exists)
        if os.path.exists(source_filepath) and not os.path.exists(backup_path):
            os.rename(source_filepath, backup_path)
            logging.info(f"  Backed up original: {os.path.basename(source_filepath)} -> .original.bak")

        # Step 2: Rename .av1.mkv → final name
        if os.path.exists(dest_path) and not os.path.exists(final_path):
            os.rename(dest_path, final_path)
            logging.info(f"  Renamed AV1 file -> {final_name}")
        elif os.path.exists(dest_path) and dest_path != final_path:
            # final_path already exists (maybe from a previous partial), overwrite
            os.replace(dest_path, final_path)

        # Step 3: Delete backup
        if os.path.exists(backup_path):
            os.remove(backup_path)
            logging.info(f"  Deleted original backup")

        state.set_file(source_filepath, FileStatus.REPLACED, final_path=final_path)
        logging.info(f"Replaced: {item['filename']} -> {final_name}")
        return True

    except Exception as e:
        logging.error(f"Replace failed: {e}")
        logging.error(f"  Manual recovery may be needed. Check: {source_dir}")
        logging.error(f"  Backup: {backup_path}, AV1: {dest_path}, Target: {final_path}")
        state.set_file(source_filepath, FileStatus.ERROR, error=str(e), stage="replace")
        return False
