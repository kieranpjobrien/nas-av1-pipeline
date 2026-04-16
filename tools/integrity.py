"""
Integrity Checker
=================
Spot-checks video files for corruption by decoding short segments at the
start, middle, and end of each file. Much faster than a full decode while
still catching the most common corruption patterns (truncated files, bad
headers, broken endings, mid-file damage).

Usage:
    python -m tools.integrity --from-state
    python -m tools.integrity --directory Z:\\Movies
    python -m tools.integrity --directory Z:\\Movies --full   # full decode (slow)
    python -m tools.integrity --from-state --recheck          # ignore cache

Uses ffmpeg -v error with -ss/-t for segment sampling.
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from paths import STAGING_DIR

STATE_FILE = STAGING_DIR / "pipeline_state.json"
CACHE_FILE = STAGING_DIR / "integrity_cache.json"

VIDEO_EXTENSIONS = {
    ".mkv",
    ".mp4",
    ".avi",
    ".m4v",
    ".wmv",
    ".flv",
    ".mov",
    ".ts",
    ".webm",
    ".mpg",
    ".mpeg",
    ".m2ts",
}


def _probe_duration(filepath: str) -> float | None:
    """Get file duration in seconds via ffprobe. Returns None on failure."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", filepath],
            capture_output=True,
            text=True,
            timeout=30,
            encoding="utf-8",
            errors="replace",
        )
        data = json.loads(result.stdout)
        return float(data["format"]["duration"])
    except Exception:
        return None


def _decode_segment(filepath: str, start_secs: float, duration_secs: float) -> str | None:
    """Decode a segment of a file. Returns error output or None if clean."""
    cmd = [
        "ffmpeg",
        "-v",
        "error",
        "-hwaccel",
        "none",
        "-ss",
        str(start_secs),
        "-t",
        str(duration_secs),
        "-i",
        filepath,
        "-f",
        "null",
        "-",
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 min max per segment
            encoding="utf-8",
            errors="replace",
        )
        stderr = result.stderr.strip()
        return stderr if stderr else None
    except subprocess.TimeoutExpired:
        return "TIMEOUT: segment decode exceeded 5 minutes"
    except Exception as e:
        return f"Exception: {e}"


def _full_decode(filepath: str) -> str | None:
    """Full decode of entire file. Slow but thorough."""
    cmd = [
        "ffmpeg",
        "-v",
        "error",
        "-hwaccel",
        "none",
        "-i",
        filepath,
        "-f",
        "null",
        "-",
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=7200,
            encoding="utf-8",
            errors="replace",
        )
        stderr = result.stderr.strip()
        return stderr if stderr else None
    except subprocess.TimeoutExpired:
        return "TIMEOUT: decode exceeded 2 hours"
    except Exception as e:
        return f"Exception: {e}"


def _preflight(filepath: str) -> str | None:
    """Quick checks before decode: exists, non-empty, container readable."""
    if not os.path.exists(filepath):
        return "File not found"
    try:
        size = os.path.getsize(filepath)
    except OSError as e:
        return f"Cannot stat file: {e}"
    if size == 0:
        return "File is empty (0 bytes)"
    if size < 1024:
        return f"File suspiciously small ({size} bytes)"

    # Quick ffprobe to check container is readable
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                filepath,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            return f"Container unreadable: {stderr[:200]}"
    except subprocess.TimeoutExpired:
        return "ffprobe timed out (container may be corrupt)"
    except Exception as e:
        return f"ffprobe failed: {e}"

    return None


def check_file(filepath: str, full: bool = False) -> dict:
    """Run integrity check on a single file.

    Spot-check mode (default): decode first 30s, last 30s, and a 30s
    sample from the middle. Scales sample duration with file length.
    Full mode: decode entire file (original behaviour).
    """
    file_size = 0
    try:
        file_size = os.path.getsize(filepath)
    except OSError:
        pass

    entry = {
        "filepath": filepath,
        "filename": os.path.basename(filepath),
        "file_size_gb": round(file_size / (1024**3), 3),
        "status": "ok",
        "errors": "",
        "check_type": "full" if full else "spot",
    }

    # Preflight
    err = _preflight(filepath)
    if err:
        entry["status"] = "error"
        entry["errors"] = err
        entry["check_type"] = "preflight"
        return entry

    if full:
        err = _full_decode(filepath)
        if err:
            entry["status"] = "error"
            entry["errors"] = err
        return entry

    # Spot-check: probe duration, then sample segments
    duration = _probe_duration(filepath)
    if duration is None or duration <= 0:
        # Can't probe duration — fall back to decoding first 60s
        err = _decode_segment(filepath, 0, 60)
        if err:
            entry["status"] = "error"
            entry["errors"] = f"[start] {err}"
        return entry

    # Scale sample length: 10s for <10min, 20s for <1hr, 30s for longer
    if duration < 600:
        sample_secs = 10
    elif duration < 3600:
        sample_secs = 20
    else:
        sample_secs = 30

    errors = []

    # Start segment
    err = _decode_segment(filepath, 0, sample_secs)
    if err:
        errors.append(f"[start 0-{sample_secs}s] {err}")

    # Middle segment
    if duration > sample_secs * 3:
        mid = (duration / 2) - (sample_secs / 2)
        err = _decode_segment(filepath, mid, sample_secs)
        if err:
            errors.append(f"[mid {mid:.0f}-{mid + sample_secs:.0f}s] {err}")

    # End segment (seek to near-end)
    if duration > sample_secs * 2:
        end_start = max(0, duration - sample_secs - 1)
        err = _decode_segment(filepath, end_start, sample_secs + 1)
        if err:
            errors.append(f"[end {end_start:.0f}s+] {err}")

    if errors:
        entry["status"] = "error"
        entry["errors"] = " | ".join(errors)

    return entry


def get_replaced_files(state_path: Path) -> list[str]:
    """Extract all 'replaced' file paths from pipeline state, using final_path if available."""
    if not state_path.exists():
        return []
    with open(state_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    paths = []
    for filepath, info in data.get("files", {}).items():
        if info.get("status") == "replaced":
            final = info.get("final_path")
            if final and os.path.exists(final):
                paths.append(final)
            elif os.path.exists(filepath):
                paths.append(filepath)
    return paths


def scan_directory(directory: str) -> list[str]:
    """Recursively find all video files in a directory."""
    files = []
    for root, _, filenames in os.walk(directory):
        for fname in filenames:
            if Path(fname).suffix.lower() in VIDEO_EXTENSIONS:
                files.append(os.path.join(root, fname))
    return files


# -- Cache -------------------------------------------------------------------


def _load_cache(cache_path: Path) -> dict:
    """Load integrity cache: {filepath: {"mtime": float, "status": str}}."""
    if not cache_path.exists():
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_cache(cache_path: Path, cache: dict) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


def _filter_cached(files: list[str], cache: dict) -> list[str]:
    """Return only files that need checking (not in cache or mtime changed)."""
    unchecked = []
    for fp in files:
        cached = cache.get(fp)
        if not cached:
            unchecked.append(fp)
            continue
        try:
            current_mtime = os.path.getmtime(fp)
        except OSError:
            unchecked.append(fp)
            continue
        if cached.get("mtime") != current_mtime or cached.get("status") != "ok":
            unchecked.append(fp)
    return unchecked


# -- ETA / formatting -------------------------------------------------------


def _format_eta(elapsed: float, completed: int, total: int) -> str:
    if completed <= 0 or elapsed <= 0:
        return "calculating..."
    avg = elapsed / completed
    remaining = (total - completed) * avg
    if remaining >= 3600:
        return f"~{remaining / 3600:.1f}h"
    if remaining >= 60:
        return f"~{remaining / 60:.0f}m"
    return f"~{remaining:.0f}s"


def main():
    parser = argparse.ArgumentParser(description="Spot-check video files for corruption (start/middle/end decode)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--from-state", action="store_true", help="Check all 'replaced' files from pipeline_state.json")
    group.add_argument("--directory", type=str, metavar="PATH", help="Check all video files in this directory")
    parser.add_argument(
        "--output", type=str, default="integrity_check.csv", help="Output CSV file (default: integrity_check.csv)"
    )
    parser.add_argument("--workers", type=int, default=0, help="Parallel workers (default: half CPU cores)")
    parser.add_argument("--full", action="store_true", help="Full decode instead of spot-check (very slow)")
    parser.add_argument("--recheck", action="store_true", help="Ignore cache, recheck all files")
    parser.add_argument(
        "--state-file", type=str, default=str(STATE_FILE), help="Path to pipeline_state.json (for --from-state)"
    )
    parser.add_argument("--cache-file", type=str, default=str(CACHE_FILE), help="Path to integrity cache file")
    args = parser.parse_args()

    if args.workers <= 0:
        args.workers = max(1, os.cpu_count() // 2)

    if args.from_state:
        all_files = get_replaced_files(Path(args.state_file))
        print(f"Found {len(all_files)} replaced files in pipeline state")
    else:
        if not os.path.isdir(args.directory):
            print(f"ERROR: Directory not found: {args.directory}", file=sys.stderr)
            sys.exit(1)
        all_files = scan_directory(args.directory)
        print(f"Found {len(all_files)} video files in {args.directory}")

    if not all_files:
        print("No files to check.")
        return

    # Filter via cache
    cache_path = Path(args.cache_file)
    cache = {} if args.recheck else _load_cache(cache_path)
    files = all_files if args.recheck else _filter_cached(all_files, cache)
    skipped = len(all_files) - len(files)
    if skipped > 0:
        print(f"Skipping {skipped} files (passed on previous run, unchanged)")

    if not files:
        print("All files already checked and clean. Use --recheck to force.")
        return

    mode = "full decode" if args.full else "spot-check (start/mid/end)"
    print(f"Checking {len(files)} files with {args.workers} workers ({mode})...")
    if not args.full:
        print("(Decodes ~90s per file — fast, catches most corruption)\n")

    results = []
    completed = 0
    errors_found = 0
    start_time = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(check_file, f, args.full): f for f in files}
        for future in as_completed(futures):
            completed += 1
            entry = future.result()

            if entry["status"] == "error":
                errors_found += 1

            results.append(entry)

            # Update cache
            try:
                mtime = os.path.getmtime(entry["filepath"])
            except OSError:
                mtime = 0
            cache[entry["filepath"]] = {
                "mtime": mtime,
                "status": entry["status"],
                "check_type": entry["check_type"],
            }

            elapsed = time.monotonic() - start_time
            eta = _format_eta(elapsed, completed, len(files))
            pct = 100 * completed / len(files)
            err_str = f", {errors_found} errors" if errors_found else ""
            status_icon = "✗" if entry["status"] == "error" else "✓"
            print(
                f"  [{completed}/{len(files)} {pct:.0f}% ETA:{eta}{err_str}] {status_icon} {entry['filename']}",
                flush=True,
            )

    # Save cache
    _save_cache(cache_path, cache)

    # Write CSV
    fieldnames = ["filepath", "filename", "file_size_gb", "status", "check_type", "errors"]
    with open(args.output, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(results)

    # Summary
    elapsed = time.monotonic() - start_time
    if elapsed >= 3600:
        elapsed_str = f"{elapsed / 3600:.1f}h"
    elif elapsed >= 60:
        elapsed_str = f"{elapsed / 60:.1f}m"
    else:
        elapsed_str = f"{elapsed:.0f}s"

    print(f"\n{'=' * 60}")
    print(f"Checked: {len(results)} files in {elapsed_str}")
    print(f"Passed:  {len(results) - errors_found}")
    print(f"Errors:  {errors_found}")
    if skipped:
        print(f"Cached:  {skipped} (skipped, already verified)")
    print(f"Results: {args.output}")

    if errors_found:
        print(f"\n{'— Errors ' + '—' * 51}")
        for r in results:
            if r["status"] == "error":
                print(f"  {r['filename']}")
                print(f"    {r['errors'][:200]}")
    print()


if __name__ == "__main__":
    main()
