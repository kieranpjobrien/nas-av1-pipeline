"""
Integrity Checker
=================
Runs ffmpeg decode-only checks to detect file corruption.

Usage:
    python -m tools.integrity --from-state          # check all "replaced" files
    python -m tools.integrity --directory Z:\\Movies  # check all videos in a dir
    python -m tools.integrity --directory Z:\\Movies --output results.csv

Uses ffmpeg -v error -i <file> -f null - to surface decode errors.
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from paths import STAGING_DIR

STATE_FILE = STAGING_DIR / "pipeline_state.json"

VIDEO_EXTENSIONS = {
    ".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".flv",
    ".mov", ".ts", ".webm", ".mpg", ".mpeg", ".m2ts",
}


def check_file(filepath: str) -> tuple[str, str | None]:
    """Run ffmpeg decode check on a single file. Returns (filepath, error_output | None)."""
    try:
        cmd = [
            "ffmpeg", "-v", "error",
            "-i", filepath,
            "-f", "null", "-",
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=7200,  # 2 hours max per file
            encoding="utf-8", errors="replace",
        )
        stderr = result.stderr.strip()
        if stderr:
            return filepath, stderr
        return filepath, None
    except subprocess.TimeoutExpired:
        return filepath, "TIMEOUT: decode check exceeded 2 hours"
    except FileNotFoundError:
        return filepath, "ffmpeg not found on PATH"
    except Exception as e:
        return filepath, f"Exception: {e}"


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


def main():
    parser = argparse.ArgumentParser(description="Check video files for corruption via ffmpeg decode")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--from-state", action="store_true",
                       help="Check all 'replaced' files from pipeline_state.json")
    group.add_argument("--directory", type=str, metavar="PATH",
                       help="Check all video files in this directory")
    parser.add_argument("--output", type=str, default="integrity_check.csv",
                        help="Output CSV file")
    parser.add_argument("--workers", type=int, default=2,
                        help="Parallel workers (default: 2, decode is heavy)")
    parser.add_argument("--state-file", type=str, default=str(STATE_FILE),
                        help="Path to pipeline_state.json (for --from-state)")
    args = parser.parse_args()

    if args.from_state:
        files = get_replaced_files(Path(args.state_file))
        print(f"Found {len(files)} replaced files in pipeline state")
    else:
        if not os.path.isdir(args.directory):
            print(f"ERROR: Directory not found: {args.directory}", file=sys.stderr)
            sys.exit(1)
        files = scan_directory(args.directory)
        print(f"Found {len(files)} video files in {args.directory}")

    if not files:
        print("No files to check.")
        return

    print(f"Checking {len(files)} files with {args.workers} workers...")
    print("(This may take a while -- full decode check per file)\n")

    results = []
    completed = 0
    errors_found = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(check_file, f): f for f in files}
        for future in as_completed(futures):
            completed += 1
            filepath, error_output = future.result()
            file_size = 0
            try:
                file_size = os.path.getsize(filepath)
            except OSError:
                pass

            status = "ok" if error_output is None else "error"
            if status == "error":
                errors_found += 1

            results.append({
                "filepath": filepath,
                "filename": os.path.basename(filepath),
                "file_size_gb": round(file_size / (1024**3), 3),
                "status": status,
                "errors": error_output or "",
            })

            if completed % 10 == 0 or completed == len(files):
                print(f"  Progress: {completed}/{len(files)} "
                      f"({errors_found} errors found)")

    fieldnames = ["filepath", "filename", "file_size_gb", "status", "errors"]
    with open(args.output, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(results)

    print(f"\nDone: {len(results)} files checked, {errors_found} with errors")
    print(f"Results: {args.output}")


if __name__ == "__main__":
    main()
