"""
Strip Non-English Subtitles
============================
Remuxes files to remove non-English subtitle streams. Stream copy only —
no re-encoding, very fast. Targets files that have already been encoded
to AV1 and still carry foreign subtitle bloat (especially PGS bitmap subs).

Usage:
    python -m tools.strip_subs                    # dry run — show what would be stripped
    python -m tools.strip_subs --execute          # actually remux files
    python -m tools.strip_subs --execute --movies # movies only
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from paths import NAS_MOVIES, NAS_SERIES, MEDIA_REPORT

KEEP_LANGS = {"eng", "en", "english", "und", ""}


def _get_files_with_non_english_subs(report_path: Path,
                                      library_filter: str | None = None) -> list[dict]:
    """Find files from the media report that have non-English subtitle streams."""
    if not report_path.exists():
        print(f"ERROR: Media report not found: {report_path}", file=sys.stderr)
        return []

    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)

    results = []
    for entry in report.get("files", []):
        if library_filter and entry.get("library_type") != library_filter:
            continue

        subs = entry.get("subtitle_streams", [])
        if not subs:
            continue

        keep_indices = []
        strip_indices = []
        strip_size_estimate = 0

        for i, sub in enumerate(subs):
            lang = (sub.get("language") or "").lower().strip()
            codec = (sub.get("codec") or "").lower()
            is_bitmap = codec in ("hdmv_pgs_subtitle", "dvd_subtitle", "dvdsub",
                                   "pgssub", "pgs")
            if lang in KEEP_LANGS:
                keep_indices.append(i)
            else:
                strip_indices.append(i)
                # Rough size estimate: bitmap ~40MB, text ~50KB
                strip_size_estimate += 40_000_000 if is_bitmap else 50_000

        if not strip_indices:
            continue

        results.append({
            "filepath": entry["filepath"],
            "filename": entry["filename"],
            "file_size_bytes": entry["file_size_bytes"],
            "file_size_gb": entry["file_size_gb"],
            "total_subs": len(subs),
            "keep_indices": keep_indices,
            "strip_indices": strip_indices,
            "strip_count": len(strip_indices),
            "strip_size_estimate": strip_size_estimate,
            "subtitle_streams": subs,
        })

    return results


def _remux_strip_subs(filepath: str, keep_indices: list[int]) -> bool:
    """Remux a file, keeping only specified subtitle stream indices. Returns True on success."""
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".mkv", dir=os.path.dirname(filepath))
    os.close(tmp_fd)

    cmd = [
        "ffmpeg", "-y",
        "-i", filepath,
        "-map", "0:v",
        "-map", "0:a",
    ]
    for idx in keep_indices:
        cmd.extend(["-map", f"0:s:{idx}"])

    cmd.extend([
        "-c", "copy",
        "-map_metadata", "0",
        tmp_path,
    ])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600,
                                encoding="utf-8", errors="replace")
        if result.returncode != 0:
            print(f"  ERROR: ffmpeg failed for {os.path.basename(filepath)}")
            print(f"  stderr: {result.stderr[:500]}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return False

        # Verify output exists and is reasonable size (at least 50% of original)
        if not os.path.exists(tmp_path):
            return False
        tmp_size = os.path.getsize(tmp_path)
        orig_size = os.path.getsize(filepath)
        if tmp_size < orig_size * 0.5:
            print(f"  ERROR: Output too small ({tmp_size} vs {orig_size}), keeping original")
            os.remove(tmp_path)
            return False

        # Replace original
        os.replace(tmp_path, filepath)
        saved = orig_size - tmp_size
        print(f"  Saved {saved / (1024**2):.1f} MB")
        return True

    except subprocess.TimeoutExpired:
        print(f"  ERROR: Timeout for {os.path.basename(filepath)}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False
    except Exception as e:
        print(f"  ERROR: {e}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False


def main():
    parser = argparse.ArgumentParser(description="Strip non-English subtitles via remux")
    parser.add_argument("--execute", action="store_true",
                        help="Actually remux files (default: dry run)")
    parser.add_argument("--movies", action="store_true", help="Movies only")
    parser.add_argument("--series", action="store_true", help="Series only")
    parser.add_argument("--report", type=str, default=str(MEDIA_REPORT),
                        help="Path to media report JSON")
    args = parser.parse_args()

    lib_filter = None
    if args.movies:
        lib_filter = "movie"
    elif args.series:
        lib_filter = "series"

    files = _get_files_with_non_english_subs(Path(args.report), lib_filter)

    if not files:
        print("No files with non-English subtitles found.")
        return

    total_streams = sum(f["strip_count"] for f in files)
    total_estimate = sum(f["strip_size_estimate"] for f in files)

    print(f"Found {len(files)} files with {total_streams} non-English subtitle streams")
    print(f"Estimated savings: {total_estimate / (1024**3):.1f} GB")
    print()

    if not args.execute:
        print("DRY RUN — showing first 30 files:")
        for f in files[:30]:
            langs = [s.get("language", "?") for i, s in enumerate(f["subtitle_streams"])
                     if i in f["strip_indices"]]
            print(f"  {f['filename']} — strip {f['strip_count']} subs ({', '.join(langs)})")
        if len(files) > 30:
            print(f"  ... and {len(files) - 30} more")
        print(f"\nRun with --execute to actually strip subtitles.")
        return

    print(f"Stripping subtitles from {len(files)} files...")
    print("(Stream copy only — no re-encoding, CPU only, safe with pipeline)\n")

    completed = 0
    success = 0
    total_saved = 0
    start_time = time.monotonic()

    for f in files:
        completed += 1
        elapsed = time.monotonic() - start_time
        if completed > 1 and elapsed > 0:
            remaining = (len(files) - completed) * (elapsed / completed)
            eta = f"~{remaining / 60:.0f}m" if remaining >= 60 else f"~{remaining:.0f}s"
        else:
            eta = "..."

        print(f"  Progress: {completed}/{len(files)} ({100 * completed / len(files):.0f}%) "
              f"ETA: {eta} — {f['filename']}", flush=True)

        orig_size = 0
        try:
            orig_size = os.path.getsize(f["filepath"])
        except OSError:
            print(f"  SKIP: file not found")
            continue

        ok = _remux_strip_subs(f["filepath"], f["keep_indices"])
        if ok:
            success += 1
            try:
                new_size = os.path.getsize(f["filepath"])
                total_saved += orig_size - new_size
            except OSError:
                pass

    elapsed = time.monotonic() - start_time
    elapsed_str = f"{elapsed / 60:.1f}m" if elapsed >= 60 else f"{elapsed:.0f}s"
    print(f"\nDone: {success}/{len(files)} files processed in {elapsed_str}")
    print(f"Total saved: {total_saved / (1024**3):.2f} GB")
    print("\nRun a library rescan to update the media report.")


if __name__ == "__main__":
    main()
