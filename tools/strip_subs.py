"""
Strip Non-English Subtitles
============================
Remuxes files to remove non-English subtitle streams using mkvmerge (preferred,
fast local staging) or ffmpeg fallback. Stream copy only — no re-encoding.

For NAS files: copies to local staging, remuxes locally, copies back. This
avoids the catastrophic slowness of ffmpeg remuxing directly over SMB.

Usage:
    python -m tools.strip_subs                    # dry run — show what would be stripped
    python -m tools.strip_subs --execute          # actually remux files
    python -m tools.strip_subs --execute --movies # movies only
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from paths import NAS_MOVIES, NAS_SERIES, MEDIA_REPORT, STAGING_DIR

KEEP_LANGS = {"eng", "en", "english", "und", ""}

# MKVToolNix common install locations
_MKVMERGE_SEARCH = [
    r"C:\Program Files\MKVToolNix\mkvmerge.exe",
    r"C:\Program Files (x86)\MKVToolNix\mkvmerge.exe",
]


def _find_mkvmerge() -> str | None:
    found = shutil.which("mkvmerge")
    if found:
        return found
    for path in _MKVMERGE_SEARCH:
        if os.path.isfile(path):
            return path
    return None


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


def _remux_mkvmerge(filepath: str, keep_indices: list[int], local_dir: str,
                    mkvmerge: str) -> bool:
    """Strip subs using mkvmerge via local staging (fast)."""
    basename = os.path.basename(filepath)
    local_src = os.path.join(local_dir, "src_" + basename)
    local_dst = os.path.join(local_dir, "dst_" + basename)

    try:
        # Copy NAS → local
        shutil.copy2(filepath, local_src)

        # Build mkvmerge command: keep only specified subtitle tracks
        # mkvmerge uses track IDs from the file. Subtitle track IDs need to be
        # determined from the stream order. We use --subtitle-tracks with
        # comma-separated indices (0-based within subtitle type).
        if keep_indices:
            sub_spec = ",".join(str(i) for i in keep_indices)
            cmd = [mkvmerge, "-o", local_dst, "--subtitle-tracks", sub_spec, local_src]
        else:
            # No subs to keep — strip all
            cmd = [mkvmerge, "-o", local_dst, "--no-subtitles", local_src]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600,
                                encoding="utf-8", errors="replace")
        # mkvmerge returns 0 for success, 1 for warnings (still ok), 2 for error
        if result.returncode >= 2:
            print(f"  ERROR: mkvmerge failed for {basename}")
            print(f"  stderr: {result.stderr[:500]}")
            return False

        if not os.path.exists(local_dst):
            print(f"  ERROR: mkvmerge produced no output for {basename}")
            return False

        dst_size = os.path.getsize(local_dst)
        src_size = os.path.getsize(local_src)
        if dst_size < src_size * 0.3:
            print(f"  ERROR: Output too small ({dst_size} vs {src_size}), keeping original")
            return False

        # Copy result back to NAS and replace
        shutil.copy2(local_dst, filepath)
        saved = src_size - dst_size
        print(f"  Saved {saved / (1024**2):.1f} MB")
        return True

    except subprocess.TimeoutExpired:
        print(f"  ERROR: Timeout for {basename}")
        return False
    except Exception as e:
        print(f"  ERROR: {e}")
        return False
    finally:
        for p in (local_src, local_dst):
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


def _remux_ffmpeg(filepath: str, keep_indices: list[int], local_dir: str) -> bool:
    """Strip subs using ffmpeg via local staging (fallback)."""
    basename = os.path.basename(filepath)
    local_src = os.path.join(local_dir, "src_" + basename)
    local_dst = os.path.join(local_dir, "dst_" + basename)

    try:
        shutil.copy2(filepath, local_src)

        cmd = ["ffmpeg", "-y", "-i", local_src, "-map", "0:v", "-map", "0:a"]
        for idx in keep_indices:
            cmd.extend(["-map", f"0:s:{idx}"])
        cmd.extend(["-c", "copy", "-map_metadata", "0", local_dst])

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600,
                                encoding="utf-8", errors="replace")
        if result.returncode != 0:
            print(f"  ERROR: ffmpeg failed for {basename}")
            print(f"  stderr: {result.stderr[:500]}")
            return False

        if not os.path.exists(local_dst):
            return False
        dst_size = os.path.getsize(local_dst)
        src_size = os.path.getsize(local_src)
        if dst_size < src_size * 0.3:
            print(f"  ERROR: Output too small ({dst_size} vs {src_size}), keeping original")
            return False

        shutil.copy2(local_dst, filepath)
        saved = src_size - dst_size
        print(f"  Saved {saved / (1024**2):.1f} MB")
        return True

    except subprocess.TimeoutExpired:
        print(f"  ERROR: Timeout for {basename}")
        return False
    except Exception as e:
        print(f"  ERROR: {e}")
        return False
    finally:
        for p in (local_src, local_dst):
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass


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

    # Set up local staging directory
    local_dir = os.path.join(str(STAGING_DIR), "strip_subs_tmp")
    os.makedirs(local_dir, exist_ok=True)

    mkvmerge = _find_mkvmerge()
    if mkvmerge:
        print(f"Using mkvmerge: {mkvmerge}")
    else:
        print("mkvmerge not found — falling back to ffmpeg")
    print(f"Local staging: {local_dir}")
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
            rate = elapsed / (completed - 1)
            remaining = (len(files) - completed) * rate
            eta = f"ETA: ~{remaining / 60:.0f}m" if remaining >= 60 else f"ETA: ~{remaining:.0f}s"
        else:
            eta = ""

        print(f"  [{completed}/{len(files)}] {eta} — {f['filename']}", flush=True)

        if not os.path.exists(f["filepath"]):
            print(f"  SKIP: file not found")
            continue

        orig_size = os.path.getsize(f["filepath"])

        if mkvmerge:
            ok = _remux_mkvmerge(f["filepath"], f["keep_indices"], local_dir, mkvmerge)
        else:
            ok = _remux_ffmpeg(f["filepath"], f["keep_indices"], local_dir)

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

    # Clean up staging
    try:
        os.rmdir(local_dir)
    except OSError:
        pass


if __name__ == "__main__":
    main()
