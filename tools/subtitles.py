"""Subtitle availability checker for NAS media libraries.

Scans media files using ffprobe and checks for external subtitle files (.srt, .ass, .sub)
to identify content missing English subtitles.

Usage:
    python -m tools.subtitles                          # Scan both libraries
    python -m tools.subtitles --movies-only             # Movies only
    python -m tools.subtitles --series-only             # Series only
    python -m tools.subtitles --csv missing_subs.csv    # Export CSV report
    python -m tools.subtitles --report report.json      # Use existing media report
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from paths import NAS_MOVIES, NAS_SERIES, MEDIA_REPORT

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".mov", ".ts", ".webm"}
SUBTITLE_EXTS = {".srt", ".ass", ".ssa", ".sub", ".sup", ".idx", ".vtt"}

# Language codes/tags considered "English"
ENGLISH_CODES = {"eng", "en", "english", "en-us", "en-gb", "en-au"}


def _probe_subtitles(filepath: str) -> list[dict]:
    """Extract subtitle stream info from a media file via ffprobe."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-select_streams", "s",
            str(filepath),
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30,
            encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            return []
        data = json.loads(result.stdout)
        return data.get("streams", [])
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return []


def _has_english_embedded(subtitle_streams: list[dict]) -> bool:
    """Check if any embedded subtitle stream is English."""
    for stream in subtitle_streams:
        tags = stream.get("tags", {})
        # Check multiple tag fields — different containers use different ones
        lang = (tags.get("language", "") or "").lower().strip()
        title = (tags.get("title", "") or "").lower().strip()

        if lang in ENGLISH_CODES:
            return True
        if "english" in title or "eng" == title:
            return True
    return False


def _find_external_subs(video_path: Path) -> list[Path]:
    """Find external subtitle files matching a video file."""
    stem = video_path.stem
    parent = video_path.parent
    subs = []
    try:
        for f in parent.iterdir():
            if f.stem.startswith(stem) and f.suffix.lower() in SUBTITLE_EXTS:
                subs.append(f)
    except OSError:
        pass
    return subs


def _has_english_external(external_subs: list[Path]) -> bool:
    """Check if any external subtitle file looks like English."""
    for sub in external_subs:
        name = sub.stem.lower()
        # Common patterns: movie.en.srt, movie.eng.srt, movie.English.srt
        # Or just movie.srt (assumed English if no language suffix)
        parts = name.rsplit(".", 1)
        if len(parts) == 1:
            # No language tag — could be English (common default)
            return True
        lang_part = parts[-1]
        if lang_part in ENGLISH_CODES:
            return True
    return False


def scan_directory(root: Path, library_type: str, report_data: dict | None = None,
                   workers: int = 8) -> list[dict]:
    """Scan a directory for files missing English subtitles.

    If report_data is provided, uses it instead of re-probing (much faster).
    """
    results = []

    if report_data:
        # Use pre-existing media report
        for entry in report_data.get("files", []):
            filepath = entry["filepath"]
            if not filepath.startswith(str(root)):
                continue
            video_path = Path(filepath)
            if not video_path.exists():
                continue

            sub_streams = entry.get("subtitle_streams", [])
            has_eng_embedded = any(
                (s.get("language", "") or "").lower().strip() in ENGLISH_CODES
                for s in sub_streams
            )
            external = _find_external_subs(video_path)
            has_eng_external = _has_english_external(external)

            results.append({
                "filepath": filepath,
                "filename": entry["filename"],
                "library_type": library_type,
                "embedded_sub_count": len(sub_streams),
                "embedded_languages": [
                    (s.get("language", "") or "unknown").lower() for s in sub_streams
                ],
                "external_sub_files": [str(s.name) for s in external],
                "has_english_embedded": has_eng_embedded,
                "has_english_external": has_eng_external,
                "has_english_any": has_eng_embedded or has_eng_external,
            })
        return results

    # No report — scan with ffprobe
    video_files = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            full = Path(dirpath) / fn
            if full.suffix.lower() in VIDEO_EXTS:
                video_files.append(full)

    print(f"  Found {len(video_files)} video files in {root}")

    def _check_file(video_path: Path) -> dict:
        sub_streams = _probe_subtitles(str(video_path))
        has_eng_embedded = _has_english_embedded(sub_streams)
        external = _find_external_subs(video_path)
        has_eng_external = _has_english_external(external)

        embedded_langs = []
        for s in sub_streams:
            lang = (s.get("tags", {}).get("language", "") or "unknown").lower()
            embedded_langs.append(lang)

        return {
            "filepath": str(video_path),
            "filename": video_path.name,
            "library_type": library_type,
            "embedded_sub_count": len(sub_streams),
            "embedded_languages": embedded_langs,
            "external_sub_files": [str(s.name) for s in external],
            "has_english_embedded": has_eng_embedded,
            "has_english_external": has_eng_external,
            "has_english_any": has_eng_embedded or has_eng_external,
        }

    import time as _time
    completed = 0
    start = _time.monotonic()
    total = len(video_files)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_check_file, vf): vf for vf in video_files}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                print(f"  ERROR: {futures[future]}: {e}", file=sys.stderr)
            completed += 1
            if completed % 50 == 0 or completed == total:
                elapsed = _time.monotonic() - start
                if completed > 0 and elapsed > 0:
                    remaining = (total - completed) * (elapsed / completed)
                    eta = f"~{remaining / 60:.0f}m" if remaining >= 60 else f"~{remaining:.0f}s"
                else:
                    eta = "..."
                print(f"  Progress: {completed}/{total} ({100 * completed / total:.0f}%) "
                      f"ETA: {eta}", flush=True)

    return results


def print_report(results: list[dict]) -> None:
    """Print summary and list files missing English subtitles."""
    total = len(results)
    has_eng = sum(1 for r in results if r["has_english_any"])
    missing = [r for r in results if not r["has_english_any"]]
    no_subs_at_all = [r for r in results if r["embedded_sub_count"] == 0 and not r["external_sub_files"]]

    print(f"\n{'=' * 70}")
    print(f"  Subtitle Report")
    print(f"{'=' * 70}")
    print(f"  Total files scanned:          {total}")
    print(f"  With English subtitles:       {has_eng} ({100 * has_eng / total:.1f}%)" if total else "")
    print(f"  Missing English subtitles:    {len(missing)}")
    print(f"  No subtitles at all:          {len(no_subs_at_all)}")
    print(f"{'=' * 70}")

    if missing:
        # Group by library type
        by_type: dict[str, list] = {}
        for r in missing:
            by_type.setdefault(r["library_type"], []).append(r)

        for lib_type, files in sorted(by_type.items()):
            print(f"\n  {lib_type.upper()} — {len(files)} missing English subs:")
            for f in sorted(files, key=lambda x: x["filepath"])[:50]:
                langs = ", ".join(f["embedded_languages"]) if f["embedded_languages"] else "none"
                ext = f" + {len(f['external_sub_files'])} ext" if f["external_sub_files"] else ""
                print(f"    {f['filename']}")
                print(f"      embedded: [{langs}]{ext}")
            if len(files) > 50:
                print(f"    ... and {len(files) - 50} more")


def export_csv(results: list[dict], csv_path: str) -> None:
    """Export missing-subtitle files to CSV."""
    missing = [r for r in results if not r["has_english_any"]]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["filepath", "filename", "library_type", "embedded_languages",
                         "external_subs", "has_any_subs"])
        for r in sorted(missing, key=lambda x: x["filepath"]):
            writer.writerow([
                r["filepath"],
                r["filename"],
                r["library_type"],
                "; ".join(r["embedded_languages"]),
                "; ".join(r["external_sub_files"]),
                "yes" if r["embedded_sub_count"] > 0 or r["external_sub_files"] else "no",
            ])
    print(f"\nExported {len(missing)} entries to {csv_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Check subtitle availability across NAS libraries")
    parser.add_argument("--movies-only", action="store_true", help="Scan movies only")
    parser.add_argument("--series-only", action="store_true", help="Scan series only")
    parser.add_argument("--root", type=str, default=None, help="Custom root directory")
    parser.add_argument("--report", type=str, default=None,
                        help="Use existing media_report.json (faster, skips ffprobe)")
    parser.add_argument("--csv", type=str, default=None, help="Export missing subs to CSV")
    parser.add_argument("--workers", type=int, default=8, help="Thread pool size for ffprobe")
    args = parser.parse_args()

    # Load report if available
    report_data = None
    report_path = args.report or str(MEDIA_REPORT)
    if os.path.exists(report_path):
        print(f"Using media report: {report_path}")
        with open(report_path, "r", encoding="utf-8") as f:
            report_data = json.load(f)

    all_results = []

    if args.root:
        root = Path(args.root)
        print(f"Scanning: {root}")
        all_results.extend(scan_directory(root, "custom", report_data, args.workers))
    else:
        if not args.series_only:
            print(f"Scanning movies: {NAS_MOVIES}")
            all_results.extend(scan_directory(NAS_MOVIES, "movie", report_data, args.workers))
        if not args.movies_only:
            print(f"Scanning series: {NAS_SERIES}")
            all_results.extend(scan_directory(NAS_SERIES, "series", report_data, args.workers))

    print_report(all_results)

    if args.csv:
        export_csv(all_results, args.csv)


if __name__ == "__main__":
    main()
