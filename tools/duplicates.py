"""
Duplicate Finder
================
Reads an existing media_report.json and identifies potential duplicate files
using fuzzy title matching and/or duration+resolution clustering.

Usage:
    python -m tools.duplicates
    python -m tools.duplicates --mode title --output dupes.csv
    python -m tools.duplicates --report path/to/media_report.json

No extra ffprobe calls -- works entirely from the scan report.
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

from paths import MEDIA_REPORT

# Tags/tokens stripped during title normalisation
_STRIP_RE = re.compile(
    r"""
    \b(
        \d{4}                          # year like 2023
        |[12]\d{3}p                    # resolution like 1080p
        |4K|UHD|SD|720p|480p
        |x264|x265|h\.?264|h\.?265|hevc|avc|av1|vp9
        |bluray|bdrip|brrip|web[.-]?dl|webrip|hdtv|dvdrip
        |remux|hdr|hdr10|dv|dolby\.?vision
        |aac|ac3|eac3|dts|truehd|flac|opus|atmos|dd[+p]?5\.1|ddp?5\.1
        |ntb|nf|amzn|dsnp|hulu|max|hbo
        |mkv|mp4|avi
        |multi|proper|repack|internal
    )\b
    |[\[\](){}\-_.]+
    """,
    re.IGNORECASE | re.VERBOSE,
)


def normalize_title(filename: str) -> str:
    """Strip year, codec tags, resolution, punctuation from a filename."""
    stem = Path(filename).stem
    cleaned = _STRIP_RE.sub(" ", stem)
    return " ".join(cleaned.lower().split())


def find_title_duration_dupes(files: list[dict], duration_tolerance: float = 30.0) -> list[dict]:
    """Group by normalised title, then sub-group by duration within tolerance."""
    by_title = defaultdict(list)
    for f in files:
        key = normalize_title(f["filename"])
        if key:
            by_title[key].append(f)

    groups = []
    group_id = 0
    for title, items in by_title.items():
        if len(items) < 2:
            continue
        # Sub-group by duration
        items_sorted = sorted(items, key=lambda x: x.get("duration_seconds", 0))
        clusters = []
        current_cluster = [items_sorted[0]]
        for item in items_sorted[1:]:
            prev_dur = current_cluster[-1].get("duration_seconds", 0)
            curr_dur = item.get("duration_seconds", 0)
            if abs(curr_dur - prev_dur) <= duration_tolerance:
                current_cluster.append(item)
            else:
                if len(current_cluster) >= 2:
                    clusters.append(current_cluster)
                current_cluster = [item]
        if len(current_cluster) >= 2:
            clusters.append(current_cluster)

        for cluster in clusters:
            group_id += 1
            for f in cluster:
                groups.append({
                    "group_id": group_id,
                    "mode": "title",
                    "normalized_title": title,
                    "filepath": f["filepath"],
                    "filename": f["filename"],
                    "resolution": f.get("video", {}).get("resolution_class", ""),
                    "codec": f.get("video", {}).get("codec", ""),
                    "duration": round(f.get("duration_seconds", 0), 1),
                    "file_size_gb": f.get("file_size_gb", 0),
                })
    return groups


def find_duration_resolution_dupes(files: list[dict], duration_tolerance: float = 2.0) -> list[dict]:
    """Group by resolution class, then cluster by exact duration match (within tolerance)."""
    by_res = defaultdict(list)
    for f in files:
        res = f.get("video", {}).get("resolution_class", "unknown")
        by_res[res].append(f)

    groups = []
    group_id = 10000  # offset to avoid collision with title groups
    for res, items in by_res.items():
        if len(items) < 2:
            continue
        items_sorted = sorted(items, key=lambda x: x.get("duration_seconds", 0))
        # Sliding window cluster by duration
        i = 0
        while i < len(items_sorted):
            cluster = [items_sorted[i]]
            j = i + 1
            while j < len(items_sorted):
                if abs(items_sorted[j].get("duration_seconds", 0) -
                       cluster[-1].get("duration_seconds", 0)) <= duration_tolerance:
                    cluster.append(items_sorted[j])
                    j += 1
                else:
                    break
            if len(cluster) >= 2:
                # Filter out clusters where all files have the same path (not dupes)
                unique_dirs = {str(Path(f["filepath"]).parent) for f in cluster}
                if len(unique_dirs) >= 2 or len(cluster) >= 2:
                    group_id += 1
                    for f in cluster:
                        groups.append({
                            "group_id": group_id,
                            "mode": "duration",
                            "filepath": f["filepath"],
                            "filename": f["filename"],
                            "resolution": res,
                            "codec": f.get("video", {}).get("codec", ""),
                            "duration": round(f.get("duration_seconds", 0), 1),
                            "file_size_gb": f.get("file_size_gb", 0),
                        })
            i = j
    return groups


_CODEC_SCORES = {"av1": 30, "hevc": 20, "h264": 10}
_RES_SCORES = {"4K": 30, "1080p": 20, "720p": 10, "480p": 0, "SD": 0}
_AUDIO_CODEC_SCORES = {"truehd": 20, "flac": 20, "dts": 15, "eac3": 10, "ac3": 10, "aac": 5, "opus": 5}


def score_file(f: dict) -> int:
    """Score a file 0-100+ based on codec, resolution, audio quality, size, and HDR."""
    video = f.get("video", {})

    # Codec (0-30)
    codec = (video.get("codec_raw") or "").lower()
    score = _CODEC_SCORES.get(codec, 0)

    # Resolution (0-30)
    res = video.get("resolution_class", "")
    score += _RES_SCORES.get(res, 0)

    # Audio quality (0-20) — best stream wins
    best_audio = 0
    for stream in f.get("audio_streams", []):
        raw = (stream.get("codec_raw") or "").lower()
        if stream.get("lossless"):
            s = 20
        elif raw in _AUDIO_CODEC_SCORES:
            s = _AUDIO_CODEC_SCORES[raw]
        else:
            s = 0
        # Bump surround formats (channels >= 6) by a small margin within tier
        if stream.get("channels", 0) >= 6 and s < 20:
            s = min(s + 2, 20)
        best_audio = max(best_audio, s)
    score += best_audio

    # HDR bonus (+5)
    if video.get("hdr"):
        score += 5

    return score


def pick_best(group: list[dict]) -> tuple[dict, list[dict]]:
    """Return (keeper, deletions) for a duplicate group, scored and tie-broken."""
    if not group:
        return group[0], []

    max_size = max(f.get("file_size_gb", 0) for f in group) or 1.0

    scored = []
    for f in group:
        base = score_file(f)
        # File size tiebreaker (0-10), proportional within group
        size_score = int(10 * f.get("file_size_gb", 0) / max_size)
        scored.append((base + size_score, f))

    # Sort: highest score first, then shortest filename on tie
    scored.sort(key=lambda x: (-x[0], len(x[1].get("filename", ""))))
    keeper = scored[0][1]
    deletions = [s[1] for s in scored[1:]]
    return keeper, deletions


def main():
    parser = argparse.ArgumentParser(description="Find potential duplicate files in media report")
    parser.add_argument("--report", type=str, default=str(MEDIA_REPORT),
                        help="Path to media_report.json")
    parser.add_argument("--output", type=str, default="duplicates.csv",
                        help="Output CSV file")
    parser.add_argument("--mode", choices=["title", "duration", "both"], default="both",
                        help="Detection mode (default: both)")
    parser.add_argument("--delete", action="store_true",
                        help="Score duplicates and show keep/delete recommendations (dry-run)")
    parser.add_argument("--execute", action="store_true",
                        help="Actually delete lower-scored copies (requires --delete)")
    args = parser.parse_args()

    if args.execute and not args.delete:
        print("ERROR: --execute requires --delete", file=sys.stderr)
        sys.exit(1)

    report_path = Path(args.report)
    if not report_path.exists():
        print(f"ERROR: Report not found: {report_path}", file=sys.stderr)
        sys.exit(1)

    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)

    files = report.get("files", [])
    print(f"Loaded {len(files)} files from report")

    results = []
    if args.mode in ("title", "both"):
        title_dupes = find_title_duration_dupes(files)
        results.extend(title_dupes)
        title_groups = len({r["group_id"] for r in title_dupes})
        print(f"Title matching: {title_groups} groups, {len(title_dupes)} files")

    if args.mode in ("duration", "both"):
        dur_dupes = find_duration_resolution_dupes(files)
        results.extend(dur_dupes)
        dur_groups = len({r["group_id"] for r in dur_dupes})
        print(f"Duration matching: {dur_groups} groups, {len(dur_dupes)} files")

    if not results:
        print("No duplicates found.")
        return

    if not args.delete:
        # Original CSV output mode
        fieldnames = ["group_id", "mode", "filepath", "filename", "resolution", "codec", "duration", "file_size_gb"]
        with open(args.output, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(results)
        total_groups = len({r["group_id"] for r in results})
        print(f"\nWrote {len(results)} rows ({total_groups} groups) to {args.output}")
        return

    # --delete mode: score each group and pick best
    # Build lookup from filepath -> full file record for scoring
    file_lookup = {f["filepath"]: f for f in files}

    # Group results by group_id
    grouped = defaultdict(list)
    for r in results:
        grouped[r["group_id"]].append(r)

    total_deleted = 0
    total_kept = 0
    for gid, group_rows in sorted(grouped.items()):
        # Resolve full file records for scoring
        full_records = []
        for row in group_rows:
            rec = file_lookup.get(row["filepath"])
            if rec:
                full_records.append(rec)
        if len(full_records) < 2:
            continue

        keeper, deletions = pick_best(full_records)
        keeper_score = score_file(keeper)
        max_size = max(f.get("file_size_gb", 0) for f in full_records) or 1.0
        keeper_total = keeper_score + int(10 * keeper.get("file_size_gb", 0) / max_size)

        print(f"\n--- Group {gid} ---")
        print(f"  KEEP  [{keeper_total:3d} pts] {keeper['filepath']}")
        for d in deletions:
            d_score = score_file(d)
            d_total = d_score + int(10 * d.get("file_size_gb", 0) / max_size)
            print(f"  DEL   [{d_total:3d} pts] {d['filepath']}")

            if args.execute:
                try:
                    os.remove(d["filepath"])
                    print(f"        -> DELETED")
                    total_deleted += 1
                except OSError as e:
                    print(f"        -> FAILED: {e}")
            else:
                total_deleted += 1  # count for dry-run summary
        total_kept += 1

    action = "Deleted" if args.execute else "Would delete"
    print(f"\n{action} {total_deleted} files across {total_kept} groups")


if __name__ == "__main__":
    main()
