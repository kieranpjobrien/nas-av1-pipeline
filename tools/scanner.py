"""
Media Library Scanner
=====================
Scans movie and series directories, extracts codec/resolution/bitrate/audio info
using ffprobe, and outputs a JSON report for analysis.

Usage:
    python -m tools.scanner --output report.json
    python -m tools.scanner --non-english-csv non_eng.csv

Requires: ffprobe (comes with ffmpeg) on PATH.
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from paths import NAS_MOVIES, NAS_SERIES, MEDIA_REPORT

VIDEO_EXTENSIONS = {
    ".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".flv",
    ".mov", ".ts", ".webm", ".mpg", ".mpeg", ".m2ts",
}


def probe_file(filepath: str) -> dict | None:
    """Run ffprobe on a single file and return parsed metadata."""
    try:
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            str(filepath),
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30, encoding="utf-8", errors="replace"
        )
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return None


def extract_info(filepath: str, probe_data: dict, library_type: str) -> dict:
    """Extract the bits we care about from ffprobe output."""
    streams = probe_data.get("streams", [])
    fmt = probe_data.get("format", {})

    # Video stream (first one)
    video = next((s for s in streams if s.get("codec_type") == "video"), None)
    # All audio streams
    audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
    # All subtitle streams
    subtitle_streams = [s for s in streams if s.get("codec_type") == "subtitle"]

    file_size_bytes = int(fmt.get("size", 0))
    duration_secs = float(fmt.get("duration", 0))

    # Overall bitrate
    overall_bitrate_kbps = None
    if fmt.get("bit_rate"):
        overall_bitrate_kbps = int(fmt["bit_rate"]) / 1000

    video_info = {}
    if video:
        width = int(video.get("width", 0))
        height = int(video.get("height", 0))

        # Classify resolution
        if height >= 2100 or width >= 3800:
            resolution_class = "4K"
        elif height >= 1000 or width >= 1900:
            resolution_class = "1080p"
        elif height >= 700 or width >= 1200:
            resolution_class = "720p"
        elif height >= 400:
            resolution_class = "480p"
        else:
            resolution_class = "SD"

        video_codec = video.get("codec_name", "unknown").lower()
        # Normalise codec names
        codec_map = {
            "hevc": "HEVC (H.265)",
            "h265": "HEVC (H.265)",
            "h264": "H.264",
            "avc": "H.264",
            "avc1": "H.264",
            "av1": "AV1",
            "mpeg4": "MPEG-4",
            "mpeg2video": "MPEG-2",
            "vp9": "VP9",
            "vp8": "VP8",
            "wmv3": "WMV",
            "vc1": "VC-1",
        }
        codec_display = codec_map.get(video_codec, video_codec.upper())

        video_bitrate_kbps = None
        if video.get("bit_rate"):
            video_bitrate_kbps = int(video["bit_rate"]) / 1000

        # HDR detection
        color_transfer = video.get("color_transfer", "")
        color_primaries = video.get("color_primaries", "")
        is_hdr = color_transfer in ("smpte2084", "arib-std-b67") or color_primaries == "bt2020"

        # Bit depth
        bit_depth = video.get("bits_per_raw_sample")
        if bit_depth:
            bit_depth = int(bit_depth)
        else:
            pix_fmt = video.get("pix_fmt", "")
            if "10" in pix_fmt:
                bit_depth = 10
            elif "12" in pix_fmt:
                bit_depth = 12
            else:
                bit_depth = 8

        video_info = {
            "codec": codec_display,
            "codec_raw": video_codec,
            "width": width,
            "height": height,
            "resolution_class": resolution_class,
            "bitrate_kbps": video_bitrate_kbps,
            "hdr": is_hdr,
            "bit_depth": bit_depth,
            "pixel_format": video.get("pix_fmt", "unknown"),
        }

    audio_info = []
    total_audio_size_estimate = 0
    for a in audio_streams:
        codec = a.get("codec_name", "unknown").lower()
        audio_codec_map = {
            "aac": "AAC",
            "ac3": "AC-3",
            "eac3": "E-AC-3",
            "dts": "DTS",
            "truehd": "TrueHD",
            "flac": "FLAC",
            "opus": "Opus",
            "vorbis": "Vorbis",
            "mp3": "MP3",
            "pcm_s16le": "PCM",
            "pcm_s24le": "PCM 24-bit",
        }
        codec_display = audio_codec_map.get(codec, codec.upper())

        # Check if lossless
        lossless_codecs = {"truehd", "flac", "pcm_s16le", "pcm_s24le", "pcm_s32le", "dts"}
        profile = a.get("profile", "").lower()
        is_lossless = codec in lossless_codecs or "hd ma" in profile or "hd-ma" in profile

        channels = int(a.get("channels", 0))
        channel_layout = a.get("channel_layout", "")

        a_bitrate = None
        if a.get("bit_rate"):
            a_bitrate = int(a["bit_rate"]) / 1000
            if duration_secs > 0:
                total_audio_size_estimate += (int(a["bit_rate"]) * duration_secs) / 8

        lang = a.get("tags", {}).get("language", "und")
        title = a.get("tags", {}).get("title", "")

        audio_info.append({
            "codec": codec_display,
            "codec_raw": codec,
            "lossless": is_lossless,
            "channels": channels,
            "channel_layout": channel_layout,
            "bitrate_kbps": a_bitrate,
            "language": lang,
            "title": title,
            "profile": profile,
        })

    subtitle_info = []
    for s in subtitle_streams:
        lang = s.get("tags", {}).get("language", "und")
        title = s.get("tags", {}).get("title", "")
        codec = s.get("codec_name", "unknown")
        subtitle_info.append({
            "codec": codec,
            "language": lang,
            "title": title,
        })

    return {
        "filepath": str(filepath),
        "filename": os.path.basename(filepath),
        "extension": Path(filepath).suffix.lower(),
        "library_type": library_type,
        "file_size_bytes": file_size_bytes,
        "file_size_gb": round(file_size_bytes / (1024**3), 3),
        "duration_seconds": duration_secs,
        "duration_display": f"{int(duration_secs // 3600)}h {int((duration_secs % 3600) // 60)}m",
        "overall_bitrate_kbps": overall_bitrate_kbps,
        "video": video_info,
        "audio_streams": audio_info,
        "audio_stream_count": len(audio_info),
        "audio_estimated_size_gb": round(total_audio_size_estimate / (1024**3), 3),
        "subtitle_streams": subtitle_info,
        "subtitle_count": len(subtitle_info),
    }


def scan_directory(directory: str, library_type: str) -> list[str]:
    """Recursively find all video files."""
    files = []
    for root, _, filenames in os.walk(directory):
        for fname in filenames:
            if Path(fname).suffix.lower() in VIDEO_EXTENSIONS:
                files.append(os.path.join(root, fname))
    return files


EN_TOKENS = {"eng", "en", "english"}


def _has_english_audio(file_info: dict) -> bool:
    """Check if a scanned file has at least one English audio track."""
    for stream in file_info.get("audio_streams", []):
        lang = (stream.get("language") or "und").strip().lower()
        if lang in EN_TOKENS:
            return True
        title = (stream.get("title") or "").lower()
        if any(tok in title for tok in EN_TOKENS):
            return True
    return False


def write_non_english_csv(report: dict, csv_path: str) -> int:
    """Filter report for files missing English audio and write a CSV. Returns count."""
    rows = []
    for f in report.get("files", []):
        if _has_english_audio(f):
            continue
        langs = {(s.get("language") or "und") for s in f.get("audio_streams", [])}
        rows.append({
            "path": f["filepath"],
            "file": f["filename"],
            "audio_languages": ",".join(sorted(langs)),
            "audio_stream_count": f["audio_stream_count"],
        })

    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["path", "file", "audio_languages", "audio_stream_count"])
        w.writeheader()
        w.writerows(rows)
    return len(rows)


def _has_english_subs(file_info: dict) -> bool:
    """Check if a scanned file has at least one English subtitle track."""
    for stream in file_info.get("subtitle_streams", []):
        lang = (stream.get("language") or "und").strip().lower()
        if lang in EN_TOKENS:
            return True
        title = (stream.get("title") or "").lower()
        if any(tok in title for tok in EN_TOKENS):
            return True
    return False


def write_missing_subs_csv(report: dict, csv_path: str) -> int:
    """Filter report for files missing subtitles or English subs. Returns count."""
    rows = []
    for f in report.get("files", []):
        sub_count = f.get("subtitle_count", 0)
        if sub_count == 0:
            reason = "no_subs"
        elif not _has_english_subs(f):
            reason = "no_english_subs"
        else:
            continue
        sub_langs = {(s.get("language") or "und") for s in f.get("subtitle_streams", [])}
        rows.append({
            "path": f["filepath"],
            "file": f["filename"],
            "reason": reason,
            "subtitle_count": sub_count,
            "subtitle_languages": ",".join(sorted(sub_langs)) if sub_langs else "",
        })

    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["path", "file", "reason", "subtitle_count", "subtitle_languages"])
        w.writeheader()
        w.writerows(rows)
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Scan media library and produce analysis report")
    parser.add_argument("--movies", type=str, default=str(NAS_MOVIES),
                        help="Path to movies directory")
    parser.add_argument("--series", type=str, default=str(NAS_SERIES),
                        help="Path to series directory")
    parser.add_argument("--output", type=str, default=str(MEDIA_REPORT),
                        help="Output JSON file")
    parser.add_argument("--workers", type=int, default=4,
                        help="Parallel ffprobe workers")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit files to scan (0 = all, useful for testing)")
    parser.add_argument("--non-english-csv", type=str, default=None, metavar="PATH",
                        help="After scan, write CSV of files missing English audio")
    parser.add_argument("--missing-subs-csv", type=str, default=None, metavar="PATH",
                        help="After scan, write CSV of files missing subtitles or English subs")
    args = parser.parse_args()

    all_files = []

    for path, lib_type in [(args.movies, "movie"), (args.series, "series")]:
        if os.path.exists(path):
            found = scan_directory(path, lib_type)
            all_files.extend([(f, lib_type) for f in found])
            print(f"Found {len(found)} video files in {path}")
        else:
            print(f"WARNING: Path not found: {path}")

    if args.limit > 0:
        all_files = all_files[:args.limit]

    print(f"\nScanning {len(all_files)} files with {args.workers} workers...")

    results = []
    errors = []
    completed = 0

    def process_file(filepath_and_type):
        filepath, lib_type = filepath_and_type
        probe = probe_file(filepath)
        if probe is None:
            return None, filepath
        return extract_info(filepath, probe, lib_type), None

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_file, f): f for f in all_files}
        for future in as_completed(futures):
            completed += 1
            if completed % 50 == 0 or completed == len(all_files):
                print(f"  Progress: {completed}/{len(all_files)} ({100*completed/len(all_files):.1f}%)")

            result, error_path = future.result()
            if result:
                results.append(result)
            elif error_path:
                errors.append(str(error_path))

    # Build summary statistics
    total_size = sum(r["file_size_bytes"] for r in results)
    movie_results = [r for r in results if r["library_type"] == "movie"]
    series_results = [r for r in results if r["library_type"] == "series"]

    summary = {
        "scan_date": datetime.now().isoformat(),
        "total_files": len(results),
        "total_size_gb": round(total_size / (1024**3), 2),
        "total_size_tb": round(total_size / (1024**4), 3),
        "movies": {
            "count": len(movie_results),
            "size_gb": round(sum(r["file_size_bytes"] for r in movie_results) / (1024**3), 2),
        },
        "series": {
            "count": len(series_results),
            "size_gb": round(sum(r["file_size_bytes"] for r in series_results) / (1024**3), 2),
        },
        "errors": len(errors),
        "error_files": errors[:20],  # First 20 errors for debugging
    }

    report = {
        "summary": summary,
        "files": results,
    }

    output_path = args.output
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*60}")
    print(f"Scan complete!")
    print(f"  Total files: {summary['total_files']}")
    print(f"  Total size:  {summary['total_size_tb']} TB ({summary['total_size_gb']} GB)")
    print(f"  Movies:      {summary['movies']['count']} ({summary['movies']['size_gb']} GB)")
    print(f"  Series:      {summary['series']['count']} ({summary['series']['size_gb']} GB)")
    print(f"  Errors:      {summary['errors']}")
    print(f"  Report:      {output_path}")
    print(f"{'='*60}")

    # Non-English CSV (uses the already-scanned report, no extra ffprobe calls)
    if args.non_english_csv:
        count = write_non_english_csv(report, args.non_english_csv)
        print(f"\nNon-English audio: {count} files written to {args.non_english_csv}")

    # Missing subs CSV
    if args.missing_subs_csv:
        count = write_missing_subs_csv(report, args.missing_subs_csv)
        print(f"\nMissing subtitles: {count} files written to {args.missing_subs_csv}")


if __name__ == "__main__":
    main()
