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
import re
import subprocess
import sys
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from paths import MEDIA_REPORT, NAS_MOVIES, NAS_SERIES
from pipeline.orchestrator import _PIPELINE_TMP_SUFFIXES

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


def probe_file(filepath: str) -> dict | None:
    """Run ffprobe on a single file and return parsed metadata."""
    try:
        cmd = [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(filepath),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, encoding="utf-8", errors="replace")
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

        audio_info.append(
            {
                "codec": codec_display,
                "codec_raw": codec,
                "lossless": is_lossless,
                "channels": channels,
                "channel_layout": channel_layout,
                "bitrate_kbps": a_bitrate,
                "language": lang,
                "title": title,
                "profile": profile,
            }
        )

    subtitle_info = []
    for s in subtitle_streams:
        lang = s.get("tags", {}).get("language", "und")
        title = s.get("tags", {}).get("title", "")
        codec = s.get("codec_name", "unknown")
        subtitle_info.append(
            {
                "codec": codec,
                "language": lang,
                "title": title,
            }
        )

    # External subtitle sidecars — Bazarr writes .srt/.ass alongside the media file. The
    # dashboard's "needs subs" check has been under-counting because it only looked at
    # internal streams. Record what's actually on disk so downstream consumers can check
    # both. Naming patterns we handle:
    #   foo.mkv, foo.en.srt, foo.en.hi.srt, foo.en.forced.ass, foo.eng.srt, foo.srt
    #
    # Delegates to pipeline.subs.scan_sidecars which uses the SCAN_EXTS set
    # (includes .vtt/.idx). We convert the SidecarSub dataclasses into the
    # dict shape media_report has always stored.
    from pipeline.subs import SCAN_EXTS
    from pipeline.subs import scan_sidecars as _scan_sidecars

    external_subs = []
    for sub in _scan_sidecars(filepath, exts=SCAN_EXTS):
        # Reconstruct the flags list from is_forced / is_hi + any remaining
        # non-language dotted parts. This keeps backward compatibility with
        # the media-report consumers that read the `flags` list directly.
        suffix = sub.stem[len(Path(filepath).stem):].lstrip(".")
        flags: list[str] = []
        for part in (p for p in suffix.split(".") if p):
            if part.lower() == sub.language:
                continue
            flags.append(part.lower())
        external_subs.append(
            {
                "filename": sub.filename,
                "language": sub.language,
                "flags": flags,  # e.g. ["hi"] for hearing-impaired, ["forced"] for forced
                "ext": Path(sub.filename).suffix.lower().lstrip("."),
            }
        )

    try:
        file_mtime = os.path.getmtime(filepath)
    except OSError:
        file_mtime = 0

    filename = os.path.basename(filepath)
    filename_matches_folder = _filename_matches_folder(filepath, library_type)

    return {
        "filepath": str(filepath),
        "filename": filename,
        "extension": Path(filepath).suffix.lower(),
        "library_type": library_type,
        "file_size_bytes": file_size_bytes,
        "file_mtime": file_mtime,
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
        "external_subtitles": external_subs,
        "external_subtitle_count": len(external_subs),
        "filename_matches_folder": filename_matches_folder,
    }


def scan_directory(directory: str, library_type: str) -> list[str]:
    """Recursively find all video files.

    Skips tmp files left by interrupted pipeline runs (see
    ``pipeline.orchestrator._PIPELINE_TMP_SUFFIXES``) — they'd otherwise be
    indexed as if they were real media, then re-encoded on top of files the
    pipeline is already trying to process.
    """
    files = []
    for root, _, filenames in os.walk(directory):
        for fname in filenames:
            if fname.endswith(_PIPELINE_TMP_SUFFIXES):
                continue
            if Path(fname).suffix.lower() in VIDEO_EXTENSIONS:
                files.append(os.path.join(root, fname))
    return files


def update_report_entry(filepath: str, report_path: str, library_type: str) -> bool:
    """Re-probe a single NAS file and patch its entry in media_report.json in-place.

    Called after stage_replace so the library tab immediately reflects the new
    codec, size, and bitrate without waiting for a full rescan. Uses the shared
    file lock for safe concurrent access.

    Args:
        filepath:     Absolute path to the now-replaced file on the NAS.
        report_path:  Path to media_report.json.
        library_type: "movie" or "series".

    Returns True on success, False if probe or I/O fails (caller should warn).
    """
    probe = probe_file(filepath)
    if probe is None:
        return False
    entry = extract_info(filepath, probe, library_type)

    from tools.report_lock import patch_report

    def _patch(report: dict) -> None:
        files = report.get("files", [])

        # Replace existing entry (match on filepath)
        updated = False
        for i, e in enumerate(files):
            if e.get("filepath") == filepath:
                # Preserve TMDb and language detection data from old entry
                old = files[i]
                if old.get("tmdb") and not entry.get("tmdb"):
                    entry["tmdb"] = old["tmdb"]
                for stream_key in ("audio_streams", "subtitle_streams"):
                    for j, s in enumerate(entry.get(stream_key, [])):
                        if j < len(old.get(stream_key, [])):
                            old_s = old[stream_key][j]
                            if old_s.get("detected_language") and not s.get("detected_language"):
                                s["detected_language"] = old_s["detected_language"]
                                s["detection_confidence"] = old_s.get("detection_confidence")
                                s["detection_method"] = old_s.get("detection_method")
                files[i] = entry
                updated = True
                break
        if not updated:
            files.append(entry)

        # Patch summary
        total_size = sum(e.get("file_size_bytes", 0) for e in files)
        movie_files = [e for e in files if e.get("library_type") == "movie"]
        series_files = [e for e in files if e.get("library_type") == "series"]
        summary = report.setdefault("summary", {})
        summary["total_files"] = len(files)
        summary["total_size_gb"] = round(total_size / 1024**3, 2)
        summary["total_size_tb"] = round(total_size / 1024**4, 3)
        summary.setdefault("movies", {})["count"] = len(movie_files)
        summary["movies"]["size_gb"] = round(sum(e.get("file_size_bytes", 0) for e in movie_files) / 1024**3, 2)
        summary.setdefault("series", {})["count"] = len(series_files)
        summary["series"]["size_gb"] = round(sum(e.get("file_size_bytes", 0) for e in series_files) / 1024**3, 2)

    try:
        patch_report(_patch)
        return True
    except Exception:
        return False


EN_TOKENS = {"eng", "en", "english"}

_SXXEXX_RE = re.compile(r"S(\d{1,4})E(\d{1,2})", re.IGNORECASE)
_YEAR_PAREN_RE = re.compile(r"\((19[2-9]\d|20[0-2]\d)\)")


def _ascii_key(s: str) -> str:
    """Normalise a title for fuzzy comparison: strip diacritics, punctuation,
    AND whitespace, lowercase the rest.

    Equivalent forms in the wild that all collapse to the same key:
      * "Bob's Burgers"      vs  "Bobs Burgers"        (apostrophe vs not)
      * "Star-Lord"          vs  "Star Lord"           (hyphen vs space)
      * "House M.D."         vs  "House MD"            (dotted abbreviation)
      * "Love, Death & Robots" vs "Love Death and Robots" (& vs and)
      * "Shōgun"             vs  "Shogun"              (diacritic stripping)

    Pre-2026-05-04 this fn replaced punctuation with whitespace which
    matched apostrophe/hyphen pairs but only when both forms used the
    same delimiter. Apostrophe-elision ("Bob's" → "Bobs") flipped the
    delimiter pattern and caused 232 false mismatches across the user's
    library. Dropping all separators makes the comparison invariant to
    whitespace and punctuation choices the source files made.
    """
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    # "and" ↔ "&" equivalence — collapse both to nothing so titles using
    # either convention key the same. Done before the punctuation strip so
    # the "&" doesn't leave a stray "and" island when there's no space
    # around it (rare but possible).
    s = re.sub(r"\s+and\s+|\s+&\s+|\s*&\s*", "", s, flags=re.IGNORECASE)
    return re.sub(r"[^\w]", "", s).lower()


def _filename_matches_folder(filepath: str, library_type: str) -> bool:
    """Check if the filename's title portion matches its parent folder's title.

    The "English Filename" hero stat treats True as compliant. False means the
    filename looks foreign-language or otherwise out of sync with the folder.
    Genuine foreign-origin films live in English-titled folders per library
    convention, so folder match is a good proxy for "English keeper name".
    """
    try:
        filename = os.path.splitext(os.path.basename(filepath))[0]
        parent = os.path.basename(os.path.dirname(filepath))
        grandparent = os.path.basename(os.path.dirname(os.path.dirname(filepath)))
    except Exception:
        return True

    # Extract title portion
    if library_type == "movie":
        # strip year: "Title (2024)" → "Title"
        m = _YEAR_PAREN_RE.search(filename)
        fn_title = filename[: m.start()].rstrip(" .-") if m else filename
        folder = _YEAR_PAREN_RE.sub("", parent).strip()
    elif library_type == "series":
        # Strip SxxExx and everything after: "Show S01E01 Title" → "Show"
        m = _SXXEXX_RE.search(filename)
        if m:
            fn_title = filename[: m.start()].rstrip(" .-")
        else:
            # No SxxExx — likely a featurette / extra (e.g. "behind-the-scenes.mkv"
            # in a season folder). Plex handles these as bonus content separately
            # so they shouldn't count against the show-name compliance check.
            return True
        # Strip disambiguators from BOTH filename title AND folder. Some files
        # carry the "(US)" in the name, some don't — both forms are fine.
        folder = grandparent  # show folder
        # Region tags appear two ways in the wild:
        #   "The Office (US)"  ← folder convention with parens
        #   "The Office US S01E01.mkv"  ← filename convention without parens
        # Strip both forms so they collapse to the same key.
        _region_paren_re = re.compile(r"\s*\((?:US|UK|AU|NZ|CA|IE|IN|ZA|BR|MX|JP|KR)\)", re.IGNORECASE)
        # End-anchored only — "Mozart in the Jungle" must NOT lose "in" because
        # IN is in the country list. The bare-tag convention only ever appears
        # at the END of the show title ("The Office US S01E01.mkv"), so we
        # restrict the match accordingly.
        _region_bare_re = re.compile(r"\s+(?:US|UK|AU|NZ|CA|IE|IN|ZA|BR|MX|JP|KR)\s*$", re.IGNORECASE)
        folder = _region_paren_re.sub("", _YEAR_PAREN_RE.sub("", folder)).strip()
        fn_title = _region_paren_re.sub("", _YEAR_PAREN_RE.sub("", fn_title)).strip()
        # Bare tag only stripped from the filename — folder convention always
        # uses parens, so matching the bare tag in the folder side would be
        # a false positive.
        fn_title = _region_bare_re.sub("", fn_title).strip()
        # Bare year in filename ("Shogun.2024.S01E04..." → strip "2024" so the
        # title resolves to just "Shogun"). Folder convention puts the year
        # in parens which the regex above already strips; bare year only
        # ever appears on the filename side. End/space-bounded so it can't
        # eat parts of the show title.
        fn_title = re.sub(r"\b(19[2-9]\d|20[0-2]\d)\b", "", fn_title).strip(" .-")
    else:
        return True

    if not fn_title or not folder:
        return True

    # If filename has NO title portion (bare "SxxExx.mkv" or just year), treat as match
    # — Plex matches on SxxExx/year alone, so absence of series name is valid.
    if not fn_title.strip() or fn_title.strip() in ("", "(", ")"):
        return True

    return _ascii_key(fn_title) == _ascii_key(folder)


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
        rows.append(
            {
                "path": f["filepath"],
                "file": f["filename"],
                "audio_languages": ",".join(sorted(langs)),
                "audio_stream_count": f["audio_stream_count"],
            }
        )

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
    """Filter report for files missing subtitles or English subs. Returns count.

    Skips files whose path matches an entry in the user-maintained
    ``subs_optional.json`` exclusion list (silent films, kids' shows the user
    has opted out of subs for, etc.) — see :mod:`pipeline.subs_exclusion`.
    """
    from pipeline.subs_exclusion import is_subs_optional

    rows = []
    for f in report.get("files", []):
        if is_subs_optional(f.get("filepath", "")):
            continue
        sub_count = f.get("subtitle_count", 0)
        if sub_count == 0:
            reason = "no_subs"
        elif not _has_english_subs(f):
            reason = "no_english_subs"
        else:
            continue
        sub_langs = {(s.get("language") or "und") for s in f.get("subtitle_streams", [])}
        rows.append(
            {
                "path": f["filepath"],
                "file": f["filename"],
                "reason": reason,
                "subtitle_count": sub_count,
                "subtitle_languages": ",".join(sorted(sub_langs)) if sub_langs else "",
            }
        )

    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["path", "file", "reason", "subtitle_count", "subtitle_languages"])
        w.writeheader()
        w.writerows(rows)
    return len(rows)


def main():
    # Force unbuffered stdout so progress appears in process logs immediately
    sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(description="Scan media library and produce analysis report")
    parser.add_argument("--movies", type=str, default=str(NAS_MOVIES), help="Path to movies directory")
    parser.add_argument("--series", type=str, default=str(NAS_SERIES), help="Path to series directory")
    parser.add_argument("--output", type=str, default=str(MEDIA_REPORT), help="Output JSON file")
    parser.add_argument("--workers", type=int, default=4, help="Parallel ffprobe workers")
    parser.add_argument("--full", action="store_true", help="Force full rescan (ignore cached results)")
    parser.add_argument("--limit", type=int, default=0, help="Limit files to scan (0 = all, useful for testing)")
    parser.add_argument(
        "--non-english-csv",
        type=str,
        default=None,
        metavar="PATH",
        help="After scan, write CSV of files missing English audio",
    )
    parser.add_argument(
        "--missing-subs-csv",
        type=str,
        default=None,
        metavar="PATH",
        help="After scan, write CSV of files missing subtitles or English subs",
    )
    args = parser.parse_args()

    # ProcessRegistry reconcile + register. A scanner run that crashed without
    # cleanup would otherwise leave a stale entry; reconcile() clears it so we
    # can register under the same role name.
    from pathlib import Path as _Path

    from paths import STAGING_DIR as _STAGING_DIR
    from pipeline.process_registry import ProcessRegistry as _ProcessRegistry

    _registry_path = _Path(_STAGING_DIR) / "control" / "agents.registry.json"
    _registry_path.parent.mkdir(parents=True, exist_ok=True)
    _registry = _ProcessRegistry(_registry_path)
    _dead = _registry.reconcile()
    print(f"Reaped {len(_dead)} dead registry entries: {_dead}")

    with _registry.register("scanner", sys.argv):
        _scan_body(args)


def _scan_body(args) -> None:
    """The actual scanner logic, invoked inside the process registry context."""
    all_files = []

    for path, lib_type in [(args.movies, "movie"), (args.series, "series")]:
        if os.path.exists(path):
            found = scan_directory(path, lib_type)
            all_files.extend([(f, lib_type) for f in found])
            print(f"Found {len(found)} video files in {path}")
        else:
            print(f"WARNING: Path not found: {path}")

    if args.limit > 0:
        all_files = all_files[: args.limit]

    # Incremental scan: reuse results from existing report for unchanged files.
    # A file is "unchanged" if its filepath, size, and mtime all match.
    #
    # IMPORTANT: two separate concerns here, wrongly collapsed in earlier versions.
    #   cached      — whole-entry reuse to avoid re-probing. Gated by --full.
    #   old_by_path — TMDb/language detection preservation across re-probes. ALWAYS
    #                 populated if an old report exists, even under --full. Otherwise
    #                 --full wipes all TMDb tags (seen on 2026-04-19 rescan).
    cached = {}
    old_by_path: dict[str, dict] = {}  # filepath -> entry for TMDb/language preservation
    old_by_dir: dict[str, list[dict]] = {}  # directory -> entries (for rename matching)
    if os.path.exists(args.output):
        try:
            with open(args.output, "r", encoding="utf-8") as f:
                old_report = json.load(f)
            for entry in old_report.get("files", []):
                fp = entry.get("filepath", "")
                sz = entry.get("file_size_bytes", -1)
                mt = entry.get("file_mtime", 0)
                # Only populate cached (reuse) if not --full. Always populate preservation
                # maps so TMDb/language-detect data survives a --full rescan.
                if not args.full:
                    cached[(fp, sz, mt)] = entry
                old_by_path[fp] = entry
                parent = os.path.dirname(fp)
                old_by_dir.setdefault(parent, []).append(entry)
            if cached:
                print(f"Loaded {len(cached)} cached entries from previous report")
            if old_by_path:
                print(f"Loaded {len(old_by_path)} entries for TMDb/language preservation")
        except Exception:
            pass

    results = []
    reused = 0
    to_probe = []

    for filepath, lib_type in all_files:
        try:
            stat = os.stat(filepath)
            sz = stat.st_size
            mt = stat.st_mtime
        except OSError:
            sz = -1
            mt = 0
        key = (filepath, sz, mt)
        if key in cached:
            results.append(cached[key])
            reused += 1
        else:
            to_probe.append((filepath, lib_type))

    if reused:
        print(f"Reused {reused} cached results, {len(to_probe)} files need probing")

    print(f"\nScanning {len(to_probe)} files with {args.workers} workers...")

    errors = []
    completed = 0
    total_to_probe = len(to_probe)

    def process_file(filepath_and_type):
        filepath, lib_type = filepath_and_type
        probe = probe_file(filepath)
        if probe is None:
            return None, filepath
        return extract_info(filepath, probe, lib_type), None

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_file, f): f for f in to_probe}
        for future in as_completed(futures):
            completed += 1
            if total_to_probe > 0 and (completed % 50 == 0 or completed == total_to_probe):
                print(f"  Progress: {completed}/{total_to_probe} ({100 * completed / total_to_probe:.1f}%)")

            result, error_path = future.result()
            if result:
                # Preserve TMDb and language detection from previous report.
                # Try exact filepath match first, then fall back to same-directory
                # match (handles renames within the same folder).
                fp = result.get("filepath", "")
                old = old_by_path.get(fp)
                if not old or not old.get("tmdb"):
                    parent = os.path.dirname(fp)
                    for candidate in old_by_dir.get(parent, []):
                        if candidate.get("tmdb") and candidate.get("filepath") != fp:
                            old = candidate
                            break
                if old:
                    if old.get("tmdb") and not result.get("tmdb"):
                        result["tmdb"] = old["tmdb"]
                    for skey in ("audio_streams", "subtitle_streams"):
                        for j, s in enumerate(result.get(skey, [])):
                            if j < len(old.get(skey, [])):
                                old_s = old[skey][j]
                                if old_s.get("detected_language") and not s.get("detected_language"):
                                    s["detected_language"] = old_s["detected_language"]
                                    s["detection_confidence"] = old_s.get("detection_confidence")
                                    s["detection_method"] = old_s.get("detection_method")
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
        "error_files": errors[:20],
    }

    # Write report via a single patch that preserves anything the pipeline wrote while
    # we were scanning. The previous "read then write-much-later" pattern had a latent
    # race: update_entry() calls between our read (t0) and our write (t30+) got
    # clobbered. Now we patch atomically, merging our fresh probe results with any
    # in-flight pipeline updates.
    from tools.report_lock import patch_report

    seen_paths = {r["filepath"] for r in results}
    result_by_path = {r["filepath"]: r for r in results}

    # Fields the pipeline / tmdb-tool owns — scanner probes don't produce
    # these, so we must always carry them forward from the existing entry.
    _PRESERVED_FIELDS = ("tmdb",)
    _PRESERVED_STREAM_FIELDS = (
        "detected_language",
        "detection_confidence",
        "detection_method",
        "whisper_attempted",
    )

    def _merge_preserved(fresh: dict, existing: dict) -> dict:
        """Copy pipeline-owned fields from existing onto a fresh probe result."""
        for k in _PRESERVED_FIELDS:
            if existing.get(k) and not fresh.get(k):
                fresh[k] = existing[k]
        # Copy per-stream detection data (audio + subs). Match by stream index.
        for kind in ("audio_streams", "subtitle_streams"):
            old_list = existing.get(kind) or []
            new_list = fresh.get(kind) or []
            for i, s in enumerate(new_list):
                if i < len(old_list):
                    for f in _PRESERVED_STREAM_FIELDS:
                        if old_list[i].get(f) and not s.get(f):
                            s[f] = old_list[i][f]
        return fresh

    def _scanner_patch(current: dict) -> None:
        current_files = current.get("files", []) or []
        merged: list = []

        # 1. Keep/update entries we re-scanned. If a pipeline update happened between
        #    our read and now, prefer whichever source has the NEWER file_mtime — so
        #    a post-encode update_entry() after the scanner started takes precedence.
        #    Either way, always carry tmdb + per-stream detection data forward.
        for existing in current_files:
            fp = existing.get("filepath")
            if fp in seen_paths:
                fresh = result_by_path[fp]
                if existing.get("file_mtime", 0) > fresh.get("file_mtime", 0):
                    merged.append(existing)
                else:
                    merged.append(_merge_preserved(fresh, existing))
                seen_paths.discard(fp)  # processed — don't re-add below
            # else: file wasn't in our scan → file is gone, drop it

        # 2. Add any entries that are in our results but weren't in the current report
        #    (new files that appeared after the last scan).
        for fp in seen_paths:
            merged.append(result_by_path[fp])

        current["files"] = merged
        # Merge summary — preserve anything like language_scan_date that we don't touch
        current_summary = current.get("summary", {}) or {}
        current_summary.update(summary)
        current["summary"] = current_summary

    patch_report(_scanner_patch)
    output_path = args.output

    # Reconcile pipeline state DB against the freshly-written report. Rows whose
    # filepath is no longer in the report are stale — file was renamed, moved,
    # or deleted. We split the cleanup by status:
    #
    #   pending / error      always drop. Phantom queue items poison the queue
    #                        builder until restart and have no audit value.
    #   done                 drop only if the scan was healthy (>= 1000 files
    #                        reconciled) AND a per-run cap is respected. The
    #                        encode_history table already preserves audit info,
    #                        so the pipeline_files row is mostly a path index.
    #                        The cap protects against a partial-scan accident
    #                        wiping thousands of done rows in one go.
    #   flagged_* / active   never touched. FLAGGED_* is the user's review
    #                        queue (FLAGGED_CORRUPT, FLAGGED_FOREIGN_AUDIO,
    #                        etc.) and ACTIVE rows belong to a running pipeline
    #                        whose state we must not disrupt.
    _DONE_DROP_CAP = 1000  # hard cap per scan — if more than this look stale, log and stop
    _MIN_HEALTHY_SCAN = 1000  # require this many files in current_paths before any DONE drops

    try:
        import sqlite3 as _sqlite3

        from paths import PIPELINE_STATE_DB
        from tools.report_lock import read_report as _read_report

        if os.path.exists(PIPELINE_STATE_DB):
            current_paths = {r["filepath"] for r in results} | {
                e.get("filepath", "") for e in (_read_report().get("files") or [])
            }
            _conn = _sqlite3.connect(PIPELINE_STATE_DB)
            _cur = _conn.cursor()

            _cur.execute("SELECT filepath FROM pipeline_files WHERE status IN ('pending', 'error')")
            stale_pending = [fp for fp, in _cur.fetchall() if fp not in current_paths]
            for fp in stale_pending:
                _cur.execute("DELETE FROM pipeline_files WHERE filepath = ?", (fp,))

            stale_done_count = 0
            stale_done_capped = False
            if len(current_paths) >= _MIN_HEALTHY_SCAN:
                _cur.execute("SELECT filepath FROM pipeline_files WHERE status = 'done'")
                stale_done = [fp for fp, in _cur.fetchall() if fp not in current_paths]
                if len(stale_done) > _DONE_DROP_CAP:
                    stale_done_capped = True
                    stale_done = stale_done[:_DONE_DROP_CAP]
                for fp in stale_done:
                    _cur.execute("DELETE FROM pipeline_files WHERE filepath = ?", (fp,))
                stale_done_count = len(stale_done)
            _conn.commit()
            _conn.close()

            if stale_pending:
                print(f"  Reconciled state DB: dropped {len(stale_pending)} stale pending/error rows")
            if stale_done_count:
                msg = f"  Reconciled state DB: dropped {stale_done_count} stale done rows (path missing on disk)"
                if stale_done_capped:
                    msg += f" — capped at {_DONE_DROP_CAP}; rerun scanner to clear the rest"
                print(msg)
    except Exception as _e:
        print(f"  WARNING: state DB reconciliation skipped: {_e}")

    print(f"\n{'=' * 60}")
    print("Scan complete!")
    print(f"  Total files: {summary['total_files']}")
    print(f"  Total size:  {summary['total_size_tb']} TB ({summary['total_size_gb']} GB)")
    print(f"  Movies:      {summary['movies']['count']} ({summary['movies']['size_gb']} GB)")
    print(f"  Series:      {summary['series']['count']} ({summary['series']['size_gb']} GB)")
    print(f"  Errors:      {summary['errors']}")
    print(f"  Report:      {output_path}")
    print(f"{'=' * 60}")

    # Non-English CSV (uses the already-scanned report, no extra ffprobe calls).
    # Read back under the lock so we see the merged result, not the pre-patch state.
    if args.non_english_csv or args.missing_subs_csv:
        from tools.report_lock import read_report

        report = read_report()
        if args.non_english_csv:
            count = write_non_english_csv(report, args.non_english_csv)
            print(f"\nNon-English audio: {count} files written to {args.non_english_csv}")
        if args.missing_subs_csv:
            count = write_missing_subs_csv(report, args.missing_subs_csv)
            print(f"\nMissing subtitles: {count} files written to {args.missing_subs_csv}")


if __name__ == "__main__":
    main()
