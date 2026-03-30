"""
Language Detection for Undetermined Subtitle and Audio Tracks
=============================================================
Scans media_report.json for tracks with undetermined (und/unk/empty) language tags,
detects language from subtitle text (langdetect) or infers from context, and optionally
writes the detected language back to the file using mkvpropedit (MKVToolNix).

Usage:
    uv run python -m tools.detect_languages                    # detect only
    uv run python -m tools.detect_languages --apply            # detect + write back to files
    uv run python -m tools.detect_languages --file "path.mkv"  # single file
    uv run python -m tools.detect_languages --min-confidence 0.85

Output: F:\\AV1_Staging\\control\\language_detections.json

Requires:
    pip: langdetect (text subtitle detection)
    system: mkvpropedit (MKVToolNix) for --apply   [winget install MKVToolNix.MKVToolNix]
    optional: faster-whisper for audio detection   [uv add faster-whisper]
"""

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

from paths import MEDIA_REPORT, STAGING_DIR

TEXT_SUB_CODECS = {"subrip", "srt", "ass", "ssa", "webvtt", "mov_text", "text", "microdvd"}
BITMAP_SUB_CODECS = {"dvd_subtitle", "hdmv_pgs_subtitle", "dvbsub", "xsub", "pgssub"}
UND_LANGS = {"und", "unk", ""}

# ISO 639-1 (2-letter) → ISO 639-2/B (3-letter) — what MKV/mkvpropedit expects
_ISO1_TO_ISO2 = {
    "af": "afr", "ar": "ara", "az": "aze", "be": "bel", "bg": "bul",
    "bn": "ben", "bs": "bos", "ca": "cat", "cs": "ces", "cy": "wel",
    "da": "dan", "de": "deu", "el": "ell", "en": "eng", "eo": "epo",
    "es": "spa", "et": "est", "eu": "baq", "fa": "per", "fi": "fin",
    "fr": "fra", "ga": "gle", "gl": "glg", "gu": "guj", "he": "heb",
    "hi": "hin", "hr": "hrv", "hu": "hun", "hy": "arm", "id": "ind",
    "is": "ice", "it": "ita", "ja": "jpn", "ka": "geo", "kk": "kaz",
    "km": "khm", "kn": "kan", "ko": "kor", "lt": "lit", "lv": "lav",
    "mk": "mac", "ml": "mal", "mn": "mon", "mr": "mar", "ms": "may",
    "mt": "mlt", "my": "bur", "nb": "nob", "ne": "nep", "nl": "dut",
    "no": "nor", "pa": "pan", "pl": "pol", "pt": "por", "ro": "ron",
    "ru": "rus", "sk": "slk", "sl": "slv", "so": "som", "sq": "alb",
    "sr": "srp", "sv": "swe", "sw": "swa", "ta": "tam", "te": "tel",
    "th": "tha", "tl": "tgl", "tr": "tur", "uk": "ukr", "ur": "urd",
    "uz": "uzb", "vi": "vie", "zh": "zho", "zh-cn": "chi", "zh-tw": "chi",
    # already 3-letter pass-throughs (from heuristic inference copying media_report codes)
    "eng": "eng", "fre": "fra", "ger": "deu", "chi": "chi", "spa": "spa",
    "por": "por", "ita": "ita", "jpn": "jpn", "kor": "kor", "rus": "rus",
    "ara": "ara", "dut": "dut", "swe": "swe", "nor": "nor", "dan": "dan",
    "fin": "fin", "pol": "pol", "hun": "hun", "ces": "ces", "ron": "ron",
    "tur": "tur", "ell": "ell", "heb": "heb", "hin": "hin", "tha": "tha",
    "vie": "vie", "ind": "ind", "hrv": "hrv", "ukr": "ukr", "slk": "slk",
    "slv": "slv", "bul": "bul", "srp": "srp", "nob": "nob",
}


def to_iso2(lang: str) -> str:
    """Normalise any detected language code to ISO 639-2 for mkvpropedit."""
    return _ISO1_TO_ISO2.get(lang.lower(), lang.lower())

RESULTS_PATH = STAGING_DIR / "control" / "language_detections.json"


# ---------------------------------------------------------------------------
# Subtitle text extraction
# ---------------------------------------------------------------------------

def extract_subtitle_text(filepath: str, sub_stream_index: int, max_chars: int = 4000) -> Optional[str]:
    """Extract plain text from a subtitle stream via ffmpeg.

    sub_stream_index is 0-based across ALL subtitle streams in the file
    (matches ffmpeg -map 0:s:N counting).  Use -t 300 to cap reading so
    streams near the end of large files don't require a full demux.
    Returns stripped text or None on failure.
    """
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", filepath,
        "-map", f"0:s:{sub_stream_index}",
        "-t", "300",   # first 5 minutes is enough for language detection
        "-f", "srt",
        "pipe:1",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=90)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None
    if result.returncode != 0 and not result.stdout:
        return None

    raw = result.stdout.decode("utf-8", errors="replace")
    # Strip SRT timing lines and sequence numbers
    raw = re.sub(r"^\d+\s*$", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"\d{2}:\d{2}:\d{2}[.,]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[.,]\d{3}", "", raw)
    # Strip HTML/ASS tags
    raw = re.sub(r"<[^>]+>", "", raw)
    raw = re.sub(r"\{[^}]+\}", "", raw)
    # Collapse whitespace
    raw = " ".join(raw.split())
    return raw[:max_chars] if raw.strip() else None


# ---------------------------------------------------------------------------
# Language detection
# ---------------------------------------------------------------------------

def _cjk_language_from_script(text: str) -> Optional[tuple[str, float]]:
    """Reliably identify CJK languages using Unicode block proportions.

    langdetect regularly confuses Chinese/Japanese/Korean because their character
    sets overlap.  Unicode ranges are definitive for Hangul (Korean) and kana (Japanese).

    Returns (lang_code, confidence) or None if not predominantly CJK.
    """
    total = len(text)
    if total == 0:
        return None

    hangul = sum(1 for c in text if "\uAC00" <= c <= "\uD7AF" or "\u1100" <= c <= "\u11FF")
    hiragana = sum(1 for c in text if "\u3040" <= c <= "\u309F")
    katakana = sum(1 for c in text if "\u30A0" <= c <= "\u30FF")
    cjk_unified = sum(1 for c in text if "\u4E00" <= c <= "\u9FFF" or "\u3400" <= c <= "\u4DBF")
    kana = hiragana + katakana

    # Korean: significant Hangul, no/minimal kana
    if hangul / total > 0.05 and kana / max(hangul, 1) < 0.1:
        return "ko", min(0.95, 0.70 + hangul / total)

    # Japanese: significant kana (Hiragana especially is unique to Japanese)
    if hiragana / total > 0.03:
        return "ja", min(0.95, 0.70 + hiragana / total * 5)

    # Chinese: predominantly CJK ideographs, no Hangul or kana
    if cjk_unified / total > 0.10 and hangul / total < 0.02 and kana / total < 0.02:
        return "zh", min(0.92, 0.70 + cjk_unified / total)

    return None


def detect_language(text: str) -> tuple[str, float]:
    """Return (iso639-1 code, confidence) from text. Falls back to ('und', 0.0).

    Uses Unicode-range analysis first for CJK scripts (more reliable than
    langdetect for Korean/Chinese/Japanese), then falls back to langdetect.
    """
    if not text.strip():
        return "und", 0.0

    # CJK check first — langdetect regularly conflates these scripts
    cjk = _cjk_language_from_script(text)
    if cjk:
        return cjk

    try:
        from langdetect import detect_langs
        results = detect_langs(text)
        if results:
            best = results[0]
            return best.lang, round(best.prob, 3)
    except Exception:
        pass
    return "und", 0.0


def infer_audio_language(file_entry: dict, audio_idx: int) -> tuple[Optional[str], str]:
    """Heuristically infer the language of an undetermined audio track.

    Returns (language_code_or_None, reason).
    Only returns a result when confident:
    - Single audio track on a file where all subtitles are one language
    - Track title contains a known language name
    """
    streams = file_entry.get("audio_streams", [])
    subs = file_entry.get("subtitle_streams", [])

    # Title-based hints
    track = streams[audio_idx]
    title = (track.get("title") or "").lower()
    TITLE_HINTS = {
        "english": "en", "eng": "en", "french": "fr", "français": "fr",
        "spanish": "es", "español": "es", "german": "de", "deutsch": "de",
        "italian": "it", "italiano": "it", "japanese": "ja", "chinese": "zh",
        "portuguese": "pt", "russian": "ru", "korean": "ko", "dutch": "nl",
        "arabic": "ar", "hindi": "hi", "swedish": "sv", "norwegian": "no",
        "danish": "da", "finnish": "fi", "polish": "pl", "czech": "cs",
        "hungarian": "hu", "romanian": "ro", "turkish": "tr", "greek": "el",
        "hebrew": "he", "thai": "th", "vietnamese": "vi",
    }
    for hint, code in TITLE_HINTS.items():
        if hint in title:
            return code, f"track title contains '{hint}'"

    # If only one audio track and all identified subtitle tracks agree on a language
    if len(streams) == 1:
        sub_langs = {
            s.get("language", "und").lower()
            for s in subs
            if s.get("language", "und").lower() not in UND_LANGS
        }
        if len(sub_langs) == 1:
            lang = next(iter(sub_langs))
            return lang, f"sole audio track, all subtitles are '{lang}'"

    # All other identified audio tracks are the same language
    known = [
        s.get("language", "und").lower()
        for i, s in enumerate(streams)
        if i != audio_idx and s.get("language", "und").lower() not in UND_LANGS
    ]
    if known and len(set(known)) == 1:
        lang = known[0]
        return lang, f"all other audio tracks are '{lang}'"

    return None, "insufficient context"


# ---------------------------------------------------------------------------
# Per-file processing
# ---------------------------------------------------------------------------

def process_file(file_entry: dict) -> list[dict]:
    """Return a list of detection results for all undetermined tracks in a file."""
    filepath = file_entry["filepath"]
    results = []

    # --- Subtitle tracks ---
    # sub_all_idx == the ffmpeg -map 0:s:N index (counts ALL subtitle streams)
    for sub_all_idx, stream in enumerate(file_entry.get("subtitle_streams", [])):
        lang = (stream.get("language") or "und").lower().strip()
        codec = stream.get("codec", "").lower()
        if lang not in UND_LANGS:
            continue

        if codec in TEXT_SUB_CODECS:
            text = extract_subtitle_text(filepath, sub_all_idx)
            if text:
                detected, confidence = detect_language(text)
                results.append({
                    "filepath": filepath,
                    "track_type": "subtitle",
                    "stream_index": sub_all_idx,
                    "codec": codec,
                    "detected_language": detected,
                    "confidence": confidence,
                    "method": "text_extraction",
                    "chars_sampled": len(text),
                })
            else:
                results.append({
                    "filepath": filepath,
                    "track_type": "subtitle",
                    "stream_index": sub_all_idx,
                    "codec": codec,
                    "detected_language": None,
                    "confidence": 0.0,
                    "method": "text_extraction_failed",
                    "chars_sampled": 0,
                })
        elif codec in BITMAP_SUB_CODECS:
            results.append({
                "filepath": filepath,
                "track_type": "subtitle",
                "stream_index": sub_all_idx,
                "codec": codec,
                "detected_language": None,
                "confidence": 0.0,
                "method": "bitmap_skipped",
                "chars_sampled": 0,
            })

    # --- Audio tracks ---
    for audio_idx, stream in enumerate(file_entry.get("audio_streams", [])):
        lang = (stream.get("language") or "und").lower().strip()
        if lang not in UND_LANGS:
            continue
        detected, reason = infer_audio_language(file_entry, audio_idx)
        results.append({
            "filepath": filepath,
            "track_type": "audio",
            "stream_index": audio_idx,
            "codec": stream.get("codec", ""),
            "detected_language": detected,
            "confidence": 0.9 if detected else 0.0,  # heuristic — no probability available
            "method": "heuristic",
            "reason": reason,
        })

    return results


# ---------------------------------------------------------------------------
# Applying detections back to files
# ---------------------------------------------------------------------------

def apply_detection(result: dict, min_confidence: float = 0.80) -> bool:
    """Write a detected language tag back to a file using mkvpropedit.

    Only acts on MKV files. Requires mkvpropedit (MKVToolNix) on PATH.
    Returns True on success.
    """
    if not result.get("detected_language"):
        return False
    if result.get("confidence", 0) < min_confidence:
        return False
    if result.get("method") in ("bitmap_skipped", "text_extraction_failed"):
        return False

    filepath = result["filepath"]
    if not filepath.lower().endswith(".mkv"):
        logging.warning(f"  Skipping non-MKV (mkvpropedit only supports MKV): {Path(filepath).name}")
        return False

    if not shutil.which("mkvpropedit"):
        logging.error("mkvpropedit not found. Install MKVToolNix: winget install MKVToolNix.MKVToolNix")
        return False

    track_type = result["track_type"]
    stream_index = result["stream_index"]
    language = result["detected_language"]

    # mkvpropedit uses 1-based track specifiers per type: track:a1, track:s1, etc.
    track_spec = f"track:{'a' if track_type == 'audio' else 's'}{stream_index + 1}"
    language = to_iso2(language)  # mkvpropedit requires ISO 639-2

    cmd = [
        "mkvpropedit", filepath,
        "--edit", track_spec,
        "--set", f"language={language}",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if proc.returncode == 0:
            logging.info(f"  Applied {language} to {track_type} track {stream_index + 1}: {Path(filepath).name}")
            return True
        else:
            logging.error(f"  mkvpropedit failed: {proc.stderr.strip()}")
            return False
    except subprocess.TimeoutExpired:
        logging.error(f"  mkvpropedit timed out for {Path(filepath).name}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Detect languages for undetermined subtitle/audio tracks")
    parser.add_argument("--apply", action="store_true",
                        help="Write detected languages back to MKV files using mkvpropedit")
    parser.add_argument("--min-confidence", type=float, default=0.80,
                        help="Minimum confidence to include/apply a detection (default: 0.80)")
    parser.add_argument("--file", type=str,
                        help="Process a single file instead of the full library")
    parser.add_argument("--workers", type=int, default=4,
                        help="Parallel workers for subtitle extraction (default: 4)")
    parser.add_argument("--output", type=str, default=str(RESULTS_PATH),
                        help="Output JSON path")
    args = parser.parse_args()

    # Load report
    try:
        with open(MEDIA_REPORT, encoding="utf-8") as f:
            report = json.load(f)
    except FileNotFoundError:
        logging.error(f"media_report.json not found at {MEDIA_REPORT}")
        sys.exit(1)

    all_files = report.get("files", [])

    if args.file:
        needle = os.path.normpath(args.file).lower()
        all_files = [e for e in all_files if os.path.normpath(e["filepath"]).lower() == needle]
        if not all_files:
            logging.error(f"File not found in report: {args.file}")
            sys.exit(1)

    # Filter to files with at least one undetermined track
    to_process = []
    for entry in all_files:
        has_und_sub = any(
            (s.get("language") or "und").lower().strip() in UND_LANGS
            for s in entry.get("subtitle_streams", [])
        )
        has_und_audio = any(
            (s.get("language") or "und").lower().strip() in UND_LANGS
            for s in entry.get("audio_streams", [])
        )
        if has_und_sub or has_und_audio:
            to_process.append(entry)

    logging.info(f"Files with undetermined tracks: {len(to_process)}")
    logging.info(f"Min confidence: {args.min_confidence}")
    if args.apply:
        if shutil.which("mkvpropedit"):
            logging.info("mkvpropedit: found — will apply detections")
        else:
            logging.warning("mkvpropedit: NOT FOUND — detections will be saved but not applied")
            logging.warning("  Install with: winget install MKVToolNix.MKVToolNix")

    # Process files in parallel (subtitle extraction is I/O bound)
    all_results = []
    completed = 0
    total = len(to_process)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_file, entry): entry for entry in to_process}
        for future in as_completed(futures):
            completed += 1
            if completed % 50 == 0 or completed == total:
                logging.info(f"  Progress: {completed}/{total}")
            try:
                results = future.result()
                all_results.extend(results)
            except Exception as e:
                entry = futures[future]
                logging.warning(f"  Error processing {entry.get('filename', '?')}: {e}")

    # Filter to usable detections
    detected = [r for r in all_results if r.get("detected_language") and r.get("confidence", 0) >= args.min_confidence]
    low_confidence = [r for r in all_results if r.get("detected_language") and r.get("confidence", 0) < args.min_confidence]
    failed = [r for r in all_results if not r.get("detected_language")]

    logging.info(f"\nResults:")
    logging.info(f"  Detected (>= {args.min_confidence}): {len(detected)}")
    logging.info(f"  Low confidence:                      {len(low_confidence)}")
    logging.info(f"  Failed/skipped:                      {len(failed)}")

    # Language distribution
    from collections import Counter
    lang_counts = Counter(r["detected_language"] for r in detected)
    logging.info(f"\nDetected language distribution:")
    for lang, count in lang_counts.most_common(15):
        logging.info(f"  {lang}: {count}")

    # Save results
    output = {
        "generated": datetime.now().isoformat(),
        "min_confidence": args.min_confidence,
        "total_processed_files": len(to_process),
        "total_tracks_checked": len(all_results),
        "detected": detected,
        "low_confidence": low_confidence,
        "failed": failed,
    }
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    logging.info(f"\nResults saved to {args.output}")

    # Apply if requested
    if args.apply:
        logging.info(f"\nApplying {len(detected)} detections...")
        applied = 0
        for result in detected:
            if apply_detection(result, args.min_confidence):
                applied += 1
        logging.info(f"Applied: {applied}/{len(detected)}")


if __name__ == "__main__":
    main()
