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

def _infer_pgs_from_siblings(
    file_entry: dict,
    detected_text_langs: dict[int, str],
) -> dict[int, tuple[str, str]]:
    """Infer PGS/DVD subtitle languages from sibling text subs.

    Uses two strategies:
    1. Positional mapping: if N text subs with known languages and N bitmap subs,
       map 1:1 by relative order (e.g. text subs 0-5 are [eng,fre,spa,...] →
       bitmap subs 6-11 follow the same order).
    2. Unanimous: if all known subs (pre-existing + detected) agree on one language,
       assign it to all bitmap subs.

    Args:
        file_entry: the file dict from media_report
        detected_text_langs: {sub_all_idx: lang_code} from text extraction pass

    Returns:
        {sub_all_idx: (lang_code, reason)} for each inferred bitmap sub.
    """
    subs = file_entry.get("subtitle_streams", [])
    inferred: dict[int, tuple[str, str]] = {}

    # Build complete known-language map: pre-existing + just-detected
    known_langs: dict[int, str] = {}
    for idx, s in enumerate(subs):
        existing = (s.get("language") or "und").lower().strip()
        if existing not in UND_LANGS:
            known_langs[idx] = existing
    known_langs.update(detected_text_langs)

    # Identify undetermined bitmap subs
    und_bitmap_idxs = []
    for idx, s in enumerate(subs):
        codec = (s.get("codec") or "").lower()
        lang = (s.get("language") or "und").lower().strip()
        if lang in UND_LANGS and codec in BITMAP_SUB_CODECS:
            und_bitmap_idxs.append(idx)

    if not und_bitmap_idxs or not known_langs:
        return inferred

    # Strategy 1: all known subs agree → assign that language to all bitmap subs
    unique_langs = set(known_langs.values())
    if len(unique_langs) == 1:
        lang = next(iter(unique_langs))
        for idx in und_bitmap_idxs:
            inferred[idx] = (lang, f"all {len(known_langs)} known subs are '{lang}'")
        return inferred

    # Strategy 2: positional mapping — known text subs and bitmap subs in matching count
    known_text_idxs = sorted(
        idx for idx in known_langs
        if (subs[idx].get("codec") or "").lower() in TEXT_SUB_CODECS
    )
    if len(known_text_idxs) == len(und_bitmap_idxs):
        for text_idx, bmp_idx in zip(known_text_idxs, und_bitmap_idxs):
            lang = known_langs[text_idx]
            inferred[bmp_idx] = (lang, f"positional match with text sub {text_idx} ('{lang}')")
        return inferred

    return inferred


def _infer_audio_from_sub_majority(
    file_entry: dict,
    audio_idx: int,
    detected_text_langs: dict[int, str],
) -> tuple[Optional[str], str]:
    """Infer audio language from majority subtitle language.

    If ≥70% of identified subtitle tracks share the same language (pre-existing +
    detected), assign that to undetermined audio. Lower confidence (0.8) because
    this is a weaker signal than the existing heuristics.
    """
    subs = file_entry.get("subtitle_streams", [])
    known_langs: list[str] = []
    for idx, s in enumerate(subs):
        lang = (s.get("language") or "und").lower().strip()
        if lang not in UND_LANGS:
            known_langs.append(lang)
        elif idx in detected_text_langs:
            known_langs.append(detected_text_langs[idx])

    if len(known_langs) < 2:
        return None, "insufficient context"

    from collections import Counter
    counts = Counter(known_langs)
    top_lang, top_count = counts.most_common(1)[0]
    ratio = top_count / len(known_langs)
    if ratio >= 0.7:
        return top_lang, f"subtitle majority {top_count}/{len(known_langs)} are '{top_lang}'"
    return None, "insufficient context"


def process_file(file_entry: dict) -> list[dict]:
    """Return a list of detection results for all undetermined tracks in a file."""
    filepath = file_entry["filepath"]
    results = []

    # --- Pass 1: Text subtitle extraction ---
    detected_text_langs: dict[int, str] = {}  # sub_all_idx → lang code
    text_results: list[dict] = []

    for sub_all_idx, stream in enumerate(file_entry.get("subtitle_streams", [])):
        lang = (stream.get("language") or "und").lower().strip()
        codec = stream.get("codec", "").lower()
        if lang not in UND_LANGS:
            continue

        if codec in TEXT_SUB_CODECS:
            text = extract_subtitle_text(filepath, sub_all_idx)
            if text:
                detected, confidence = detect_language(text)
                entry = {
                    "filepath": filepath,
                    "track_type": "subtitle",
                    "stream_index": sub_all_idx,
                    "codec": codec,
                    "detected_language": detected,
                    "confidence": confidence,
                    "method": "text_extraction",
                    "chars_sampled": len(text),
                }
                text_results.append(entry)
                if detected and detected != "und" and confidence >= 0.5:
                    detected_text_langs[sub_all_idx] = detected
            else:
                text_results.append({
                    "filepath": filepath,
                    "track_type": "subtitle",
                    "stream_index": sub_all_idx,
                    "codec": codec,
                    "detected_language": None,
                    "confidence": 0.0,
                    "method": "text_extraction_failed",
                    "chars_sampled": 0,
                })

    results.extend(text_results)

    # --- Pass 2: Infer PGS/DVD subs from sibling text subs ---
    pgs_inferred = _infer_pgs_from_siblings(file_entry, detected_text_langs)
    for sub_all_idx, stream in enumerate(file_entry.get("subtitle_streams", [])):
        lang = (stream.get("language") or "und").lower().strip()
        codec = stream.get("codec", "").lower()
        if lang not in UND_LANGS or codec not in BITMAP_SUB_CODECS:
            continue

        if sub_all_idx in pgs_inferred:
            inferred_lang, reason = pgs_inferred[sub_all_idx]
            results.append({
                "filepath": filepath,
                "track_type": "subtitle",
                "stream_index": sub_all_idx,
                "codec": codec,
                "detected_language": inferred_lang,
                "confidence": 0.85,
                "method": "sibling_inference",
                "reason": reason,
            })
        else:
            results.append({
                "filepath": filepath,
                "track_type": "subtitle",
                "stream_index": sub_all_idx,
                "codec": codec,
                "detected_language": None,
                "confidence": 0.0,
                "method": "bitmap_no_match",
                "chars_sampled": 0,
            })

    # --- Pass 3: Audio tracks (existing heuristics + subtitle majority fallback) ---
    for audio_idx, stream in enumerate(file_entry.get("audio_streams", [])):
        lang = (stream.get("language") or "und").lower().strip()
        if lang not in UND_LANGS:
            continue
        detected, reason = infer_audio_language(file_entry, audio_idx)
        if not detected:
            detected, reason = _infer_audio_from_sub_majority(
                file_entry, audio_idx, detected_text_langs,
            )
        results.append({
            "filepath": filepath,
            "track_type": "audio",
            "stream_index": audio_idx,
            "codec": stream.get("codec", ""),
            "detected_language": detected,
            "confidence": 0.9 if detected and "majority" not in reason else (0.8 if detected else 0.0),
            "method": "heuristic",
            "reason": reason,
        })

    return results


# ---------------------------------------------------------------------------
# Applying detections back to files
# ---------------------------------------------------------------------------

def _apply_file_mkvpropedit(filepath: str, detections: list[dict]) -> tuple[int, int]:
    """Apply all language detections for a file in a single mkvpropedit call.

    Batches every track edit into one process invocation (fast, in-place).
    Returns (applied_count, failed_count).
    """
    args: list[str] = ["mkvpropedit", filepath]
    for det in detections:
        track_type = det["track_type"]
        stream_index = det["stream_index"]
        language = to_iso2(det["detected_language"])
        # mkvpropedit 1-based track specifiers per type: track:a1, track:s1, etc.
        track_spec = f"track:{'a' if track_type == 'audio' else 's'}{stream_index + 1}"
        args += ["--edit", track_spec, "--set", f"language={language}"]

    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=60)
        if proc.returncode == 0:
            for det in detections:
                logging.info(
                    f"  Applied {to_iso2(det['detected_language'])} to {det['track_type']} "
                    f"track {det['stream_index'] + 1}: {Path(filepath).name}"
                )
            return len(detections), 0
        else:
            logging.error(f"  mkvpropedit failed for {Path(filepath).name}: {proc.stderr.strip()}")
            return 0, len(detections)
    except subprocess.TimeoutExpired:
        logging.error(f"  mkvpropedit timed out for {Path(filepath).name}")
        return 0, len(detections)


def _apply_file_ffmpeg(filepath: str, detections: list[dict]) -> tuple[int, int]:
    """Apply all language detections for a file in a single ffmpeg -c copy call.

    Writes to a temp file then replaces the original. Slower than mkvpropedit
    but requires no extra install beyond ffmpeg.
    Returns (applied_count, failed_count).
    """
    tmp_path = filepath + ".langfix_tmp.mkv"
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", filepath, "-c", "copy"]
    for det in detections:
        track_type = det["track_type"]
        stream_index = det["stream_index"]
        language = to_iso2(det["detected_language"])
        # ffmpeg metadata stream specifiers: s:a:N for audio, s:s:N for subtitle
        stream_type = "a" if track_type == "audio" else "s"
        cmd += [f"-metadata:s:{stream_type}:{stream_index}", f"language={language}"]
    cmd.append(tmp_path)

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if proc.returncode == 0:
            os.replace(tmp_path, filepath)
            for det in detections:
                logging.info(
                    f"  Applied {to_iso2(det['detected_language'])} to {det['track_type']} "
                    f"track {det['stream_index'] + 1}: {Path(filepath).name}"
                )
            return len(detections), 0
        else:
            logging.error(f"  ffmpeg failed for {Path(filepath).name}: {proc.stderr.strip()}")
            return 0, len(detections)
    except subprocess.TimeoutExpired:
        logging.error(f"  ffmpeg timed out for {Path(filepath).name}")
        return 0, len(detections)
    finally:
        # Clean up temp file on any failure path
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def apply_detections_for_file(
    filepath: str,
    detections: list[dict],
    min_confidence: float = 0.80,
) -> tuple[int, int]:
    """Apply language detections for a single file.

    Filters by min_confidence and skips failed/bitmap detections, then tries
    mkvpropedit (preferred — fast, in-place) for MKV files if available,
    falling back to ffmpeg -c copy (slower, no extra install required).
    Returns (applied_count, failed_count).
    """
    # Filter to actionable detections
    actionable = [
        d for d in detections
        if d.get("detected_language")
        and d.get("confidence", 0) >= min_confidence
        and d.get("method") not in ("bitmap_skipped", "text_extraction_failed")
    ]
    if not actionable:
        return 0, 0

    is_mkv = filepath.lower().endswith(".mkv")
    use_mkvpropedit = is_mkv and shutil.which("mkvpropedit") is not None

    if use_mkvpropedit:
        return _apply_file_mkvpropedit(filepath, actionable)
    else:
        if not is_mkv:
            logging.warning(f"  Non-MKV file — using ffmpeg remux: {Path(filepath).name}")
        else:
            logging.info(f"  mkvpropedit not found — falling back to ffmpeg: {Path(filepath).name}")
        return _apply_file_ffmpeg(filepath, actionable)


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
            logging.info("mkvpropedit: found — will apply via mkvpropedit")
        else:
            logging.info("mkvpropedit: not found — will apply via ffmpeg (slower)")
            logging.info("  To use mkvpropedit: winget install MKVToolNix.MKVToolNix")

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

    # Apply if requested — group detections by filepath and process one file at a time
    if args.apply:
        logging.info(f"\nApplying {len(detected)} detections across files...")
        by_file: dict[str, list[dict]] = {}
        for det in detected:
            by_file.setdefault(det["filepath"], []).append(det)

        total_applied = 0
        total_failed = 0
        for file_num, (fp, file_dets) in enumerate(by_file.items(), 1):
            logging.info(f"  [{file_num}/{len(by_file)}] {Path(fp).name} ({len(file_dets)} track(s))")
            applied, failed = apply_detections_for_file(fp, file_dets, args.min_confidence)
            total_applied += applied
            total_failed += failed

        logging.info(f"Applied: {total_applied}/{len(detected)}  Failed: {total_failed}")


if __name__ == "__main__":
    main()
