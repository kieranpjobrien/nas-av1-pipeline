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
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

# Common install locations for mkvpropedit (may not be on PATH)
_MKVPROPEDIT_SEARCH = [
    r"C:\Program Files\MKVToolNix\mkvpropedit.exe",
    r"C:\Program Files (x86)\MKVToolNix\mkvpropedit.exe",
]


def _find_mkvpropedit() -> Optional[str]:
    """Find mkvpropedit binary — check PATH first, then common install dirs."""
    found = shutil.which("mkvpropedit")
    if found:
        return found
    for path in _MKVPROPEDIT_SEARCH:
        if os.path.isfile(path):
            return path
    return None

from paths import MEDIA_REPORT, STAGING_DIR

TEXT_SUB_CODECS = {"subrip", "srt", "ass", "ssa", "webvtt", "mov_text", "text", "microdvd"}
BITMAP_SUB_CODECS = {"dvd_subtitle", "hdmv_pgs_subtitle", "dvbsub", "xsub", "pgssub"}
UND_LANGS = {"und", "unk", ""}

# Tesseract common install locations
_TESSERACT_SEARCH = [
    r"C:\Program Files\Tesseract-OCR\tesseract.exe",
    r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
]


def _find_tesseract() -> Optional[str]:
    """Find tesseract binary — check PATH first, then common install dirs."""
    found = shutil.which("tesseract")
    if found:
        return found
    for path in _TESSERACT_SEARCH:
        if os.path.isfile(path):
            return path
    return None

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


def extract_bitmap_subtitle_text(
    filepath: str,
    sub_stream_index: int,
    sample_frames: int = 10,
    max_chars: int = 4000,
) -> Optional[str]:
    """Extract text from a bitmap subtitle stream (PGS/DVD) via ffmpeg + Tesseract OCR.

    Extracts the first N subtitle frames as PNG images, runs Tesseract on each,
    and aggregates the text for language detection.

    Returns stripped text or None on failure / if Tesseract is not installed.
    """
    tesseract = _find_tesseract()
    if not tesseract:
        return None

    tmp_dir = os.path.join(str(STAGING_DIR), "ocr_tmp", f"{os.getpid()}_{sub_stream_index}")
    os.makedirs(tmp_dir, exist_ok=True)

    try:
        # Extract subtitle frames as images
        pattern = os.path.join(tmp_dir, "sub_%04d.png")
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-i", filepath,
            "-map", f"0:s:{sub_stream_index}",
            "-t", "300",   # first 5 minutes
            "-frames:v", str(sample_frames),
            pattern,
        ]
        try:
            subprocess.run(cmd, capture_output=True, timeout=120)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None

        # Find extracted images
        images = sorted(
            f for f in os.listdir(tmp_dir) if f.endswith(".png")
        )[:sample_frames]

        if not images:
            return None

        # OCR each image
        all_text = []
        for img_name in images:
            img_path = os.path.join(tmp_dir, img_name)
            try:
                ocr_cmd = [tesseract, img_path, "stdout", "--oem", "3", "--psm", "6"]
                result = subprocess.run(ocr_cmd, capture_output=True, text=True, timeout=30)
                if result.returncode == 0 and result.stdout.strip():
                    all_text.append(result.stdout.strip())
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue

        if not all_text:
            return None

        combined = " ".join(all_text)
        # Clean OCR artifacts
        combined = re.sub(r"[|_]{2,}", "", combined)
        combined = " ".join(combined.split())
        return combined[:max_chars] if combined.strip() else None

    finally:
        # Clean up temp images
        try:
            for f in os.listdir(tmp_dir):
                os.remove(os.path.join(tmp_dir, f))
            os.rmdir(tmp_dir)
        except OSError:
            pass


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
# Whisper-based audio language detection
# ---------------------------------------------------------------------------

_whisper_tiny = None
_whisper_small = None


def _get_whisper_model(size: str = "tiny"):
    """Lazy-load a faster-whisper model. Tiny for fast screening, small for confirmation."""
    global _whisper_tiny, _whisper_small
    ref = _whisper_tiny if size == "tiny" else _whisper_small
    if ref is not None:
        return ref

    try:
        from faster_whisper import WhisperModel
        model = WhisperModel(size, device="cuda", compute_type="float16")
        logging.info(f"Loaded faster-whisper model ({size}, cuda/float16)")
    except Exception as e:
        logging.warning(f"Failed to load whisper {size} on GPU, trying CPU: {e}")
        try:
            from faster_whisper import WhisperModel
            model = WhisperModel(size, device="cpu", compute_type="int8")
            logging.info(f"Loaded faster-whisper model ({size}, cpu/int8)")
        except Exception as e2:
            logging.error(f"Failed to load whisper model {size}: {e2}")
            return None

    if size == "tiny":
        _whisper_tiny = model
    else:
        _whisper_small = model
    return model


def _extract_all_audio_samples(
    filepath: str,
    audio_indices: list[int],
    duration_secs: float,
    sample_duration: int = 10,
) -> dict[int, list[str]]:
    """Extract audio samples for ALL tracks of a file in one ffmpeg call.

    Returns {audio_index: [wav_path, wav_path, wav_path], ...}.
    Single file open over SMB regardless of how many tracks.
    """
    tmp_dir = os.path.join(str(STAGING_DIR), "whisper_tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    base = f"{os.getpid()}"

    total = max(duration_secs, 120)
    offsets = [
        int(min(60, total * 0.05)),
        int(total * 0.3),
        int(total * 0.6),
    ]

    result: dict[int, list[str]] = {}
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", filepath]

    for aidx in audio_indices:
        paths = []
        for si, offset in enumerate(offsets):
            wav_path = os.path.join(tmp_dir, f"{base}_a{aidx}_s{si}.wav")
            paths.append(wav_path)
            cmd.extend([
                "-ss", str(offset),
                "-t", str(sample_duration),
                "-map", f"0:a:{aidx}",
                "-ac", "1", "-ar", "16000",
                "-y", wav_path,
            ])
        result[aidx] = paths

    try:
        subprocess.run(cmd, capture_output=True, timeout=90)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # Filter to files that got created
    for aidx in list(result.keys()):
        result[aidx] = [p for p in result[aidx] if os.path.exists(p) and os.path.getsize(p) > 10_000]

    return result


def _whisper_detect_one(model, wav_path: str) -> tuple[Optional[str], float]:
    """Run whisper language detection on a single WAV sample."""
    try:
        segments, info = model.transcribe(wav_path, beam_size=1, best_of=1,
                                          language=None, without_timestamps=True)
        for _ in segments:
            break
        if info.language and info.language_probability > 0.3:
            return info.language, round(info.language_probability, 3)
    except Exception:
        pass
    return None, 0.0


def _majority_vote(detections: list[tuple[str, float]]) -> tuple[Optional[str], float]:
    """Majority vote across multiple whisper detections."""
    if not detections:
        return None, 0.0
    if len(detections) == 1:
        return detections[0]

    from collections import Counter
    lang_counts = Counter(lang for lang, _ in detections)
    majority_lang, majority_count = lang_counts.most_common(1)[0]

    if majority_count == len(detections):
        avg_prob = sum(p for _, p in detections) / len(detections)
        return majority_lang, round(avg_prob, 3)

    if majority_count > len(detections) / 2:
        avg_prob = sum(p for l, p in detections if l == majority_lang) / majority_count
        return majority_lang, round(avg_prob * 0.85, 3)

    best = max(detections, key=lambda x: x[1])
    return best[0], round(best[1] * 0.7, 3)


def detect_audio_language_whisper(
    filepath: str,
    audio_stream_index: int,
    duration_secs: float = 0,
) -> tuple[Optional[str], float]:
    """Detect language using tiny-first with small escalation.

    1. Extract 3 x 10s samples (one ffmpeg call)
    2. Run whisper-tiny on first sample — if confidence >= 0.8, done
    3. If low confidence, run tiny on remaining samples + majority vote
    4. If still < 0.8, escalate to whisper-small on first sample

    Returns (lang_code, confidence) or (None, 0.0) on failure.
    """
    samples = _extract_all_audio_samples(filepath, [audio_stream_index], duration_secs)
    wav_paths = samples.get(audio_stream_index, [])
    if not wav_paths:
        return None, 0.0

    try:
        # Step 1: tiny model on first sample (fast screening)
        tiny = _get_whisper_model("tiny")
        if not tiny:
            return None, 0.0

        lang, prob = _whisper_detect_one(tiny, wav_paths[0])
        if lang and prob >= 0.8:
            return lang, prob

        # Step 2: tiny on remaining samples, majority vote
        detections = [(lang, prob)] if lang else []
        for wav_path in wav_paths[1:]:
            l, p = _whisper_detect_one(tiny, wav_path)
            if l:
                detections.append((l, p))

        result_lang, result_conf = _majority_vote(detections)
        if result_lang and result_conf >= 0.8:
            return result_lang, result_conf

        # Step 3: escalate to small model on first sample for confirmation
        small = _get_whisper_model("small")
        if small:
            s_lang, s_prob = _whisper_detect_one(small, wav_paths[0])
            if s_lang and s_prob > result_conf:
                return s_lang, s_prob

        return result_lang or lang, result_conf or prob

    except Exception as e:
        logging.debug(f"Whisper detection failed for {os.path.basename(filepath)} a:{audio_stream_index}: {e}")
        return None, 0.0
    finally:
        for wav_path in wav_paths:
            try:
                os.remove(wav_path)
            except OSError:
                pass


def detect_audio_languages_for_file(
    filepath: str,
    audio_indices: list[int],
    duration_secs: float = 0,
) -> dict[int, tuple[Optional[str], float]]:
    """Detect languages for ALL audio tracks of a file efficiently.

    Batch-extracts all samples in one ffmpeg call, then runs whisper on each.
    Uses tiny model first, escalates to small only when needed.
    Returns {audio_index: (lang, confidence), ...}.
    """
    # Extract all tracks' samples in one file open
    logging.info(f"    Extracting {len(audio_indices)} tracks × 3 samples...")
    all_samples = _extract_all_audio_samples(filepath, audio_indices, duration_secs)
    extracted = sum(len(v) for v in all_samples.values())
    logging.info(f"    Extracted {extracted} samples")

    results: dict[int, tuple[Optional[str], float]] = {}
    tiny = _get_whisper_model("tiny")
    if not tiny:
        return {i: (None, 0.0) for i in audio_indices}

    for aidx in audio_indices:
        wav_paths = all_samples.get(aidx, [])
        if not wav_paths:
            results[aidx] = (None, 0.0)
            continue

        # Tiny on first sample
        lang, prob = _whisper_detect_one(tiny, wav_paths[0])
        if lang and prob >= 0.8:
            results[aidx] = (lang, prob)
            continue

        # Tiny on all samples, majority vote
        detections = [(lang, prob)] if lang else []
        for wp in wav_paths[1:]:
            l, p = _whisper_detect_one(tiny, wp)
            if l:
                detections.append((l, p))

        result_lang, result_conf = _majority_vote(detections)
        if result_lang and result_conf >= 0.8:
            results[aidx] = (result_lang, result_conf)
            continue

        # Escalate to small
        small = _get_whisper_model("small")
        if small:
            s_lang, s_prob = _whisper_detect_one(small, wav_paths[0])
            if s_lang and s_prob > (result_conf or 0):
                results[aidx] = (s_lang, s_prob)
                continue

        results[aidx] = (result_lang, result_conf or 0.0)

    # Clean up all temp files
    for wav_list in all_samples.values():
        for wp in wav_list:
            try:
                os.remove(wp)
            except OSError:
                pass

    return results


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


def process_file(
    file_entry: dict,
    use_whisper: bool = False,
    whisper_all: bool = False,
    audio_only: bool = False,
) -> list[dict]:
    """Return a list of detection results for all undetermined tracks in a file."""
    filepath = file_entry["filepath"]
    results = []

    # --- Pass 1: Text subtitle extraction (skip if audio_only) ---
    detected_text_langs: dict[int, str] = {}  # sub_all_idx → lang code
    text_results: list[dict] = []

    if not audio_only:
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

        # --- Pass 2: Bitmap subs — OCR first, then sibling inference fallback ---
        pgs_inferred = _infer_pgs_from_siblings(file_entry, detected_text_langs)
        for sub_all_idx, stream in enumerate(file_entry.get("subtitle_streams", [])):
            lang = (stream.get("language") or "und").lower().strip()
            codec = stream.get("codec", "").lower()
            if lang not in UND_LANGS or codec not in BITMAP_SUB_CODECS:
                continue

            # Try OCR first (most accurate for bitmap subs)
            ocr_text = extract_bitmap_subtitle_text(filepath, sub_all_idx)
            if ocr_text:
                detected, confidence = detect_language(ocr_text)
                if detected and detected != "und" and confidence >= 0.5:
                    detected_text_langs[sub_all_idx] = detected
                    results.append({
                        "filepath": filepath,
                        "track_type": "subtitle",
                        "stream_index": sub_all_idx,
                        "codec": codec,
                        "detected_language": detected,
                        "confidence": confidence,
                        "method": "ocr_extraction",
                        "chars_sampled": len(ocr_text),
                    })
                    continue

            # Fall back to sibling inference
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

    # --- Pass 3: Audio tracks (heuristics → subtitle majority → whisper) ---
    # Collect which tracks need whisper so we can batch-extract all at once
    audio_heuristics: dict[int, tuple] = {}  # idx → (detected, conf, method, reason)
    whisper_candidates: list[int] = []

    for audio_idx, stream in enumerate(file_entry.get("audio_streams", [])):
        lang = (stream.get("language") or "und").lower().strip()
        is_und = lang in UND_LANGS

        # Skip tracks already detected in a previous run
        if stream.get("detected_language") and stream.get("detection_method") == "whisper":
            continue

        if not is_und and not (use_whisper and whisper_all):
            continue

        detected, reason = None, "insufficient context"
        conf = 0.0

        if is_und:
            detected, reason = infer_audio_language(file_entry, audio_idx)
            if not detected:
                detected, reason = _infer_audio_from_sub_majority(
                    file_entry, audio_idx, detected_text_langs,
                )
            conf = 0.9 if detected and "majority" not in reason else (0.8 if detected else 0.0)

        audio_heuristics[audio_idx] = (detected, conf, "heuristic", reason)

        if use_whisper and (whisper_all or (is_und and not detected)):
            whisper_candidates.append(audio_idx)

    # Batch whisper: extract all tracks' samples in one ffmpeg call
    whisper_results: dict[int, tuple] = {}
    if whisper_candidates:
        whisper_results = detect_audio_languages_for_file(
            filepath, whisper_candidates, file_entry.get("duration_seconds", 0),
        )

    # Merge heuristic + whisper results
    audio_streams = file_entry.get("audio_streams", [])
    for audio_idx, (detected, conf, method, reason) in audio_heuristics.items():
        if audio_idx in whisper_results:
            w_lang, w_conf = whisper_results[audio_idx]
            if w_lang and w_conf > 0.5 and (not detected or w_conf > conf):
                detected = w_lang
                conf = w_conf
                method = "whisper"
                reason = f"whisper detection (prob={w_conf:.2f})"

        audio_codec = audio_streams[audio_idx].get("codec", "") if audio_idx < len(audio_streams) else ""
        if detected:
            results.append({
                "filepath": filepath,
                "track_type": "audio",
                "stream_index": audio_idx,
                "codec": audio_codec,
                "detected_language": detected,
                "confidence": conf,
                "method": method,
                "reason": reason,
            })
        else:
            lang = (audio_streams[audio_idx].get("language") or "und").lower() if audio_idx < len(audio_streams) else "und"
            if lang in UND_LANGS:
                results.append({
                    "filepath": filepath,
                    "track_type": "audio",
                    "stream_index": audio_idx,
                    "codec": audio_codec,
                    "detected_language": None,
                    "confidence": 0.0,
                    "method": method,
                    "reason": reason,
                })

    return results


# ---------------------------------------------------------------------------
# Enriching media report in-place
# ---------------------------------------------------------------------------

def enrich_report(
    report: dict,
    use_whisper: bool = False,
    whisper_all: bool = False,
    workers: int = 6,
    min_confidence: float = 0.80,
) -> dict:
    """Run language detection on all files and patch results into report entries.

    Modifies the report dict in-place — adds `detected_language`, `detection_confidence`,
    and `detection_method` fields to each audio_stream and subtitle_stream entry that
    has an undetermined language tag.

    Returns the modified report.
    """
    files = report.get("files", [])

    # Filter to files that need processing
    if whisper_all:
        # Whisper all: process every file with audio tracks
        to_process = [e for e in files if e.get("audio_streams")]
    else:
        # Normal: only files with undetermined tracks
        to_process = []
        for entry in files:
            has_und = any(
                (s.get("language") or "und").lower().strip() in UND_LANGS
                for streams in (entry.get("subtitle_streams", []), entry.get("audio_streams", []))
                for s in streams
            )
            if has_und:
                to_process.append(entry)

    if not to_process:
        logging.info("No tracks to process — skipping language detection")
        return report

    logging.info(f"Language detection: {len(to_process)} files with undetermined tracks")
    if _find_tesseract():
        logging.info(f"  Tesseract: found at {_find_tesseract()}")
    else:
        logging.info("  Tesseract: not found — bitmap OCR disabled")

    completed = 0
    total = len(to_process)
    detection_stats = {"detected": 0, "failed": 0}

    if use_whisper:
        logging.info("  Whisper: enabled — processing sequentially (GPU)")
        _get_whisper_model("tiny")
        for entry in to_process:
            completed += 1
            n_audio = len(entry.get("audio_streams", []))
            logging.info(f"  [{completed}/{total}] {entry.get('filename', '?')} ({n_audio} audio)")
            try:
                _apply_detections_to_entry(entry, use_whisper, whisper_all, min_confidence, detection_stats)
            except Exception as e:
                logging.warning(f"    Error: {e}")
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_file, entry): entry for entry in to_process}
            for future in as_completed(futures):
                completed += 1
                if completed % 50 == 0 or completed == total:
                    logging.info(f"  Language progress: {completed}/{total}")
                try:
                    entry = futures[future]
                    results = future.result()
                    _patch_entry_from_results(entry, results, min_confidence, detection_stats)
                except Exception as e:
                    logging.warning(f"  Language detection error: {e}")

    logging.info(f"  Language detection complete: {detection_stats['detected']} detected, "
                 f"{detection_stats['failed']} unresolved")

    # Add scan metadata to summary
    report.setdefault("summary", {})["language_scan_date"] = datetime.now().isoformat()
    report["summary"]["language_detected_count"] = detection_stats["detected"]
    report["summary"]["language_unresolved_count"] = detection_stats["failed"]

    return report


def _apply_detections_to_entry(
    entry: dict, use_whisper: bool, whisper_all: bool,
    min_confidence: float, stats: dict,
) -> None:
    """Process a single file entry and patch detection results in-place (whisper mode)."""
    # When running whisper, skip subtitle work entirely — audio only
    results = process_file(entry, use_whisper=use_whisper, whisper_all=whisper_all,
                           audio_only=use_whisper)
    _patch_entry_from_results(entry, results, min_confidence, stats)


def _patch_entry_from_results(
    entry: dict, results: list[dict], min_confidence: float, stats: dict,
) -> None:
    """Patch detection results directly onto the stream dicts in the entry."""
    for det in results:
        track_type = det["track_type"]
        idx = det["stream_index"]
        streams = entry.get(f"{track_type}_streams", [])
        if idx >= len(streams):
            continue

        lang = det.get("detected_language")
        conf = det.get("confidence", 0)
        method = det.get("method", "")

        if lang and lang != "und" and conf >= min_confidence:
            streams[idx]["detected_language"] = lang
            streams[idx]["detection_confidence"] = conf
            streams[idx]["detection_method"] = method
            stats["detected"] = stats.get("detected", 0) + 1
        else:
            stats["failed"] = stats.get("failed", 0) + 1


# ---------------------------------------------------------------------------
# Applying detections back to files
# ---------------------------------------------------------------------------

def _apply_file_mkvpropedit(filepath: str, detections: list[dict]) -> tuple[int, int]:
    """Apply all language detections for a file in a single mkvpropedit call.

    Batches every track edit into one process invocation (fast, in-place).
    Returns (applied_count, failed_count).
    """
    mkvprop = _find_mkvpropedit() or "mkvpropedit"
    args: list[str] = [mkvprop, filepath]
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
    use_mkvpropedit = is_mkv and _find_mkvpropedit() is not None

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
    sys.stdout.reconfigure(line_buffering=True)
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)

    parser = argparse.ArgumentParser(description="Detect languages for undetermined subtitle/audio tracks")
    parser.add_argument("--apply", action="store_true",
                        help="Write detected languages back to MKV files using mkvpropedit/ffmpeg")
    parser.add_argument("--min-confidence", type=float, default=0.85,
                        help="Minimum confidence to apply a detection (default: 0.85)")
    parser.add_argument("--whisper", action="store_true",
                        help="Use faster-whisper (GPU) for audio tracks that heuristics can't resolve")
    parser.add_argument("--whisper-all", action="store_true",
                        help="Run whisper on ALL undetermined audio tracks")
    parser.add_argument("--workers", type=int, default=6,
                        help="Parallel workers for subtitle extraction (default: 6)")
    args = parser.parse_args()

    use_whisper = args.whisper or args.whisper_all

    # Check pipeline isn't encoding (whisper competes for GPU)
    if use_whisper:
        pipeline_state_path = STAGING_DIR / "pipeline_state.json"
        if pipeline_state_path.exists():
            try:
                with open(pipeline_state_path, encoding="utf-8") as f:
                    pstate = json.load(f)
                encoding = [fp for fp, info in pstate.get("files", {}).items()
                            if info.get("status") == "encoding" and not info.get("audio_only")]
                if encoding:
                    logging.error(f"Pipeline is actively encoding {len(encoding)} file(s) on GPU.")
                    logging.error("Whisper would compete for VRAM. Stop the pipeline first, or run without --whisper.")
                    sys.exit(1)
            except Exception:
                pass

    # Load media report under lock
    from tools.report_lock import read_report, write_report

    try:
        report = read_report()
    except FileNotFoundError:
        logging.error(f"media_report.json not found at {MEDIA_REPORT}")
        sys.exit(1)

    if args.apply:
        # Apply-only mode: write existing detections to MKV files, no re-detection
        logging.info(f"Applying detections to files via mkvpropedit/ffmpeg...")
        if _find_mkvpropedit():
            logging.info(f"  mkvpropedit: {_find_mkvpropedit()}")
        else:
            logging.info("  mkvpropedit: not found — using ffmpeg (slower)")

        total_applied = 0
        total_failed = 0
        file_count = 0

        for entry in report.get("files", []):
            detections = []
            for i, s in enumerate(entry.get("subtitle_streams", [])):
                if s.get("detected_language"):
                    detections.append({
                        "track_type": "subtitle",
                        "stream_index": i,
                        "detected_language": s["detected_language"],
                        "confidence": s.get("detection_confidence", 0),
                        "method": s.get("detection_method", ""),
                    })
            for i, a in enumerate(entry.get("audio_streams", [])):
                if a.get("detected_language"):
                    detections.append({
                        "track_type": "audio",
                        "stream_index": i,
                        "detected_language": a["detected_language"],
                        "confidence": a.get("detection_confidence", 0),
                        "method": a.get("detection_method", ""),
                    })

            if not detections:
                continue

            file_count += 1
            if file_count % 50 == 0 or file_count == 1:
                logging.info(f"  Progress: {file_count} files processed")
            applied, failed_count = apply_detections_for_file(
                entry["filepath"], detections, args.min_confidence,
            )
            total_applied += applied
            total_failed += failed_count

            # Update report: promote detected_language → language, clear detection fields
            if applied > 0:
                for det in detections:
                    if det.get("confidence", 0) < args.min_confidence:
                        continue
                    streams = entry.get(f"{det['track_type']}_streams", [])
                    idx = det["stream_index"]
                    if idx < len(streams):
                        streams[idx]["language"] = to_iso2(det["detected_language"])
                        streams[idx].pop("detected_language", None)
                        streams[idx].pop("detection_confidence", None)
                        streams[idx].pop("detection_method", None)

        # Save updated report (languages promoted, detection fields cleared)
        write_report(report)
        logging.info(f"Applied: {total_applied}  Failed: {total_failed}")
        logging.info(f"Report updated")
    else:
        # Detection mode: run language detection and save to report
        report = enrich_report(
            report,
            use_whisper=use_whisper,
            whisper_all=args.whisper_all,
            workers=args.workers,
            min_confidence=args.min_confidence,
        )
        write_report(report)
        logging.info(f"Updated {MEDIA_REPORT}")


if __name__ == "__main__":
    main()
