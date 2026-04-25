"""Language detection for subtitle and audio tracks.

Consolidates text-based (langdetect), OCR (Tesseract) and audio (faster-whisper)
detection into a single module.  Exposes:

* ``detect_all_languages(item, use_whisper)``   - programmatic entry point used
  by the encode pipeline (full_gamut, gap_filler, orchestrator).
* ``enrich_report()`` + ``apply_detections_for_file()`` - bulk mode used by
  the CLI and the server's process_manager entries.
* ``main()`` - argparse CLI (``uv run python -m pipeline.language``).

Was previously split between this module and ``tools/detect_languages.py``;
the latter is now a thin shim over ``main()`` here.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Literal, Optional

from paths import STAGING_DIR
from pipeline.metadata import _find_mkvpropedit

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TEXT_SUB_CODECS = {"subrip", "srt", "ass", "ssa", "webvtt", "mov_text", "text", "microdvd"}
BITMAP_SUB_CODECS = {"dvd_subtitle", "hdmv_pgs_subtitle", "dvbsub", "xsub", "pgssub"}
UND_LANGS = {"und", "unk", ""}

# ISO 639-1 (2-letter) -> ISO 639-2/B (3-letter) — what MKV/mkvpropedit expects
_ISO1_TO_ISO2 = {
    "af": "afr",
    "ar": "ara",
    "az": "aze",
    "be": "bel",
    "bg": "bul",
    "bn": "ben",
    "bs": "bos",
    "ca": "cat",
    "cs": "ces",
    "cy": "wel",
    "da": "dan",
    "de": "deu",
    "el": "ell",
    "en": "eng",
    "eo": "epo",
    "es": "spa",
    "et": "est",
    "eu": "baq",
    "fa": "per",
    "fi": "fin",
    "fr": "fra",
    "ga": "gle",
    "gl": "glg",
    "gu": "guj",
    "he": "heb",
    "hi": "hin",
    "hr": "hrv",
    "hu": "hun",
    "hy": "arm",
    "id": "ind",
    "is": "ice",
    "it": "ita",
    "ja": "jpn",
    "ka": "geo",
    "kk": "kaz",
    "km": "khm",
    "kn": "kan",
    "ko": "kor",
    "lt": "lit",
    "lv": "lav",
    "mk": "mac",
    "ml": "mal",
    "mn": "mon",
    "mr": "mar",
    "ms": "may",
    "mt": "mlt",
    "my": "bur",
    "nb": "nob",
    "ne": "nep",
    "nl": "dut",
    "no": "nor",
    "pa": "pan",
    "pl": "pol",
    "pt": "por",
    "ro": "ron",
    "ru": "rus",
    "sk": "slk",
    "sl": "slv",
    "so": "som",
    "sq": "alb",
    "sr": "srp",
    "sv": "swe",
    "sw": "swa",
    "ta": "tam",
    "te": "tel",
    "th": "tha",
    "tl": "tgl",
    "tr": "tur",
    "uk": "ukr",
    "ur": "urd",
    "uz": "uzb",
    "vi": "vie",
    "zh": "zho",
    "zh-cn": "chi",
    "zh-tw": "chi",
    # already 3-letter pass-throughs (from heuristic inference copying media_report codes)
    "eng": "eng",
    "fre": "fra",
    "ger": "deu",
    "chi": "chi",
    "spa": "spa",
    "por": "por",
    "ita": "ita",
    "jpn": "jpn",
    "kor": "kor",
    "rus": "rus",
    "ara": "ara",
    "dut": "dut",
    "swe": "swe",
    "nor": "nor",
    "dan": "dan",
    "fin": "fin",
    "pol": "pol",
    "hun": "hun",
    "ces": "ces",
    "ron": "ron",
    "tur": "tur",
    "ell": "ell",
    "heb": "heb",
    "hin": "hin",
    "tha": "tha",
    "vie": "vie",
    "ind": "ind",
    "hrv": "hrv",
    "ukr": "ukr",
    "slk": "slk",
    "slv": "slv",
    "bul": "bul",
    "srp": "srp",
    "nob": "nob",
}

# Tesseract common install locations
_TESSERACT_SEARCH = [
    r"C:\Program Files\Tesseract-OCR\tesseract.exe",
    r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
]


_TITLE_HINTS = {
    "english": "en",
    "eng": "en",
    "french": "fr",
    "français": "fr",
    "spanish": "es",
    "español": "es",
    "german": "de",
    "deutsch": "de",
    "italian": "it",
    "italiano": "it",
    "japanese": "ja",
    "chinese": "zh",
    "portuguese": "pt",
    "russian": "ru",
    "korean": "ko",
    "dutch": "nl",
    "arabic": "ar",
    "hindi": "hi",
    "swedish": "sv",
    "norwegian": "no",
    "danish": "da",
    "finnish": "fi",
    "polish": "pl",
    "czech": "cs",
    "hungarian": "hu",
    "romanian": "ro",
    "turkish": "tr",
    "greek": "el",
    "hebrew": "he",
    "thai": "th",
    "vietnamese": "vi",
}


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


def to_iso2(lang: str) -> str:
    """Normalise any detected language code to ISO 639-2 for mkvpropedit."""
    return _ISO1_TO_ISO2.get(lang.lower(), lang.lower())


def _find_tesseract() -> Optional[str]:
    """Find tesseract binary — check PATH first, then common install dirs."""
    found = shutil.which("tesseract")
    if found:
        return found
    for path in _TESSERACT_SEARCH:
        if os.path.isfile(path):
            return path
    return None


def _safe_remove(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


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
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        filepath,
        "-map",
        f"0:s:{sub_stream_index}",
        "-t",
        "300",
        "-f",
        "srt",
        "pipe:1",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=90)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None
    if result.returncode != 0 and not result.stdout:
        return None

    raw = result.stdout.decode("utf-8", errors="replace")
    raw = re.sub(r"^\d+\s*$", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"\d{2}:\d{2}:\d{2}[.,]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[.,]\d{3}", "", raw)
    raw = re.sub(r"<[^>]+>", "", raw)
    raw = re.sub(r"\{[^}]+\}", "", raw)
    raw = " ".join(raw.split())
    return raw[:max_chars] if raw.strip() else None


def extract_bitmap_subtitle_text(
    filepath: str,
    sub_stream_index: int,
    duration_secs: float = 0,
    sample_frames: int = 30,
    max_chars: int = 4000,
) -> Optional[str]:
    """Extract text from a bitmap subtitle stream (PGS/DVD) via ffmpeg + Tesseract OCR.

    Samples from 3 windows across the file (20-30%, 45-55%, 70-80%) to avoid
    empty opening credits. Extracts ~10 frames per window, runs Tesseract on each.

    Returns stripped text or None on failure / if Tesseract is not installed.
    """
    tesseract = _find_tesseract()
    if not tesseract:
        return None

    import uuid

    tmp_dir = os.path.join(str(STAGING_DIR), "ocr_tmp", f"{uuid.uuid4().hex[:8]}_{sub_stream_index}")
    os.makedirs(tmp_dir, exist_ok=True)

    total_dur = max(duration_secs, 600)
    windows = [
        (int(total_dur * 0.20), int(total_dur * 0.10)),
        (int(total_dur * 0.45), int(total_dur * 0.10)),
        (int(total_dur * 0.70), int(total_dur * 0.10)),
    ]
    frames_per_window = max(sample_frames // 3, 5)

    try:
        all_text: list[str] = []
        for win_idx, (offset, win_dur) in enumerate(windows):
            pattern = os.path.join(tmp_dir, f"sub_w{win_idx}_%04d.png")
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                str(offset),
                "-i",
                filepath,
                "-map",
                f"0:s:{sub_stream_index}",
                "-t",
                str(win_dur),
                "-frames:v",
                str(frames_per_window),
                pattern,
            ]
            try:
                subprocess.run(cmd, capture_output=True, timeout=120)
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue

        images = sorted(f for f in os.listdir(tmp_dir) if f.endswith(".png"))
        if not images:
            return None

        for img_name in images[:sample_frames]:
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
        combined = re.sub(r"[|_]{2,}", "", combined)
        combined = " ".join(combined.split())
        return combined[:max_chars] if combined.strip() else None

    finally:
        try:
            for f in os.listdir(tmp_dir):
                os.remove(os.path.join(tmp_dir, f))
            os.rmdir(tmp_dir)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Language detection from text
# ---------------------------------------------------------------------------


def _cjk_language_from_script(text: str) -> Optional[tuple[str, float]]:
    """Reliably identify CJK languages using Unicode block proportions.

    langdetect regularly confuses Chinese/Japanese/Korean because their character
    sets overlap.  Unicode ranges are definitive for Hangul (Korean) and kana
    (Japanese).

    Returns (lang_code, confidence) or None if not predominantly CJK.
    """
    total = len(text)
    if total == 0:
        return None

    hangul = sum(1 for c in text if "\uac00" <= c <= "\ud7af" or "\u1100" <= c <= "\u11ff")
    hiragana = sum(1 for c in text if "\u3040" <= c <= "\u309f")
    katakana = sum(1 for c in text if "\u30a0" <= c <= "\u30ff")
    cjk_unified = sum(1 for c in text if "\u4e00" <= c <= "\u9fff" or "\u3400" <= c <= "\u4dbf")
    kana = hiragana + katakana

    if hangul / total > 0.05 and kana / max(hangul, 1) < 0.1:
        return "ko", min(0.95, 0.70 + hangul / total)

    if hiragana / total > 0.03:
        return "ja", min(0.95, 0.70 + hiragana / total * 5)

    if cjk_unified / total > 0.10 and hangul / total < 0.02 and kana / total < 0.02:
        return "zh", min(0.92, 0.70 + cjk_unified / total)

    return None


def detect_language(text: str) -> tuple[str, float]:
    """Return (iso639-1 code, confidence) from text. Falls back to ('und', 0.0)."""
    if not text.strip():
        return "und", 0.0

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


# ---------------------------------------------------------------------------
# Heuristic audio inference (title / sibling-based)
# ---------------------------------------------------------------------------


def infer_audio_language(file_entry: dict, audio_idx: int) -> tuple[Optional[str], str]:
    """Heuristically infer the language of an undetermined audio track.

    Returns (language_code_or_None, reason). Only returns a result when confident:
    - Single audio track on a file where all subtitles are one language
    - Track title contains a known language name
    - All other identified audio tracks agree on one language
    """
    streams = file_entry.get("audio_streams", [])
    subs = file_entry.get("subtitle_streams", [])

    track = streams[audio_idx]
    title = (track.get("title") or "").lower()
    for hint, code in _TITLE_HINTS.items():
        if hint in title:
            return code, f"track title contains '{hint}'"

    if len(streams) == 1:
        sub_langs = {
            s.get("language", "und").lower() for s in subs if s.get("language", "und").lower() not in UND_LANGS
        }
        if len(sub_langs) == 1:
            lang = next(iter(sub_langs))
            return lang, f"sole audio track, all subtitles are '{lang}'"

    known = [
        s.get("language", "und").lower()
        for i, s in enumerate(streams)
        if i != audio_idx and s.get("language", "und").lower() not in UND_LANGS
    ]
    if known and len(set(known)) == 1:
        lang = known[0]
        return lang, f"all other audio tracks are '{lang}'"

    return None, "insufficient context"


def _infer_pgs_from_siblings(
    file_entry: dict,
    detected_text_langs: dict[int, str],
) -> dict[int, tuple[str, str]]:
    """Infer PGS/DVD subtitle languages from sibling text subs.

    Strategies:
    1. Unanimous: all known subs (pre-existing + detected) agree on one language.
    2. Positional mapping: N text subs w/ known langs and N bitmap subs -> 1:1.

    Returns {sub_all_idx: (lang_code, reason)} for each inferred bitmap sub.
    """
    subs = file_entry.get("subtitle_streams", [])
    inferred: dict[int, tuple[str, str]] = {}

    known_langs: dict[int, str] = {}
    for idx, s in enumerate(subs):
        existing = (s.get("language") or "und").lower().strip()
        if existing not in UND_LANGS:
            known_langs[idx] = existing
    known_langs.update(detected_text_langs)

    und_bitmap_idxs: list[int] = []
    for idx, s in enumerate(subs):
        codec = (s.get("codec") or "").lower()
        lang = (s.get("language") or "und").lower().strip()
        if lang in UND_LANGS and codec in BITMAP_SUB_CODECS:
            und_bitmap_idxs.append(idx)

    if not und_bitmap_idxs or not known_langs:
        return inferred

    unique_langs = set(known_langs.values())
    if len(unique_langs) == 1:
        lang = next(iter(unique_langs))
        for idx in und_bitmap_idxs:
            inferred[idx] = (lang, f"all {len(known_langs)} known subs are '{lang}'")
        return inferred

    known_text_idxs = sorted(idx for idx in known_langs if (subs[idx].get("codec") or "").lower() in TEXT_SUB_CODECS)
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
    """Infer audio language from majority subtitle language (>=70% share -> 0.8 conf)."""
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

    counts = Counter(known_langs)
    top_lang, top_count = counts.most_common(1)[0]
    ratio = top_count / len(known_langs)
    if ratio >= 0.7:
        return top_lang, f"subtitle majority {top_count}/{len(known_langs)} are '{top_lang}'"
    return None, "insufficient context"


# ---------------------------------------------------------------------------
# Whisper-based audio language detection
# ---------------------------------------------------------------------------

_whisper_tiny = None
_whisper_small = None
_whisper_medium = None


def _assert_encoder_not_running() -> None:
    """Raise if the NVENC encoder is currently active — whisper on GPU + NVENC
    on the same chip triggered the 2026-04-21 BSOD. Caller must stop the pipeline
    before invoking the whisper ladder. Sample extraction is safe; only whisper
    inference is the exclusive-GPU step.

    gap_filler-only is fine — it doesn't use the GPU. Only block if the FULL
    pipeline (with GPU workers) is running.
    """
    import json as _json
    import urllib.request as _req

    try:
        resp = _req.urlopen("http://localhost:8002/api/process/pipeline/status", timeout=2)
        data = _json.loads(resp.read())
        if data.get("status") == "running":
            raise RuntimeError(
                "Full pipeline is currently encoding — stop it before running whisper on GPU "
                "(POST /api/process/pipeline/stop). gap_filler-only is fine."
            )
    except (ConnectionError, OSError, TimeoutError):
        pass


def _get_whisper_model(size: str = "tiny"):
    """Lazy-load a faster-whisper model on GPU (CUDA / float16).

    Falls back to CPU on load failure but the expectation is GPU. The encoder
    pipeline must be stopped first (see _assert_encoder_not_running) because
    NVENC + whisper on the same RTX 4080 triggered a kernel BSOD on 2026-04-21.
    """
    global _whisper_tiny, _whisper_small, _whisper_medium
    if size == "tiny":
        ref = _whisper_tiny
    elif size == "small":
        ref = _whisper_small
    else:
        ref = _whisper_medium
    if ref is not None:
        return ref

    # The encoder-conflict check only matters for GPU mode — running whisper on
    # CUDA shares the chip with NVENC and triggered a BSOD on 2026-04-21. CPU
    # mode has no such contention, so allow it through even with the encoder
    # running. Useful for ad-hoc verification and the bulk audit pass which we
    # want to run alongside the pipeline.
    force_cpu = os.environ.get("WHISPER_FORCE_CPU", "").strip() in {"1", "true", "yes"}
    if not force_cpu:
        _assert_encoder_not_running()

    # CTranslate2 was built against CUDA 12 but this machine has CUDA 13.2 —
    # we install cublas64_12.dll + cudnn*.dll via pip into the venv. The loader
    # won't see them until we explicitly add the dirs to the DLL search path.
    try:
        import site

        for base in site.getsitepackages():
            for sub in ("nvidia/cublas/bin", "nvidia/cudnn/bin"):
                dll_dir = os.path.join(base, *sub.split("/"))
                if os.path.isdir(dll_dir):
                    if hasattr(os, "add_dll_directory"):
                        os.add_dll_directory(dll_dir)
                    if dll_dir not in os.environ.get("PATH", ""):
                        os.environ["PATH"] = dll_dir + os.pathsep + os.environ.get("PATH", "")
    except Exception as e:
        logging.debug(f"Couldn't add nvidia DLL dirs: {e}")

    model = None
    if not force_cpu:
        try:
            from faster_whisper import WhisperModel

            model = WhisperModel(size, device="cuda", compute_type="float16")
            # Warmup to surface any late CUDA errors before a mid-sweep deadlock.
            import tempfile

            with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tf:
                silent_wav = tf.name
            try:
                subprocess.run(
                    [
                        "ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
                        "-i", "anullsrc=r=16000:cl=mono", "-t", "0.5", "-y", silent_wav,
                    ],
                    capture_output=True,
                    timeout=10,
                )
                _segs, _info = model.transcribe(
                    silent_wav, beam_size=1, best_of=1, language=None, without_timestamps=True,
                )
                try:
                    next(iter(_segs))
                except StopIteration:
                    pass
                logging.info(f"Loaded faster-whisper model ({size}, cuda/float16) — warmup OK")
            finally:
                try:
                    os.unlink(silent_wav)
                except OSError:
                    pass
        except Exception as e:
            logging.warning(f"Whisper GPU path failed for {size} ({e}) — falling back to CPU")
            model = None

    if model is None:
        try:
            from faster_whisper import WhisperModel

            model = WhisperModel(size, device="cpu", compute_type="int8")
            logging.info(f"Loaded faster-whisper model ({size}, cpu/int8)")
        except Exception as e2:
            logging.error(f"Failed to load whisper model {size}: {e2}")
            return None

    if size == "tiny":
        _whisper_tiny = model
    elif size == "small":
        _whisper_small = model
    else:
        _whisper_medium = model
    return model


def _evenly_spread_offsets(duration_secs: float, count: int) -> list[int]:
    """Pick ``count`` time offsets evenly spread across the runtime.

    Skips the first/last 5% so we don't sample studio logos or end credits
    where the audio is often music or silent. Falls back to clipping to a
    minimum runtime of 120s for very short files.
    """
    total = max(duration_secs, 120)
    if count <= 1:
        return [int(total * 0.5)]
    # Spread across [5%, 95%] of runtime
    step = (total * 0.9) / (count - 1)
    return [int(total * 0.05 + step * i) for i in range(count)]


def _extract_all_audio_samples(
    filepath: str,
    audio_indices: list[int],
    duration_secs: float,
    sample_duration: int = 30,
    sample_count: int = 5,
) -> dict[int, list[str]]:
    """Extract audio samples for ALL tracks of a file in one ffmpeg call.

    ``sample_count`` × ``sample_duration`` WAVs spread evenly across the
    runtime, mono 16 kHz (whisper's native rate). Single file open over SMB
    regardless of how many tracks or samples per track. Result paths
    correspond to {audio_index: [wav_path, ...], ...}; files smaller than
    10 KB (likely silence or extraction failure) are filtered out.
    """
    tmp_dir = os.path.join(str(STAGING_DIR), "whisper_tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    base = f"{os.getpid()}_{threading.current_thread().ident}"

    offsets = _evenly_spread_offsets(duration_secs, sample_count)

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
                "-ac", "1",
                "-ar", "16000",
                "-y", wav_path,
            ])
        result[aidx] = paths

    # Timeout scales with sample count + duration. Stage 3 (10 × 300s) needs
    # generous headroom because it's reading 50 min of audio off SMB.
    expected_seconds = sample_count * sample_duration * len(audio_indices)
    timeout = max(300, expected_seconds * 2 + 60)
    try:
        subprocess.run(cmd, capture_output=True, timeout=timeout)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    for aidx in list(result.keys()):
        result[aidx] = [p for p in result[aidx] if os.path.exists(p) and os.path.getsize(p) > 10_000]

    return result


def _whisper_detect_one(model, wav_path: str) -> tuple[Optional[str], float]:
    """Run whisper language detection on a single WAV sample."""
    try:
        segments, info = model.transcribe(
            wav_path, beam_size=1, best_of=1, language=None, without_timestamps=True
        )
        for _ in segments:
            break
        if info.language and info.language_probability > 0.1:
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

    lang_counts = Counter(lang for lang, _ in detections)
    majority_lang, majority_count = lang_counts.most_common(1)[0]

    if majority_count == len(detections):
        avg_prob = sum(p for _, p in detections) / len(detections)
        return majority_lang, round(avg_prob, 3)

    if majority_count > len(detections) / 2:
        avg_prob = sum(p for la, p in detections if la == majority_lang) / majority_count
        return majority_lang, round(avg_prob * 0.85, 3)

    best = max(detections, key=lambda x: x[1])
    return best[0], round(best[1] * 0.7, 3)


def _run_whisper_on(wavs: list[str], model_size: str) -> tuple[Optional[str], float]:
    """Run whisper on a list of wav samples and majority-vote the result."""
    if not wavs:
        return None, 0.0
    model = _get_whisper_model(model_size)
    if not model:
        return None, 0.0
    detections: list[tuple[str, float]] = []
    for wav in wavs:
        try:
            lang, prob = _whisper_detect_one(model, wav)
            if lang:
                detections.append((lang, prob))
        except Exception as e:
            logging.debug(f"Whisper {model_size} error on {os.path.basename(wav)}: {e}")
    return _majority_vote(detections)


def _cache_db_path() -> str:
    """SQLite path for the language-detection cache.

    Lives next to the rest of the pipeline state DBs so it's swept up by the
    same backups + ignored-file rules. Schema is created on first connect.
    """
    return os.path.join(str(STAGING_DIR), "language_cache.sqlite")


def _file_cache_key(filepath: str, audio_stream_index: int) -> Optional[str]:
    """Key the cache on (path, mtime, size, audio_index) so a file edit busts the cache.

    Returns None if the file is unreachable (we just skip caching in that case
    rather than fail the detection itself). Using mtime+size is cheaper than
    sha256 and good enough — the only failure modes (sub-second mtime+same
    size after edit) are hyper-rare and would still produce a re-detection if
    we ever DID re-encode the file (because the fresh encode shifts size).
    """
    try:
        st = os.stat(filepath)
    except OSError:
        return None
    return f"{filepath}|{int(st.st_mtime)}|{st.st_size}|{audio_stream_index}"


def _cache_get(key: str) -> Optional[tuple[str, float, str]]:
    """Return cached (lang, confidence, method) for ``key`` or None on miss."""
    if not key:
        return None
    try:
        import sqlite3
        conn = sqlite3.connect(_cache_db_path(), timeout=5)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lang_cache (
                    cache_key  TEXT PRIMARY KEY,
                    language   TEXT,
                    confidence REAL,
                    method     TEXT,
                    detected_at TEXT
                )
            """)
            conn.commit()
            row = conn.execute(
                "SELECT language, confidence, method FROM lang_cache WHERE cache_key = ?",
                (key,),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        lang, conf, method = row
        return (lang or "und"), float(conf or 0.0), (method or "")
    except Exception as exc:
        logging.debug(f"language cache read failed: {exc}")
        return None


def _cache_set(key: str, language: str, confidence: float, method: str) -> None:
    """Persist a detection result to the cache. Best-effort — failures are logged but not raised."""
    if not key:
        return
    try:
        import sqlite3
        conn = sqlite3.connect(_cache_db_path(), timeout=5)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lang_cache (
                    cache_key  TEXT PRIMARY KEY,
                    language   TEXT,
                    confidence REAL,
                    method     TEXT,
                    detected_at TEXT
                )
            """)
            conn.execute(
                "INSERT OR REPLACE INTO lang_cache (cache_key, language, confidence, method, detected_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (key, language, float(confidence), method, datetime.now().isoformat(timespec="seconds")),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logging.debug(f"language cache write failed: {exc}")


# --- Whisper escalation ladder ----------------------------------------------
#
# Stage spec (per the user's brief, 2026-04-25):
#
#   1. tiny  / 3 samples  / 30s each  — most files settle here (~5-15s wall)
#   2. small / 5 samples  / 60s each  — escalation for borderline (~30-60s)
#   3. base  / 10 samples / 300s each — heavy for genuinely ambiguous (~5-10 min)
#
# Each stage exits early if confidence ≥ MIN_CONFIDENCE AND the samples agree
# (handled inside _majority_vote — we only return a confident lang when the
# majority count equals the sample count, otherwise the confidence is
# discounted, which fails the threshold and pushes to the next stage).
#
# Stage 3 represents heavy CPU spend (~10 min on slow CPUs); it should fire
# only on the small minority of files where stages 1 + 2 couldn't agree.

_LADDER_STAGES: list[tuple[str, int, int]] = [
    # (model_size, sample_count, sample_duration_secs)
    ("tiny",  3,  30),
    ("small", 5,  60),
    ("base",  10, 300),
]
_LADDER_MIN_CONFIDENCE = 0.85


def detect_audio_language_deep(
    filepath: str,
    audio_stream_index: int,
    duration_secs: float,
    min_confidence: float = _LADDER_MIN_CONFIDENCE,
    *,
    use_cache: bool = True,
) -> tuple[Optional[str], float, str]:
    """Three-stage escalation ladder for audio language detection.

    Walks ``_LADDER_STAGES`` in order. At each stage, extracts the relevant
    number/duration of samples and runs whisper at the corresponding model
    size. Returns at the first stage whose result meets ``min_confidence``;
    otherwise escalates to the next stage. After all three stages exhaust,
    returns the best result we got (language might still be "und").

    Cached by ``(filepath, mtime, size, audio_index)`` in
    ``language_cache.sqlite`` — re-running on an unchanged file is free.

    Caller MUST stop the NVENC encoder before invoking this on GPU; the
    sample-extraction step is safe but whisper inference shares the same
    chip as NVENC and a double-bind triggers BSOD (2026-04-21 incident).

    Args:
        filepath: full path to the media file.
        audio_stream_index: 0-based index within ``streams.audio``.
        duration_secs: full runtime in seconds (used to spread sample offsets).
        min_confidence: stage-pass threshold; default 0.85.
        use_cache: set False to force a fresh detection (rarely needed).

    Returns:
        (lang, confidence, method) — method is e.g. ``whisper_tiny_3x30``,
        ``whisper_small_5x60``, ``whisper_base_10x300``, ``cached``, or
        ``whisper_exhausted`` when no stage produced a usable result.
    """
    import time as _time

    name = os.path.basename(filepath)
    cache_key = _file_cache_key(filepath, audio_stream_index)

    if use_cache and cache_key:
        hit = _cache_get(cache_key)
        if hit is not None:
            lang_c, conf_c, method_c = hit
            logging.info(
                f"    {name} a:{audio_stream_index} -> {lang_c} ({conf_c:.2f}) "
                f"cached ({method_c})"
            )
            return lang_c, conf_c, "cached"

    t0 = _time.monotonic()
    best_lang: Optional[str] = None
    best_conf = 0.0
    best_method = "whisper_exhausted"

    for stage_idx, (model_size, sample_count, sample_duration) in enumerate(_LADDER_STAGES, start=1):
        t_stage = _time.monotonic()
        samples = _extract_all_audio_samples(
            filepath,
            [audio_stream_index],
            duration_secs,
            sample_duration=sample_duration,
            sample_count=sample_count,
        )
        wavs = samples.get(audio_stream_index, [])
        if not wavs:
            logging.warning(
                f"    {name} a:{audio_stream_index} — stage {stage_idx} extraction "
                f"failed (model={model_size}, want={sample_count}x{sample_duration}s)"
            )
            continue

        try:
            lang, conf = _run_whisper_on(wavs, model_size=model_size)
            method = f"whisper_{model_size}_{sample_count}x{sample_duration}"
            elapsed_stage = _time.monotonic() - t_stage
            elapsed_total = _time.monotonic() - t0
            if lang:
                logging.info(
                    f"    {name} a:{audio_stream_index} stage {stage_idx} ({method}) -> "
                    f"{lang} ({conf:.2f}) in {elapsed_stage:.1f}s (total {elapsed_total:.1f}s)"
                )
                if conf > best_conf:
                    best_lang, best_conf, best_method = lang, conf, method
                if conf >= min_confidence:
                    if cache_key:
                        _cache_set(cache_key, lang, conf, method)
                    return lang, conf, method
            else:
                logging.info(
                    f"    {name} a:{audio_stream_index} stage {stage_idx} ({method}) -> "
                    f"no detection in {elapsed_stage:.1f}s"
                )
        finally:
            for w in wavs:
                _safe_remove(w)

    # All stages exhausted. Return best-so-far if any, else explicit und.
    if best_lang:
        method_low = f"{best_method}_low_conf"
        if cache_key:
            _cache_set(cache_key, best_lang, best_conf, method_low)
        logging.info(
            f"    {name} a:{audio_stream_index} -> {best_lang} ({best_conf:.2f}) "
            f"{method_low} (total {_time.monotonic() - t0:.1f}s)"
        )
        return best_lang, best_conf, method_low

    if cache_key:
        _cache_set(cache_key, "und", 0.0, "whisper_exhausted")
    logging.info(
        f"    {name} a:{audio_stream_index} -> und (whisper_exhausted, "
        f"total {_time.monotonic() - t0:.1f}s)"
    )
    return "und", 0.0, "whisper_exhausted"


def detect_audio_language_whisper(
    filepath: str,
    audio_stream_index: int,
    duration_secs: float = 0,
) -> tuple[Optional[str], float]:
    """Detect language using whisper-tiny with multi-sample majority vote.

    Extracts 5 x 30s samples in one ffmpeg call, runs whisper-tiny on each,
    majority-votes. Returns (lang_code, confidence) or (None, 0.0) on failure.
    """
    samples = _extract_all_audio_samples(filepath, [audio_stream_index], duration_secs)
    wav_paths = samples.get(audio_stream_index, [])
    if not wav_paths:
        return None, 0.0

    try:
        tiny = _get_whisper_model("tiny")
        if not tiny:
            return None, 0.0

        detections: list[tuple[str, float]] = []
        for wav_path in wav_paths:
            la, p = _whisper_detect_one(tiny, wav_path)
            if la:
                detections.append((la, p))

        return _majority_vote(detections)

    except Exception as e:
        logging.debug(f"Whisper detection failed for {os.path.basename(filepath)} a:{audio_stream_index}: {e}")
        return None, 0.0
    finally:
        for wav_path in wav_paths:
            _safe_remove(wav_path)


def detect_audio_languages_for_file(
    filepath: str,
    audio_indices: list[int],
    duration_secs: float = 0,
) -> dict[int, tuple[Optional[str], float]]:
    """Detect languages for ALL audio tracks of a file efficiently.

    Batch-extracts all samples (5 x 30s per track) in one ffmpeg call, runs
    tiny with majority vote, escalates to small if low confidence.
    Returns {audio_index: (lang, confidence), ...}.
    """
    logging.info(f"    Extracting {len(audio_indices)} tracks x 5 samples (30s each)...")
    all_samples = _extract_all_audio_samples(filepath, audio_indices, duration_secs, sample_duration=30)
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

        detections: list[tuple[str, float]] = []
        for wp in wav_paths:
            la, p = _whisper_detect_one(tiny, wp)
            if la:
                detections.append((la, p))

        lang, conf = _majority_vote(detections)

        if not lang or conf < 0.7:
            small = _get_whisper_model("small")
            if small:
                logging.info(f"    Escalating track {aidx} to small model (tiny conf={conf:.2f})")
                detections_small: list[tuple[str, float]] = []
                for wp in wav_paths:
                    la, p = _whisper_detect_one(small, wp)
                    if la:
                        detections_small.append((la, p))
                lang_s, conf_s = _majority_vote(detections_small)
                if conf_s > conf:
                    lang, conf = lang_s, conf_s

        results[aidx] = (lang, conf)

    for wav_list in all_samples.values():
        for wp in wav_list:
            _safe_remove(wp)

    return results


# ---------------------------------------------------------------------------
# Per-file processing
# ---------------------------------------------------------------------------


def process_file(
    file_entry: dict,
    use_whisper: bool = False,
    whisper_all: bool = False,
    audio_only: bool = False,
) -> list[dict]:
    """Return a list of detection results for all undetermined tracks in a file.

    Used by the bulk (enrich_report) path — emits one dict per track with
    track_type / stream_index / codec / detected_language / confidence / method.
    """
    filepath = file_entry["filepath"]
    results: list[dict] = []

    detected_text_langs: dict[int, str] = {}
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
                    text_results.append(
                        {
                            "filepath": filepath,
                            "track_type": "subtitle",
                            "stream_index": sub_all_idx,
                            "codec": codec,
                            "detected_language": None,
                            "confidence": 0.0,
                            "method": "text_extraction_empty",
                            "chars_sampled": 0,
                        }
                    )

        results.extend(text_results)

        pgs_inferred = _infer_pgs_from_siblings(file_entry, detected_text_langs)
        for sub_all_idx, stream in enumerate(file_entry.get("subtitle_streams", [])):
            lang = (stream.get("language") or "und").lower().strip()
            codec = stream.get("codec", "").lower()
            if lang not in UND_LANGS or codec not in BITMAP_SUB_CODECS:
                continue

            file_duration = file_entry.get("duration_seconds", 0)
            ocr_text = extract_bitmap_subtitle_text(filepath, sub_all_idx, duration_secs=file_duration)
            if ocr_text:
                detected, confidence = detect_language(ocr_text)
                if detected and detected != "und" and confidence >= 0.5:
                    detected_text_langs[sub_all_idx] = detected
                    results.append(
                        {
                            "filepath": filepath,
                            "track_type": "subtitle",
                            "stream_index": sub_all_idx,
                            "codec": codec,
                            "detected_language": detected,
                            "confidence": confidence,
                            "method": "ocr_extraction",
                            "chars_sampled": len(ocr_text),
                        }
                    )
                    continue

            if sub_all_idx in pgs_inferred:
                inferred_lang, reason = pgs_inferred[sub_all_idx]
                results.append(
                    {
                        "filepath": filepath,
                        "track_type": "subtitle",
                        "stream_index": sub_all_idx,
                        "codec": codec,
                        "detected_language": inferred_lang,
                        "confidence": 0.85,
                        "method": "sibling_inference",
                        "reason": reason,
                    }
                )
            else:
                results.append(
                    {
                        "filepath": filepath,
                        "track_type": "subtitle",
                        "stream_index": sub_all_idx,
                        "codec": codec,
                        "detected_language": None,
                        "confidence": 0.0,
                        "method": "bitmap_no_match",
                        "chars_sampled": 0,
                    }
                )

    audio_heuristics: dict[int, tuple] = {}
    whisper_candidates: list[int] = []

    for audio_idx, stream in enumerate(file_entry.get("audio_streams", [])):
        lang = (stream.get("language") or "und").lower().strip()
        is_und = lang in UND_LANGS

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

    whisper_results: dict[int, tuple] = {}
    if whisper_candidates:
        whisper_results = detect_audio_languages_for_file(
            filepath,
            whisper_candidates,
            file_entry.get("duration_seconds", 0),
        )

    audio_streams = file_entry.get("audio_streams", [])
    for audio_idx, (detected, conf, method, reason) in audio_heuristics.items():
        if audio_idx in whisper_results:
            w_lang, w_conf = whisper_results[audio_idx]
            if w_lang and w_conf > 0.5 and (not detected or w_conf > conf):
                detected = w_lang
                conf = w_conf
                method = "whisper"
                reason = f"whisper detection (prob={w_conf:.2f})"

        if audio_idx in whisper_candidates and audio_idx < len(audio_streams):
            audio_streams[audio_idx]["whisper_attempted"] = True

        audio_codec = audio_streams[audio_idx].get("codec", "") if audio_idx < len(audio_streams) else ""
        if detected:
            results.append(
                {
                    "filepath": filepath,
                    "track_type": "audio",
                    "stream_index": audio_idx,
                    "codec": audio_codec,
                    "detected_language": detected,
                    "confidence": conf,
                    "method": method,
                    "reason": reason,
                }
            )
        else:
            lang = (
                (audio_streams[audio_idx].get("language") or "und").lower() if audio_idx < len(audio_streams) else "und"
            )
            if lang in UND_LANGS:
                results.append(
                    {
                        "filepath": filepath,
                        "track_type": "audio",
                        "stream_index": audio_idx,
                        "codec": audio_codec,
                        "detected_language": None,
                        "confidence": 0.0,
                        "method": method,
                        "reason": reason,
                    }
                )

    return results


# ---------------------------------------------------------------------------
# High-level convenience function (used by pipeline/full_gamut, gap_filler, orchestrator)
# ---------------------------------------------------------------------------


def detect_all_languages(file_entry: dict, use_whisper: bool = False) -> dict:
    """Detect languages for all undetermined tracks in a file entry.

    Returns a deep copy of ``file_entry`` with ``detected_language`` /
    ``detection_confidence`` / ``detection_method`` fields populated on
    audio_streams and subtitle_streams. Does NOT modify the input.
    """
    import copy

    entry = copy.deepcopy(file_entry)
    filepath = entry["filepath"]

    detected_text_langs: dict[int, str] = {}

    for sub_all_idx, stream in enumerate(entry.get("subtitle_streams", [])):
        lang = (stream.get("language") or "und").lower().strip()
        codec = stream.get("codec", "").lower()
        if lang not in UND_LANGS:
            continue

        if codec in TEXT_SUB_CODECS:
            text = extract_subtitle_text(filepath, sub_all_idx)
            if text:
                detected, confidence = detect_language(text)
                if detected and detected != "und" and confidence >= 0.5:
                    stream["detected_language"] = detected
                    stream["detection_confidence"] = confidence
                    stream["detection_method"] = "text_extraction"
                    detected_text_langs[sub_all_idx] = detected

        elif codec in BITMAP_SUB_CODECS:
            ocr_text = extract_bitmap_subtitle_text(
                filepath, sub_all_idx, duration_secs=entry.get("duration_seconds", 0),
            )
            if ocr_text:
                detected, confidence = detect_language(ocr_text)
                if detected and detected != "und" and confidence >= 0.5:
                    stream["detected_language"] = detected
                    stream["detection_confidence"] = confidence
                    stream["detection_method"] = "ocr_extraction"
                    detected_text_langs[sub_all_idx] = detected

    for audio_idx, stream in enumerate(entry.get("audio_streams", [])):
        lang = (stream.get("language") or "und").lower().strip()
        if lang not in UND_LANGS:
            continue

        # Title-based hint is allowed because the title field is human-authored
        # ("English 5.1", "VFF", "Castellano") and gives a high-signal label
        # when present. False positives here are rare.
        title = (stream.get("title") or "").lower()
        detected_from_title = None
        for hint, code in _TITLE_HINTS.items():
            if hint in title:
                detected_from_title = code
                break

        if detected_from_title:
            stream["detected_language"] = detected_from_title
            stream["detection_confidence"] = 0.9
            stream["detection_method"] = "title_hint"
            continue

        # NOTE: the previous "single-audio + single-sub-language → infer audio
        # matches sub" heuristic was DELETED on 2026-04-25. It misidentified
        # foreign-dub episodes (e.g. Bluey with Swedish audio + Bazarr-added
        # English sub got labelled English audio because there was 1 audio +
        # 1 sub language). The only reliable signal for foreign audio is
        # actually listening to it — which is what whisper does.
        #
        # No fallback inference here. If we have no whisper result and no
        # title hint, the track stays `und` and the qualify stage flags it
        # as FLAGGED_UNDETERMINED so the user can act.

        if use_whisper:
            w_lang, w_conf, w_method = detect_audio_language_deep(
                filepath,
                audio_idx,
                entry.get("duration_seconds", 0),
            )
            if w_lang and w_lang != "und" and w_conf > 0.5:
                stream["detected_language"] = w_lang
                stream["detection_confidence"] = w_conf
                stream["detection_method"] = w_method

    return entry


def clear_legacy_heuristic_detections(file_entry: dict) -> tuple[dict, int]:
    """Strip detection results that came from the deleted broken heuristic.

    Returns ``(entry_copy, n_cleared)``. Streams whose ``detection_method``
    is the now-defunct ``"heuristic"`` get their ``detected_language`` /
    ``detection_confidence`` / ``detection_method`` fields wiped, so the
    next ``detect_all_languages`` pass treats them as untouched and runs
    whisper on them.

    Called by qualify (Step 4) before re-detection on already-encoded files
    so we don't trust stale labels. Title-hint detections (``title_hint``)
    are kept — those are still considered valid.
    """
    import copy

    entry = copy.deepcopy(file_entry)
    cleared = 0
    for track_kind in ("audio_streams", "subtitle_streams"):
        for stream in entry.get(track_kind, []) or []:
            method = stream.get("detection_method") or ""
            if method == "heuristic":
                stream.pop("detected_language", None)
                stream.pop("detection_confidence", None)
                stream.pop("detection_method", None)
                cleared += 1
    return entry, cleared


# ---------------------------------------------------------------------------
# Bulk (report-enrichment) orchestration
# ---------------------------------------------------------------------------


def _patch_entry_from_results(
    entry: dict,
    results: list[dict],
    min_confidence: float,
    stats: dict,
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
            streams[idx]["detection_method"] = method or "failed"
            stats["failed"] = stats.get("failed", 0) + 1


def _incremental_save(our_report: dict, processed_entries: list[dict]) -> None:
    """Merge language detection results into the on-disk report without stomping
    other changes (TMDb / scanner may run concurrently).
    """
    from tools.report_lock import read_report, write_report

    try:
        disk_report = read_report()
    except Exception:
        write_report(our_report)
        return

    our_lookup: dict[str, dict] = {}
    for entry in processed_entries:
        our_lookup[entry.get("filepath", "")] = entry

    for disk_entry in disk_report.get("files", []):
        fp = disk_entry.get("filepath", "")
        if fp not in our_lookup:
            continue
        our_entry = our_lookup[fp]

        for i, our_stream in enumerate(our_entry.get("audio_streams", [])):
            if i < len(disk_entry.get("audio_streams", [])):
                for field in ("detected_language", "detection_confidence", "detection_method", "whisper_attempted"):
                    if our_stream.get(field):
                        disk_entry["audio_streams"][i][field] = our_stream[field]

        for i, our_stream in enumerate(our_entry.get("subtitle_streams", [])):
            if i < len(disk_entry.get("subtitle_streams", [])):
                for field in ("detected_language", "detection_confidence", "detection_method"):
                    if our_stream.get(field):
                        disk_entry["subtitle_streams"][i][field] = our_stream[field]

    disk_report.setdefault("summary", {})["language_scan_date"] = datetime.now().isoformat()
    write_report(disk_report)


def _persist_whisper_detection(
    entry: dict,
    aidx: int,
    lang: Optional[str],
    conf: float,
    method: str,
    min_confidence: float,
    stats: dict,
) -> None:
    """Write whisper detection outcome directly onto the entry's audio stream.

    Shared between text_whisper and deep strategies — keeps field naming and
    stats accounting consistent.
    """
    streams = entry.get("audio_streams", [])
    if aidx >= len(streams):
        return
    stream = streams[aidx]
    if lang and lang != "und" and conf >= min_confidence:
        stream["detected_language"] = lang
        stream["detection_confidence"] = round(conf, 3)
        stream["detection_method"] = method
        stats["detected"] = stats.get("detected", 0) + 1
    else:
        stream["detected_language"] = lang if lang else None
        stream["detection_confidence"] = round(conf, 3) if conf else 0.0
        stream["detection_method"] = method
        stats["failed"] = stats.get("failed", 0) + 1
    stream["whisper_attempted"] = True


def _build_whisper_worklist(
    to_process: list[dict],
    whisper_all: bool,
    retry_unresolved: bool = False,
) -> list[tuple[dict, list[int]]]:
    """Build per-file list of (entry, und_audio_indices) for whisper strategies."""
    work: list[tuple[dict, list[int]]] = []
    for entry in to_process:
        und_audio: list[int] = []
        for i, a in enumerate(entry.get("audio_streams", [])):
            lang = (a.get("language") or "und").lower().strip()
            detected = (a.get("detected_language") or "").lower().strip()
            if whisper_all:
                und_audio.append(i)
                continue
            if retry_unresolved:
                if not a.get("whisper_attempted"):
                    continue
                a.pop("whisper_attempted", None)
                und_audio.append(i)
                continue
            if detected and detected not in UND_LANGS:
                continue
            if a.get("detection_method") == "whisper" or a.get("whisper_attempted"):
                continue
            if lang in UND_LANGS:
                und_audio.append(i)
        if und_audio:
            work.append((entry, und_audio))
    return work


def _run_text_strategy(
    to_process: list[dict],
    workers: int,
    min_confidence: float,
    stats: dict,
) -> None:
    """ThreadPoolExecutor of process_file for text/OCR + sibling inference."""
    completed = 0
    total = len(to_process)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_file, entry): entry for entry in to_process}
        for future in as_completed(futures):
            completed += 1
            if completed % 50 == 0 or completed == total:
                logging.info(f"  Language progress: {completed}/{total}")
            try:
                entry = futures[future]
                results = future.result()
                _patch_entry_from_results(entry, results, min_confidence, stats)
            except Exception as e:
                logging.warning(f"  Language detection error: {e}")


def _run_text_whisper_strategy(
    to_process: list[dict],
    whisper_all: bool,
    min_confidence: float,
    stats: dict,
    report: dict,
    retry_unresolved: bool = False,
) -> None:
    """4-thread per-file whisper pool running detect_audio_language_deep per track.

    Replaces the previous 4-phase _escalating_whisper_detect (which duplicated
    detect_audio_language_deep). Saves the report after every file.
    """
    work_queue: queue.Queue = queue.Queue()
    result_lock = threading.Lock()
    save_lock = threading.Lock()

    work = _build_whisper_worklist(to_process, whisper_all, retry_unresolved=retry_unresolved)
    for entry, und_audio in work:
        work_queue.put((entry, und_audio))

    actual_total = work_queue.qsize()
    if actual_total == 0:
        logging.info("  No audio tracks need whisper detection")
        return

    logging.info(
        f"  {actual_total} files to process (whisper ladder: tiny 3x30 -> tiny 5x30 -> small 5x30)"
    )

    tiny = _get_whisper_model("tiny")
    if not tiny:
        logging.error("  Failed to load whisper model")
        return

    completed = [0]

    def worker() -> None:
        while True:
            try:
                entry, audio_indices = work_queue.get(timeout=2)
            except queue.Empty:
                break

            filepath = entry["filepath"]
            duration = entry.get("duration_seconds", 0) or 120
            results: dict[int, tuple[Optional[str], float, str]] = {}

            for aidx in audio_indices:
                lang, conf, method = detect_audio_language_deep(
                    filepath, aidx, duration, min_confidence=min_confidence,
                )
                results[aidx] = (lang, conf, method)

            apply_detections: list[dict] = []
            with result_lock:
                for aidx, (lang, conf, method) in results.items():
                    _persist_whisper_detection(entry, aidx, lang, conf, method, min_confidence, stats)
                    if lang and lang != "und" and conf >= min_confidence:
                        apply_detections.append(
                            {
                                "track_type": "audio",
                                "stream_index": aidx,
                                "detected_language": lang,
                                "confidence": conf,
                            }
                        )
                completed[0] += 1
                c = completed[0]

            if apply_detections and filepath.lower().endswith(".mkv"):
                try:
                    apply_detections_for_file(filepath, apply_detections, min_confidence)
                except Exception:
                    pass

            if c % 5 == 0 or c == actual_total:
                logging.info(
                    f"  Whisper: {c}/{actual_total} "
                    f"({stats.get('detected', 0)} detected, {stats.get('failed', 0)} unresolved)"
                )

            with save_lock:
                _incremental_save(report, [entry])

            work_queue.task_done()

    threads = [threading.Thread(target=worker, daemon=True, name=f"whisper-{i}") for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    logging.info(
        f"  Whisper complete: {stats.get('detected', 0)} detected, {stats.get('failed', 0)} unresolved"
    )


def _run_deep_strategy(
    to_process: list[dict],
    whisper_all: bool,
    min_confidence: float,
    stats: dict,
) -> None:
    """Pass-based deep detection — 3 extractor threads feed GPU passes
    (tiny -> small -> medium). Samples extracted once, reused across passes.
    """
    from tools.report_lock import patch_report

    def _und_audio_tracks(entry: dict):
        for i, a in enumerate(entry.get("audio_streams", []) or []):
            lang = (a.get("language") or "und").lower().strip()
            detected = (a.get("detected_language") or "").lower().strip()
            if not whisper_all and detected and detected not in UND_LANGS:
                continue
            if whisper_all or lang in UND_LANGS:
                yield (entry, i, entry.get("duration_seconds", 0))

    work_items: list[tuple[dict, int, float]] = []
    for e in to_process:
        work_items.extend(_und_audio_tracks(e))

    total_tracks = len(work_items)
    logging.info(
        f"  Deep pass-based: {len(to_process)} files, {total_tracks} audio tracks to detect. "
        f"3 extractor threads feeding GPU passes."
    )

    patch_lock = threading.Lock()

    def _persist(entry: dict, aidx: int, lang: Optional[str], conf: float, method: str) -> None:
        _persist_whisper_detection(entry, aidx, lang, conf, method, min_confidence, stats)
        fp = entry.get("filepath")

        def _patch(r: dict, _fp=fp, _new=entry) -> None:
            for ent in r.get("files", []):
                if ent.get("filepath") == _fp:
                    ent["audio_streams"] = _new.get("audio_streams", ent.get("audio_streams"))
                    break

        try:
            with patch_lock:
                patch_report(_patch)
        except Exception as exc:
            logging.warning(f"  Patch failed for {os.path.basename(fp or '?')}: {exc}")

    extract_q: Queue = Queue(maxsize=6)

    def _extract_worker(items: list[tuple[dict, int, float]]) -> None:
        for entry, aidx, dur in items:
            try:
                samples = _extract_all_audio_samples(
                    entry.get("filepath", ""), [aidx], dur, sample_duration=30,
                )
                wavs = samples.get(aidx, [])
            except Exception as exc:
                logging.debug(f"  extract failed {entry.get('filename')}: {exc}")
                wavs = []
            extract_q.put((entry, aidx, dur, wavs))
        extract_q.put(None)

    extractor_count = 3
    chunks = [work_items[i::extractor_count] for i in range(extractor_count)]
    extractor_threads = [
        threading.Thread(target=_extract_worker, args=(c,), daemon=True) for c in chunks
    ]
    for t in extractor_threads:
        t.start()

    residuals_p2: list[tuple[dict, int, float, list[str]]] = []
    sentinels_seen = 0
    consumed = 0
    while sentinels_seen < extractor_count:
        item = extract_q.get()
        if item is None:
            sentinels_seen += 1
            continue
        entry, aidx, dur, wavs = item
        consumed += 1
        if not wavs:
            _persist(entry, aidx, "und", 0.0, "whisper_exhausted_no_samples")
            continue
        lang, conf = _run_whisper_on(wavs[:3], model_size="tiny")
        if lang and conf >= min_confidence:
            _persist(entry, aidx, lang, conf, "whisper_tiny_3x30")
            for w in wavs:
                _safe_remove(w)
        else:
            residuals_p2.append((entry, aidx, dur, wavs))

        if consumed % 25 == 0 or consumed == total_tracks:
            logging.info(
                f"  Pass 1: {consumed}/{total_tracks} attempted — "
                f"{stats.get('detected', 0)} resolved, "
                f"{len(residuals_p2)} queued for pass 2"
            )

    for t in extractor_threads:
        t.join(timeout=5)

    logging.info(f"  Pass 1 done — {len(residuals_p2)} residuals for pass 2 (small all-5)")

    residuals_p3: list[tuple[dict, int, float, list[str]]] = []
    for i, (entry, aidx, dur, wavs) in enumerate(residuals_p2, 1):
        lang, conf = _run_whisper_on(wavs, model_size="small")
        if lang and conf >= min_confidence:
            _persist(entry, aidx, lang, conf, "whisper_small_5x30")
            for w in wavs:
                _safe_remove(w)
        else:
            residuals_p3.append((entry, aidx, dur, wavs))
        if i % 25 == 0 or i == len(residuals_p2):
            logging.info(
                f"  Pass 2 (small): {i}/{len(residuals_p2)} attempted — "
                f"{len(residuals_p3)} residuals for pass 3"
            )

    logging.info(f"  Pass 2 done — {len(residuals_p3)} residuals for pass 3 (medium all-5)")

    exhausted: list[tuple[dict, int, float, list[str]]] = []
    for i, (entry, aidx, dur, wavs) in enumerate(residuals_p3, 1):
        lang, conf = _run_whisper_on(wavs, model_size="medium")
        if lang and conf >= min_confidence:
            _persist(entry, aidx, lang, conf, "whisper_medium_5x30")
        elif lang and conf > 0:
            _persist(entry, aidx, lang, conf, "whisper_medium_5x30_low_conf")
            exhausted.append((entry, aidx, dur, wavs))
        else:
            _persist(entry, aidx, "und", 0.0, "whisper_exhausted_medium")
            exhausted.append((entry, aidx, dur, wavs))
        for w in wavs:
            _safe_remove(w)
        if i % 10 == 0 or i == len(residuals_p3):
            logging.info(
                f"  Pass 3 (medium): {i}/{len(residuals_p3)} attempted — "
                f"{len(exhausted)} still ambiguous"
            )

    logging.info(
        f"  Deep pass-based complete — "
        f"{stats.get('detected', 0)} detected, "
        f"{stats.get('failed', 0)} failed/ambiguous, "
        f"{len(exhausted)} files where even whisper-medium-5x30 couldn't decide"
    )


def enrich_report(
    report: dict,
    use_whisper: bool = False,
    whisper_all: bool = False,
    workers: int = 6,
    min_confidence: float = 0.80,
    retry_unresolved: bool = False,
    deep: bool = False,
) -> dict:
    """Run language detection on all files and patch results into report entries.

    Strategy picked from flags:
      * ``deep=True``        -> pass-based GPU ladder (tiny -> small -> medium).
      * ``use_whisper=True`` -> 4-thread per-file whisper ladder on GPU.
      * otherwise            -> ThreadPoolExecutor of process_file (text/OCR).
    """
    files = report.get("files", [])

    if whisper_all:
        to_process = [e for e in files if e.get("audio_streams")]
    elif use_whisper:
        to_process = []
        for entry in files:
            has_und_audio = any(
                (a.get("language") or "und").lower().strip() in UND_LANGS
                and not (a.get("detection_method") == "whisper")
                for a in entry.get("audio_streams", [])
            )
            if has_und_audio:
                to_process.append(entry)
    else:
        to_process = []
        for entry in files:
            has_und = any(
                (s.get("language") or "und").lower().strip() in UND_LANGS
                and not s.get("detection_method")
                for streams in (entry.get("subtitle_streams", []), entry.get("audio_streams", []))
                for s in streams
            )
            if has_und:
                to_process.append(entry)

    if not to_process:
        logging.info("No tracks to process — skipping language detection")
        return report

    logging.info(
        f"Language detection: {len(to_process)} files to process (whisper={use_whisper}, all={whisper_all})"
    )
    if _find_tesseract():
        logging.info(f"  Tesseract: found at {_find_tesseract()}")
    else:
        logging.info("  Tesseract: not found — bitmap OCR disabled")

    stats = {"detected": 0, "failed": 0}

    if use_whisper and deep:
        logging.info("  Whisper: DEEP mode (3 extractor threads + GPU inference)")
        _run_deep_strategy(to_process, whisper_all, min_confidence, stats)
    elif use_whisper:
        logging.info("  Whisper: parallel mode (per-file worker pool)")
        _run_text_whisper_strategy(
            to_process, whisper_all, min_confidence, stats, report,
            retry_unresolved=retry_unresolved,
        )
    else:
        _run_text_strategy(to_process, workers, min_confidence, stats)

    logging.info(
        f"  Language detection complete: {stats['detected']} detected, {stats['failed']} unresolved"
    )

    _incremental_save(report, to_process)
    return report


# ---------------------------------------------------------------------------
# Applying detections back to files
# ---------------------------------------------------------------------------


def _probe_stream_counts(filepath: str) -> tuple[int, int, int] | None:
    """Return ``(video, audio, subtitle)`` stream counts for ``filepath``.

    Counts only real video streams (skips ``attached_pic`` cover art).
    Returns ``None`` on any ffprobe error or non-JSON output.
    """
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", filepath]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None
    if proc.returncode != 0:
        return None
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None
    video = audio = sub = 0
    for s in data.get("streams") or []:
        ctype = s.get("codec_type")
        disposition = s.get("disposition") or {}
        if ctype == "video":
            if disposition.get("attached_pic"):
                continue
            video += 1
        elif ctype == "audio":
            audio += 1
        elif ctype == "subtitle":
            sub += 1
    return video, audio, sub


def _apply_file_mkvpropedit(filepath: str, detections: list[dict]) -> tuple[int, int]:
    """Apply all language detections for a file in a single mkvpropedit call.

    Returns (applied_count, failed_count).
    """
    mkvprop = _find_mkvpropedit() or "mkvpropedit"
    args: list[str] = [mkvprop, filepath]
    for det in detections:
        track_type = det["track_type"]
        stream_index = det["stream_index"]
        language = to_iso2(det["detected_language"])
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
            err = proc.stderr.strip() or proc.stdout.strip()
            logging.error(f"  mkvpropedit failed for {Path(filepath).name}: {err}")
            return 0, len(detections)
    except subprocess.TimeoutExpired:
        logging.error(f"  mkvpropedit timed out for {Path(filepath).name}")
        return 0, len(detections)


def _apply_file_ffmpeg(filepath: str, detections: list[dict]) -> tuple[int, int]:
    """Apply all language detections for a file in a single ffmpeg -c copy call.

    Writes to a temp file then replaces the original.

    Safety contract:
    * ``-map 0`` is passed so ALL source streams are explicitly mapped.
    * Pre-probe source counts; post-probe staged output; refuse replacement
      if any count dropped (v/a/s regression is a hard fail).
    * Absolute zero-audio floor: refuse replacement if staged file has zero
      audio regardless of source count.

    Returns (applied_count, failed_count).
    """
    src_counts = _probe_stream_counts(filepath)
    if src_counts is None:
        logging.error(f"  source ffprobe failed for {Path(filepath).name}; refusing apply")
        return 0, len(detections)
    src_v, src_a, src_s = src_counts

    tmp_path = filepath + ".langfix_tmp.mkv"
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", filepath,
        "-map", "0",
        "-c", "copy",
    ]
    for det in detections:
        track_type = det["track_type"]
        stream_index = det["stream_index"]
        language = to_iso2(det["detected_language"])
        stream_type = "a" if track_type == "audio" else "s"
        cmd += [f"-metadata:s:{stream_type}:{stream_index}", f"language={language}"]
    cmd.append(tmp_path)

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if proc.returncode != 0:
            logging.error(f"  ffmpeg failed for {Path(filepath).name}: {proc.stderr.strip()}")
            return 0, len(detections)

        dst_counts = _probe_stream_counts(tmp_path)
        if dst_counts is None:
            logging.error(f"  post-verify ffprobe failed for {Path(filepath).name}; refusing replace")
            return 0, len(detections)
        dst_v, dst_a, dst_s = dst_counts
        if dst_a < 1:
            logging.error(
                f"  REFUSE replace: output has zero audio streams for {Path(filepath).name} "
                f"(src v={src_v} a={src_a} s={src_s}, dst v={dst_v} a={dst_a} s={dst_s})"
            )
            return 0, len(detections)
        if dst_v < src_v or dst_a < src_a or dst_s < src_s:
            logging.error(
                f"  REFUSE replace: stream regression for {Path(filepath).name} "
                f"(src v={src_v} a={src_a} s={src_s}, dst v={dst_v} a={dst_a} s={dst_s})"
            )
            return 0, len(detections)

        os.replace(tmp_path, filepath)
        for det in detections:
            logging.info(
                f"  Applied {to_iso2(det['detected_language'])} to {det['track_type']} "
                f"track {det['stream_index'] + 1}: {Path(filepath).name}"
            )
        return len(detections), 0
    except subprocess.TimeoutExpired:
        logging.error(f"  ffmpeg timed out for {Path(filepath).name}")
        return 0, len(detections)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def apply_detections_for_file(
    filepath: str,
    detections: list[dict],
    min_confidence: float = 0.80,
    use_mkvpropedit: bool = True,
) -> tuple[int, int]:
    """Apply language detections for a single file.

    Filters by min_confidence and skips failed/bitmap detections, then tries
    mkvpropedit (preferred — fast, in-place) for MKV files if available,
    falling back to ffmpeg -c copy (slower, no extra install required).

    ``use_mkvpropedit=False`` forces the ffmpeg path (used by tests that want
    to exercise the safety contract deterministically).
    """
    actionable = [
        d
        for d in detections
        if d.get("detected_language")
        and (d.get("confidence") or 0) >= min_confidence
        and d.get("method") not in ("bitmap_skipped", "text_extraction_failed")
    ]
    if not actionable:
        return 0, 0

    is_mkv = filepath.lower().endswith(".mkv")
    mkvprop_available = use_mkvpropedit and is_mkv and _find_mkvpropedit() is not None

    if not is_mkv:
        logging.info(f"  Non-MKV — skipping apply (tags saved in report): {Path(filepath).name}")
        return 0, 0

    if mkvprop_available:
        applied, failed = _apply_file_mkvpropedit(filepath, actionable)
        if failed and applied == 0:
            logging.warning(f"  Skipping (not a valid MKV): {Path(filepath).name}")
            return 0, 0
        return applied, failed
    else:
        logging.info(f"  mkvpropedit not found — falling back to ffmpeg: {Path(filepath).name}")
        return _apply_file_ffmpeg(filepath, actionable)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


Strategy = Literal["text", "text_whisper", "deep"]


def _run_spot_check(report: dict, sample_size: int) -> None:
    """Whisper spot-check: verify N random already-tagged tracks, report mismatches."""
    import random

    _get_whisper_model("tiny")
    candidates: list[tuple[dict, int, str]] = []
    for entry in report.get("files", []):
        for i, a in enumerate(entry.get("audio_streams", [])):
            lang = (a.get("language") or "und").lower().strip()
            if lang not in UND_LANGS:
                candidates.append((entry, i, lang))

    sample_size = min(sample_size, len(candidates))
    sample = random.sample(candidates, sample_size)
    logging.info(f"Spot-checking {sample_size} already-tagged audio tracks...")

    mismatches: list[dict] = []
    for idx, (entry, audio_idx, existing_tag) in enumerate(sample):
        if (idx + 1) % 20 == 0 or idx + 1 == sample_size:
            logging.info(f"  Progress: {idx + 1}/{sample_size} ({len(mismatches)} mismatches)")
        w_lang, w_conf = detect_audio_language_whisper(
            entry["filepath"], audio_idx, entry.get("duration_seconds", 0),
        )
        if w_lang and w_conf >= 0.5:
            w_iso = to_iso2(w_lang)
            existing_iso = to_iso2(existing_tag)
            if w_iso != existing_iso:
                mismatches.append({
                    "filename": entry["filename"],
                    "track": audio_idx,
                    "tagged_as": existing_tag,
                    "whisper_says": w_lang,
                    "confidence": w_conf,
                })

    logging.info(f"\nSpot-check complete: {len(mismatches)} mismatches out of {sample_size}")
    if mismatches:
        logging.info("\nMismatched tracks:")
        for m in mismatches:
            logging.info(
                f"  {m['filename']} track {m['track']}: "
                f"tagged '{m['tagged_as']}' but whisper says '{m['whisper_says']}' "
                f"(conf={m['confidence']:.2f})"
            )
    else:
        logging.info("All spot-checked tags match whisper detection.")


def _run_apply(report: dict, min_confidence: float) -> None:
    """Walk the report and apply every stored detection to the underlying MKV file."""
    from tools.report_lock import write_report

    logging.info("Applying detections to files via mkvpropedit/ffmpeg...")
    if _find_mkvpropedit():
        logging.info(f"  mkvpropedit: {_find_mkvpropedit()}")
    else:
        logging.info("  mkvpropedit: not found — using ffmpeg (slower)")

    total_applied = 0
    total_failed = 0
    file_count = 0

    for entry in report.get("files", []):
        detections: list[dict] = []
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
            entry["filepath"], detections, min_confidence,
        )
        total_applied += applied
        total_failed += failed_count

        if applied > 0:
            for det in detections:
                if det.get("confidence", 0) < min_confidence:
                    continue
                streams = entry.get(f"{det['track_type']}_streams", [])
                idx = det["stream_index"]
                if idx < len(streams):
                    streams[idx]["language"] = to_iso2(det["detected_language"])
                    streams[idx].pop("detected_language", None)
                    streams[idx].pop("detection_confidence", None)
                    streams[idx].pop("detection_method", None)

    write_report(report)
    logging.info(f"Applied: {total_applied}  Failed: {total_failed}")
    logging.info("Report updated")


def main() -> None:
    """CLI entry point for ``python -m pipeline.language``."""
    sys.stdout.reconfigure(line_buffering=True)
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)

    parser = argparse.ArgumentParser(
        description="Detect languages for undetermined subtitle/audio tracks",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Write detected languages back to MKV files using mkvpropedit/ffmpeg",
    )
    parser.add_argument(
        "--min-confidence", type=float, default=0.85,
        help="Minimum confidence to apply a detection (default: 0.85)",
    )
    parser.add_argument(
        "--whisper", action="store_true",
        help="Use faster-whisper for audio tracks that heuristics can't resolve",
    )
    parser.add_argument(
        "--whisper-all", action="store_true",
        help="Run whisper on ALL audio tracks (verify existing tags)",
    )
    parser.add_argument(
        "--deep", action="store_true",
        help=(
            "Pass-based escalation ladder — tiny 3x30 -> small 5x30 -> medium 5x30 "
            "across every und audio track. Needs the encoder stopped."
        ),
    )
    parser.add_argument(
        "--retry-unresolved", action="store_true",
        help="Retry previously unresolved whisper tracks",
    )
    parser.add_argument(
        "--spot-check", type=int, default=0, metavar="N",
        help="Whisper spot-check: verify N random already-tagged tracks",
    )
    parser.add_argument(
        "--workers", type=int, default=6,
        help="Parallel workers for subtitle extraction (default: 6)",
    )
    args = parser.parse_args()

    use_whisper = (
        args.whisper or args.whisper_all or args.spot_check > 0 or args.retry_unresolved or args.deep
    )

    if use_whisper:
        from paths import PIPELINE_STATE_DB

        db_path = str(PIPELINE_STATE_DB)
        if os.path.exists(db_path):
            try:
                import sqlite3

                conn = sqlite3.connect(db_path, timeout=5)
                conn.execute("PRAGMA journal_mode=WAL")
                encoding = conn.execute(
                    "SELECT COUNT(*) FROM pipeline_files WHERE status = 'processing' AND stage = 'encoding'"
                ).fetchone()[0]
                conn.close()
                if encoding:
                    logging.error(f"Pipeline is actively encoding {encoding} file(s) on GPU.")
                    logging.error(
                        "Whisper would compete for VRAM. Stop the pipeline first, or run without --whisper."
                    )
                    sys.exit(1)
            except Exception:
                pass

    from paths import MEDIA_REPORT
    from tools.report_lock import read_report

    try:
        report = read_report()
    except FileNotFoundError:
        logging.error(f"media_report.json not found at {MEDIA_REPORT}")
        sys.exit(1)

    if args.spot_check > 0:
        _run_spot_check(report, args.spot_check)
        return

    if args.apply:
        logging.info("Running language detection...")
        enrich_report(
            report,
            use_whisper=use_whisper,
            whisper_all=args.whisper_all,
            workers=args.workers,
            min_confidence=args.min_confidence,
            retry_unresolved=args.retry_unresolved,
            deep=args.deep,
        )
        report = read_report()
        _run_apply(report, args.min_confidence)
    else:
        enrich_report(
            report,
            use_whisper=use_whisper,
            whisper_all=args.whisper_all,
            workers=args.workers,
            min_confidence=args.min_confidence,
            retry_unresolved=args.retry_unresolved,
            deep=args.deep,
        )
        logging.info(f"Updated {MEDIA_REPORT}")


if __name__ == "__main__":
    main()
