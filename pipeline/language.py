"""Language detection for subtitle and audio tracks.
Text extraction + langdetect for text subs, Tesseract OCR for bitmap subs,
faster-whisper for audio. Extracted from tools/detect_languages.py."""

import logging
import os
import re
import shutil
import subprocess
from typing import Optional

from paths import STAGING_DIR

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
        "300",  # first 5 minutes is enough for language detection
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
    sample_frames: int = 500,
    max_chars: int = 8000,
) -> Optional[str]:
    """Extract text from a bitmap subtitle stream (PGS/DVD) via ffmpeg + Tesseract OCR.

    Uses 500 frames by default for reliable language detection across varied content.

    Extracts the first N subtitle frames as PNG images, runs Tesseract on each,
    and aggregates the text for language detection.

    Returns stripped text or None on failure / if Tesseract is not installed.
    """
    tesseract = _find_tesseract()
    if not tesseract:
        return None

    import uuid

    tmp_dir = os.path.join(str(STAGING_DIR), "ocr_tmp", f"{uuid.uuid4().hex[:8]}_{sub_stream_index}")
    os.makedirs(tmp_dir, exist_ok=True)

    try:
        # Extract subtitle frames as images
        pattern = os.path.join(tmp_dir, "sub_%04d.png")
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
            "300",  # first 5 minutes
            "-frames:v",
            str(sample_frames),
            pattern,
        ]
        try:
            subprocess.run(cmd, capture_output=True, timeout=120)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None

        # Find extracted images
        images = sorted(f for f in os.listdir(tmp_dir) if f.endswith(".png"))[:sample_frames]

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

    hangul = sum(1 for c in text if "\uac00" <= c <= "\ud7af" or "\u1100" <= c <= "\u11ff")
    hiragana = sum(1 for c in text if "\u3040" <= c <= "\u309f")
    katakana = sum(1 for c in text if "\u30a0" <= c <= "\u30ff")
    cjk_unified = sum(1 for c in text if "\u4e00" <= c <= "\u9fff" or "\u3400" <= c <= "\u4dbf")
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


# ---------------------------------------------------------------------------
# Whisper-based audio language detection
# ---------------------------------------------------------------------------

_whisper_tiny = None
_whisper_small = None


def _assert_encoder_not_running() -> None:
    """Raise if the NVENC encoder is currently active — whisper on GPU + NVENC
    on the same chip triggered the 2026-04-21 BSOD. Caller must stop the pipeline
    before invoking the whisper ladder. Sample extraction (CPU/ffmpeg-decode) is
    safe while encoder is running; only whisper inference is the exclusive-GPU step.

    The gap_filler-only pipeline is fine — it doesn't use the GPU, just remote
    mkvmerge over SSH. Only block if the FULL pipeline (with GPU workers) is running.
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
        pass  # backend not reachable, assume stand-alone run


def _get_whisper_model(size: str = "tiny"):
    """Lazy-load a faster-whisper model on GPU (CUDA / float16).

    Falls back to CPU on load failure but the expectation is GPU — tiny at
    ~0.05s/sample on GPU, small at ~0.15s/sample. The encoder pipeline must be
    stopped before running whisper (see _assert_encoder_not_running) because
    NVENC + whisper on the same RTX 4080 triggered a kernel BSOD on 2026-04-21.
    """
    global _whisper_tiny, _whisper_small
    ref = _whisper_tiny if size == "tiny" else _whisper_small
    if ref is not None:
        return ref

    _assert_encoder_not_running()

    # CTranslate2 was built against CUDA 12 but this machine has CUDA 13.2 —
    # we install cublas64_12.dll + cudnn*.dll via pip (nvidia-cublas-cu12 /
    # nvidia-cudnn-cu12) into the venv. But the loader won't see them until we
    # explicitly add the dirs to the DLL search path. Do that before import.
    try:
        import site
        for base in site.getsitepackages():
            for sub in ("nvidia/cublas/bin", "nvidia/cudnn/bin"):
                dll_dir = os.path.join(base, *sub.split("/"))
                if os.path.isdir(dll_dir):
                    if hasattr(os, "add_dll_directory"):
                        os.add_dll_directory(dll_dir)
                    # Also prepend to PATH as a belt-and-braces measure
                    if dll_dir not in os.environ.get("PATH", ""):
                        os.environ["PATH"] = dll_dir + os.pathsep + os.environ.get("PATH", "")
    except Exception as e:
        logging.debug(f"Couldn't add nvidia DLL dirs: {e}")

    force_cpu = os.environ.get("WHISPER_FORCE_CPU", "").strip() in {"1", "true", "yes"}

    model = None
    if not force_cpu:
        try:
            from faster_whisper import WhisperModel
            model = WhisperModel(size, device="cuda", compute_type="float16")
            # Warmup: run a 0.5s silent sample to trigger any late CUDA errors.
            # If cublas/cudnn DLLs are missing, this raises; without this we
            # discover it mid-sweep and the error path deadlocks the consumer.
            import tempfile, subprocess
            with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tf:
                silent_wav = tf.name
            try:
                subprocess.run(
                    ["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
                     "-i", "anullsrc=r=16000:cl=mono", "-t", "0.5", "-y", silent_wav],
                    capture_output=True, timeout=10,
                )
                _segs, _info = model.transcribe(
                    silent_wav, beam_size=1, best_of=1, language=None, without_timestamps=True,
                )
                # Force eager evaluation — errors may only surface on iteration
                try:
                    next(iter(_segs))
                except StopIteration:
                    pass
                logging.info(f"Loaded faster-whisper model ({size}, cuda/float16) — warmup OK")
            finally:
                try: os.unlink(silent_wav)
                except OSError: pass
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
    else:
        _whisper_small = model
    return model


def _extract_samples_at_offsets(
    filepath: str,
    audio_stream_index: int,
    offsets_secs: list[int],
    sample_duration: int,
) -> list[str]:
    """Extract audio samples at EXPLICIT time offsets — used by the deep-detect
    ladder that needs 10 samples at 5%/10%/.../50% of a film's duration.

    Single ffmpeg call, all samples come from one file open.
    """
    tmp_dir = os.path.join(str(STAGING_DIR), "whisper_tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    base = f"{os.getpid()}_{audio_stream_index}"
    paths: list[str] = []
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", filepath]
    for i, off in enumerate(offsets_secs):
        wav = os.path.join(tmp_dir, f"{base}_deep_{i}.wav")
        paths.append(wav)
        cmd.extend([
            "-ss", str(off),
            "-t", str(sample_duration),
            "-map", f"0:a:{audio_stream_index}",
            "-ac", "1", "-ar", "16000", "-y", wav,
        ])
    try:
        subprocess.run(cmd, capture_output=True, timeout=300)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return [p for p in paths if os.path.exists(p) and os.path.getsize(p) > 10_000]


def detect_audio_language_deep(
    filepath: str,
    audio_stream_index: int,
    duration_secs: float,
    min_confidence: float = 0.8,
    enable_deep_sweep: bool = False,
) -> tuple[Optional[str], float, str]:
    """Progressive-effort detection ladder on GPU (whisper cuda/float16).

    Steps (all on same pre-extracted 5 × 30s samples):
        1. 3 × 30s, whisper-tiny
        2. 5 × 30s, whisper-tiny (all samples)
        3. 5 × 30s, whisper-SMALL

    enable_deep_sweep=True adds step 4:
        4. 10 × 120s at 5/10/.../50% of runtime, whisper-small.

    Step 4 is an ADDITIONAL ~15-30 min per REMUX file on SMB — don't run it
    automatically across the whole library. Use it as a surgical follow-up on
    files that stopped at `whisper_small_5x30_low_conf` in the first pass.

    Returns (lang, confidence, method). Logs timing per file so you can see
    which files got stuck where. Caller MUST stop the NVENC encoder first.
    """
    import time as _time

    name = os.path.basename(filepath)
    t0 = _time.monotonic()

    # --- Extract once: 5 × 30s spread samples. Used for steps 1, 2, 3. ---
    samples = _extract_all_audio_samples(
        filepath, [audio_stream_index], duration_secs, sample_duration=30
    )
    wavs = samples.get(audio_stream_index, [])
    t_extract = _time.monotonic() - t0
    if not wavs:
        logging.info(f"    {name} a:{audio_stream_index} — extraction failed ({t_extract:.1f}s)")
        return "und", 0.0, "whisper_exhausted"

    try:
        lang, conf = _run_whisper_on(wavs[:3], model_size="tiny")
        if lang and conf >= min_confidence:
            logging.info(f"    {name} a:{audio_stream_index} → {lang} ({conf:.2f}) tiny_3x30  {_time.monotonic() - t0:.1f}s")
            return lang, conf, "whisper_tiny_3x30"

        if len(wavs) > 3:
            lang2, conf2 = _run_whisper_on(wavs, model_size="tiny")
            if lang2 and conf2 >= min_confidence:
                logging.info(f"    {name} a:{audio_stream_index} → {lang2} ({conf2:.2f}) tiny_5x30  {_time.monotonic() - t0:.1f}s")
                return lang2, conf2, "whisper_tiny_5x30"

        lang3, conf3 = _run_whisper_on(wavs, model_size="small")
        if lang3 and conf3 >= min_confidence:
            logging.info(f"    {name} a:{audio_stream_index} → {lang3} ({conf3:.2f}) small_5x30  {_time.monotonic() - t0:.1f}s")
            return lang3, conf3, "whisper_small_5x30"
    finally:
        for w in wavs:
            _safe_remove(w)

    # Step 3 gave a best-guess but below threshold — record it even if we don't
    # escalate to the expensive step 4.
    if not enable_deep_sweep:
        if lang3 and conf3 > 0:
            logging.info(f"    {name} a:{audio_stream_index} → {lang3} ({conf3:.2f}) small_5x30_low_conf  {_time.monotonic() - t0:.1f}s")
            return lang3, conf3, "whisper_small_5x30_low_conf"
        logging.info(f"    {name} a:{audio_stream_index} → und (step 3 exhausted)  {_time.monotonic() - t0:.1f}s")
        return "und", 0.0, "whisper_exhausted_step3"

    # --- Step 4: deep sweep (opt-in via enable_deep_sweep) ---
    pct_offsets = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50]
    if duration_secs >= 1200:
        sample_dur = 120
    else:
        sample_dur = max(5, int(duration_secs * 0.66 / len(pct_offsets)))
    offsets = [max(0, int(duration_secs * p)) for p in pct_offsets]
    t_step4 = _time.monotonic()
    wavs4 = _extract_samples_at_offsets(filepath, audio_stream_index, offsets, sample_dur)
    try:
        lang4, conf4 = _run_whisper_on(wavs4, model_size="small")
        if lang4 and conf4 > 0:
            method = "whisper_small_deep" if conf4 >= min_confidence else "whisper_small_deep_low_conf"
            logging.info(f"    {name} a:{audio_stream_index} → {lang4} ({conf4:.2f}) {method}  step4 {_time.monotonic() - t_step4:.1f}s, total {_time.monotonic() - t0:.1f}s")
            return lang4, conf4, method
    finally:
        for w in wavs4:
            _safe_remove(w)

    logging.info(f"    {name} a:{audio_stream_index} → und (fully exhausted)  {_time.monotonic() - t0:.1f}s")
    return "und", 0.0, "whisper_exhausted"


def _run_whisper_on(wavs: list[str], model_size: str) -> tuple[Optional[str], float]:
    """Run whisper on a list of wav samples and majority-vote the result."""
    if not wavs:
        return None, 0.0
    model = _get_whisper_model(model_size)
    if not model:
        return None, 0.0
    detections = []
    for wav in wavs:
        try:
            lang, prob = _whisper_detect_one(model, wav)
            if lang:
                detections.append((lang, prob))
        except Exception as e:
            logging.debug(f"Whisper {model_size} error on {os.path.basename(wav)}: {e}")
    return _majority_vote(detections)


def _safe_remove(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def _extract_all_audio_samples(
    filepath: str,
    audio_indices: list[int],
    duration_secs: float,
    sample_duration: int = 30,
) -> dict[int, list[str]]:
    """Extract audio samples for ALL tracks of a file in one ffmpeg call.

    5 x 30s samples from different points for reliable language detection.
    Single file open over SMB regardless of how many tracks.
    """
    tmp_dir = os.path.join(str(STAGING_DIR), "whisper_tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    base = f"{os.getpid()}"

    total = max(duration_secs, 120)
    offsets = [
        int(min(60, total * 0.05)),
        int(total * 0.2),
        int(total * 0.4),
        int(total * 0.6),
        int(total * 0.8),
    ]

    result: dict[int, list[str]] = {}
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", filepath]

    for aidx in audio_indices:
        paths = []
        for si, offset in enumerate(offsets):
            wav_path = os.path.join(tmp_dir, f"{base}_a{aidx}_s{si}.wav")
            paths.append(wav_path)
            cmd.extend(
                [
                    "-ss",
                    str(offset),
                    "-t",
                    str(sample_duration),
                    "-map",
                    f"0:a:{aidx}",
                    "-ac",
                    "1",
                    "-ar",
                    "16000",
                    "-y",
                    wav_path,
                ]
            )
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
    """Run whisper language detection on a single WAV sample.

    Uses transcribe with early break — on CPU with tiny model this takes ~0.35s per sample.
    """
    try:
        segments, info = model.transcribe(wav_path, beam_size=1, best_of=1, language=None, without_timestamps=True)
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
    """Detect language using whisper-tiny with multi-sample majority vote.

    1. Extract 3 x 10s samples (one ffmpeg call)
    2. Run whisper-tiny on first sample — if high confidence, done
    3. If low confidence, run tiny on remaining samples + majority vote

    No small model escalation — tiny at 0.35s/sample on CPU is fast enough.
    Three agreeing samples compensate for lower per-sample confidence.

    Returns (lang_code, confidence) or (None, 0.0) on failure.
    """
    samples = _extract_all_audio_samples(filepath, [audio_stream_index], duration_secs)
    wav_paths = samples.get(audio_stream_index, [])
    if not wav_paths:
        return None, 0.0

    try:
        tiny = _get_whisper_model("tiny")
        if not tiny:
            return None, 0.0

        # Run all 5 samples for reliable majority vote
        detections = []
        for wav_path in wav_paths:
            l, p = _whisper_detect_one(tiny, wav_path)
            if l:
                detections.append((l, p))

        return _majority_vote(detections)

    except Exception as e:
        logging.debug(f"Whisper detection failed for {os.path.basename(filepath)} a:{audio_stream_index}: {e}")
        return None, 0.0
    finally:
        for wav_path in wav_paths:
            try:
                os.remove(wav_path)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# High-level convenience function
# ---------------------------------------------------------------------------


def detect_all_languages(file_entry: dict, use_whisper: bool = False) -> dict:
    """Detect languages for all undetermined tracks in a file entry.

    Returns dict with updated audio_streams and subtitle_streams
    containing detected_language, detection_confidence, detection_method fields.
    """
    import copy

    entry = copy.deepcopy(file_entry)
    filepath = entry["filepath"]

    # --- Pass 1: Text subtitle extraction ---
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
            ocr_text = extract_bitmap_subtitle_text(filepath, sub_all_idx)
            if ocr_text:
                detected, confidence = detect_language(ocr_text)
                if detected and detected != "und" and confidence >= 0.5:
                    stream["detected_language"] = detected
                    stream["detection_confidence"] = confidence
                    stream["detection_method"] = "ocr_extraction"
                    detected_text_langs[sub_all_idx] = detected

    # --- Pass 2: Audio tracks ---
    for audio_idx, stream in enumerate(entry.get("audio_streams", [])):
        lang = (stream.get("language") or "und").lower().strip()
        if lang not in UND_LANGS:
            continue

        # Heuristic: track title hints
        title = (stream.get("title") or "").lower()
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

        # Heuristic: sole audio track + unanimous subtitle language
        audio_streams = entry.get("audio_streams", [])
        subs = entry.get("subtitle_streams", [])
        if len(audio_streams) == 1:
            sub_langs = set()
            for s in subs:
                sl = (s.get("language") or "und").lower().strip()
                if sl not in UND_LANGS:
                    sub_langs.add(sl)
            for idx, sl_detected in detected_text_langs.items():
                sub_langs.add(sl_detected)
            if len(sub_langs) == 1:
                inferred = next(iter(sub_langs))
                stream["detected_language"] = inferred
                stream["detection_confidence"] = 0.9
                stream["detection_method"] = "heuristic"
                continue

        # Whisper fallback
        if use_whisper:
            w_lang, w_conf = detect_audio_language_whisper(filepath, audio_idx, entry.get("duration_seconds", 0))
            if w_lang and w_conf > 0.5:
                stream["detected_language"] = w_lang
                stream["detection_confidence"] = w_conf
                stream["detection_method"] = "whisper"

    return entry
