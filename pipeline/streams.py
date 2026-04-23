"""Stream introspection + selection for audio/subtitle tracks.

Single source of truth for:
  - parsing ffprobe stream dicts into typed AudioStream/SubStream dataclasses
  - detecting hearing-impaired (HI/SDH) subtitles from disposition + title
  - detecting HI sidecar subtitles from filename tokens
  - normalising codec / language strings
  - selecting which audio + subtitle indices to keep under the library's
    "original + English/und" policy

Consolidates logic previously duplicated across:
  - pipeline/ffmpeg.py:_select_audio_streams, _map_subtitle_streams
  - pipeline/gap_filler.py:analyse_gaps (audio + sub selection blocks)
  - pipeline/full_gamut.py:finalize_upload compliance iteration
  - tools/compliance.py (audio/sub compliance, _is_hi, _is_hi_external)
  - tools/mux_external_subs.py (_is_hi_ext, _is_hi_int)

NOTE ON SEMANTIC DIFFERENCES preserved here (documented, not fixed):
  - Internal HI detection: this module follows compliance.py / mux_external_subs.py
    in checking BOTH disposition flags (hearing_impaired / captions) AND the title
    regex `\\b(hi|sdh|hearing|cc|closed.caption)\\b`. The older ffmpeg.py and
    gap_filler.py versions did a looser title substring check
    (`"hearing" in title or "sdh" in title or ".hi" in title`), which would not
    catch `cc` or `closed.caption` but would false-positive on substring matches.
    The stricter regex is the safer default; any file that previously slipped
    through the looser check is still handled correctly by the stricter one.
  - External HI detection: follows the compliance.py token split on `filename.split(".")`
    with `{"hi", "sdh", "cc"}`. The ffmpeg.py / gap_filler.py / full_gamut.py variants
    used a simple `".hi." in basename or ".sdh." in basename` substring check
    which doesn't catch `cc` and can false-positive on titles containing those
    literal strings.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from pipeline.config import ENG_LANGS, KEEP_LANGS

# ---------------------------------------------------------------------------
# HI (hearing-impaired) detection
# ---------------------------------------------------------------------------

# Title regex matching compliance.py / mux_external_subs.py (strict, word-boundary).
_HI_TITLE_RE = re.compile(r"\b(hi|sdh|hearing|cc|closed.caption)\b", re.IGNORECASE)

# Tokens that signal a hearing-impaired sidecar. Matches compliance._is_hi_external
# (checks tokens in filename.split(".")[1:-1]).
_HI_SIDECAR_TOKENS: frozenset[str] = frozenset({"hi", "sdh", "cc"})


def is_hi_internal(sub: dict[str, Any]) -> bool:
    """Return True if an internal subtitle stream is hearing-impaired.

    Checks disposition flags (hearing_impaired, captions) first, then falls back
    to a word-boundary regex on the track title.
    """
    disp = sub.get("disposition") or {}
    if disp.get("hearing_impaired") or disp.get("captions"):
        return True
    title = (sub.get("title") or "").lower()
    return bool(_HI_TITLE_RE.search(title))


def is_hi_external(sidecar_filename: str) -> bool:
    """Return True if an external sidecar filename contains HI/SDH/CC tokens.

    Parses dot-separated parts between the stem and the extension:
    e.g. ``Show.en.hi.srt`` -> parts ``["en", "hi"]`` -> True.
    """
    parts = (sidecar_filename or "").lower().split(".")
    return any(p in _HI_SIDECAR_TOKENS for p in parts[1:-1])


# ---------------------------------------------------------------------------
# Codec / language normalisation
# ---------------------------------------------------------------------------

# Map raw ffprobe codec names / common spellings to a canonical lowercase key.
_CODEC_ALIAS: dict[str, str] = {
    "e-ac-3": "eac3",
    "eac-3": "eac3",
    "ac-3": "ac3",
    "h.264": "h264",
    "h.265": "h265",
    "avc": "h264",
    "avc1": "h264",
    "hevc": "h265",
    "dts-hd ma": "dts-hd ma",
    "dts-hd.ma": "dts-hd ma",
}


def normalise_codec(raw: str | None) -> str:
    """Normalise a codec string to canonical lowercase form.

    Examples:
        ``"E-AC-3"`` -> ``"eac3"``
        ``"H.264"`` -> ``"h264"``
        ``""`` / ``None`` -> ``""``
    """
    if not raw:
        return ""
    key = raw.strip().lower()
    return _CODEC_ALIAS.get(key, key)


# Map common English variants (including a handful of full names) to a canonical
# short code. Used only inside normalise_language — callers wanting "is English?"
# should use ``KEEP_LANGS`` / ``ENG_LANGS`` from pipeline.config.
_LANG_ALIAS: dict[str, str] = {
    "english": "eng",
    "french": "fre",
    "german": "ger",
    "spanish": "spa",
    "italian": "ita",
    "japanese": "jpn",
    "korean": "kor",
    "chinese": "chi",
    "russian": "rus",
    "portuguese": "por",
    "dutch": "dut",
    "arabic": "ara",
    "hindi": "hin",
}


def normalise_language(raw: str | None) -> str:
    """Normalise a language string to lowercase, mapping full names to ISO codes.

    Examples:
        ``"EN"`` -> ``"en"``
        ``"English"`` -> ``"eng"``
        ``None`` -> ``""``
    """
    if not raw:
        return ""
    key = raw.strip().lower()
    return _LANG_ALIAS.get(key, key)


# ---------------------------------------------------------------------------
# Typed stream records
# ---------------------------------------------------------------------------


@dataclass
class AudioStream:
    """Parsed view of an audio stream.

    Codec is normalised (e.g. ``eac3`` not ``e-ac-3``).
    Language is lowercase; ``""`` or ``und`` for unknown.
    """

    index: int
    codec: str
    language: str
    channels: int
    channel_layout: str
    bitrate_kbps: int | None
    lossless: bool
    detected_language: str | None


@dataclass
class SubStream:
    """Parsed view of a subtitle stream.

    ``is_hi`` follows :func:`is_hi_internal` (disposition + title regex).
    """

    index: int
    codec: str
    language: str
    title: str
    is_forced: bool
    is_hi: bool
    detected_language: str | None


# Codecs treated as lossless — matches scanner.py and compliance.py usage.
_LOSSLESS_CODECS: frozenset[str] = frozenset(
    {"truehd", "flac", "pcm_s16le", "pcm_s24le", "pcm_s32le", "dts"}
)


def parse_audio_stream(raw: dict[str, Any], index: int = 0) -> AudioStream:
    """Parse a raw ffprobe / media-report audio stream dict into an AudioStream."""
    codec_raw = raw.get("codec_raw") or raw.get("codec", "") or ""
    codec = normalise_codec(codec_raw)

    # Language can be directly on the stream (media-report format) or nested
    # under tags (ffprobe raw format).
    lang = raw.get("language")
    if lang is None:
        lang = (raw.get("tags") or {}).get("language")
    language = (lang or "").lower().strip()

    channels = int(raw.get("channels", 0) or 0)
    channel_layout = raw.get("channel_layout", "") or ""

    bitrate = raw.get("bitrate_kbps")
    if bitrate is None and raw.get("bit_rate"):
        try:
            bitrate = int(raw["bit_rate"]) // 1000
        except (TypeError, ValueError):
            bitrate = None

    # Lossless detection: scanner.py's heuristic — codec in lossless set OR profile
    # contains HD MA.
    profile = (raw.get("profile") or "").lower()
    lossless = bool(raw.get("lossless"))
    if not lossless:
        lossless = codec in _LOSSLESS_CODECS or "hd ma" in profile or "hd-ma" in profile

    return AudioStream(
        index=index,
        codec=codec,
        language=language,
        channels=channels,
        channel_layout=channel_layout,
        bitrate_kbps=int(bitrate) if bitrate is not None else None,
        lossless=lossless,
        detected_language=raw.get("detected_language"),
    )


def parse_sub_stream(raw: dict[str, Any], index: int = 0) -> SubStream:
    """Parse a raw ffprobe / media-report subtitle stream dict into a SubStream."""
    codec = (raw.get("codec") or raw.get("codec_name") or "").strip().lower()

    lang = raw.get("language")
    if lang is None:
        lang = (raw.get("tags") or {}).get("language")
    language = (lang or "").lower().strip()

    title = raw.get("title") or ""
    if not title:
        title = (raw.get("tags") or {}).get("title", "") or ""

    title_lower = title.lower()
    is_forced = "forced" in title_lower or "foreign" in title_lower
    if not is_forced:
        # Check disposition for forced flag as well
        disp = raw.get("disposition") or {}
        if disp.get("forced"):
            is_forced = True

    return SubStream(
        index=index,
        codec=codec,
        language=language,
        title=title,
        is_forced=is_forced,
        is_hi=is_hi_internal(raw),
        detected_language=raw.get("detected_language"),
    )


# ---------------------------------------------------------------------------
# Selection policies
# ---------------------------------------------------------------------------


def select_audio_keep_indices(
    streams: list[AudioStream],
    keep_langs: set[str] | None = None,
) -> list[int]:
    """Return the audio stream indices to keep under the library's "original + english/und" rule.

    Always keeps stream 0 (original language). Also keeps any stream whose
    language is in ``keep_langs`` (defaults to :data:`pipeline.config.KEEP_LANGS`).

    Callers decide whether the resulting list implies a strip:
      - ``len(kept) < len(streams)`` — strip needed
      - ``len(kept) >= len(streams)`` — no-op, everything kept

    NOTE: This function does NOT implement the ffmpeg.py historical short-circuit
    of ``return None`` when ``len(streams) <= 2``. Callers that want that behaviour
    (skip stripping 1-2 tracks as "not worth it") must check it themselves.
    Similarly, gap_filler's historical short-circuit was ``len(streams) > 1``.
    Keeping this module single-purpose avoids conflating those policies.
    """
    if keep_langs is None:
        keep_langs = KEEP_LANGS

    if not streams:
        return []

    keep: set[int] = {0}  # always keep stream 0 (original language)
    for s in streams:
        lang = s.language or (s.detected_language or "")
        lang = lang.lower().strip()
        if lang in keep_langs:
            keep.add(s.index)

    return sorted(keep)


def select_sub_keep_indices(
    streams: list[SubStream],
    eng_langs: set[str] | None = None,
) -> list[int]:
    """Return subtitle stream indices to keep.

    Policy:
      - ALWAYS keep forced / foreign-parts subs (any language).
      - KEEP the first regular English non-HI sub.
      - STRIP everything else (HI, non-English, duplicate English, und).

    ``eng_langs`` defaults to :data:`pipeline.config.ENG_LANGS`.
    """
    if eng_langs is None:
        eng_langs = ENG_LANGS

    keep: list[int] = []
    found_regular_eng = False
    for s in streams:
        if s.is_forced:
            keep.append(s.index)
            continue
        lang = s.language or (s.detected_language or "")
        lang = lang.lower().strip()
        if lang in eng_langs and not s.is_hi and not found_regular_eng:
            keep.append(s.index)
            found_regular_eng = True
        # else: strip (HI, non-English, duplicate English, und)

    return keep
