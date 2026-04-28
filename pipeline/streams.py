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


# ISO 639-1 (TMDb) -> ISO 639-2/B (ffprobe) for the languages we actually see.
# Used by :func:`tmdb_keeper_langs` to accept both forms when a file's TMDb
# record says e.g. "ja" but the audio stream is tagged "jpn".
_ISO1_TO_ISO2: dict[str, str] = {
    "en": "eng",
    "ja": "jpn",
    "ko": "kor",
    "zh": "chi",
    "fr": "fre",
    "de": "ger",
    "es": "spa",
    "it": "ita",
    "pt": "por",
    "ru": "rus",
    "sv": "swe",
    "no": "nor",
    "da": "dan",
    "fi": "fin",
    "nl": "dut",
    "pl": "pol",
    "cs": "cze",
    "hu": "hun",
    "tr": "tur",
    "ar": "ara",
    "hi": "hin",
    "th": "tha",
    "he": "heb",
    "el": "gre",
}


def tmdb_keeper_langs(tmdb_original_language: str | None) -> set[str] | None:
    """Return the set of language codes acceptable as audio for a given TMDb original_language.

    Always includes ``""`` and ``"und"`` (unknown is acceptable). If ``tmdb_original_language``
    is a known ISO 639-1 code, the corresponding ISO 639-2 form is added so ffprobe-tagged
    streams (which typically use ISO 639-2) match.

    Returns ``None`` when ``tmdb_original_language`` is empty/unknown - callers should treat
    ``None`` as "permissive: accept any language" since no ground truth is available.
    """
    if not tmdb_original_language:
        return None
    orig = tmdb_original_language.strip().lower()
    if not orig:
        return None
    keepers: set[str] = {"und", "", orig}
    iso2 = _ISO1_TO_ISO2.get(orig)
    if iso2:
        keepers.add(iso2)
    return keepers


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
_LOSSLESS_CODECS: frozenset[str] = frozenset({"truehd", "flac", "pcm_s16le", "pcm_s24le", "pcm_s32le", "dts"})


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


# ISO 639-1 ↔ 639-2/3 ↔ name equivalence buckets. Used by the
# original-language selector so a TMDb `es` matches an MKV `spa` matches a
# whisper-detected `Spanish`. Mirrors qualify._ISO1_EQUIV — kept in sync there.
_ORIG_LANG_BUCKETS: dict[str, frozenset[str]] = {
    "en": frozenset({"en", "eng", "english"}),
    "sv": frozenset({"sv", "swe", "swedish"}),
    "nl": frozenset({"nl", "nld", "dut", "dutch"}),
    "de": frozenset({"de", "deu", "ger", "german"}),
    "fr": frozenset({"fr", "fra", "fre", "french"}),
    "es": frozenset({"es", "spa", "spanish"}),
    "it": frozenset({"it", "ita", "italian"}),
    "ja": frozenset({"ja", "jpn", "japanese"}),
    "ko": frozenset({"ko", "kor", "korean"}),
    "zh": frozenset({"zh", "cn", "chi", "zho", "yue", "cmn", "chinese", "mandarin", "cantonese"}),
    "pt": frozenset({"pt", "por", "portuguese"}),
    "ru": frozenset({"ru", "rus", "russian"}),
    "ar": frozenset({"ar", "ara", "arabic"}),
    "hi": frozenset({"hi", "hin", "hindi"}),
    "no": frozenset({"no", "nor", "nob", "nno", "norwegian", "bokmål", "bokmal", "nynorsk"}),
    "da": frozenset({"da", "dan", "danish"}),
    "fi": frozenset({"fi", "fin", "finnish"}),
    "pl": frozenset({"pl", "pol", "polish"}),
    "cs": frozenset({"cs", "ces", "cze", "czech"}),
    "tr": frozenset({"tr", "tur", "turkish"}),
    "he": frozenset({"he", "heb", "hebrew"}),
    "th": frozenset({"th", "tha", "thai"}),
    "vi": frozenset({"vi", "vie", "vietnamese"}),
    "el": frozenset({"el", "ell", "gre", "greek"}),
}

_UND_TOKENS: frozenset[str] = frozenset({"", "und", "unk", "undetermined"})


def _stream_lang_resolved(stream) -> bool:
    """Return True if a stream's language is known (tagged or whisper-detected).

    Used by the strip-eligibility predicates below to enforce the inviolate
    rule (2026-04-29): never strip a track whose language we haven't actually
    identified. A stream is considered resolved when EITHER:
      * ``language`` is set to a non-und value, OR
      * ``detected_language`` is set to a non-und value

    Accepts both AudioStream/SubStream dataclasses and raw ffprobe dicts —
    callers in gap_filler use dicts; the internal selectors use dataclasses.
    """
    if stream is None:
        return False
    # Dataclass attribute access
    lang = getattr(stream, "language", None)
    detected = getattr(stream, "detected_language", None)
    if lang is None and detected is None and isinstance(stream, dict):
        lang = stream.get("language")
        detected = stream.get("detected_language")
    lang = (lang or "").lower().strip()
    detected = (detected or "").lower().strip()
    if lang and lang not in _UND_TOKENS:
        return True
    if detected and detected not in _UND_TOKENS:
        return True
    return False


def all_languages_known(streams) -> bool:
    """Return True only when every stream in ``streams`` has a resolved language.

    A False return means at least one track is `und`/empty with no whisper
    detection — the strip code MUST defer in that case (inviolate rule).
    Empty input returns True (nothing to be uncertain about).
    """
    return all(_stream_lang_resolved(s) for s in (streams or []))


def _lang_in_bucket(lang: str, target: str) -> bool:
    """True if ``lang`` belongs to the same equivalence bucket as ``target``.

    Both arguments are lowercased and stripped. Empty / und / unk values
    match nothing — caller must decide what to do with unknowns.
    """
    a = (lang or "").lower().strip()
    b = (target or "").lower().strip()
    if not a or not b or a in _UND_TOKENS or b in _UND_TOKENS:
        return False
    if a == b:
        return True
    for codes in _ORIG_LANG_BUCKETS.values():
        if a in codes and b in codes:
            return True
    return False


def select_audio_keep_indices_by_original_language(
    streams: list[AudioStream],
    original_language: str | None,
    *,
    keep_english_too: bool = False,
) -> list[int] | None:
    """Keep only audio tracks whose language matches TMDb ``original_language``.

    This is the "strict" rule: foreign dubs (including English dubs of
    foreign-origin films) are stripped. Used by the audio-keep policy
    ``"original_language"``. Behaviour:

      * ``original_language is None`` (no TMDb data): return None — caller
        falls back to legacy KEEP_LANGS rule rather than guessing.
      * Track language matches ``original_language`` bucket: KEEP.
      * Track language matches English AND ``keep_english_too``: KEEP.
      * Track language is ``und``/empty AND whisper hasn't resolved it:
        KEEP (conservative — never strip what we can't identify).
      * Track language is a known foreign dub: STRIP.

    Returns ``None`` when no stripping is needed (everything would be kept)
    so the caller can no-op the audio map. Returns ``[]`` only if the input
    list is empty.

    The ``keep_english_too`` flag exists for users who want the original
    audio AND an English dub on top (convenience). Default False matches
    the policy "strip non-original including English".
    """
    if not streams:
        return []
    if not original_language:
        return None  # no TMDb signal — defer to legacy rule

    # Inviolate rule (2026-04-29): "find the language of everything before
    # stripping it." If ANY track is unresolved (no tag, no whisper detection),
    # defer the entire strip decision for this file. The previous behaviour
    # KEPT und tracks but still stripped known-foreign tracks alongside —
    # under the new rule, even known-foreign tracks survive until every
    # track has been identified, so the user can review the file holistically.
    if not all_languages_known(streams):
        return None

    keep: set[int] = set()
    for s in streams:
        # detected_language (whisper) takes precedence over the MKV tag
        # because we set it specifically when text metadata is unreliable.
        detected = (s.detected_language or "").lower().strip()
        tagged = (s.language or "").lower().strip()
        effective = detected or tagged

        if _lang_in_bucket(effective, original_language):
            keep.add(s.index)
            continue

        if keep_english_too and _lang_in_bucket(effective, "en"):
            keep.add(s.index)
            continue

        # Known foreign dub — strip.

    if len(keep) >= len(streams):
        return None  # nothing to strip
    return sorted(keep)


def select_sub_keep_indices(
    streams: list[SubStream],
    eng_langs: set[str] | None = None,
) -> list[int] | None:
    """Return subtitle stream indices to keep.

    Returns ``None`` when the file is NOT eligible for sub strip — caller
    must keep all tracks. Returns a list of indices to keep otherwise.

    Eligibility (inviolate rule, 2026-04-29): every subtitle track must
    have a resolved language (`language` or `detected_language` set to a
    non-und value). If ANY track is unresolved, return None and the caller
    keeps all subs until language detection runs. The user-facing rule is
    "never strip a track without first knowing its language" — this
    predicate enforces it at the strip-decision layer.

    Pick policy (when eligible):
      - ALWAYS keep forced / foreign-parts subs (any language).
      - KEEP one English track. Prefer regular non-HI; fall back to HI
        if that's the only English available.
      - STRIP everything else (non-English, the unchosen English duplicate).

    ``eng_langs`` defaults to :data:`pipeline.config.ENG_LANGS`.
    """
    if eng_langs is None:
        eng_langs = ENG_LANGS

    # Inviolate-rule guard: refuse to strip when any track is unresolved.
    # Empty input is fine (nothing to strip, all-known is vacuously true).
    if streams and not all_languages_known(streams):
        return None

    def _is_eng(s: SubStream) -> bool:
        lang = (s.language or s.detected_language or "").lower().strip()
        return lang in eng_langs

    keep: list[int] = []
    # First pass: forced + first regular (non-HI) English.
    chosen_eng_idx: int | None = None
    for s in streams:
        if s.is_forced:
            keep.append(s.index)
            continue
        if _is_eng(s) and not s.is_hi and chosen_eng_idx is None:
            chosen_eng_idx = s.index
            keep.append(s.index)

    # Fallback: no regular English picked — take the first HI English.
    if chosen_eng_idx is None:
        for s in streams:
            if s.is_forced:
                continue  # already kept
            if _is_eng(s) and s.is_hi:
                keep.append(s.index)
                break

    return keep
