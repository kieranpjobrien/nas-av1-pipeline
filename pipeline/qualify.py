"""Pre-encode qualification — cheap CPU-bound prep that decides if a file
should be encoded, flagged for the user, or already-good-enough.

Why a separate stage
--------------------
Pre-2026-04-25 the pipeline was monolithic: queue builder → fetch → ffmpeg
NVENC encode (the GPU-bound expensive step) → upload → done. Language
detection was tucked inside ``full_gamut`` between fetch and encode, which
meant:

* Detection ran while the GPU sat idle — wasted parallelism.
* When detection said "this is foreign / dubbed / unidentifiable", the
  pipeline still had to decide whether to encode-anyway, and the existing
  code had no way to surface "this file is bad, ask the user" — it just
  encoded with whatever data it had.

This module factors the decision out. Qualification produces one of:

* ``QualifyResult.QUALIFIED``        — encode normally (most files)
* ``QualifyResult.FLAGGED_FOREIGN``  — audio language ≠ original_language
                                       (Bluey dubbed Swedish, Amelie
                                       English-dub-only, Spirited Away
                                       English-dub-only — user wants
                                       original language regardless)
* ``QualifyResult.FLAGGED_UND``      — audio is `und` and whisper couldn't
                                       confidently identify it
* ``QualifyResult.NOTHING_TO_DO``    — file is already compliant; mark DONE
                                       without encoding

The qualify stage runs many files in parallel on CPU (or GPU for whisper).
The encode stage then consumes only QUALIFIED entries and trusts the
pre-computed audio_keep_indices / sub_keep_indices the qualifier produced.

Where this fits
---------------
Step 4 of the 2026-04-25 build (see CLAUDE.md history). Step 5 will refactor
``full_gamut`` to consume QualifyResult instead of re-doing the analysis,
removing the redundant detection that lives there today.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from pipeline.gap_filler import analyse_gaps
from pipeline.language import (
    clear_legacy_heuristic_detections,
    detect_all_languages,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


class QualifyOutcome(str, Enum):
    """The verdict of one qualification pass.

    Maps onto FileStatus values:
        QUALIFIED       -> FileStatus.QUALIFIED      (proceed to encode)
        NOTHING_TO_DO   -> FileStatus.DONE           (already compliant)
        FLAGGED_FOREIGN -> FileStatus.FLAGGED_FOREIGN_AUDIO
        FLAGGED_UND     -> FileStatus.FLAGGED_UNDETERMINED
        FLAGGED_MANUAL  -> FileStatus.FLAGGED_MANUAL (catch-all)
        ERROR           -> FileStatus.ERROR          (transient — retry next pass)
    """

    QUALIFIED = "qualified"
    NOTHING_TO_DO = "nothing_to_do"
    FLAGGED_FOREIGN = "flagged_foreign"
    FLAGGED_UND = "flagged_und"
    FLAGGED_MANUAL = "flagged_manual"
    ERROR = "error"


@dataclass
class QualifyResult:
    """Outcome of one qualify pass + everything the encode stage needs.

    The encode stage consumes ``audio_keep_indices`` / ``sub_keep_indices``
    directly without re-analysing — that's the whole point of separating
    the stages. ``rationale`` is a human-readable explanation surfaced in
    the UI (especially for FLAGGED_*).
    """

    outcome: QualifyOutcome
    rationale: str = ""

    # Decisions baked in by qualification — encode stage trusts these
    audio_keep_indices: list[int] = field(default_factory=list)
    sub_keep_indices: list[int] = field(default_factory=list)

    # Whisper findings (per audio track, by index)
    detected_audio_languages: dict[int, tuple[str, float, str]] = field(default_factory=dict)

    # Original language from TMDb (used for the foreign-audio check).
    # None when TMDb hasn't been enriched yet — qualifier will skip the
    # foreign check rather than guess.
    original_language: Optional[str] = None

    # The full enriched file_entry dict, for downstream consumers that need
    # the (mutated) audio/subtitle stream lists with detected_language fields.
    enriched_entry: Optional[dict] = None


# ---------------------------------------------------------------------------
# TMDb original_language extraction
# ---------------------------------------------------------------------------


def _original_language(file_entry: dict) -> Optional[str]:
    """Return TMDb's original_language as an ISO 639-1 code, or None if unknown.

    TMDb stores this consistently as 2-letter (``en``, ``sv``, ``ja``, etc.)
    on both movies and TV shows. Some entries pre-date enrichment and won't
    have it — caller must handle None gracefully.
    """
    tmdb = file_entry.get("tmdb") or {}
    raw = (tmdb.get("original_language") or "").strip().lower()
    return raw or None


# ---------------------------------------------------------------------------
# Audio-language verification
# ---------------------------------------------------------------------------


# Convert ISO 639-1 (whisper output) to ISO 639-2/B (MKV tag) for direct
# comparison with TMDb's original_language. A small subset is enough — every
# language whisper might detect that we'd see in this library.
_ISO1_EQUIV: dict[str, set[str]] = {
    "en": {"en", "eng", "english"},
    "sv": {"sv", "swe", "swedish"},
    "nl": {"nl", "nld", "dut", "dutch"},
    "de": {"de", "deu", "ger", "german"},
    "fr": {"fr", "fra", "fre", "french"},
    "es": {"es", "spa", "spanish"},
    "it": {"it", "ita", "italian"},
    "ja": {"ja", "jpn", "japanese"},
    "ko": {"ko", "kor", "korean"},
    # Chinese is genuinely ambiguous on tags (Mandarin vs Cantonese vs
    # generic). TMDb uses cn (legacy) and zh; whisper returns zh. MKV tags
    # are chi/zho/yue/cmn. Treat them as one bucket for the foreign-audio
    # check — if any of them are present and TMDb says zh-anything, the
    # file's "in original language" enough.
    "zh": {"zh", "cn", "chi", "zho", "yue", "cmn", "chinese", "mandarin", "cantonese"},
    "pt": {"pt", "por", "portuguese"},
    "ru": {"ru", "rus", "russian"},
    "ar": {"ar", "ara", "arabic"},
    "hi": {"hi", "hin", "hindi"},
    "no": {"no", "nor", "norwegian"},
    "da": {"da", "dan", "danish"},
    "fi": {"fi", "fin", "finnish"},
    "pl": {"pl", "pol", "polish"},
    "cs": {"cs", "ces", "cze", "czech"},
    "tr": {"tr", "tur", "turkish"},
    "he": {"he", "heb", "hebrew"},
    "th": {"th", "tha", "thai"},
    "vi": {"vi", "vie", "vietnamese"},
    "el": {"el", "ell", "gre", "greek"},
}


def _languages_equivalent(a: str, b: str) -> bool:
    """True if a and b refer to the same language under ISO 639-1/2 conventions.

    Whisper produces 2-letter codes; TMDb returns 2-letter; MKV stream tags
    are typically 3-letter. This handles the cross-mapping. Empty / und
    / unk values are NEVER equivalent to anything.
    """
    a, b = a.lower().strip(), b.lower().strip()
    if not a or not b or a in {"und", "unk"} or b in {"und", "unk"}:
        return False
    if a == b:
        return True
    for codes in _ISO1_EQUIV.values():
        if a in codes and b in codes:
            return True
    return False


def _audio_track_language(stream: dict) -> str:
    """Return the best-known language for one audio stream.

    Preference: explicit MKV tag > whisper-detected > title-hint detected.
    Returns lower-case string ("eng", "sv", etc.) or "und" when nothing's
    known. Never returns None.
    """
    raw = (stream.get("language") or "").lower().strip()
    if raw and raw not in {"und", "unk", ""}:
        return raw
    detected = (stream.get("detected_language") or "").lower().strip()
    if detected and detected not in {"und", "unk", ""}:
        return detected
    return "und"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def qualify_file(
    file_entry: dict,
    config: dict,
    *,
    use_whisper: bool = True,
    treat_legacy_heuristic_as_und: bool = True,
) -> QualifyResult:
    """Qualify ``file_entry`` for encoding.

    Steps:
        1. (Optionally) clear stale legacy ``heuristic`` detections so we
           don't trust the deleted broken codepath.
        2. Run language detection — title hints + whisper for `und` audio.
           Whisper is GPU-only; caller must coordinate with NVENC.
        3. Compute keep-indices via ``analyse_gaps`` on the freshly
           re-detected entry, so the result reflects current truth.
        4. Compare detected audio language(s) against TMDb original_language.
           If no audio track matches the original AND none is in KEEP_LANGS,
           flag the file (FLAGGED_FOREIGN_AUDIO).
        5. If the file already meets compliance with no further work
           needed, return NOTHING_TO_DO so the caller can mark DONE.

    Args:
        file_entry: media_report-shaped dict with audio_streams, subtitle_streams,
                    duration_seconds, library_type, tmdb, etc.
        config: build_config() dict (used for strip_non_english_audio etc.)
        use_whisper: whether to invoke the whisper ladder for `und` audio.
                     Set False in unit tests; production callers leave True.
        treat_legacy_heuristic_as_und: clear any ``detection_method ==
                     "heuristic"`` results before re-detection. The deleted
                     heuristic mis-IDed Bluey dubs as English; we don't want
                     to trust it on existing rows.

    Returns:
        QualifyResult capturing the outcome and the keep_indices the encode
        stage will use.
    """
    filepath = file_entry.get("filepath", "<unknown>")
    name = filepath.rsplit("\\", 1)[-1].rsplit("/", 1)[-1]

    # 1. Sweep stale legacy detections so we don't trust the deleted heuristic.
    if treat_legacy_heuristic_as_und:
        cleaned, n_cleared = clear_legacy_heuristic_detections(file_entry)
        if n_cleared:
            logger.info("  %s: cleared %d legacy heuristic detection(s)", name, n_cleared)
        file_entry = cleaned

    # 2. Detect languages. The detect_all_languages function handles whisper
    #    invocation per `und` track via the ladder.
    enriched = detect_all_languages(file_entry, use_whisper=use_whisper)

    # 3. Compute gap analysis on the NEWLY-enriched entry — this is what the
    #    encode stage will use for its strip decisions.
    gaps = analyse_gaps(enriched, config)
    # The qualify stage owns language detection — gap_filler shouldn't try
    # to redo it.
    gaps.needs_language_detect = False

    # 4. Foreign-audio check.
    original_lang = _original_language(enriched)
    audio_streams = enriched.get("audio_streams") or []

    # Capture detected language per audio track for the result + UI surfacing.
    detected: dict[int, tuple[str, float, str]] = {}
    for idx, stream in enumerate(audio_streams):
        lang = _audio_track_language(stream)
        conf = float(stream.get("detection_confidence") or 0.0)
        method = str(stream.get("detection_method") or "")
        detected[idx] = (lang, conf, method)

    # Build the result skeleton; we set outcome below.
    result = QualifyResult(
        outcome=QualifyOutcome.QUALIFIED,  # provisional
        audio_keep_indices=list(gaps.audio_keep_indices or []),
        sub_keep_indices=list(gaps.sub_keep_indices or []),
        detected_audio_languages=detected,
        original_language=original_lang,
        enriched_entry=enriched,
    )

    if not audio_streams:
        # Zero-audio sources can't be encoded (rule 8 / pipeline policy).
        # Don't auto-flag — surface as ERROR so the user sees it on the
        # standard error queue rather than the flagged pane.
        result.outcome = QualifyOutcome.ERROR
        result.rationale = "source has zero audio streams; refusing to qualify"
        return result

    # Determine the intent-check pool. We trust:
    #   * Tracks with an explicit non-und MKV language tag.
    #   * Tracks where whisper / title_hint produced a confident detection.
    # We do NOT trust:
    #   * `und`-tagged tracks with no detection (whisper exhausted or skipped).
    track_langs: list[tuple[int, str, float, str]] = []
    for idx, (lang, conf, method) in detected.items():
        track_langs.append((idx, lang, conf, method))

    has_original_track = False
    has_und_track = False
    for _idx, lang, _conf, _method in track_langs:
        if lang in {"und", "unk", ""}:
            has_und_track = True
            continue
        if original_lang and _languages_equivalent(lang, original_lang):
            has_original_track = True

    # Apply flag rules.
    #
    # User policy (2026-04-25): "we want to watch in original language always.
    # Amelie and Spirited Away should be in the original with subs." So the
    # only acceptable audio is one matching TMDb's ``original_language``.
    # English-dub-only of a foreign-original film FLAGS — even though English
    # is in KEEP_LANGS for stripping purposes, it's the wrong language for
    # films whose original is, say, French or Japanese.
    #
    # If ``original_language`` is unknown (TMDb not yet enriched), we can't
    # make the call — we skip the foreign check and fall through to the
    # normal qualified path. The user can re-qualify after enrichment.
    if original_lang:
        if not has_original_track:
            # No track matches original_language. Two sub-cases distinguished
            # by whether we have ANY confident detection at all.
            if has_und_track and all(
                lang in {"und", "unk", ""} for _, lang, _, _ in track_langs
            ):
                # Every track is undetermined. Whisper either skipped or
                # couldn't decide. User reviews via Flagged pane.
                result.outcome = QualifyOutcome.FLAGGED_UND
                result.rationale = (
                    f"audio is `und` and whisper couldn't confidently identify it; "
                    f"original_language={original_lang}"
                )
            else:
                detected_summary = ", ".join(
                    f"a:{i}={lang}" for i, lang, _, _ in track_langs
                )
                result.outcome = QualifyOutcome.FLAGGED_FOREIGN
                result.rationale = (
                    f"no audio track matches original_language={original_lang} "
                    f"(detected: {detected_summary})"
                )
            return result
    else:
        # No TMDb original_language — can't evaluate foreign-audio without
        # ground truth. Only flag if EVERY audio track is undetermined; with
        # at least one identified track, encode normally and let the user
        # re-qualify after TMDb enrichment if needed.
        if has_und_track and all(
            lang in {"und", "unk", ""} for _, lang, _, _ in track_langs
        ):
            result.outcome = QualifyOutcome.FLAGGED_UND
            result.rationale = (
                "audio is `und` and whisper couldn't identify it; "
                "TMDb original_language not yet enriched (re-run after metadata)"
            )
            return result

    # 5. Already-compliant short-circuit.
    if not gaps.needs_anything:
        result.outcome = QualifyOutcome.NOTHING_TO_DO
        result.rationale = "no gaps detected — file is already compliant"
        return result

    # Normal happy path: ready for encode.
    result.outcome = QualifyOutcome.QUALIFIED
    needs = gaps.describe()
    result.rationale = f"will encode: {needs}"
    return result
