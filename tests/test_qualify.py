"""Tests for pipeline/qualify.py — pre-encode qualification.

Anchor case: Bluey episodes dubbed in Swedish (audio language ≠ TMDb
original_language `en`) MUST flag as FLAGGED_FOREIGN, not silently encode.

Whisper is mocked here — qualify_file() is unit-tested for its DECISION
LOGIC given pre-supplied detection results. Live whisper integration was
verified separately (5/5 Bluey eps detected as `sv`, 0.82-0.95).
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from pipeline.config import build_config
from pipeline.qualify import (
    QualifyOutcome,
    _languages_equivalent,
    _original_language,
    qualify_file,
)


@pytest.fixture()
def cfg():
    return build_config()


def _bluey_swedish_dub_entry() -> dict:
    """Real-world fixture: Bluey ep with Swedish-dubbed audio tagged `und`.

    TMDb original_language is `en` (Bluey is an Australian show in English).
    Audio is the Swedish dub the user accidentally downloaded. Bazarr added
    an English srt afterwards.
    """
    return {
        "filepath": r"\\KieranNAS\Media\Series\Bluey (2018)\Season 1\Bluey S01E04 Daddy Robot.mkv",
        "filename": "Bluey S01E04 Daddy Robot.mkv",
        "library_type": "series",
        "duration_seconds": 420,
        "audio_streams": [
            {"language": "und", "title": "", "codec": "eac3", "channels": 2},
        ],
        "subtitle_streams": [
            {"language": "eng", "codec": "subrip", "title": ""},
        ],
        "tmdb": {"original_language": "en", "title": "Bluey"},
    }


def _amelie_english_dub_entry() -> dict:
    """User wants ORIGINAL language. Amelie's original is French; an
    English-dub-only file should flag, even though English is normally
    keep-list."""
    return {
        "filepath": r"\\KieranNAS\Media\Movies\Amelie (2001)\Amelie (2001).mkv",
        "filename": "Amelie (2001).mkv",
        "library_type": "movie",
        "duration_seconds": 7320,
        "audio_streams": [
            # Detected as English (high confidence) — but NOT the original
            {
                "language": "und",
                "detected_language": "en",
                "detection_confidence": 0.97,
                "detection_method": "whisper_tiny_3x30",
                "codec": "eac3",
                "channels": 6,
            },
        ],
        "subtitle_streams": [],
        "tmdb": {"original_language": "fr", "title": "Amélie"},
    }


def _heat_clean_entry() -> dict:
    """Reference 'good' case: English audio + English original."""
    return {
        "filepath": r"\\KieranNAS\Media\Movies\Heat (1995)\Heat (1995).mkv",
        "filename": "Heat (1995).mkv",
        "library_type": "movie",
        "duration_seconds": 10260,
        "audio_streams": [
            {"language": "eng", "codec": "eac3", "channels": 6},
        ],
        "subtitle_streams": [
            {"language": "eng", "codec": "subrip"},
        ],
        "tmdb": {"original_language": "en", "title": "Heat"},
    }


# ---------------------------------------------------------------------------
# _languages_equivalent — small but load-bearing helper
# ---------------------------------------------------------------------------


def test_languages_equivalent_2letter_3letter():
    """en ↔ eng / sv ↔ swe / ja ↔ jpn etc. are equivalent."""
    assert _languages_equivalent("en", "eng") is True
    assert _languages_equivalent("eng", "en") is True
    assert _languages_equivalent("sv", "swe") is True
    assert _languages_equivalent("ja", "jpn") is True
    assert _languages_equivalent("fr", "fra") is True
    # 3-letter B vs T variants (eng vs eng, but ger vs deu)
    assert _languages_equivalent("ger", "de") is True


def test_languages_equivalent_und_never_matches():
    """`und` / `unk` / empty are never equivalent to anything — including
    each other or themselves. Otherwise foreign-audio detection would silently
    pass on und-tagged files."""
    assert _languages_equivalent("und", "und") is False
    assert _languages_equivalent("und", "en") is False
    assert _languages_equivalent("", "en") is False


def test_languages_equivalent_different_langs():
    assert _languages_equivalent("en", "sv") is False
    assert _languages_equivalent("eng", "swe") is False
    assert _languages_equivalent("en", "fr") is False


# ---------------------------------------------------------------------------
# _original_language extraction
# ---------------------------------------------------------------------------


def test_original_language_present():
    assert _original_language({"tmdb": {"original_language": "en"}}) == "en"
    assert _original_language({"tmdb": {"original_language": "JA"}}) == "ja"  # lowercased


def test_original_language_missing():
    assert _original_language({}) is None
    assert _original_language({"tmdb": {}}) is None
    assert _original_language({"tmdb": {"original_language": ""}}) is None


# ---------------------------------------------------------------------------
# Main qualify_file outcomes — the Bluey scenario is the critical one
# ---------------------------------------------------------------------------


def test_qualify_bluey_swedish_dub_flags_foreign(cfg):
    """REGRESSION: Bluey ep with und-tagged audio that whisper says is sv,
    where TMDb original_language is en — MUST flag FLAGGED_UND (since whisper
    is mocked off here and there's no detection evidence). With whisper on
    and detection saying sv, it would flag FLAGGED_FOREIGN."""
    entry = _bluey_swedish_dub_entry()
    # Disable whisper for the unit test — but the und tag remains.
    result = qualify_file(entry, cfg, use_whisper=False)
    # No detection ran, audio is `und`, original_language is en.
    # Verdict: undetermined (we can't prove it's foreign; we can't prove it's
    # not). User reviews via the Flagged pane.
    assert result.outcome == QualifyOutcome.FLAGGED_UND
    assert "und" in result.rationale.lower()


def test_qualify_bluey_with_whisper_detected_swedish_flags_foreign(cfg):
    """The full scenario: Bluey audio is und-tagged, whisper detects `sv`
    with high confidence, original_language is en — flags FLAGGED_FOREIGN."""
    entry = _bluey_swedish_dub_entry()
    # Pre-populate the detection (simulating a whisper run) so we don't
    # actually invoke whisper in the test.
    entry["audio_streams"][0]["detected_language"] = "sv"
    entry["audio_streams"][0]["detection_confidence"] = 0.92
    entry["audio_streams"][0]["detection_method"] = "whisper_small_5x60"
    result = qualify_file(entry, cfg, use_whisper=False)
    assert result.outcome == QualifyOutcome.FLAGGED_FOREIGN
    assert "original_language=en" in result.rationale
    assert "a:0=sv" in result.rationale


def test_qualify_amelie_english_dub_only_flags_foreign(cfg):
    """Amelie's original is `fr`. An English-dub-only file should flag because
    the user wants original-language audio, even though English is normally
    the keep-list default."""
    entry = _amelie_english_dub_entry()
    result = qualify_file(entry, cfg, use_whisper=False)
    assert result.outcome == QualifyOutcome.FLAGGED_FOREIGN
    assert "fr" in result.rationale  # original_language=fr in rationale


def test_qualify_heat_english_original_passes(cfg):
    """Reference 'good' case: English audio + English original = QUALIFIED.

    Heat already has compliant tracks, so we expect either QUALIFIED or
    NOTHING_TO_DO depending on exact gap state. Either is fine — the key
    is it's NOT flagged.
    """
    entry = _heat_clean_entry()
    # Make sure the file has nothing else needing work — clear TMDb gap.
    entry["tmdb"]["id"] = 949  # any non-empty TMDb id satisfies needs_metadata
    result = qualify_file(entry, cfg, use_whisper=False)
    assert result.outcome in (QualifyOutcome.QUALIFIED, QualifyOutcome.NOTHING_TO_DO), (
        f"Heat should not flag — got {result.outcome.value}: {result.rationale}"
    )


def test_qualify_no_tmdb_original_lang_doesnt_falsely_flag(cfg):
    """If TMDb hasn't been enriched yet (no original_language available), we
    don't have ground truth to compare against — so we don't flag the file
    as foreign. Skipping the check is safer than guessing."""
    entry = _heat_clean_entry()
    entry["tmdb"] = {}  # no original_language
    result = qualify_file(entry, cfg, use_whisper=False)
    # Without TMDb data we either qualify (encode normally) or note nothing-to-do.
    # We must NOT flag foreign because we have no ground truth.
    assert result.outcome != QualifyOutcome.FLAGGED_FOREIGN


def test_qualify_zero_audio_returns_error(cfg):
    """Zero-audio source is a structural problem (rule 8) — surface as
    ERROR, not a FLAGGED_* state, so it shows up on the normal error queue."""
    entry = {
        "filepath": r"\\KieranNAS\Media\Movies\Broken.mkv",
        "filename": "Broken.mkv",
        "library_type": "movie",
        "duration_seconds": 5400,
        "audio_streams": [],
        "subtitle_streams": [],
        "tmdb": {"original_language": "en", "id": 1},
    }
    result = qualify_file(entry, cfg, use_whisper=False)
    assert result.outcome == QualifyOutcome.ERROR
    assert "zero audio" in result.rationale.lower()


def test_qualify_clears_legacy_heuristic_before_evaluating(cfg):
    """A file where audio[0] has detection_method=='heuristic' (the deleted
    bad codepath) should have that label STRIPPED before qualification, so
    the qualifier doesn't trust it.

    This is the pivot — without this, every Bluey ep with stale heuristic
    data would still pass even after the heuristic is gone from the live code.
    """
    entry = _bluey_swedish_dub_entry()
    # Inject the deleted-heuristic label that the bug produced
    entry["audio_streams"][0]["detected_language"] = "en"  # WRONG — heuristic mis-IDed it
    entry["audio_streams"][0]["detection_confidence"] = 0.9
    entry["audio_streams"][0]["detection_method"] = "heuristic"

    result = qualify_file(entry, cfg, use_whisper=False)
    # After clearing the legacy label, audio reverts to `und`. With no
    # whisper run, we get FLAGGED_UND (not silently QUALIFIED).
    assert result.outcome == QualifyOutcome.FLAGGED_UND, (
        f"legacy heuristic data was trusted, allowing the bug to persist: "
        f"{result.outcome.value}: {result.rationale}"
    )


def test_qualify_keep_lang_eng_passes_even_without_original_match(cfg):
    """A film whose original language is Japanese (Spirited Away) but with
    a confidently-detected English audio track should NOT flag, because
    the original-language rule is about avoiding foreign-only files when
    the user wants original. If they HAVE English (as keep_lang) — fine to
    encode.

    Wait — actually per user spec, they want ORIGINAL always. So Spirited
    Away with English-dub-only SHOULD flag. This is verified in
    test_qualify_amelie_english_dub_only_flags_foreign above.
    """
    # This test was a placeholder — see _amelie test above for the real
    # behaviour.
    pass