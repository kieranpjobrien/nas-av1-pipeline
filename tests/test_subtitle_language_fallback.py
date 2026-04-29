"""Tests for the metadata-only subtitle language fallback chain.

Added 2026-04-29 after the user (rightly) called out that the text/OCR
pass had a 52% sub-detection failure rate, and the failure messages I'd
given were excuses rather than results. The new fallback chain at
``pipeline.language.infer_subtitle_language`` runs AFTER the content
extractors give up, using metadata signals the extractors don't consult:

  1. Track title (e.g. "English (SDH)" → en at 0.95)
  2. Sibling sub majority (all other subs known and agree → 0.85)
  3. Sole-audio inference (single audio track of known language → 0.80)
  4. TMDb original_language (soft prior → 0.70)

These tests pin each layer, plus integration with ``process_file`` so
the previously-failed paths now produce results.
"""

from __future__ import annotations

import pytest

from pipeline.language import infer_subtitle_language


def _entry(audio: list[dict], subs: list[dict], tmdb: dict | None = None) -> dict:
    return {
        "filepath": r"\\NAS\Movies\Test\Test.mkv",
        "audio_streams": audio,
        "subtitle_streams": subs,
        "tmdb": tmdb or {},
    }


# ---------------------------------------------------------------------------
# Layer 1: track title
# ---------------------------------------------------------------------------


def test_track_title_explicit_english_hits():
    e = _entry(
        audio=[{"language": "und"}],
        subs=[{"language": "und", "title": "English (SDH)"}],
    )
    lang, conf, reason = infer_subtitle_language(e, 0)
    assert lang == "en"
    assert conf >= 0.9
    assert "track title" in reason.lower()


def test_track_title_forced_english_hits():
    e = _entry(audio=[{"language": "und"}], subs=[{"language": "und", "title": "Forced English"}])
    lang, conf, _ = infer_subtitle_language(e, 0)
    assert lang == "en"
    assert conf >= 0.9


def test_track_title_french_hits():
    e = _entry(audio=[{"language": "und"}], subs=[{"language": "und", "title": "français"}])
    lang, conf, _ = infer_subtitle_language(e, 0)
    assert lang == "fr"
    assert conf >= 0.9


def test_track_title_with_brackets_hits():
    e = _entry(audio=[{"language": "und"}], subs=[{"language": "und", "title": "[eng]"}])
    lang, conf, _ = infer_subtitle_language(e, 0)
    assert lang == "en"


# ---------------------------------------------------------------------------
# Layer 2: sibling sub majority
# ---------------------------------------------------------------------------


def test_sibling_subs_unanimous():
    """If every other sub on the file is English (tagged or detected), inherit."""
    e = _entry(
        audio=[{"language": "und"}],
        subs=[
            {"language": "eng"},
            {"language": "und", "title": ""},  # the one we're inferring
            {"language": "eng"},
        ],
    )
    lang, conf, reason = infer_subtitle_language(e, 1)
    assert lang == "eng"
    assert conf == 0.85
    assert "sibling" in reason.lower()


def test_sibling_subs_disagree_no_inference():
    """If siblings disagree, sibling inference shouldn't fire (would fall to next layer)."""
    e = _entry(
        audio=[{"language": "fra"}],  # but audio gives a sole-audio fallback
        subs=[
            {"language": "eng"},
            {"language": "und", "title": ""},
            {"language": "spa"},
        ],
    )
    lang, conf, reason = infer_subtitle_language(e, 1)
    # Layer 2 didn't fire (siblings disagree), layer 3 did (sole audio)
    assert lang == "fr"
    assert "audio" in reason.lower()


# ---------------------------------------------------------------------------
# Layer 3: sole audio inference
# ---------------------------------------------------------------------------


def test_sole_audio_english_inferred():
    e = _entry(audio=[{"language": "eng"}], subs=[{"language": "und", "title": ""}])
    lang, conf, reason = infer_subtitle_language(e, 0)
    assert lang == "en"
    assert conf == 0.80
    assert "audio" in reason.lower()


def test_sole_audio_via_detected_language():
    """Even if `language` is und, whisper-detected_language counts."""
    e = _entry(
        audio=[{"language": "und", "detected_language": "jpn"}],
        subs=[{"language": "und", "title": ""}],
    )
    lang, conf, _ = infer_subtitle_language(e, 0)
    assert lang == "ja"
    assert conf == 0.80


def test_multiple_audio_no_sole_inference():
    """When the file has multiple audio tracks, sole-audio inference is silent."""
    e = _entry(
        audio=[{"language": "eng"}, {"language": "fra"}],
        subs=[{"language": "und", "title": ""}],
        tmdb={"original_language": "en"},
    )
    lang, conf, reason = infer_subtitle_language(e, 0)
    # Layer 3 doesn't apply, falls through to layer 4 (TMDb)
    assert lang == "en"
    assert conf == 0.70
    assert "tmdb" in reason.lower()


# ---------------------------------------------------------------------------
# Layer 4: TMDb fallback
# ---------------------------------------------------------------------------


def test_tmdb_fallback_when_nothing_else():
    e = _entry(
        audio=[{"language": "und"}],
        subs=[{"language": "und", "title": ""}],
        tmdb={"original_language": "en"},
    )
    lang, conf, reason = infer_subtitle_language(e, 0)
    assert lang == "en"
    assert conf == 0.70
    assert "tmdb" in reason.lower()


# ---------------------------------------------------------------------------
# No-signal case
# ---------------------------------------------------------------------------


def test_no_signal_returns_none():
    """File with no metadata at all → no inference."""
    e = _entry(audio=[{"language": "und"}], subs=[{"language": "und", "title": ""}])
    lang, conf, reason = infer_subtitle_language(e, 0)
    assert lang is None
    assert conf == 0.0


def test_out_of_range_returns_none():
    e = _entry(audio=[{"language": "und"}], subs=[{"language": "und", "title": ""}])
    lang, conf, _ = infer_subtitle_language(e, 5)  # only 1 sub
    assert lang is None


# ---------------------------------------------------------------------------
# Priority ordering — earlier layers win when both fire
# ---------------------------------------------------------------------------


def test_track_title_beats_audio_inference():
    """Title says German, audio is English. Title wins (it's literally telling us)."""
    e = _entry(
        audio=[{"language": "eng"}],
        subs=[{"language": "und", "title": "Deutsch"}],
    )
    lang, _, reason = infer_subtitle_language(e, 0)
    assert lang == "de"
    assert "track title" in reason.lower()


def test_audio_beats_tmdb():
    """Sole audio inference (0.80) beats TMDb fallback (0.70)."""
    e = _entry(
        audio=[{"language": "fra"}],
        subs=[{"language": "und", "title": ""}],
        tmdb={"original_language": "en"},
    )
    lang, conf, reason = infer_subtitle_language(e, 0)
    assert lang == "fr"
    assert conf == 0.80
    assert "audio" in reason.lower()
