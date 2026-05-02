"""Regression tests for the 'zxx' (no linguistic content) handling.

The 2026-05-02 finding: orchestral shorts like Paperman, The Lost Thing,
Inner Workings, Feast have audio tracks with no dialogue — whisper
correctly fails to detect a language because there's no speech. Without
an explicit ``zxx`` tag they sit in the und queue forever and the dashboard
"Langs Known" undercounts.

Two changes pinned by these tests:
  1. ``KEEP_LANGS`` includes ``zxx`` so a zxx-tagged track is non-foreign
     for compliance + track strip.
  2. ``tmdb_keeper_langs`` always adds ``zxx`` to its returned set so the
     audio_lang_ok check accepts a dialogue-free track regardless of the
     TMDb-stated original_language.
"""

from __future__ import annotations

import pytest


def test_keep_langs_includes_zxx():
    """Regression: ``zxx`` must be in KEEP_LANGS so the strip layer leaves
    no-dialogue tracks alone and compliance doesn't flag them as foreign."""
    from pipeline.config import KEEP_LANGS

    assert "zxx" in KEEP_LANGS


def test_keep_langs_keeps_existing_members():
    """Don't accidentally drop the existing keepers when adding zxx."""
    from pipeline.config import KEEP_LANGS

    for required in ("eng", "en", "english", "und", ""):
        assert required in KEEP_LANGS, f"{required!r} missing from KEEP_LANGS"


def test_tmdb_keeper_langs_includes_zxx_when_orig_set():
    """Even with a non-English original_language, zxx must be acceptable —
    a dialogue-free track is intrinsically compatible with any language
    policy."""
    from pipeline.streams import tmdb_keeper_langs

    keepers = tmdb_keeper_langs("ja")  # Japanese-original film with a zxx track
    assert keepers is not None
    assert "zxx" in keepers
    assert "ja" in keepers
    assert "jpn" in keepers  # iso2 form should also be there


def test_tmdb_keeper_langs_includes_zxx_for_english():
    """English-original films (Paperman is en) also need zxx in the keeper
    set so the dialogue-free original audio passes audio_lang_ok."""
    from pipeline.streams import tmdb_keeper_langs

    keepers = tmdb_keeper_langs("en")
    assert keepers is not None
    assert "zxx" in keepers
    assert "en" in keepers
    assert "eng" in keepers


def test_tmdb_keeper_langs_returns_none_for_empty():
    """Empty original_language stays permissive (None) — no regression."""
    from pipeline.streams import tmdb_keeper_langs

    assert tmdb_keeper_langs("") is None
    assert tmdb_keeper_langs(None) is None


def test_invariant_skips_zxx_tracks():
    """The 2026-05-02 finding: ``no_done_with_foreign_audio`` was flagging
    Inner Workings and Feast as proven-foreign because their audio tags
    are ``zxx`` ≠ TMDb ``en``. zxx is "no linguistic content" — it's
    outside the comparison space, not a foreign language. The check must
    skip these tracks entirely so dialogue-free orchestral shorts don't
    permanently fail the invariant."""
    src = open("tools/invariants.py", encoding="utf-8").read()
    # The set of codes treated as "not a language" must include zxx.
    assert '"zxx"' in src or "'zxx'" in src, "zxx not whitelisted in invariants"
    # And the comment naming the rationale should be present so future
    # editors know why this exception exists.
    assert "no linguistic content" in src.lower(), (
        "rationale comment missing — without it the next person to edit "
        "this will likely re-introduce the bug"
    )


def test_tag_no_dialogue_main_requires_arg():
    """CLI surface check — running without --file or --titles is a
    parser error, not a silent no-op."""
    from tools.tag_no_dialogue import main

    import sys

    old_argv = sys.argv
    sys.argv = ["tag_no_dialogue"]
    try:
        with pytest.raises(SystemExit):
            main()
    finally:
        sys.argv = old_argv
