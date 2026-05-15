"""Pin the 2026-05-13 pre-encode stream-strip architecture.

The user demanded the obvious architectural fix after watching the
post-encode compliance fixer chase its tail for two days: do every
fixable strip on the LOCAL fetched file BEFORE the GPU encodes, so
the encoder consumes a guaranteed-clean input. Post-encode
compliance becomes a thin verifier.

This module's job is computing per-type drop indices using the same
policy logic as the long-running encoder selectors, then handing
them off to ``_mkvmerge_drop_streams`` against the LOCAL file.
"""

from __future__ import annotations

import pytest

from pipeline.prep_streams import (
    MAX_REGULAR_ENGLISH_SUBS,
    compute_audio_drop_indices,
    compute_sub_drop_indices,
    strip_streams_locally,
)


def _audio(codec="eac3", channels=6, lang="eng", title=""):
    return {
        "codec": codec.upper(), "codec_raw": codec, "channels": channels,
        "language": lang, "title": title,
    }


def _sub(lang="eng", title="", forced=False):
    return {
        "codec": "subrip", "language": lang, "title": title, "forced": forced,
    }


# --------------------------------------------------------------------------
# Audio drop selection
# --------------------------------------------------------------------------


def test_audio_drop_keeps_english_strips_foreign():
    """Foreign dub gets dropped; English original kept."""
    item = {
        "audio_streams": [
            _audio(lang="eng", title="English"),
            _audio(lang="fre", title="French dub"),
            _audio(lang="ger", title="German dub"),
        ],
        "tmdb": {"original_language": "en"},
    }
    config = {
        "strip_non_english_audio": True,
        "audio_keep_policy": "english_und",  # simpler policy for the test
    }
    # Audio count > 2 so the legacy guard doesn't bail.
    drop = compute_audio_drop_indices(item, config)
    # English at index 0 kept; French + German dropped
    assert sorted(drop) == [1, 2]


def test_audio_drop_strips_commentary_by_title():
    """Title-based commentary detection drops even English-tagged tracks."""
    item = {
        "audio_streams": [
            _audio(lang="eng", title="English"),
            _audio(lang="eng", title="Commentary by Director"),
            _audio(lang="eng", title="Isolated Music"),
        ],
        "tmdb": {"original_language": "en"},
    }
    config = {
        "strip_non_english_audio": True,
        "strip_commentary_audio": True,
        "audio_keep_policy": "english_und",
    }
    drop = compute_audio_drop_indices(item, config)
    assert 1 in drop, "commentary track must be in drop set"
    assert 2 in drop, "isolated music must be in drop set"
    assert 0 not in drop, "main English must be kept"


def test_audio_drop_empty_when_strip_disabled():
    """If config disables strip, drop list is empty (encoder keeps all)."""
    item = {
        "audio_streams": [_audio(lang="eng"), _audio(lang="fre")],
        "tmdb": {},
    }
    config = {"strip_non_english_audio": False}
    assert compute_audio_drop_indices(item, config) == []


def test_audio_drop_safety_refuses_to_empty_audio_implicit_via_strip():
    """``strip_streams_locally`` (not compute_*) is what refuses to drop
    all audio — see the strip wrapper. The selector itself can return
    a full drop list; the wrapper catches it."""
    # Pin via strip wrapper, not selector — see test_strip_refuses_all_audio


# --------------------------------------------------------------------------
# Subtitle drop selection
# --------------------------------------------------------------------------


def test_sub_drop_keeps_english_drops_foreign():
    """Foreign subs dropped; English kept (within the regular-count cap)."""
    item = {
        "subtitle_streams": [
            _sub(lang="eng", title="English"),
            _sub(lang="fre", title="Français"),
            _sub(lang="ger", title="Deutsch"),
            _sub(lang="ita", title="Italiano"),
        ],
    }
    config = {"strip_non_english_subs": True}
    drop = compute_sub_drop_indices(item, config)
    assert 0 not in drop
    assert sorted(drop) == [1, 2, 3]


def test_sub_drop_caps_regular_english_subs():
    """More than MAX_REGULAR_ENGLISH_SUBS regular English subs → drop extras."""
    assert MAX_REGULAR_ENGLISH_SUBS == 1, (
        "this test assumes max 1 regular English sub"
    )
    item = {
        "subtitle_streams": [
            _sub(lang="eng", title="English"),
            _sub(lang="eng", title="English (CC)"),
            _sub(lang="eng", title="English"),
        ],
    }
    config = {"strip_non_english_subs": True}
    drop = compute_sub_drop_indices(item, config)
    # First "English" kept; SDH kept (CC = hearing-impaired); third dropped
    assert 0 not in drop
    assert 1 not in drop, "SDH variant kept"
    assert 2 in drop


def test_sub_drop_keeps_forced_english_separately():
    """Forced English subs aren't counted against the regular-cap."""
    item = {
        "subtitle_streams": [
            _sub(lang="eng", title="English (forced)", forced=True),
            _sub(lang="eng", title="English"),
        ],
    }
    config = {"strip_non_english_subs": True}
    drop = compute_sub_drop_indices(item, config)
    assert drop == [], "forced + 1 regular is within the cap"


def test_sub_drop_und_zxx_kept_when_no_eng():
    """No-eng-fallback case — und/zxx are kept per the 2026-04-29 inviolate
    rule: never strip the LAST sub of a possibly-relevant language. Whisper
    can resolve them on a later pass."""
    item = {
        "subtitle_streams": [
            _sub(lang="und", title="??"),
            _sub(lang="zxx", title="No dialogue"),
            _sub(lang="fre", title="Foreign"),
        ],
    }
    drop = compute_sub_drop_indices(item, {"strip_non_english_subs": True})
    assert drop == [2], (
        "with no confirmed eng track, und + zxx must stay so whisper can "
        "resolve them later; only the explicit foreign is dropped"
    )


def test_sub_drop_und_dropped_when_confirmed_eng_present():
    """Pin the 2026-05-15 refinement.

    Thelma & Louise (1991) shape: 36 subs total, including one
    ``language=und title="Chinese (Cantonese)"`` and two confirmed eng
    (regular + SDH). Pre-fix, the und triggered the inviolate-rule
    defer in ``_map_subtitle_streams`` and the prep step kept the und
    alongside everything else. Post-fix, a confirmed eng track makes
    und droppable: the eng is the user-facing fallback, the und is
    dispensable (whatever its real language).
    """
    item = {
        "subtitle_streams": [
            _sub(lang="eng", title="English"),
            _sub(lang="und", title="Chinese (Cantonese)"),
            _sub(lang="zxx", title=""),
            _sub(lang="fre", title="Foreign"),
        ],
    }
    drop = compute_sub_drop_indices(item, {"strip_non_english_subs": True})
    assert 0 not in drop, "confirmed eng must be kept"
    assert 1 in drop, "und must be dropped when a confirmed eng track is present"
    assert 2 in drop, "zxx droppable too when eng is present"
    assert 3 in drop, "foreign always dropped"


def test_sub_drop_thelma_and_louise_shape():
    """Full Thelma & Louise (1991) reproduction — pin the canonical
    shape that motivated the und/eng refinement. 36-sub source with
    one und ``title="Chinese (Cantonese)"`` and two confirmed eng
    (regular + SDH). Strip should keep exactly those two and drop the
    other 34."""
    subs = [
        _sub(lang="eng", title="English"),                       # 0  kept
        _sub(lang="eng", title="English (SDH)"),                 # 1  kept (SDH separate slot)
        _sub(lang="ara", title="Arabic"),
        _sub(lang="und", title="Chinese (Cantonese)"),           # 3  the canary — must drop
        _sub(lang="chi", title="Chinese (Traditional)"),
        _sub(lang="hrv", title="Croatian"),
        _sub(lang="cze", title="Czech"),
        _sub(lang="dan", title="Danish"),
        _sub(lang="dut", title="Dutch"),
        _sub(lang="fil", title="Filipino"),
        _sub(lang="fin", title="Finnish"),
        _sub(lang="fre", title="French (Parisian)"),
        _sub(lang="ger", title="German"),
        _sub(lang="gre", title="Greek"),
        _sub(lang="heb", title="Hebrew"),
        _sub(lang="hun", title="Hungarian"),
        _sub(lang="ind", title="Indonesian"),
        _sub(lang="ita", title="Italian"),
        _sub(lang="jpn", title="Japanese"),
        _sub(lang="kor", title="Korean"),
        _sub(lang="may", title="Malay"),
        _sub(lang="nor", title="Norwegian"),
        _sub(lang="pol", title="Polish"),
        _sub(lang="por", title="Portuguese (Brazilian)"),
        _sub(lang="por", title="Portuguese (Iberian)"),
        _sub(lang="rus", title="Russian"),
        _sub(lang="slv", title="Slovenian"),
        _sub(lang="spa", title="Spanish (Castilian)"),
        _sub(lang="spa", title="Spanish (Latin American)"),
        _sub(lang="swe", title="Swedish"),
        _sub(lang="tha", title="Thai"),
        _sub(lang="tur", title="Turkish"),
        _sub(lang="vie", title="Vietnamese"),
        _sub(lang="jpn", title="Japanese (Commentary #1)"),
        _sub(lang="kor", title="Korean (Commentary #1)"),
        _sub(lang="jpn", title="Japanese (Commentary #2)"),
    ]
    item = {"subtitle_streams": subs}
    drop = compute_sub_drop_indices(item, {"strip_non_english_subs": True})
    # Two eng subs kept, 34 others dropped
    assert sorted(drop) == sorted(i for i in range(36) if i not in (0, 1))
    assert 3 in drop, "the und 'Chinese (Cantonese)' canary must drop"


def test_sub_drop_empty_when_strip_disabled():
    item = {"subtitle_streams": [_sub(lang="fre")]}
    config = {"strip_non_english_subs": False}
    assert compute_sub_drop_indices(item, config) == []


# --------------------------------------------------------------------------
# strip_streams_locally wrapper
# --------------------------------------------------------------------------


def test_strip_returns_ok_when_nothing_to_drop(monkeypatch):
    """If neither audio nor sub needs stripping, the wrapper returns
    True with the ORIGINAL local_path (no strip happened)."""
    item = {
        "audio_streams": [_audio(lang="eng")],
        "subtitle_streams": [_sub(lang="eng")],
        "tmdb": {"original_language": "en"},
    }
    config = {"strip_non_english_audio": True, "strip_non_english_subs": True}

    # If mkvmerge were called we'd see this assertion fire
    def should_not_run(*a, **kw):
        raise AssertionError("mkvmerge should NOT run when nothing to drop")
    monkeypatch.setattr(
        "pipeline.compliance_fixers._mkvmerge_drop_streams_to_path", should_not_run
    )
    ok, path = strip_streams_locally("/fake.mkv", item, config)
    assert ok is True
    assert path == "/fake.mkv", (
        f"no-strip case must return the original local path so the encoder "
        f"consumes the fetched file directly, got {path!r}"
    )


def test_strip_refuses_all_audio(monkeypatch):
    """Safety guard: if the drop list covers EVERY audio track, refuse
    rather than produce a silent file."""
    item = {
        "audio_streams": [
            _audio(lang="fre"), _audio(lang="ger"), _audio(lang="ita"),
        ],
        "subtitle_streams": [],
    }
    monkeypatch.setattr(
        "pipeline.prep_streams.compute_audio_drop_indices",
        lambda item, config: [0, 1, 2],
    )

    mkvmerge_called = {"yes": False}
    def should_not_run(*a, **kw):
        mkvmerge_called["yes"] = True
        raise AssertionError("mkvmerge must NOT run when refusing to drop all audio")
    monkeypatch.setattr(
        "pipeline.compliance_fixers._mkvmerge_drop_streams_to_path", should_not_run
    )

    ok, msg = strip_streams_locally("/fake.mkv", item, {})
    assert ok is False
    assert "all" in msg.lower() and "audio" in msg.lower()
    assert mkvmerge_called["yes"] is False


def test_strip_returns_sibling_path_on_success(monkeypatch):
    """When the strip helper succeeds, the wrapper returns
    (True, <sibling path>) — NOT the original local path. The encoder
    consumes the sibling so the fetched source is never modified
    (eliminates the os.replace lock race against Windows antivirus)."""
    item = {
        "audio_streams": [
            _audio(lang="eng"),
            _audio(lang="fre"),
            _audio(lang="ger"),
        ],
        "subtitle_streams": [_sub(lang="eng"), _sub(lang="fre")],
        "tmdb": {"original_language": "en"},
    }
    config = {
        "strip_non_english_audio": True,
        "strip_non_english_subs": True,
        "audio_keep_policy": "english_und",
    }
    captured = {"src": None, "dst": None, "drop_a": None, "drop_s": None}

    def fake_drop(src, dst, *, drop_audio_indices=None, drop_sub_indices=None):
        captured["src"] = src
        captured["dst"] = dst
        captured["drop_a"] = drop_audio_indices
        captured["drop_s"] = drop_sub_indices
        return True

    monkeypatch.setattr(
        "pipeline.compliance_fixers._mkvmerge_drop_streams_to_path", fake_drop
    )
    ok, path = strip_streams_locally("/fake/X.mkv", item, config)
    assert ok is True
    # Strip writes to a sibling, NOT to the original
    assert path == "/fake/X.mkv.stripped.mkv", (
        f"strip output must be a sibling, got {path!r}"
    )
    assert captured["src"] == "/fake/X.mkv"
    assert captured["dst"] == "/fake/X.mkv.stripped.mkv"
    # Foreign audio (1, 2) dropped; foreign sub (1) dropped
    assert sorted(captured["drop_a"] or []) == [1, 2]
    assert captured["drop_s"] == [1]


def test_strip_propagates_mkvmerge_failure(monkeypatch):
    """If the underlying helper returns False (proof-of-work mismatch
    or rc!=0), the wrapper surfaces a usable error message."""
    item = {
        "audio_streams": [
            _audio(lang="eng"), _audio(lang="fre"), _audio(lang="ger"),
        ],
        "subtitle_streams": [],
        "tmdb": {"original_language": "en"},
    }
    config = {
        "strip_non_english_audio": True,
        "audio_keep_policy": "english_und",
    }
    monkeypatch.setattr(
        "pipeline.compliance_fixers._mkvmerge_drop_streams_to_path",
        lambda src, dst, **kw: False,
    )
    ok, msg = strip_streams_locally("/fake.mkv", item, config)
    assert ok is False
    assert "fail" in msg.lower()
