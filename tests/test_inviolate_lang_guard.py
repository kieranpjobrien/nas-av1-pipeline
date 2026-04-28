"""Inviolate rule (2026-04-29): never strip a track without first knowing its language.

These tests pin down the guard at every strip-decision point in the pipeline:
  * pipeline.streams.select_sub_keep_indices              (gap_filler path)
  * pipeline.streams.select_audio_keep_indices_by_original_language (encode + gap_filler)
  * pipeline.ffmpeg._map_subtitle_streams                 (full_gamut encode)
  * pipeline.ffmpeg._select_audio_streams (legacy path)   (full_gamut encode)
  * pipeline.gap_filler.analyse_gaps                      (gap_filler queue build)

In each case: a single unresolved (`und` / empty) track in the file must
prevent any strip decision from being taken. Files become eligible only
when every track has either a real `language` tag or a real
`detected_language` from whisper.
"""

from __future__ import annotations

from pipeline.ffmpeg import _select_audio_streams
from pipeline.gap_filler import analyse_gaps
from pipeline.streams import (
    SubStream,
    all_languages_known,
    parse_audio_stream,
    select_audio_keep_indices_by_original_language,
    select_sub_keep_indices,
)


# ---------------------------------------------------------------------------
# Predicate
# ---------------------------------------------------------------------------


def test_all_languages_known_empty_input_is_true():
    """Empty stream list is vacuously known."""
    assert all_languages_known([]) is True


def test_all_languages_known_recognises_real_lang_tag():
    s = SubStream(index=0, codec="srt", language="eng", title="", is_forced=False, is_hi=False, detected_language=None)
    assert all_languages_known([s]) is True


def test_all_languages_known_recognises_detected_language():
    """No tag, but whisper resolved it — that counts as known."""
    s = SubStream(index=0, codec="srt", language="", title="", is_forced=False, is_hi=False, detected_language="eng")
    assert all_languages_known([s]) is True


def test_all_languages_known_rejects_und_with_no_detection():
    s = SubStream(index=0, codec="srt", language="und", title="", is_forced=False, is_hi=False, detected_language=None)
    assert all_languages_known([s]) is False


def test_all_languages_known_rejects_empty_with_no_detection():
    s = SubStream(index=0, codec="srt", language="", title="", is_forced=False, is_hi=False, detected_language=None)
    assert all_languages_known([s]) is False


def test_all_languages_known_rejects_any_unresolved_track():
    """One bad apple — even if other tracks are tagged."""
    streams = [
        SubStream(index=0, codec="srt", language="eng", title="", is_forced=False, is_hi=False, detected_language=None),
        SubStream(index=1, codec="srt", language="und", title="", is_forced=False, is_hi=False, detected_language=None),
    ]
    assert all_languages_known(streams) is False


def test_all_languages_known_works_on_dicts():
    """Gap-filler hands raw ffprobe dicts in some paths — predicate must accept those."""
    assert all_languages_known([{"language": "eng"}]) is True
    assert all_languages_known([{"language": "und", "detected_language": None}]) is False
    assert all_languages_known([{"language": "und", "detected_language": "eng"}]) is True


# ---------------------------------------------------------------------------
# select_sub_keep_indices: returns None when any sub is unresolved
# ---------------------------------------------------------------------------


def _sub(idx: int, language: str = "eng", *, hi: bool = False, forced: bool = False, detected: str | None = None) -> SubStream:
    return SubStream(
        index=idx,
        codec="srt",
        language=language,
        title="",
        is_forced=forced,
        is_hi=hi,
        detected_language=detected,
    )


def test_select_sub_keep_indices_returns_none_when_any_und():
    """File with [eng, und, und] subs is NOT eligible for strip."""
    subs = [_sub(0, "eng"), _sub(1, "und"), _sub(2, "und")]
    assert select_sub_keep_indices(subs) is None


def test_select_sub_keep_indices_keeps_one_eng_when_all_known():
    """All tracks tagged → strip proceeds normally, picks first regular English."""
    subs = [_sub(0, "eng"), _sub(1, "fre"), _sub(2, "spa")]
    keep = select_sub_keep_indices(subs)
    assert keep == [0]


def test_select_sub_keep_indices_treats_detected_language_as_resolved():
    """Whisper-detected und becomes resolved — strip can proceed."""
    subs = [
        _sub(0, "eng"),
        _sub(1, "und", detected="fre"),  # whisper resolved this as French → strippable
    ]
    keep = select_sub_keep_indices(subs)
    assert keep == [0]


def test_select_sub_keep_indices_empty_input_returns_empty_list():
    """No subs to pick from — return empty (vacuously known)."""
    assert select_sub_keep_indices([]) == []


# ---------------------------------------------------------------------------
# select_audio_keep_indices_by_original_language: returns None on any und
# ---------------------------------------------------------------------------


def _audio(idx: int, language: str = "eng", *, detected: str | None = None) -> object:
    raw = {"language": language, "codec": "eac3", "channels": 6}
    if detected:
        raw["detected_language"] = detected
    return parse_audio_stream(raw, index=idx)


def test_audio_select_returns_none_when_any_und():
    """[eng, und] audio with TMDb=en still defers — und blocks the whole decision."""
    streams = [_audio(0, "eng"), _audio(1, "und")]
    assert select_audio_keep_indices_by_original_language(streams, "en") is None


def test_audio_select_strips_foreign_when_all_known():
    streams = [_audio(0, "eng"), _audio(1, "fre")]
    keep = select_audio_keep_indices_by_original_language(streams, "en")
    assert keep == [0]


def test_audio_select_und_resolved_by_whisper_unblocks():
    """Whisper-resolved und no longer blocks — strip proceeds."""
    streams = [
        _audio(0, "eng"),
        _audio(1, "und", detected="fre"),  # resolved as French
    ]
    keep = select_audio_keep_indices_by_original_language(streams, "en")
    assert keep == [0]


# ---------------------------------------------------------------------------
# _map_subtitle_streams (full_gamut encode): defers to -map 0:s? on any und
# ---------------------------------------------------------------------------


def test_map_subtitle_streams_defers_when_any_und():
    """Encode-time sub map: file with [eng, und] subs gets -map 0:s? (keep all)."""
    cmd: list[str] = []
    item = {
        "subtitle_streams": [
            {"language": "eng", "title": ""},
            {"language": "und", "title": ""},
        ]
    }
    config = {"strip_non_english_subs": True}
    from pipeline.ffmpeg import _map_subtitle_streams

    _map_subtitle_streams(cmd, item, config)
    # Should fall back to wildcard map, not per-index strip
    assert cmd == ["-map", "0:s?"]


def test_map_subtitle_streams_strips_when_all_known():
    """All subs tagged — proceeds to per-index strip and picks the English one."""
    cmd: list[str] = []
    item = {
        "subtitle_streams": [
            {"language": "eng", "title": ""},
            {"language": "fre", "title": ""},
        ]
    }
    config = {"strip_non_english_subs": True}
    from pipeline.ffmpeg import _map_subtitle_streams

    _map_subtitle_streams(cmd, item, config)
    # Maps eng (index 0) only — French is stripped
    assert "0:s:0?" in cmd
    assert "0:s:1?" not in cmd


# ---------------------------------------------------------------------------
# _select_audio_streams legacy path: defers when any und
# ---------------------------------------------------------------------------


def test_select_audio_streams_legacy_defers_when_any_und():
    """Legacy "english_und" policy must also defer when languages are unresolved.

    Trigger via no-TMDb item (forces the legacy fallback path) with mixed
    [eng, und, fre] — under the new rule we don't strip the French either.
    """
    item = {
        "audio_streams": [
            {"language": "eng", "codec": "eac3", "channels": 6},
            {"language": "und", "codec": "eac3", "channels": 6},
            {"language": "fre", "codec": "eac3", "channels": 6},
        ],
        # No tmdb -> falls through to legacy path
    }
    config = {"strip_non_english_audio": True, "audio_keep_policy": "english_und"}
    result = _select_audio_streams(item, config)
    # Defer signal: returns None means "keep everything".
    assert result is None


def test_select_audio_streams_legacy_strips_when_all_known():
    """Legacy path strips when every track has a real language."""
    item = {
        "audio_streams": [
            {"language": "eng", "codec": "eac3", "channels": 6},
            {"language": "fre", "codec": "eac3", "channels": 6},
            {"language": "spa", "codec": "eac3", "channels": 6},
        ],
    }
    config = {"strip_non_english_audio": True, "audio_keep_policy": "english_und"}
    result = _select_audio_streams(item, config)
    # First stream always kept; foreign dubs stripped
    assert result == [0]


# ---------------------------------------------------------------------------
# gap_filler.analyse_gaps: doesn't set needs_track_removal when unresolved
# ---------------------------------------------------------------------------


def _gap_entry(audio: list[dict], subs: list[dict], *, tmdb_lang: str | None = "en") -> dict:
    return {
        "filepath": r"\\NAS\Media\Series\Test\E01.mkv",
        "filename": "E01.mkv",
        "library_type": "series",
        "video": {"codec_raw": "av1"},
        "audio_streams": audio,
        "subtitle_streams": subs,
        "tmdb": {"id": 1, "original_language": tmdb_lang} if tmdb_lang else {"id": 1},
    }


def test_analyse_gaps_does_not_strip_subs_when_und_present():
    """Gap_filler must not flag a file for sub strip when any sub is unresolved."""
    config = {"strip_non_english_subs": True, "strip_non_english_audio": True}
    entry = _gap_entry(
        audio=[{"codec_raw": "eac3", "language": "eng"}],
        subs=[
            {"language": "eng", "title": ""},
            {"language": "und", "title": ""},  # unresolved — defers strip
        ],
    )
    gaps = analyse_gaps(entry, config)
    # Inviolate rule: needs_track_removal must NOT fire for the sub-strip case
    # when any sub is unresolved. (Audio side may set it independently — we
    # check the sub-keep state instead.)
    assert gaps.sub_keep_indices == []
    # And language detection IS flagged so the file gets resolved later.
    assert gaps.needs_language_detect is True


def test_analyse_gaps_strips_subs_when_all_known():
    """All subs have real language tags → strip proceeds normally."""
    config = {"strip_non_english_subs": True, "strip_non_english_audio": False}
    entry = _gap_entry(
        audio=[{"codec_raw": "eac3", "language": "eng"}],
        subs=[
            {"language": "eng", "title": ""},
            {"language": "fre", "title": "French"},
        ],
    )
    gaps = analyse_gaps(entry, config)
    assert gaps.needs_track_removal is True
    assert 0 in gaps.sub_keep_indices  # English kept
    assert 1 not in gaps.sub_keep_indices  # French stripped
