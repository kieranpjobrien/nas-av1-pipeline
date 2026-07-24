"""Regression: an audio-only strip must never drop subtitles.

When the sub selector DEFERS (unresolved language — inviolate rule 2026-04-29)
it leaves sub_keep_indices empty. The audio block can independently set
needs_track_removal, and _build_keep_ids_from_identify then read empty-plus-
removal as "strip every subtitle" -> mkvmerge --no-subtitles -> every subtitle
track wiped in place on the NAS, no backup. The post-mux verify only counted
video and audio, so nothing caught it. Found 2026-07-25.
"""

from pipeline.config import build_config
from pipeline.gap_filler import GapAnalysis, _build_keep_ids_from_identify, analyse_gaps

_ID_DATA = {
    "tracks": [
        {"id": 0, "type": "video", "properties": {}},
        {"id": 1, "type": "audio", "properties": {}},
        {"id": 2, "type": "audio", "properties": {}},
        {"id": 3, "type": "audio", "properties": {}},
        {"id": 4, "type": "subtitles", "properties": {"track_name": "English"}},
        {"id": 5, "type": "subtitles", "properties": {"track_name": ""}},
    ]
}


def _entry():
    """Multi-audio file needing an audio strip, with one unresolved-language sub."""
    return {
        "filepath": r"\\KieranNAS\Media\Movies\Matilda (1996)\Matilda (1996).mkv",
        "filename": "Matilda (1996).mkv",
        "library_type": "movie",
        "video": {"codec_raw": "av1", "codec": "AV1", "resolution_class": "1080p"},
        "audio_streams": [
            {"codec_raw": "eac3", "language": "eng", "channels": 6},
            {"codec_raw": "eac3", "language": "fre", "channels": 6},
            {"codec_raw": "eac3", "language": "ger", "channels": 6},
        ],
        "subtitle_streams": [
            {"codec": "subrip", "language": "eng", "title": ""},
            {"codec": "subrip", "language": "und", "title": ""},  # unresolved
        ],
    }


def test_deferred_sub_strip_is_flagged_not_treated_as_strip_all():
    gaps = analyse_gaps(_entry(), build_config({}))
    assert gaps.needs_track_removal, "audio strip should still be planned"
    assert gaps.sub_strip_deferred, "unresolved sub language must record a deferral"
    assert gaps.sub_keep_indices == [], "deferral leaves the keep-set empty"
    assert gaps.source_sub_count == 2


def test_audio_strip_with_deferred_subs_does_not_drop_subtitles():
    gaps = analyse_gaps(_entry(), build_config({}))
    audio_keep, sub_keep, no_subs = _build_keep_ids_from_identify(_ID_DATA, gaps)

    assert no_subs is False, (
        "audio-only strip must NOT set --no-subtitles when the sub strip was "
        "deferred — this wiped every subtitle track in place"
    )
    assert sub_keep is None, "None = keep all subtitles"
    assert audio_keep == [1], "audio strip still applies (keep stream 0 only)"


def test_genuine_strip_all_subs_still_supported():
    """A real 'remove every sub' plan (not a deferral) must still work, so the
    fix doesn't disable legitimate stripping."""
    gaps = GapAnalysis()
    gaps.needs_track_removal = True
    gaps.sub_keep_indices = []
    gaps.sub_strip_deferred = False
    _a, _s, no_subs = _build_keep_ids_from_identify(_ID_DATA, gaps)
    assert no_subs is True
