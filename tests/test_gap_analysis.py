"""Tests for pipeline/gap_filler.py analyse_gaps — gap analysis for already-AV1 files."""

import pytest

from pipeline.config import build_config
from pipeline.gap_filler import GapAnalysis, analyse_gaps


@pytest.fixture()
def default_config():
    """Return the default pipeline config for gap analysis tests."""
    return build_config()


def _make_entry(
    audio_streams=None,
    subtitle_streams=None,
    tmdb=None,
    filepath=r"\\KieranNAS\Media\Movies\Test (2020)\Test (2020).mkv",
    filename="Test (2020).mkv",
    library_type="movie",
):
    """Build a minimal file_entry dict for analyse_gaps."""
    return {
        "filepath": filepath,
        "filename": filename,
        "library_type": library_type,
        "audio_streams": audio_streams or [],
        "subtitle_streams": subtitle_streams or [],
        "tmdb": tmdb,
    }


class TestNeedsAnything:
    """Tests for the needs_anything aggregate property."""

    def test_fully_clean_file(self, default_config):
        """A file with all EAC-3 audio, one English sub, and TMDb metadata needs nothing."""
        entry = _make_entry(
            audio_streams=[{"codec_raw": "eac3", "language": "eng", "channels": 6}],
            subtitle_streams=[{"language": "eng", "title": ""}],
            tmdb={"id": 123, "title": "Test"},
        )
        gaps = analyse_gaps(entry, default_config)
        assert gaps.needs_anything is False

    def test_fully_clean_no_subs(self, default_config):
        """A file with EAC-3 audio, no subs, and TMDb is fully clean."""
        entry = _make_entry(
            audio_streams=[{"codec_raw": "eac3", "language": "eng"}],
            subtitle_streams=[],
            tmdb={"id": 1},
        )
        gaps = analyse_gaps(entry, default_config)
        assert gaps.needs_anything is False


class TestForeignAudio:
    """Foreign audio track detection and track removal."""

    def test_foreign_audio_triggers_removal(self, default_config):
        """A file with non-English audio tracks (beyond track 0) needs track removal."""
        entry = _make_entry(
            audio_streams=[
                {"codec_raw": "eac3", "language": "eng", "channels": 6},
                {"codec_raw": "eac3", "language": "fra", "channels": 6},
                {"codec_raw": "eac3", "language": "deu", "channels": 6},
            ],
            subtitle_streams=[{"language": "eng", "title": ""}],
            tmdb={"id": 1},
        )
        gaps = analyse_gaps(entry, default_config)
        assert gaps.needs_track_removal is True
        # Track 0 (eng) kept as original, track 1 (fra) and track 2 (deu) stripped
        assert 0 in gaps.audio_keep_indices
        assert 1 not in gaps.audio_keep_indices
        assert 2 not in gaps.audio_keep_indices

    def test_english_and_und_audio_kept(self, default_config):
        """English and 'und' audio tracks (beyond track 0) are kept."""
        entry = _make_entry(
            audio_streams=[
                {"codec_raw": "eac3", "language": "eng"},
                {"codec_raw": "eac3", "language": "und"},
            ],
            subtitle_streams=[{"language": "eng", "title": ""}],
            tmdb={"id": 1},
        )
        gaps = analyse_gaps(entry, default_config)
        # Both are English/und — no removal needed from the audio side alone
        assert 0 in gaps.audio_keep_indices
        assert 1 in gaps.audio_keep_indices


class TestSubtitleSelection:
    """Subtitle track selection: keep 1 English, strip HI, strip duplicates."""

    def test_two_english_subs_keeps_first(self, default_config):
        """Only the first regular English subtitle is kept; duplicates are stripped."""
        entry = _make_entry(
            audio_streams=[{"codec_raw": "eac3", "language": "eng"}],
            subtitle_streams=[
                {"language": "eng", "title": ""},
                {"language": "eng", "title": "English 2"},
            ],
            tmdb={"id": 1},
        )
        gaps = analyse_gaps(entry, default_config)
        assert gaps.needs_track_removal is True
        assert len(gaps.sub_keep_indices) == 1
        assert gaps.sub_keep_indices[0] == 0

    def test_hi_sub_stripped(self, default_config):
        """A subtitle titled 'SDH' (hearing-impaired) is stripped."""
        entry = _make_entry(
            audio_streams=[{"codec_raw": "eac3", "language": "eng"}],
            subtitle_streams=[
                {"language": "eng", "title": "SDH"},
                {"language": "eng", "title": ""},
            ],
            tmdb={"id": 1},
        )
        gaps = analyse_gaps(entry, default_config)
        assert gaps.needs_track_removal is True
        # SDH (index 0) stripped, regular (index 1) kept
        assert 0 not in gaps.sub_keep_indices
        assert 1 in gaps.sub_keep_indices

    def test_forced_sub_always_kept(self, default_config):
        """Forced/foreign-parts subs are always kept alongside the regular English sub."""
        entry = _make_entry(
            audio_streams=[{"codec_raw": "eac3", "language": "eng"}],
            subtitle_streams=[
                {"language": "eng", "title": "Forced"},
                {"language": "eng", "title": ""},
            ],
            tmdb={"id": 1},
        )
        gaps = analyse_gaps(entry, default_config)
        assert 0 in gaps.sub_keep_indices  # forced kept
        assert 1 in gaps.sub_keep_indices  # regular eng kept

    def test_non_english_subs_stripped(self, default_config):
        """Non-English subtitles are stripped."""
        entry = _make_entry(
            audio_streams=[{"codec_raw": "eac3", "language": "eng"}],
            subtitle_streams=[
                {"language": "eng", "title": ""},
                {"language": "fra", "title": "French"},
                {"language": "deu", "title": "German"},
            ],
            tmdb={"id": 1},
        )
        gaps = analyse_gaps(entry, default_config)
        assert gaps.needs_track_removal is True
        assert gaps.sub_keep_indices == [0]


class TestAudioTranscode:
    """Audio codec transcoding detection."""

    def test_truehd_is_passthrough_preserves_atmos(self, default_config):
        """TrueHD is the primary Dolby Atmos carrier — preserve bit-exact, don't transcode.

        Sonos Arc decodes TrueHD-Atmos natively. Transcoding to EAC-3 7.1 would
        drop the height-channel object layer. See CLAUDE.md rule 9a.
        """
        entry = _make_entry(
            audio_streams=[{"codec_raw": "truehd", "language": "eng", "channels": 8}],
            subtitle_streams=[{"language": "eng", "title": ""}],
            tmdb={"id": 1},
        )
        gaps = analyse_gaps(entry, default_config)
        assert gaps.needs_audio_transcode is False, (
            "TrueHD must passthrough — Atmos carrier (see _should_transcode_audio)"
        )
        assert 0 not in gaps.audio_transcode_indices
        assert gaps.needs_fetch is False

    def test_aac_needs_transcode(self, default_config):
        """AAC audio needs transcoding (not EAC-3)."""
        entry = _make_entry(
            audio_streams=[{"codec_raw": "aac", "language": "eng", "channels": 2}],
            subtitle_streams=[{"language": "eng", "title": ""}],
            tmdb={"id": 1},
        )
        gaps = analyse_gaps(entry, default_config)
        assert gaps.needs_audio_transcode is True
        assert gaps.needs_fetch is True

    def test_eac3_no_transcode(self, default_config):
        """EAC-3 audio does NOT need transcoding (already target codec)."""
        entry = _make_entry(
            audio_streams=[{"codec_raw": "eac3", "language": "eng"}],
            subtitle_streams=[{"language": "eng", "title": ""}],
            tmdb={"id": 1},
        )
        gaps = analyse_gaps(entry, default_config)
        assert gaps.needs_audio_transcode is False
        assert gaps.needs_fetch is False


class TestMetadata:
    """TMDb metadata detection."""

    def test_missing_tmdb_needs_metadata(self, default_config):
        """A file without TMDb data needs metadata enrichment."""
        entry = _make_entry(
            audio_streams=[{"codec_raw": "eac3", "language": "eng"}],
            subtitle_streams=[{"language": "eng", "title": ""}],
            tmdb=None,
        )
        gaps = analyse_gaps(entry, default_config)
        assert gaps.needs_metadata is True

    def test_present_tmdb_no_metadata_needed(self, default_config):
        """A file with TMDb data does not need metadata enrichment."""
        entry = _make_entry(
            audio_streams=[{"codec_raw": "eac3", "language": "eng"}],
            subtitle_streams=[{"language": "eng", "title": ""}],
            tmdb={"id": 1, "title": "Test"},
        )
        gaps = analyse_gaps(entry, default_config)
        assert gaps.needs_metadata is False


class TestGapAnalysisDescribe:
    """GapAnalysis.describe() returns a human-readable summary."""

    def test_describe_nothing(self):
        """A gap analysis with no needs describes as 'nothing'."""
        gaps = GapAnalysis()
        assert gaps.describe() == "nothing"

    def test_describe_multiple(self):
        """Multiple needs are joined with ' + '."""
        gaps = GapAnalysis(needs_track_removal=True, needs_metadata=True)
        desc = gaps.describe()
        assert "strip tracks" in desc
        assert "write metadata" in desc
        assert " + " in desc
