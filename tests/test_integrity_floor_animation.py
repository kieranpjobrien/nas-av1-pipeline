"""The output-integrity bitrate floor must not bin legitimate animation encodes.

2026-09-11: the pipeline deleted 35 finished Archer encodes and re-encoded and
deleted them again (one auto-retry each) before parking them in ERROR:

    Output integrity check FAILED: codec='av1'
    output_bitrate=1787kbps input_bitrate=38125kbps (minimum: 200kbps and >=5% of source)

1787/38125 = 4.7%. The encodes were fine. Animation is drawn, not filmed - flat
cel shading, hard edges, big uniform areas, no sensor grain - so AV1 compresses
it far harder than live action, and it lands just under the 5% AV1 floor.

The floor exists to catch "ffmpeg wrote three seconds of frames and stamped the
full duration in the container header". That failure is an order of magnitude
below this, so 2% still catches it while leaving animation alone.
"""

import pytest

from pipeline.full_gamut import (
    ANIMATION_MIN_RATIO,
    AV1_MIN_RATIO,
    DEFAULT_MIN_RATIO,
    _is_animation,
    _min_bitrate_ratio,
)


def entry(*genres):
    return {"tmdb": {"genres": list(genres)}}


class TestAnimationDetection:
    def test_animation_genre_is_detected(self):
        assert _is_animation(entry("Animation", "Comedy"))

    def test_case_and_position_do_not_matter(self):
        assert _is_animation(entry("Comedy", "animation"))

    def test_live_action_is_not_animation(self):
        assert not _is_animation(entry("Drama", "Thriller"))

    def test_missing_entry_is_not_animation(self):
        """No TMDb row must fall back to the stricter floor, never the looser one."""
        assert not _is_animation(None)
        assert not _is_animation({})
        assert not _is_animation({"tmdb": {}})


class TestRatioFloor:
    def test_animation_av1_gets_the_lower_floor(self):
        assert _min_bitrate_ratio("av1", entry("Animation")) == ANIMATION_MIN_RATIO

    def test_live_action_av1_keeps_the_av1_floor(self):
        assert _min_bitrate_ratio("av1", entry("Drama")) == AV1_MIN_RATIO

    def test_av1_nvenc_is_treated_as_av1(self):
        assert _min_bitrate_ratio("av1_nvenc", entry("Animation")) == ANIMATION_MIN_RATIO

    def test_non_av1_ignores_genre_entirely(self):
        """h264-in/h264-out has no efficiency win to justify a low floor."""
        assert _min_bitrate_ratio("h264", entry("Animation")) == DEFAULT_MIN_RATIO
        assert _min_bitrate_ratio("hevc", entry("Animation")) == DEFAULT_MIN_RATIO

    def test_unknown_entry_does_not_loosen_the_floor(self):
        assert _min_bitrate_ratio("av1", None) == AV1_MIN_RATIO


class TestTheArcherRegression:
    """The three real failures from the 2026-09-11 log, verbatim."""

    ARCHER = [(1539, 30870), (1560, 31590), (1802, 36854), (1787, 38125)]

    @pytest.mark.parametrize(("out_kbps", "in_kbps"), ARCHER)
    def test_archer_passes_under_the_animation_floor(self, out_kbps, in_kbps):
        floor = _min_bitrate_ratio("av1", entry("Animation", "Comedy"))
        assert out_kbps >= in_kbps * floor

    @pytest.mark.parametrize(("out_kbps", "in_kbps"), ARCHER)
    def test_archer_would_still_fail_the_old_flat_floor(self, out_kbps, in_kbps):
        """Proves the test is actually exercising the bug, not passing vacuously."""
        assert out_kbps < in_kbps * AV1_MIN_RATIO


class TestTheFloorStillCatchesTruncation:
    """A three-second encode of a 40-minute source is what this guard is for."""

    def test_truncated_output_fails_even_for_animation(self):
        floor = _min_bitrate_ratio("av1", entry("Animation"))
        # ffmpeg died early: ~0.2% of source bitrate
        assert not (70 >= 38125 * floor)

    def test_floor_ordering_is_sane(self):
        assert 0 < ANIMATION_MIN_RATIO < AV1_MIN_RATIO < DEFAULT_MIN_RATIO
