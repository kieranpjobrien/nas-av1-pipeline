"""An unparseable colour tag must never cost the encode.

2026-10-19: all 21 IT Crowd episodes sat in `error`. They are PAL 480p and tag
their transfer characteristic "bt470bg" - legal for primaries and colorspace,
but NOT for color_trc, where that curve is called "gamma28". The pipeline
copied the source tag straight to the encoder and av1_nvenc refused the whole
command with EINVAL (ffmpeg exit 4294967274 = -22).
"""

import pytest

from pipeline.ffmpeg import sanitise_color_tags


class TestTheITCrowdCase:
    def test_bt470bg_transfer_becomes_gamma28(self):
        _p, trc, _m = sanitise_color_tags("bt470bg", "bt470bg", "bt470bg")
        assert trc == "gamma28"

    def test_bt470bg_is_still_fine_for_primaries_and_matrix(self):
        """It is a legal value there - only color_trc rejects it."""
        prim, _trc, mat = sanitise_color_tags("bt470bg", "bt470bg", "bt470bg")
        assert prim == "bt470bg"
        assert mat == "bt470bg"

    def test_ntsc_bt470m_transfer_becomes_gamma22(self):
        _p, trc, _m = sanitise_color_tags("bt470m", "bt470m", "smpte170m")
        assert trc == "gamma22"


class TestFallbacks:
    @pytest.mark.parametrize("bad", ["", None, "nonsense", "reserved", "unknown"])
    def test_unparseable_values_fall_back_to_bt709(self, bad):
        assert sanitise_color_tags(bad, bad, bad) == ("bt709", "bt709", "bt709")

    def test_case_and_whitespace_are_tolerated(self):
        assert sanitise_color_tags(" BT709 ", "  SMPTE2084", "Bt2020nc") == (
            "bt709",
            "smpte2084",
            "bt2020nc",
        )

    def test_underscore_spellings_are_mapped(self):
        _p, trc, _m = sanitise_color_tags("bt2020", "arib_std_b67", "bt2020nc")
        assert trc == "arib-std-b67"


class TestGoodTagsSurviveUntouched:
    """The fallback must not quietly flatten correct HDR/SDR metadata."""

    def test_hdr10(self):
        assert sanitise_color_tags("bt2020", "smpte2084", "bt2020nc") == (
            "bt2020",
            "smpte2084",
            "bt2020nc",
        )

    def test_hlg(self):
        _p, trc, _m = sanitise_color_tags("bt2020", "arib-std-b67", "bt2020nc")
        assert trc == "arib-std-b67"

    def test_ordinary_sdr(self):
        assert sanitise_color_tags("bt709", "bt709", "bt709") == ("bt709", "bt709", "bt709")

    def test_dvd_era_ntsc(self):
        assert sanitise_color_tags("smpte170m", "smpte170m", "smpte170m") == (
            "smpte170m",
            "smpte170m",
            "smpte170m",
        )

    def test_a_matrix_only_value_is_not_accepted_as_primaries(self):
        """'ycgco' is a colorspace, never a primaries value."""
        prim, _trc, mat = sanitise_color_tags("ycgco", "bt709", "ycgco")
        assert prim == "bt709"
        assert mat == "ycgco"
