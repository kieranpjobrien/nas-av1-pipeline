"""Single-layer Dolby Vision must never reach the encoder.

2026-11-11: Sideways played with magenta skin and fluorescent greens. Its
source was Sideways.2004.MULTI.DV.2160p.WEB.H265-LOST - Dolby Vision Profile 5,
single-layer, no HDR10 fallback, colour stored as ICtCp - and the pipeline
re-encoded it to AV1. av1_nvenc cannot carry a DV RPU, so the RPU was dropped
and the ICtCp planes are now decoded as ordinary YCbCr. The damage is in the
pixel data, so retagging and remuxing are both useless; the title has to be
grabbed again. Measured saturation on the output was satmax 195 against 83 on a
known-good control.

The tests pin both directions, because over-refusing is its own failure:

  * profiles 4 and 5 must refuse - they are the single-layer ones;
  * profiles 7 and 8 must be allowed - they carry a conformant HDR10 base
    layer, so a re-encode loses Dolby Vision but keeps correct colour, and
    refusing them would strand 186 perfectly good disc-sourced titles.
"""

import copy
from unittest.mock import patch

import pytest

from pipeline import ffmpeg as fx


def _item(**over):
    item = {
        "audio_streams": [{"codec": "eac3", "channels": 6, "language": "eng"}],
        "hdr": True,
        "resolution": "4K",
        "duration_seconds": 6000,
    }
    item.update(over)
    return item


def _config():
    """The project's real defaults.

    The allow-cases have to run the full builder, so a hand-rolled stub is no
    good - it just fails on the first key it is missing and would pass the
    refuse-cases for the wrong reason.
    """
    from pipeline.config import DEFAULT_CONFIG

    cfg = copy.deepcopy(DEFAULT_CONFIG)
    cfg["audio_mode"] = "copy"
    return cfg


class TestRefusesSingleLayer:
    @pytest.mark.parametrize("profile", [4, 5])
    def test_profile_from_item_is_refused(self, profile):
        with pytest.raises(ValueError, match="single-layer Dolby Vision"):
            fx.build_ffmpeg_cmd("in.mkv", "out.mkv", _item(dv_profile=profile), _config())

    def test_message_says_to_re_source(self):
        with pytest.raises(ValueError, match="[Rr]e-source"):
            fx.build_ffmpeg_cmd("in.mkv", "out.mkv", _item(dv_profile=5), _config())

    def test_falls_back_to_probing_when_item_has_no_field(self):
        """Rows scanned before dv_profile existed carry no such key."""
        with patch.object(fx, "probe_dv_profile", return_value=5) as probe:
            with pytest.raises(ValueError, match="profile 5"):
                fx.build_ffmpeg_cmd("in.mkv", "out.mkv", _item(), _config())
        probe.assert_called_once()

    def test_profile_5_refused_before_any_subprocess_encode(self):
        """The refusal must happen while building, not after ffmpeg starts."""
        with patch("subprocess.run") as run:
            with pytest.raises(ValueError):
                fx.build_ffmpeg_cmd("in.mkv", "out.mkv", _item(dv_profile=5), _config())
            # get_duration / colour probes may not run, but no encode may be launched.
            for call in run.call_args_list:
                argv = call.args[0] if call.args else []
                assert "av1_nvenc" not in " ".join(map(str, argv))


class TestAllowsDualLayer:
    @pytest.mark.parametrize("profile", [7, 8])
    def test_disc_profiles_are_not_refused(self, profile):
        """P7/P8 keep an HDR10 base layer - refusing them would strand 186 titles."""
        with patch.object(fx, "probe_dv_profile", return_value=profile):
            cmd = fx.build_ffmpeg_cmd("in.mkv", "out.mkv", _item(dv_profile=profile), _config())
        assert isinstance(cmd, list) and cmd

    def test_no_dv_at_all_is_not_refused(self):
        with patch.object(fx, "probe_dv_profile", return_value=None):
            cmd = fx.build_ffmpeg_cmd("in.mkv", "out.mkv", _item(), _config())
        assert isinstance(cmd, list) and cmd

    def test_unreadable_probe_does_not_refuse(self):
        """A failed probe must not be treated as proof of profile 5."""
        with patch.object(fx, "probe_dv_profile", return_value=None):
            cmd = fx.build_ffmpeg_cmd("in.mkv", "out.mkv", _item(), _config())
        assert cmd


class TestProbe:
    def test_reads_dv_profile_from_side_data(self):
        payload = '{"streams":[{"side_data_list":[{"dv_profile":5,"dv_level":6}]}]}'
        with patch("subprocess.run") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = payload
            assert fx.probe_dv_profile("x.mkv") == 5

    def test_returns_none_when_no_dv_side_data(self):
        with patch("subprocess.run") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = '{"streams":[{"side_data_list":[]}]}'
            assert fx.probe_dv_profile("x.mkv") is None

    def test_returns_none_on_probe_failure(self):
        with patch("subprocess.run") as run:
            run.return_value.returncode = 1
            run.return_value.stdout = ""
            assert fx.probe_dv_profile("x.mkv") is None

    def test_returns_none_on_garbage_output(self):
        with patch("subprocess.run") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = "not json"
            assert fx.probe_dv_profile("x.mkv") is None

    def test_single_layer_constant_covers_4_and_5_only(self):
        assert set(fx.DV_SINGLE_LAYER_PROFILES) == {4, 5}
