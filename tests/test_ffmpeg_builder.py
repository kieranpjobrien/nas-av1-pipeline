"""Regression tests for ffmpeg command-builder invariants.

Rules (from the 2026-04-23 audio-loss incident postmortem):
  - No global ``-err_detect ignore_err`` (must be ``-err_detect:v``). A corrupt
    audio header combined with the global form caused ffmpeg to silently
    skip the audio stream and exit 0.
  - No ``-map 0:a?`` (optional audio map) — the ``?`` silently produced
    zero-audio output when combined with a corrupt header or stale
    audio_streams list.
  - Refuse to build commands when the source has zero audio streams. Either
    the scanner misreported (fix the scanner) or the source is damaged
    (delete + re-source) — either way, never emit an encode command.
  - TrueHD + Opus + EAC-3 passthrough — never transcode these. TrueHD
    is the Atmos carrier and the user has a Sonos Arc that decodes it.
"""
from __future__ import annotations

import pytest

from pipeline.config import build_config
from pipeline.ffmpeg import _should_transcode_audio, build_audio_remux_cmd, build_ffmpeg_cmd


def _base_config() -> dict:
    """Full default config with the fields the builders rely on."""
    return build_config(
        {
            "video_codec": "av1_nvenc",
            "audio_mode": "smart",
            "audio_eac3_surround_bitrate": "640k",
            "audio_eac3_stereo_bitrate": "256k",
            "strip_non_english_audio": True,
            "strip_non_english_subs": True,
        }
    )


def _base_item() -> dict:
    """Minimal item dict with one English stereo AAC audio stream."""
    return {
        "audio_streams": [{"codec_raw": "aac", "channels": 2, "language": "eng"}],
        "subtitle_streams": [],
        "duration_seconds": 1000,
        "hdr": False,
    }


def _assert_no_optional_audio_map(cmd: list[str]) -> None:
    """Check the command never uses the optional-audio-map form."""
    for i, tok in enumerate(cmd[:-1]):
        if tok == "-map":
            assert cmd[i + 1] != "0:a?", (
                f"optional audio map at index {i}: {cmd[i:i + 2]}. "
                "The `?` form silently produces zero-audio output on corrupt sources."
            )


def _assert_err_detect_scoped_to_video(cmd: list[str]) -> None:
    """Check any ``-err_detect`` is scoped to video (``:v``), not global."""
    # There should never be a bare ``-err_detect`` followed by ``ignore_err``.
    for i, tok in enumerate(cmd[:-1]):
        if tok == "-err_detect":
            assert cmd[i + 1] != "ignore_err", (
                f"bare global -err_detect ignore_err forbidden at index {i}. "
                "Must be -err_detect:v (scoped to video stream only)."
            )
    # ``-err_detect:v`` is fine — don't enforce its presence, just its scoping.


class TestBuildFfmpegCmdInvariants:
    """Invariants for the AV1 re-encode ffmpeg command builder."""

    def test_no_optional_audio_map(self) -> None:
        cmd = build_ffmpeg_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=_base_item(),
            config=_base_config(),
        )
        _assert_no_optional_audio_map(cmd)

    def test_err_detect_scoped_to_video(self) -> None:
        cmd = build_ffmpeg_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=_base_item(),
            config=_base_config(),
        )
        _assert_err_detect_scoped_to_video(cmd)

    def test_refuses_zero_audio_source(self) -> None:
        item = _base_item()
        item["audio_streams"] = []
        with pytest.raises(ValueError, match="zero audio streams"):
            build_ffmpeg_cmd(
                input_path="in.mkv",
                output_path="out.mkv",
                item=item,
                config=_base_config(),
            )

    def test_refuses_missing_audio_streams_key(self) -> None:
        """A dict with no ``audio_streams`` key at all is also refused."""
        item = {"subtitle_streams": [], "duration_seconds": 1000, "hdr": False}
        with pytest.raises(ValueError, match="zero audio streams"):
            build_ffmpeg_cmd(
                input_path="in.mkv",
                output_path="out.mkv",
                item=item,
                config=_base_config(),
            )

    def test_invariants_hold_with_stripped_audio(self) -> None:
        """Same invariants with multi-stream strip scenario (non-English stripped)."""
        item = _base_item()
        item["audio_streams"] = [
            {"codec_raw": "aac", "channels": 2, "language": "eng"},
            {"codec_raw": "aac", "channels": 2, "language": "fra"},
            {"codec_raw": "aac", "channels": 2, "language": "jpn"},
        ]
        cmd = build_ffmpeg_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=item,
            config=_base_config(),
        )
        _assert_no_optional_audio_map(cmd)
        _assert_err_detect_scoped_to_video(cmd)


class TestBuildAudioRemuxCmdInvariants:
    """Invariants for the audio-remux-only ffmpeg command builder."""

    def test_no_optional_audio_map(self) -> None:
        cmd = build_audio_remux_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=_base_item(),
            config=_base_config(),
        )
        _assert_no_optional_audio_map(cmd)

    def test_err_detect_scoped_to_video(self) -> None:
        # audio-remux doesn't currently emit -err_detect, but if someone adds
        # it in future, it must be :v-scoped like build_ffmpeg_cmd.
        cmd = build_audio_remux_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=_base_item(),
            config=_base_config(),
        )
        _assert_err_detect_scoped_to_video(cmd)

    def test_refuses_zero_audio_source(self) -> None:
        item = _base_item()
        item["audio_streams"] = []
        with pytest.raises(ValueError, match="zero audio streams"):
            build_audio_remux_cmd(
                input_path="in.mkv",
                output_path="out.mkv",
                item=item,
                config=_base_config(),
            )

    def test_refuses_missing_audio_streams_key(self) -> None:
        item = {"subtitle_streams": [], "duration_seconds": 1000, "hdr": False}
        with pytest.raises(ValueError, match="zero audio streams"):
            build_audio_remux_cmd(
                input_path="in.mkv",
                output_path="out.mkv",
                item=item,
                config=_base_config(),
            )


class TestAudioPassthroughPolicy:
    """Codecs that must NEVER be transcoded (rule 9a + audio policy)."""

    def test_eac3_passthrough(self) -> None:
        # Already target codec — bit-exact passthrough (preserves EAC-3-JOC / Atmos)
        assert _should_transcode_audio({"codec_raw": "eac3"}, _base_config()) is False
        assert _should_transcode_audio({"codec": "e-ac-3"}, _base_config()) is False

    def test_truehd_passthrough_preserves_atmos(self) -> None:
        # TrueHD is the primary Dolby Atmos carrier. User has Sonos Arc.
        # Transcoding to EAC-3 would drop the object layer.
        assert _should_transcode_audio({"codec_raw": "truehd", "channels": 8}, _base_config()) is False

    def test_opus_passthrough(self) -> None:
        # Opus is already an efficient lossy codec — no benefit to re-encoding.
        assert _should_transcode_audio({"codec_raw": "opus"}, _base_config()) is False

    def test_dts_hd_ma_still_transcoded(self) -> None:
        # DTS-HD MA is lossless but doesn't carry Atmos — transcode to EAC-3 640k.
        assert _should_transcode_audio({"codec_raw": "dts", "profile": "DTS-HD MA"}, _base_config()) is True

    def test_flac_still_transcoded(self) -> None:
        assert _should_transcode_audio({"codec_raw": "flac"}, _base_config()) is True

    def test_ac3_still_transcoded(self) -> None:
        # Plain AC-3 → upgrade to EAC-3 (better codec at same 640k).
        assert _should_transcode_audio({"codec_raw": "ac3"}, _base_config()) is True
