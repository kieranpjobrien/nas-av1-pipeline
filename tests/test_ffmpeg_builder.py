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


class TestHwaccelCuda:
    """NVDEC decode path — ``-hwaccel cuda -hwaccel_output_format cuda``.

    Root cause (2026-04-24 observation): ffmpeg at 520% CPU / 691 MB RAM while
    the GPU encoder chip was only at 52% utilisation. The decode was happening
    on CPU via libavcodec, PCIe-copying to GPU, encoding on NVENC, then copying
    back. Adding the hwaccel pair routes decode through NVDEC (a separate chip
    from NVENC on Ada cards — no encode/decode contention).

    Invariants the tests enforce:
      * Default build has the hwaccel pair BEFORE ``-i`` (ffmpeg requires this
        ordering; placed after ``-i`` they're ignored).
      * ``use_hwaccel=False`` omits them entirely — this is the fallback path
        for NVDEC-incompatible sources (10-bit H.264 High-10, MPEG-4 ASP, etc.).
      * Hwaccel presence does not break the err_detect-scoping or
        audio-map-never-optional invariants (regression check).
    """

    def test_default_adds_hwaccel_before_input(self) -> None:
        cmd = build_ffmpeg_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=_base_item(),
            config=_base_config(),
        )
        assert "-hwaccel" in cmd, "hwaccel should be present by default"
        hwa_idx = cmd.index("-hwaccel")
        assert cmd[hwa_idx + 1] == "cuda"
        # Ordering: -hwaccel pair must be before -i (ffmpeg ignores hwaccel flags
        # placed after -i without warning — silent performance regression).
        i_idx = cmd.index("-i")
        assert hwa_idx < i_idx, "hwaccel must precede -i or ffmpeg ignores it"
        # Output-format pair
        assert "-hwaccel_output_format" in cmd
        hof_idx = cmd.index("-hwaccel_output_format")
        assert cmd[hof_idx + 1] == "cuda"
        assert hof_idx < i_idx

    def test_use_hwaccel_false_omits_flags(self) -> None:
        cmd = build_ffmpeg_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=_base_item(),
            config=_base_config(),
            use_hwaccel=False,
        )
        assert "-hwaccel" not in cmd, (
            "use_hwaccel=False must omit -hwaccel — this is the fallback path "
            "for NVDEC-incompatible sources"
        )
        assert "-hwaccel_output_format" not in cmd

    def test_hwaccel_does_not_break_other_invariants(self) -> None:
        """Adding hwaccel must not regress the audio-map or err_detect rules."""
        cmd = build_ffmpeg_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=_base_item(),
            config=_base_config(),
        )
        _assert_no_optional_audio_map(cmd)
        _assert_err_detect_scoped_to_video(cmd)

    def test_hwaccel_uses_scale_cuda_not_pix_fmt(self) -> None:
        """With hwaccel on, pixel format must come from ``scale_cuda`` — not ``-pix_fmt``.

        Regression test for the 2026-04-24 filter-graph failure:

          Impossible to convert between the formats supported by the filter
          'Parsed_null_0' and the filter 'auto_scale_0'
          src: cuda  dst: yuv420p ... p010le ...

        ``-pix_fmt`` on the output with CUDA-memory input makes ffmpeg insert
        a CPU-side ``auto_scale`` filter that can't accept GPU frames. The fix
        is to do pixel-format conversion with ``scale_cuda`` (GPU-side) and
        omit ``-pix_fmt`` entirely when hwaccel is on.
        """
        cmd = build_ffmpeg_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=_base_item(),
            config=_base_config(),
        )
        assert any("scale_cuda" in c for c in cmd), (
            "expected scale_cuda filter when hwaccel is on; cmd: " + " ".join(cmd)
        )
        assert "-pix_fmt" not in cmd, (
            "-pix_fmt with hwaccel causes the CPU auto_scale filter to reject "
            "CUDA frames. Use scale_cuda=format=... instead."
        )

    def test_no_hwaccel_uses_pix_fmt_not_scale_cuda(self) -> None:
        """Without hwaccel, ``-pix_fmt`` is the right mechanism — scale_cuda would
        fail because the input is not in CUDA memory. Inverse of the above test.
        """
        cmd = build_ffmpeg_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=_base_item(),
            config=_base_config(),
            use_hwaccel=False,
        )
        assert "-pix_fmt" in cmd
        assert not any("scale_cuda" in c for c in cmd), (
            "scale_cuda requires CUDA-memory input; can't use it without hwaccel"
        )


class TestSubtitleMapOptional:
    """Per-index subtitle maps must use the ``?`` (optional) suffix.

    Root cause (2026-04-24 incident, IT Crowd .mp4 → .mkv remux):
      * Source .mp4 had one mov_text subtitle stream.
      * ``_remux_to_mkv`` attempt 1 failed (mov_text can't copy to MKV).
      * ``_remux_to_mkv`` attempt 2 succeeded by dropping subtitles.
      * ``item["subtitle_streams"]`` was never updated — still reported "1 sub".
      * ``_map_subtitle_streams`` emitted hard ``-map 0:s:0`` against the
        sub-less remuxed input → ffmpeg: ``Stream map '' matches no streams``.

    The fix: per-index maps use ``?`` so ffmpeg silently skips missing
    indices. This is INTENTIONAL divergence from the audio-map policy —
    rule 10 bans ``-map 0:a?`` (audio is mandatory; silent drop is the
    incident we rebuilt discipline around). Subs are legitimately optional
    (Bazarr backfill; sources without subs; remux drops).
    """

    def test_single_eng_sub_map_is_optional(self) -> None:
        item = _base_item()
        item["subtitle_streams"] = [{"language": "eng", "title": ""}]
        cmd = build_ffmpeg_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=item,
            config=_base_config(),
        )
        # The per-index sub map must be 0:s:0? not 0:s:0
        assert "0:s:0?" in cmd, f"expected optional per-index sub map; cmd: {cmd}"
        assert "0:s:0" not in cmd or cmd[cmd.index("0:s:0?") - 1] == "-map", (
            "hard 0:s:0 map present without ? — will crash ffmpeg on "
            "stale metadata (e.g. remux dropped subs)"
        )

    def test_forced_and_regular_eng_both_optional(self) -> None:
        item = _base_item()
        item["subtitle_streams"] = [
            {"language": "eng", "title": "Forced"},
            {"language": "eng", "title": ""},
        ]
        cmd = build_ffmpeg_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=item,
            config=_base_config(),
        )
        # Both kept indices must be optional
        assert "0:s:0?" in cmd
        assert "0:s:1?" in cmd
        # No hard per-index map survived
        for i, tok in enumerate(cmd[:-1]):
            if tok == "-map" and cmd[i + 1].startswith("0:s:") and not cmd[i + 1].endswith("?"):
                pytest.fail(f"hard sub map at index {i}: {cmd[i:i + 2]}")

    def test_stale_sub_metadata_survives(self) -> None:
        """Regression: item says 1 sub, input (hypothetically) has 0 — builder must not crash.

        We can't exec ffmpeg here, but we can assert the emitted command
        carries the ``?`` suffix so ffmpeg would skip silently instead of
        aborting with ``Stream map 0:s:0 matches no streams``.
        """
        item = _base_item()
        item["subtitle_streams"] = [{"language": "eng", "title": ""}]
        cmd = build_ffmpeg_cmd(
            input_path="in.mkv",
            output_path="out.mkv",
            item=item,
            config=_base_config(),
        )
        # Every per-index sub map must end with ?
        for i, tok in enumerate(cmd[:-1]):
            if tok == "-map" and cmd[i + 1].startswith("0:s:"):
                assert cmd[i + 1].endswith("?"), (
                    f"per-index sub map {cmd[i + 1]!r} missing ? — "
                    "stale metadata after remux will crash ffmpeg"
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

    def test_opus_now_transcoded(self) -> None:
        # Opus was previously passthrough on the assumption of "efficient lossy,
        # already good", but Sonos Arc cannot decode Opus natively — Plex transcoded
        # it on every play. Pre-transcoding to EAC-3 once eliminates that overhead.
        assert _should_transcode_audio({"codec_raw": "opus"}, _base_config()) is True

    def test_dts_hd_ma_still_transcoded(self) -> None:
        # DTS-HD MA is lossless but doesn't carry Atmos — transcode to EAC-3 640k.
        assert _should_transcode_audio({"codec_raw": "dts", "profile": "DTS-HD MA"}, _base_config()) is True

    def test_flac_still_transcoded(self) -> None:
        assert _should_transcode_audio({"codec_raw": "flac"}, _base_config()) is True

    def test_ac3_still_transcoded(self) -> None:
        # Plain AC-3 → upgrade to EAC-3 (better codec at same 640k).
        assert _should_transcode_audio({"codec_raw": "ac3"}, _base_config()) is True


class TestOriginalLanguageAudioKeep:
    """The "original_language" audio-keep policy.

    Strips foreign dubs (including English dubs of foreign-origin films).
    Falls back to the legacy KEEP_LANGS rule when there's no TMDb data.
    Conservative on `und` tracks whisper hasn't resolved.
    """

    @staticmethod
    def _config() -> dict:
        cfg = _base_config()
        cfg["audio_keep_policy"] = "original_language"
        cfg["audio_keep_english_with_original"] = False
        return cfg

    def test_spanish_original_strips_english_dub(self) -> None:
        """Y tu mamá también-style: TMDb original=es, keep Spanish, strip English dub."""
        from pipeline.ffmpeg import _select_audio_streams

        item = {
            "filename": "Y Tu Mama Tambien.mkv",
            "tmdb": {"original_language": "es"},
            "audio_streams": [
                {"index": 0, "codec_raw": "eac3", "language": "eng", "title": "English"},
                {"index": 1, "codec_raw": "eac3", "language": "spa", "title": "Spanish"},
                {"index": 2, "codec_raw": "eac3", "language": "fra", "title": "French dub"},
            ],
        }
        kept = _select_audio_streams(item, self._config())
        assert kept == [1], "should strip English + French, keep Spanish"

    def test_english_original_keeps_english_strips_dub(self) -> None:
        """Bluey-style: TMDb original=en, English original kept, Swedish dub stripped."""
        from pipeline.ffmpeg import _select_audio_streams

        item = {
            "filename": "Bluey S01E01.mkv",
            "tmdb": {"original_language": "en"},
            "audio_streams": [
                {"index": 0, "codec_raw": "eac3", "language": "eng"},
                {"index": 1, "codec_raw": "eac3", "language": "swe"},
            ],
        }
        kept = _select_audio_streams(item, self._config())
        assert kept == [0], "should strip Swedish dub, keep English original"

    def test_no_tmdb_falls_back_to_legacy_rule(self) -> None:
        """When TMDb has no original_language, use the historical English+und+stream0 rule."""
        from pipeline.ffmpeg import _select_audio_streams

        item = {
            "filename": "Untagged.mkv",
            "tmdb": {},  # no TMDb data
            "audio_streams": [
                {"index": 0, "codec_raw": "eac3", "language": "fra"},
                {"index": 1, "codec_raw": "eac3", "language": "eng"},
                {"index": 2, "codec_raw": "eac3", "language": "spa"},
            ],
        }
        kept = _select_audio_streams(item, self._config())
        # Legacy rule: stream 0 + English/und. French stream 0 stays, English stays, Spanish goes.
        assert kept == [0, 1]

    def test_und_track_defers_entire_strip_decision(self) -> None:
        """Inviolate rule (2026-04-29): one unresolved track defers all strip.

        Previously the policy was "keep und tracks but still strip known-foreign
        tracks alongside them" — a half-measure. The new policy: if ANY track
        is unresolved, no strip happens on the file at all. The user wants
        whisper to identify everything first, then strip with full knowledge.
        """
        from pipeline.ffmpeg import _select_audio_streams

        item = {
            "filename": "Movie.mkv",
            "tmdb": {"original_language": "es"},
            "audio_streams": [
                {"index": 0, "codec_raw": "eac3", "language": "spa"},
                {"index": 1, "codec_raw": "eac3", "language": "und"},  # unresolved
                {"index": 2, "codec_raw": "eac3", "language": "deu"},  # German dub
            ],
        }
        kept = _select_audio_streams(item, self._config())
        # None signals "keep all" — even the German dub survives until whisper
        # resolves the und track. Then the next pass can strip the German with
        # certainty.
        assert kept is None

    def test_keep_english_too_flag(self) -> None:
        """When keep_english_with_original=True, English is kept alongside the original."""
        from pipeline.ffmpeg import _select_audio_streams

        cfg = self._config()
        cfg["audio_keep_english_with_original"] = True

        item = {
            "filename": "Foreign Film.mkv",
            "tmdb": {"original_language": "ja"},
            "audio_streams": [
                {"index": 0, "codec_raw": "eac3", "language": "jpn"},
                {"index": 1, "codec_raw": "eac3", "language": "eng"},
                {"index": 2, "codec_raw": "eac3", "language": "fra"},
            ],
        }
        kept = _select_audio_streams(item, cfg)
        assert kept == [0, 1], "should keep Japanese (original) + English (extra), strip French"

    def test_iso_code_equivalence(self) -> None:
        """ISO 639-1 (es) matches ISO 639-2 (spa) and the English name."""
        from pipeline.ffmpeg import _select_audio_streams

        item = {
            "filename": "Movie.mkv",
            "tmdb": {"original_language": "es"},  # ISO 639-1
            "audio_streams": [
                {"index": 0, "codec_raw": "eac3", "language": "spa"},  # ISO 639-2
                {"index": 1, "codec_raw": "eac3", "language": "spanish"},  # English name
                {"index": 2, "codec_raw": "eac3", "language": "eng"},  # genuinely foreign
            ],
        }
        kept = _select_audio_streams(item, self._config())
        assert kept == [0, 1], "es ≡ spa ≡ spanish (all match); eng is foreign"

    def test_whisper_overrides_metadata(self) -> None:
        """detected_language (whisper) takes precedence over the language tag."""
        from pipeline.ffmpeg import _select_audio_streams

        item = {
            "filename": "Movie.mkv",
            "tmdb": {"original_language": "es"},
            "audio_streams": [
                # Tagged eng but whisper detected Spanish — whisper wins.
                {"index": 0, "codec_raw": "eac3", "language": "eng", "detected_language": "spa"},
                # Genuinely English.
                {"index": 1, "codec_raw": "eac3", "language": "eng"},
            ],
        }
        kept = _select_audio_streams(item, self._config())
        assert kept == [0], "whisper-detected Spanish wins over tagged eng"

    def test_disabled_master_switch_keeps_all(self) -> None:
        """strip_non_english_audio=False overrides the policy — keep everything."""
        from pipeline.ffmpeg import _select_audio_streams

        cfg = self._config()
        cfg["strip_non_english_audio"] = False
        item = {
            "filename": "Anything.mkv",
            "tmdb": {"original_language": "es"},
            "audio_streams": [
                {"index": 0, "codec_raw": "eac3", "language": "fra"},
                {"index": 1, "codec_raw": "eac3", "language": "deu"},
            ],
        }
        assert _select_audio_streams(item, cfg) is None  # None = keep all


class TestTMDbMetadataPreEncode:
    """ffmpeg -metadata flags from TMDb data (task C — TMDb tags pre-encode).

    Container-level title/date/language/comment must land in the encoded MKV
    immediately so it doesn't depend on the post-encode mkvpropedit step
    succeeding. Rich tags (director, cast) still flow through mkvpropedit.
    """

    def test_movie_writes_title_date_language(self) -> None:
        from pipeline.ffmpeg import _build_tmdb_metadata_args

        item = {
            "filename": "Inception (2010).mkv",
            "tmdb": {
                "title": "Inception",
                "release_year": 2010,
                "original_language": "en",
            },
        }
        args = _build_tmdb_metadata_args(item)
        assert "-metadata" in args
        assert "title=Inception" in args
        assert "date=2010" in args
        assert "language=en" in args

    def test_series_uses_first_air_year(self) -> None:
        from pipeline.ffmpeg import _build_tmdb_metadata_args

        item = {
            "filename": "The Wire S01E01.mkv",
            "tmdb": {
                "name": "The Wire",
                "first_air_year": 2002,
                "original_language": "en",
            },
        }
        args = _build_tmdb_metadata_args(item)
        assert "title=The Wire" in args
        assert "date=2002" in args

    def test_no_tmdb_returns_empty_list(self) -> None:
        from pipeline.ffmpeg import _build_tmdb_metadata_args

        assert _build_tmdb_metadata_args({"filename": "Untagged.mkv"}) == []
        assert _build_tmdb_metadata_args({"filename": "Untagged.mkv", "tmdb": {}}) == []

    def test_filename_fallback_when_no_tmdb_title(self) -> None:
        """If TMDb has data but no title, fall back to the filename stem."""
        from pipeline.ffmpeg import _build_tmdb_metadata_args

        item = {
            "filename": "Some Movie (2020).mkv",
            "tmdb": {"original_language": "en", "release_year": 2020},
        }
        args = _build_tmdb_metadata_args(item)
        assert "title=Some Movie (2020)" in args

    def test_metadata_appears_in_full_ffmpeg_cmd(self) -> None:
        """The full build_ffmpeg_cmd must include -metadata flags for tagged items."""
        item = _base_item()
        item["tmdb"] = {
            "title": "Test Movie",
            "release_year": 2024,
            "original_language": "es",
        }
        cmd = build_ffmpeg_cmd("/in/file.mkv", "/out/file.mkv", item, _base_config())
        # -map_metadata -1 must come BEFORE our -metadata flags so source bloat
        # is stripped before our values land.
        joined = " ".join(cmd)
        assert "-map_metadata -1" in joined
        assert "title=Test Movie" in cmd
        assert "date=2024" in cmd
        assert "language=es" in cmd
        # The comment marker survives so future runs can spot pipeline-encoded files.
        assert any("comment=encoded by NASCleanup" in arg for arg in cmd)
