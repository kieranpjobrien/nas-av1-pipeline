"""Tests for pipeline/config.py — configuration building, resolution keys, and encode params."""

import pytest

from pipeline.config import DEFAULT_CONFIG, build_config, get_res_key, resolve_encode_params


class TestBuildConfig:
    """build_config merges DEFAULT_CONFIG with optional overrides."""

    def test_no_overrides_returns_defaults(self):
        """build_config() with no overrides returns a copy of DEFAULT_CONFIG."""
        config = build_config()
        assert config["video_codec"] == "av1_nvenc"
        assert config["max_staging_bytes"] == 2_500_000_000_000
        assert "cq" in config
        assert "nvenc_preset" in config

    def test_returns_all_required_keys(self):
        """The config dict contains every key needed by the pipeline."""
        config = build_config()
        required = [
            "max_staging_bytes",
            "max_fetch_buffer_bytes",
            "min_free_space_bytes",
            "video_codec",
            "cq",
            "nvenc_preset",
            "nvenc_multipass",
            "nvenc_lookahead",
            "nvenc_maxrate",
            "nvenc_bufsize",
            "audio_mode",
            "audio_eac3_surround_bitrate",
            "audio_eac3_stereo_bitrate",
            "lossless_audio_codecs",
            "strip_non_english_subs",
            "strip_non_english_audio",
            "overwrite_existing",
            "replace_original",
        ]
        for key in required:
            assert key in config, f"Missing required key: {key}"

    def test_simple_override(self):
        """A top-level override replaces the default value."""
        config = build_config({"max_staging_bytes": 999})
        assert config["max_staging_bytes"] == 999

    def test_deep_merge_cq(self):
        """Nested overrides deep-merge into the existing config structure."""
        config = build_config({"cq": {"movie": {"1080p": 26}}})
        assert config["cq"]["movie"]["1080p"] == 26
        # Other CQ values remain unchanged
        assert config["cq"]["movie"]["4K_HDR"] == DEFAULT_CONFIG["cq"]["movie"]["4K_HDR"]
        assert config["cq"]["series"]["1080p"] == DEFAULT_CONFIG["cq"]["series"]["1080p"]

    def test_override_does_not_mutate_defaults(self):
        """build_config never mutates DEFAULT_CONFIG itself."""
        original_cq = DEFAULT_CONFIG["cq"]["movie"]["1080p"]
        build_config({"cq": {"movie": {"1080p": 99}}})
        assert DEFAULT_CONFIG["cq"]["movie"]["1080p"] == original_cq


class TestGetResKey:
    """get_res_key maps item dicts to resolution keys."""

    @pytest.mark.parametrize(
        "item,expected",
        [
            ({"resolution": "4K", "hdr": True}, "4K_HDR"),
            ({"resolution": "4K", "hdr": False}, "4K_SDR"),
            ({"resolution": "4K"}, "4K_SDR"),
            ({"resolution": "1080p"}, "1080p"),
            ({"resolution": "720p"}, "720p"),
            ({"resolution": "480p"}, "480p"),
            ({"resolution": "SD"}, "SD"),
            ({}, "1080p"),  # default resolution is 1080p when missing
            ({"resolution": "unknown"}, "SD"),
        ],
    )
    def test_resolution_mapping(self, item, expected):
        """Each resolution/hdr combination maps to the correct config key."""
        assert get_res_key(item) == expected


class TestResolveEncodeParams:
    """resolve_encode_params looks up encode params by content type + resolution."""

    def test_movie_1080p(self):
        """1080p movie returns the CQ/preset from the config table."""
        config = build_config()
        item = {"library_type": "movie", "resolution": "1080p", "hdr": False}
        params = resolve_encode_params(config, item)
        assert params["cq"] == config["cq"]["movie"]["1080p"]
        assert params["preset"] == config["nvenc_preset"]["movie"]["1080p"]
        assert params["content_type"] == "movie"
        assert params["res_key"] == "1080p"

    def test_cq_4k_hdr_movie(self):
        """4K HDR movie gets the correct CQ from the config table."""
        config = build_config()
        item = {"library_type": "movie", "resolution": "4K", "hdr": True}
        params = resolve_encode_params(config, item)
        assert params["cq"] == config["cq"]["movie"]["4K_HDR"]
        assert params["res_key"] == "4K_HDR"

    def test_cq_720p_series(self):
        """720p series gets the correct CQ from the config table."""
        config = build_config()
        item = {"library_type": "series", "resolution": "720p"}
        params = resolve_encode_params(config, item)
        assert params["cq"] == config["cq"]["series"]["720p"]

    def test_series_library_types_normalized(self):
        """Library types 'show', 'tv', 'anime' all map to content_type 'series'."""
        config = build_config()
        for lib_type in ("series", "show", "tv", "anime"):
            item = {"library_type": lib_type, "resolution": "1080p"}
            params = resolve_encode_params(config, item)
            assert params["content_type"] == "series", f"{lib_type} should be 'series'"

    def test_maxrate_and_bufsize_present(self):
        """4K HDR movie includes maxrate and bufsize values."""
        config = build_config()
        item = {"library_type": "movie", "resolution": "4K", "hdr": True}
        params = resolve_encode_params(config, item)
        assert params["maxrate"] == "40M"
        assert params["bufsize"] == "80M"

    def test_maxrate_none_for_low_res(self):
        """720p movies have no maxrate cap (None)."""
        config = build_config()
        item = {"library_type": "movie", "resolution": "720p"}
        params = resolve_encode_params(config, item)
        assert params["maxrate"] is None
