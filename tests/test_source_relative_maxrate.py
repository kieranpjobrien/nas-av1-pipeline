"""Regression tests for the source-relative output cap (2026-06-14).

Replaces the old flat ``nvenc_maxrate`` (40M for every 4K_HDR movie, regardless
of source) with::

    cap = min(value-tier ceiling, source_bitrate × cap_factor, max_output_gb/runtime)

Motivation: ``tools/diagnose_av1_bloat.py`` found 2,745 of 5,686 AV1 files (48%)
came out at or above their source bitrate — 4.45 TB of zero-benefit bloat —
because CQ 22 drove straight up to the fixed 40M ceiling on sources that never
had that much detail (Schindler's: 10.6 Mbps source → 34.5 Mbps AV1).
"""

import pytest

from pipeline.config import DEFAULT_CONFIG, _resolve_target_maxrate, _value_band, resolve_encode_params


def _maxrate_mbps(item: dict, config: dict | None = None) -> float | None:
    params = resolve_encode_params(config or DEFAULT_CONFIG, item)
    return None if params["maxrate"] is None else float(params["maxrate"][:-1])


def _item(**kw) -> dict:
    base = {"library_type": "movie", "resolution": "4K", "hdr": True, "duration_seconds": 7200, "tmdb": {}}
    base.update(kw)
    return base


class TestValueBand:
    def test_keeper_overrides_low_vote(self):
        assert _value_band({"is_keeper": True, "tmdb": {"vote_average": 1.0}}) == "treasured"

    def test_high_vote_treasured(self):
        assert _value_band({"tmdb": {"vote_average": 8.0}}) == "treasured"

    def test_mid_vote_normal(self):
        assert _value_band({"tmdb": {"vote_average": 7.0}}) == "normal"

    def test_low_vote_casual(self):
        assert _value_band({"tmdb": {"vote_average": 4.0}}) == "casual"

    def test_missing_tmdb_casual(self):
        assert _value_band({}) == "casual"


class TestSourceRelativeCap:
    def test_starved_source_capped_at_source_not_tier(self):
        # Schindler's: treasured 4K but only a 10.6 Mbps source → cap ~10.6, NOT tier 30.
        assert _maxrate_mbps(_item(bitrate_kbps=10583, tmdb={"vote_average": 8.6})) == pytest.approx(10.6, abs=0.1)

    def test_decent_source_capped_at_source(self):
        # The bug this fixes: was a flat 40M, now capped at the 17.9 Mbps source.
        mr = _maxrate_mbps(_item(bitrate_kbps=17900, tmdb={"vote_average": 7.0}))
        assert mr == pytest.approx(17.9, abs=0.1)
        assert mr < 40

    def test_rich_remux_capped_at_treasured_tier(self):
        # 70 Mbps remux, treasured → capped at tier 24*1.25 = 30 (well under source).
        assert _maxrate_mbps(_item(bitrate_kbps=70000, tmdb={"vote_average": 8.3})) == pytest.approx(30.0, abs=0.1)

    def test_normal_band_ceiling(self):
        assert _maxrate_mbps(_item(bitrate_kbps=70000, tmdb={"vote_average": 7.0})) == pytest.approx(24.0, abs=0.1)

    def test_casual_band_ceiling(self):
        assert _maxrate_mbps(_item(bitrate_kbps=70000, tmdb={"vote_average": 4.0})) == pytest.approx(20.4, abs=0.1)

    def test_missing_bitrate_falls_back_to_tier(self):
        # No source bitrate known → tier ceiling still applies; must not crash.
        assert _maxrate_mbps(_item(bitrate_kbps=0, tmdb={"vote_average": 8.2})) == pytest.approx(30.0, abs=0.1)

    def test_gb_backstop_binds_on_long_film(self):
        # Treasured 4h remux: tier 30 and source 60 both lose to 45GB/14400s = 25 Mbps.
        mr = _maxrate_mbps(_item(bitrate_kbps=60000, duration_seconds=14400, tmdb={"vote_average": 8.6}))
        assert mr == pytest.approx(25.0, abs=0.1)

    def test_never_exceeds_source_at_factor_one(self):
        # cap_factor 1.0 guarantees output ceiling <= source → no possible bloat.
        for src in (8000, 12000, 18000, 25000):
            assert _maxrate_mbps(_item(bitrate_kbps=src, tmdb={"vote_average": 7.0})) <= src / 1000 + 0.01

    def test_bufsize_is_double_maxrate(self):
        params = resolve_encode_params(DEFAULT_CONFIG, _item(bitrate_kbps=18000, tmdb={"vote_average": 7.0}))
        assert float(params["bufsize"][:-1]) == pytest.approx(2 * float(params["maxrate"][:-1]))

    def test_1080p_series_now_capped(self):
        # Vikings-class: 1080p series used to be uncapped (None); now source-relative.
        mr = _maxrate_mbps(
            {
                "library_type": "series",
                "resolution": "1080p",
                "hdr": False,
                "bitrate_kbps": 32800,
                "duration_seconds": 2700,
                "tmdb": {"vote_average": 8.05},
            }
        )
        assert mr is not None
        assert mr < 32.8

    def test_720p_movie_no_cap(self):
        # 720p has no target_mbps entry → defers to the static table (None), unchanged.
        mr = _maxrate_mbps(
            {
                "library_type": "movie",
                "resolution": "720p",
                "hdr": False,
                "bitrate_kbps": 4000,
                "duration_seconds": 6000,
                "tmdb": {},
            }
        )
        assert mr is None


class TestCapFactor:
    def test_lower_cap_factor_shrinks_below_source(self):
        cfg = {**DEFAULT_CONFIG, "source_relative_cap_factor": 0.8}
        mr = _maxrate_mbps(_item(bitrate_kbps=18000, tmdb={"vote_average": 7.0}), config=cfg)
        assert mr == pytest.approx(14.4, abs=0.1)  # 18 * 0.8

    def test_helper_returns_none_for_unknown_res_key(self):
        assert _resolve_target_maxrate(DEFAULT_CONFIG, _item(bitrate_kbps=5000), "movie", "SD") is None
