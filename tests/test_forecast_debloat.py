"""Forecast fixes (2026-06-30): source-fallback tiering, remux exclusion, and a
sequential de-bloat phase folded into the ETA.

The dashboard forecast had badly under-counted the 4K tail: recent history rows
stopped writing res_key (bucketed as 'unknown') and old per-tier averages were
dragged to a fraction of a real film's time by hundreds of sub-floor remux/audio
ops. It also ignored the parked de-bloat queue, which runs after convert
(GPU-exclusive) and stacks onto the timeline.
"""

import server.routers.admin as admin


def _entry(ts, *, encode_secs, res_key=None, source_res=None, source_hdr=False, saved=1_000_000_000):
    e = {
        "timestamp": ts,
        "input_bytes": 30_000_000_000,
        "output_bytes": 30_000_000_000 - saved,
        "saved_bytes": saved,
        "encode_time_secs": encode_secs,
        "compression_ratio": 0.5,
    }
    if res_key is not None:
        e["res_key"] = res_key
    if source_res is not None:
        e["source"] = {"video": {"resolution_class": source_res, "hdr": source_hdr}}
    return e


def test_res_key_from_video():
    assert admin._res_key_from_video({"resolution_class": "4K", "hdr": True}) == "4K_HDR"
    assert admin._res_key_from_video({"resolution_class": "4K", "hdr": False}) == "4K_SDR"
    assert admin._res_key_from_video({"resolution_class": "1080p"}) == "1080p"
    assert admin._res_key_from_video({}) == "unknown"


def test_entry_res_key_prefers_explicit_then_falls_back_to_source():
    assert admin._entry_res_key({"res_key": "1080p"}) == "1080p"
    # res_key absent/None -> recover from source.video (the recent-entries case)
    assert admin._entry_res_key({"source": {"video": {"resolution_class": "4K", "hdr": True}}}) == "4K_HDR"
    assert (
        admin._entry_res_key({"res_key": None, "source": {"video": {"resolution_class": "4K", "hdr": False}}})
        == "4K_SDR"
    )


def test_remux_ops_excluded_from_tier_average(monkeypatch):
    # 2 genuine 4K_HDR encodes (3600s) + 50 sub-floor remux ops (5s) in the same
    # tier. The remux ops must not drag the per-tier average toward zero.
    history = [
        _entry("2026-05-09T10:00:00+00:00", encode_secs=3600, res_key="4K_HDR"),
        _entry("2026-05-10T10:00:00+00:00", encode_secs=3600, res_key="4K_HDR"),
    ]
    history += [_entry(f"2026-05-10T11:{i:02d}:00+00:00", encode_secs=5, res_key="4K_HDR") for i in range(50)]
    monkeypatch.setattr(admin, "_read_history", lambda *a, **k: history)
    monkeypatch.setattr(
        admin, "_get_pipeline_state", lambda: {"files": {"a.mkv": {"status": "pending", "filepath": "a.mkv"}}}
    )
    monkeypatch.setattr(
        "server.helpers.read_report_cached",
        lambda _p: {"files": [{"filepath": "a.mkv", "video": {"resolution_class": "4K", "hdr": True}}]},
    )
    monkeypatch.setattr("tools.reclaim_debloat.candidates", lambda: [])
    monkeypatch.setattr("server.helpers.read_json_safe", lambda _p: {})

    fc = admin.get_history_summary()["forecast"]
    # Median of genuine 4K_HDR encodes is 60 min; remux 5s ops are below the 4K floor.
    assert fc["per_tier_avg_minutes"]["4K_HDR"] >= 55, fc["per_tier_avg_minutes"]


def test_forecast_folds_in_sequential_debloat_phase(monkeypatch):
    history = [
        _entry("2026-05-09T10:00:00+00:00", encode_secs=3600, res_key="4K_HDR"),
        _entry("2026-05-10T11:00:00+00:00", encode_secs=3600, res_key="4K_HDR"),
    ]
    monkeypatch.setattr(admin, "_read_history", lambda *a, **k: history)
    monkeypatch.setattr(
        admin, "_get_pipeline_state", lambda: {"files": {"a.mkv": {"status": "pending", "filepath": "a.mkv"}}}
    )
    monkeypatch.setattr(
        "server.helpers.read_report_cached",
        lambda _p: {"files": [{"filepath": "a.mkv", "video": {"resolution_class": "4K", "hdr": True}}]},
    )
    # 4 de-bloat candidates, empty ledger -> none terminal -> all 4 remaining
    monkeypatch.setattr(
        "tools.reclaim_debloat.candidates",
        lambda: [{"fp": f"d{i}.mkv", "f": {"video": {"resolution_class": "4K", "hdr": True}}} for i in range(4)],
    )
    monkeypatch.setattr("server.helpers.read_json_safe", lambda _p: {})

    fc = admin.get_history_summary()["forecast"]
    assert fc["debloat_remaining"] == 4
    assert fc["debloat_by_tier"] == {"4K_HDR": 4}
    assert fc["debloat_days"] > 0
    # De-bloat is sequential, so the all-done date stacks past convert's.
    assert fc["all_done_days"] > fc["est_days_remaining"]
    assert fc["all_done_date"] >= fc["est_completion_date"]


def test_debloat_failure_does_not_break_convert_forecast(monkeypatch):
    history = [
        _entry("2026-05-09T10:00:00+00:00", encode_secs=3600, res_key="4K_HDR"),
        _entry("2026-05-10T11:00:00+00:00", encode_secs=3600, res_key="4K_HDR"),
    ]
    monkeypatch.setattr(admin, "_read_history", lambda *a, **k: history)
    monkeypatch.setattr(
        admin, "_get_pipeline_state", lambda: {"files": {"a.mkv": {"status": "pending", "filepath": "a.mkv"}}}
    )
    monkeypatch.setattr(
        "server.helpers.read_report_cached",
        lambda _p: {"files": [{"filepath": "a.mkv", "video": {"resolution_class": "4K", "hdr": True}}]},
    )

    def _boom():
        raise RuntimeError("ledger unreadable")

    monkeypatch.setattr("tools.reclaim_debloat.candidates", _boom)

    fc = admin.get_history_summary()["forecast"]
    # Convert forecast still produced; de-bloat just contributes nothing.
    assert fc["est_days_remaining"] > 0
    assert fc["debloat_remaining"] == 0
    assert fc["all_done_date"] == fc["est_completion_date"]
