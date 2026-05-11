"""Pin the 2026-05-11 forecast refactor — replaces naive ``remaining_files /
avg_per_day`` with tier-weighted prediction summing per-file expected
encode time.

Why: a 50 GB 4K HDR film takes ~90 min to encode; a 470 MB TV episode
takes ~3 min. Files-per-day is a meaningless yardstick across that 30×
range. The old forecast lurched between "275 days" and "288 days" between
adjacent days purely because the recent 7-day window shifted from
many-small-files (TV) to few-large-files (movies). New math weights by
``encode_time_secs`` per ``res_key``, so the estimate tracks expected
GPU work hours rather than file count.
"""

from __future__ import annotations

from datetime import datetime, timezone

import server.routers.admin as admin


def _entry(ts: str, *, count: int, encode_secs: float, res_key: str = "4K_HDR",
           saved: int = 1_000_000_000) -> dict:
    """One encode_history entry."""
    return {
        "timestamp": ts,
        "input_bytes": 30_000_000_000,
        "output_bytes": 30_000_000_000 - saved,
        "saved_bytes": saved,
        "encode_time_secs": encode_secs,
        "res_key": res_key,
        "compression_ratio": 0.5,
    }


def test_forecast_weights_by_tier(monkeypatch, tmp_path):
    """A queue of all-4K-HDR files projects out longer than a queue of
    all-1080p files, even at the same files-per-day rate."""
    # History across 2 days (the forecast block requires len(days_list) >= 2).
    # Total: 1 4K_HDR encode at 60 min, 1 1080p encode at 3 min = 3780s of work
    # across 2 days = 1890s/day GPU throughput.
    history = [
        _entry("2026-05-09T10:00:00+00:00", count=1, encode_secs=3600, res_key="4K_HDR"),
        _entry("2026-05-10T11:00:00+00:00", count=1, encode_secs=180, res_key="1080p"),
    ]
    monkeypatch.setattr(admin, "_read_history", lambda *_a, **_k: history)

    # Queue A: 10 × 4K_HDR remaining
    state_a = {"files": {f"a{i}.mkv": {"status": "pending", "filepath": f"a{i}.mkv"} for i in range(10)}}
    report_a = {"files": [
        {"filepath": f"a{i}.mkv", "video": {"resolution_class": "4K", "hdr": True}}
        for i in range(10)
    ]}
    monkeypatch.setattr(admin, "_get_pipeline_state", lambda: state_a)
    monkeypatch.setattr("server.helpers.read_report_cached", lambda _p: report_a)

    result_a = admin.get_history_summary()
    fc_a = result_a["forecast"]
    assert fc_a is not None
    # 10 × 4K_HDR × 3600s = 36000s predicted.
    # GPU throughput = (3600 + 180) / 2 days = 1890s/day.
    # days = 36000 / 1890 ≈ 19.05
    assert 18 < fc_a["est_days_remaining"] < 21, fc_a["est_days_remaining"]
    assert fc_a["remaining_by_tier"] == {"4K_HDR": 10}

    # Queue B: same 10 files but all 1080p — should be MUCH faster.
    state_b = {"files": {f"b{i}.mkv": {"status": "pending", "filepath": f"b{i}.mkv"} for i in range(10)}}
    report_b = {"files": [
        {"filepath": f"b{i}.mkv", "video": {"resolution_class": "1080p", "hdr": False}}
        for i in range(10)
    ]}
    monkeypatch.setattr(admin, "_get_pipeline_state", lambda: state_b)
    monkeypatch.setattr("server.helpers.read_report_cached", lambda _p: report_b)

    result_b = admin.get_history_summary()
    fc_b = result_b["forecast"]
    # 10 × 1080p × 180s = 1800s predicted. GPU throughput same 1890s/day.
    # days = 1800 / 1890 ≈ 0.95
    assert 0.6 < fc_b["est_days_remaining"] < 1.3, fc_b["est_days_remaining"]
    assert fc_b["remaining_by_tier"] == {"1080p": 10}

    # The forecast difference is what matters — 4K HDR queue takes ~20× longer
    # than 1080p queue with the same file count. The old files/day forecast
    # would have given identical estimates for both.
    assert fc_a["est_days_remaining"] > 5 * fc_b["est_days_remaining"], (
        f"Tier weighting failed — 4K HDR ({fc_a['est_days_remaining']}d) "
        f"should be way longer than 1080p ({fc_b['est_days_remaining']}d)"
    )


def test_forecast_falls_back_to_overall_when_tier_unseen(monkeypatch):
    """A remaining file with res_key the encoder hasn't seen before in
    history falls back to overall mean rather than crashing or
    silently treating it as 0 seconds."""
    history = [
        _entry("2026-05-09T10:00:00+00:00", count=1, encode_secs=600, res_key="1080p"),
        _entry("2026-05-10T10:00:00+00:00", count=1, encode_secs=600, res_key="1080p"),
    ]
    monkeypatch.setattr(admin, "_read_history", lambda *_a, **_k: history)

    # Queue: 1 × 4K_HDR (not in history)
    state = {"files": {"new.mkv": {"status": "pending", "filepath": "new.mkv"}}}
    report = {"files": [
        {"filepath": "new.mkv", "video": {"resolution_class": "4K", "hdr": True}}
    ]}
    monkeypatch.setattr(admin, "_get_pipeline_state", lambda: state)
    monkeypatch.setattr("server.helpers.read_report_cached", lambda _p: report)

    result = admin.get_history_summary()
    fc = result["forecast"]
    # No 4K_HDR history → falls back to overall mean (600s for the one entry).
    # Throughput = 600s/day, predicted = 600s → 1 day.
    assert 0.5 < fc["est_days_remaining"] < 1.5, fc["est_days_remaining"]
    assert fc["remaining_by_tier"] == {"4K_HDR": 1}


def test_forecast_excludes_terminal_statuses(monkeypatch):
    """Done / error / flagged_* rows must not be counted as remaining work."""
    history = [
        _entry("2026-05-09T10:00:00+00:00", count=1, encode_secs=600, res_key="1080p"),
        _entry("2026-05-10T10:00:00+00:00", count=1, encode_secs=600, res_key="1080p"),
    ]
    monkeypatch.setattr(admin, "_read_history", lambda *_a, **_k: history)

    state = {"files": {
        "pending.mkv":    {"status": "pending",                  "filepath": "pending.mkv"},
        "done.mkv":       {"status": "done",                     "filepath": "done.mkv"},
        "error.mkv":      {"status": "error",                    "filepath": "error.mkv"},
        "flagged.mkv":    {"status": "flagged_foreign_audio",    "filepath": "flagged.mkv"},
        "corrupt.mkv":    {"status": "flagged_corrupt",          "filepath": "corrupt.mkv"},
        "in_flight.mkv":  {"status": "processing",               "filepath": "in_flight.mkv"},
    }}
    report = {"files": [
        {"filepath": fp, "video": {"resolution_class": "1080p", "hdr": False}}
        for fp in state["files"].keys()
    ]}
    monkeypatch.setattr(admin, "_get_pipeline_state", lambda: state)
    monkeypatch.setattr("server.helpers.read_report_cached", lambda _p: report)

    fc = admin.get_history_summary()["forecast"]
    # Only pending + in_flight count as remaining (4 terminal: done, error, 2 flagged).
    assert fc["remaining_files"] == 2, (
        f"expected 2 (pending + processing), got {fc['remaining_files']}"
    )
    assert fc["remaining_by_tier"] == {"1080p": 2}


def test_forecast_returns_diagnostics():
    """Forecast must expose the underlying numbers (predicted hours, GPU
    throughput, queue breakdown) so the UI can show its reasoning. Magic
    numbers without context are how we lost trust on the old forecast."""
    # Just check the keys exist by mocking minimal inputs in a small case.
    import unittest.mock as mock
    history = [
        _entry("2026-05-09T10:00:00+00:00", count=1, encode_secs=600, res_key="1080p"),
        _entry("2026-05-10T10:00:00+00:00", count=1, encode_secs=600, res_key="1080p"),
    ]
    state = {"files": {"x.mkv": {"status": "pending", "filepath": "x.mkv"}}}
    report = {"files": [{"filepath": "x.mkv", "video": {"resolution_class": "1080p", "hdr": False}}]}

    with mock.patch.object(admin, "_read_history", return_value=history), \
         mock.patch.object(admin, "_get_pipeline_state", return_value=state), \
         mock.patch("server.helpers.read_report_cached", return_value=report):
        fc = admin.get_history_summary()["forecast"]

    expected_keys = {
        "remaining_files", "avg_files_per_day", "avg_saved_per_day_gb",
        "est_completion_date", "est_days_remaining",
        "predicted_total_encode_hours", "gpu_active_hours_per_day",
        "remaining_by_tier", "per_tier_avg_minutes",
    }
    missing = expected_keys - set(fc.keys())
    assert not missing, f"forecast missing keys: {missing}"
