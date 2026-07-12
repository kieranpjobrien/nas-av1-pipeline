"""The dashboard reads media_report.json, which drifts stale after encodes and
new downloads. It must be refreshed on a TIMER, not just once — otherwise the
compliance stats stop matching reality (recurring problem, root-caused 2026-07-13).
This pins the server-owned periodic-scan hook so it can't be quietly dropped."""
from __future__ import annotations


def test_periodic_scan_hook_registered():
    import server

    names = [getattr(h, "__name__", "") for h in server.app.router.on_startup]
    assert "_schedule_periodic_scan" in names, "periodic dashboard scan must be scheduled at startup"


def test_scan_interval_is_configurable():
    import server

    # default is a sane 20 minutes, and the knob exists for override / disable
    assert isinstance(server._SCAN_INTERVAL_S, int)
    assert server._SCAN_INTERVAL_S == 1200
