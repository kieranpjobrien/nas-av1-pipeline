"""Pin the de-bloat in_progress selection (2026-06-26).

The dashboard surfaced a stale "Pirates" entry left non-terminal by a kill mid-swap,
because the endpoint grabbed the FIRST active-phase ledger row. ``_pick_in_progress``
now tracks the file actually in flight (matches the live inflight file, else the
most-recently-added active row), so an interrupted-run orphan no longer masks the
real current file.
"""

from server.routers.reclaim import _pick_in_progress


def _row(name, phase, status=None):
    return {"name": name, "phase": phase, "status": status}


def test_prefers_inflight_match_over_orphan():
    led = {
        "/nas/Pirates.mkv": _row("Pirates.mkv", "moving_original"),  # orphan from a killed run
        "/nas/Angel.mkv": _row("Angel.mkv", "encoding"),  # actually in flight
    }
    _, v = _pick_in_progress(led, {"fp": "/nas/Angel.mkv", "progress_pct": 40.4})
    assert v["name"] == "Angel.mkv"


def test_falls_back_to_most_recent_active_without_inflight():
    led = {
        "/nas/Pirates.mkv": _row("Pirates.mkv", "moving_original"),  # orphan, added earlier
        "/nas/Angel.mkv": _row("Angel.mkv", "encoding"),  # current, added later
    }
    _, v = _pick_in_progress(led, {})  # no live inflight
    assert v["name"] == "Angel.mkv"


def test_none_when_no_active_entries():
    led = {"/nas/Done.mkv": _row("Done.mkv", "done", status="reclaimed")}
    assert _pick_in_progress(led, {}) is None


def test_inflight_fp_not_in_ledger_falls_back_to_active():
    led = {"/nas/Angel.mkv": _row("Angel.mkv", "encoding")}
    _, v = _pick_in_progress(led, {"fp": "/nas/Gone.mkv"})  # inflight points at a non-ledger file
    assert v["name"] == "Angel.mkv"
