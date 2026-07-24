"""Rule 14 enforcement: an ACTIVE row that stops progressing must FAIL.

Until 2026-07-25 the rule the discipline contract spends the most words on had
no programmatic check at all. The 2026-05-19 incident: supervisor died at 10:34,
counters kept saying "165 done", in-flight rows sat at age=460min, and the
verdict given was "no concerns".
"""

import sqlite3
from datetime import datetime, timedelta

import pytest

from tools.invariants import check_no_stale_active_rows


def _db(tmp_path, rows):
    """Build a minimal pipeline_files table with (status, minutes_ago) rows."""
    p = tmp_path / "pipeline_state.db"
    con = sqlite3.connect(str(p))
    con.execute(
        "CREATE TABLE pipeline_files (filepath TEXT PRIMARY KEY, status TEXT, "
        "stage TEXT, last_updated TEXT, extras TEXT)"
    )
    for i, (status, mins) in enumerate(rows):
        con.execute(
            "INSERT INTO pipeline_files VALUES (?,?,?,?,?)",
            (
                f"X{i}.mkv",
                status,
                "encoding",
                (datetime.now() - timedelta(minutes=mins)).isoformat(),
                "{}",
            ),
        )
    con.commit()
    con.close()
    return p


@pytest.fixture(autouse=True)
def _not_paused(monkeypatch):
    """The check stands down when paused; force 'not paused' for these tests."""
    import pipeline.control as ctrl

    monkeypatch.setattr(ctrl.PipelineControl, "_get_pause_type", lambda self: None)


def test_stalled_active_row_fails(tmp_path):
    db = _db(tmp_path, [("processing", 460)])  # the 2026-05-19 age
    r = check_no_stale_active_rows(db_path=db)
    assert not r.passed, "a row ACTIVE for 7.5h with no progress must fail"
    assert r.severity == "HIGH", "must be able to fail the pre-flight gate"
    assert "X0.mkv" in r.violations


def test_one_stale_row_is_enough(tmp_path):
    """Rule 14: even ONE row over the threshold means something is wrong —
    a pile of recent activity does not excuse it."""
    db = _db(tmp_path, [("processing", 1), ("uploading", 2), ("fetching", 45)])
    r = check_no_stale_active_rows(db_path=db)
    assert not r.passed
    assert r.violations == ["X2.mkv"]


def test_live_pipeline_passes(tmp_path):
    """Actively-encoding rows tick last_updated continuously, so they're fresh."""
    db = _db(tmp_path, [("processing", 0), ("fetching", 3), ("uploading", 8)])
    assert check_no_stale_active_rows(db_path=db).passed


def test_terminal_rows_are_not_active(tmp_path):
    """A DONE row from last week is not a stall."""
    db = _db(tmp_path, [("done", 10000), ("flagged_corrupt", 10000)])
    assert check_no_stale_active_rows(db_path=db).passed


def test_paused_stands_down(tmp_path, monkeypatch):
    import pipeline.control as ctrl

    monkeypatch.setattr(ctrl.PipelineControl, "_get_pause_type", lambda self: "all")
    db = _db(tmp_path, [("processing", 460)])
    r = check_no_stale_active_rows(db_path=db)
    assert r.passed and "paused" in r.message
