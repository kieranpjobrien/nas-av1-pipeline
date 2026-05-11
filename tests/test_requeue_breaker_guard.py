"""Pin the 2026-05-12 requeue guard.

The circuit breaker (added earlier today) transitions repeat-failure files
to ``FileStatus.FLAGGED_CORRUPT`` — a terminal state that the queue builder
skips. That's how the loop stops.

But the dashboard ``/api/file/requeue`` and ``/api/files/requeue-batch``
endpoints unconditionally reset rows to ``pending``. Ford v Ferrari hit the
integrity breaker (counter to 10) but the row was still in fetching status
when caught — proof that the breaker fired and was then UNDONE by a
requeue. The breaker is a fiction if the dashboard can silently override it.

Guards added:
  * single requeue raises 409 if the target row is ``flagged_corrupt``
    and the body does not include ``force_flagged=True``.
  * batch requeue skips (does not raise) flagged_corrupt rows for the same
    reason — the batch UI shouldn't fail the whole batch on one row.

Both endpoints accept ``force_flagged=True`` for the legitimate override
case: user has re-acquired the source (Sonarr/Radarr) and wants to retry.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest


@pytest.fixture
def state_db(tmp_path, monkeypatch):
    """Build a minimal pipeline_state DB with a few rows in different states."""
    db_path = tmp_path / "state.db"
    con = sqlite3.connect(str(db_path))
    con.execute(
        """CREATE TABLE pipeline_files (
            filepath TEXT PRIMARY KEY,
            status   TEXT,
            stage    TEXT,
            error    TEXT,
            reason   TEXT,
            extras   TEXT,
            added         TEXT,
            last_updated  TEXT,
            tier          TEXT,
            audio_only    INTEGER,
            cleanup_strip INTEGER,
            sub_strip     INTEGER,
            local_path    TEXT,
            output_path   TEXT,
            dest_path     TEXT,
            res_key       TEXT,
            mode          TEXT
        )"""
    )
    # NAS-rooted paths so the endpoint's NAS-prefix guard accepts them.
    nas_movies = tmp_path / "NAS" / "Media" / "Movies"
    nas_series = tmp_path / "NAS" / "Media" / "Series"
    nas_movies.mkdir(parents=True)
    nas_series.mkdir(parents=True)
    files = {
        "corrupt.mkv": ("flagged_corrupt", '{"integrity_failure_count": 10}'),
        "done.mkv":    ("done", "{}"),
        "pending.mkv": ("pending", "{}"),
        "error.mkv":   ("error", "{}"),
    }
    for name, (status, extras) in files.items():
        # Put the file in NAS/Movies/<dir>/<name>
        d = nas_movies / name.replace(".mkv", "")
        d.mkdir()
        f = d / name
        f.write_bytes(b"x" * 100)
        con.execute(
            "INSERT INTO pipeline_files (filepath, status, extras) VALUES (?, ?, ?)",
            (str(f), status, extras),
        )
    con.commit()
    con.close()

    # Patch paths module to point at our throwaway DB + NAS dirs.
    import paths
    monkeypatch.setattr(paths, "PIPELINE_STATE_DB", db_path)
    monkeypatch.setattr(paths, "NAS_MOVIES", nas_movies)
    monkeypatch.setattr(paths, "NAS_SERIES", nas_series)
    return {
        "db": db_path,
        "nas_movies": nas_movies,
        "nas_series": nas_series,
        "corrupt": str(nas_movies / "corrupt" / "corrupt.mkv"),
        "done":    str(nas_movies / "done"    / "done.mkv"),
        "pending": str(nas_movies / "pending" / "pending.mkv"),
        "error":   str(nas_movies / "error"   / "error.mkv"),
    }


def _status_of(db_path: Path, fp: str) -> str:
    con = sqlite3.connect(str(db_path))
    cur = con.execute("SELECT status FROM pipeline_files WHERE filepath = ?", (fp,))
    row = cur.fetchone()
    con.close()
    return row[0] if row else ""


def test_single_requeue_refuses_flagged_corrupt(state_db):
    """Single requeue with no force flag must reject a flagged_corrupt row."""
    from fastapi import HTTPException

    from server.routers.files import file_requeue

    with pytest.raises(HTTPException) as ei:
        file_requeue({"path": state_db["corrupt"]})
    assert ei.value.status_code == 409
    assert "flagged_corrupt" in ei.value.detail.lower() or "breaker" in ei.value.detail.lower()
    # Row must still be flagged_corrupt — the guard MUST NOT mutate state.
    assert _status_of(state_db["db"], state_db["corrupt"]) == "flagged_corrupt"


def test_single_requeue_with_force_flagged_allows_flagged_corrupt(state_db):
    """The legit override path: user has re-acquired the source. force_flagged=true wins."""
    from server.routers.files import file_requeue

    result = file_requeue({"path": state_db["corrupt"], "force_flagged": True})
    assert result["ok"] is True
    assert _status_of(state_db["db"], state_db["corrupt"]) == "pending"


def test_single_requeue_still_works_for_normal_rows(state_db):
    """Regression check: error / pending rows still requeue as before."""
    from server.routers.files import file_requeue

    result = file_requeue({"path": state_db["error"]})
    assert result["ok"] is True
    assert _status_of(state_db["db"], state_db["error"]) == "pending"


def test_batch_requeue_skips_flagged_corrupt(state_db):
    """Bulk requeue must skip flagged_corrupt rows (without failing the batch)."""
    from server.routers.files import files_requeue_batch

    result = files_requeue_batch({
        "paths": [state_db["corrupt"], state_db["error"], state_db["pending"]],
    })
    # error + pending requeue; corrupt is skipped
    assert result["queued"] == 2
    assert result["skipped"] == 1
    skipped_reasons = [s["reason"] for s in result["skipped_detail"]]
    assert any("flagged_corrupt" in r.lower() or "breaker" in r.lower() for r in skipped_reasons)
    # State preserved
    assert _status_of(state_db["db"], state_db["corrupt"]) == "flagged_corrupt"
    assert _status_of(state_db["db"], state_db["error"]) == "pending"


def test_batch_requeue_with_force_flagged_revives_corrupt(state_db):
    """Bulk force_flagged=true revives the cohort (legit user override path)."""
    from server.routers.files import files_requeue_batch

    result = files_requeue_batch({
        "paths": [state_db["corrupt"], state_db["error"]],
        "force_flagged": True,
    })
    assert result["queued"] == 2
    assert result["skipped"] == 0
    assert _status_of(state_db["db"], state_db["corrupt"]) == "pending"
    assert _status_of(state_db["db"], state_db["error"]) == "pending"
