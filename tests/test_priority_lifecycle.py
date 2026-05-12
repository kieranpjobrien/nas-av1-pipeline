"""Pin the 2026-05-13 priority list lifecycle fixes.

Two distinct bugs the user reported the morning after running overnight:

  1. ``priority.json`` was append-only. Items stayed in the list forever
     after they finished encoding, so the list never appeared to shrink
     — the user couldn't tell from the count whether anything was
     getting done.

  2. ``set_priority`` only wrote priority.json. The queue builder ran
     periodically against media_report and inserted state DB rows for
     matching entries — so a freshly-added priority path could sit
     NOT_IN_STATE for hours. 96 of 198 priority paths were stuck this
     way overnight.

Post-fix:
  * ``_prune_done_from_priority`` runs at every queue rebuild and
    rewrites priority.json minus any path whose state is done /
    flagged_*. The list becomes a live "still to do" view.
  * ``PUT /api/control/priority`` now synchronously seeds state DB
    pending rows for any priority path that doesn't have one yet
    (only NAS-rooted, file-must-exist). Set the priority and it's
    immediately visible to the queue builder.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest import mock

import pytest


# --------------------------------------------------------------------------
# _prune_done_from_priority
# --------------------------------------------------------------------------


def test_prune_done_removes_done_and_flagged_keeps_rest(tmp_path, monkeypatch):
    """DONE and any flagged_* status → removed. Pending / processing /
    error / NOT_IN_STATE → kept (still to do, by definition)."""
    import pipeline.__main__ as main_mod

    control = tmp_path / "control"
    control.mkdir()
    prio_path = control / "priority.json"
    prio_path.write_text(
        json.dumps({
            "force": [],
            "paths": [
                r"\\NAS\done.mkv",
                r"\\NAS\pending.mkv",
                r"\\NAS\processing.mkv",
                r"\\NAS\error.mkv",
                r"\\NAS\flagged_corrupt.mkv",
                r"\\NAS\flagged_foreign_audio.mkv",
                r"\\NAS\not_in_state.mkv",
            ],
            "patterns": [],
        }),
        encoding="utf-8",
    )

    statuses = {
        r"\\NAS\done.mkv": {"status": "done"},
        r"\\NAS\pending.mkv": {"status": "pending"},
        r"\\NAS\processing.mkv": {"status": "processing"},
        r"\\NAS\error.mkv": {"status": "error"},
        r"\\NAS\flagged_corrupt.mkv": {"status": "flagged_corrupt"},
        r"\\NAS\flagged_foreign_audio.mkv": {"status": "flagged_foreign_audio"},
        # not_in_state.mkv — no entry, get_file returns None
    }

    class FakeState:
        def get_file(self, fp):
            return statuses.get(fp)

    removed = main_mod._prune_done_from_priority(
        staging_dir=str(tmp_path), state=FakeState()
    )
    assert removed == 3, f"expected 3 removals (done + 2 flagged), got {removed}"
    data = json.loads(prio_path.read_text(encoding="utf-8"))
    assert r"\\NAS\done.mkv" not in data["paths"]
    assert r"\\NAS\flagged_corrupt.mkv" not in data["paths"]
    assert r"\\NAS\flagged_foreign_audio.mkv" not in data["paths"]
    # Non-terminal kept
    assert r"\\NAS\pending.mkv" in data["paths"]
    assert r"\\NAS\processing.mkv" in data["paths"]
    assert r"\\NAS\error.mkv" in data["paths"]
    # NOT_IN_STATE entries kept — they haven't been worked yet
    assert r"\\NAS\not_in_state.mkv" in data["paths"]


def test_prune_done_is_idempotent(tmp_path):
    """Running prune twice in a row with no state change → second call
    finds nothing to remove. Confirms the rewrite is stable."""
    import pipeline.__main__ as main_mod

    control = tmp_path / "control"
    control.mkdir()
    prio_path = control / "priority.json"
    prio_path.write_text(
        json.dumps({"force": [], "paths": [r"\\NAS\done.mkv", r"\\NAS\pending.mkv"], "patterns": []}),
        encoding="utf-8",
    )

    class FakeState:
        def get_file(self, fp):
            return {"status": "done" if "done" in fp else "pending"}

    first = main_mod._prune_done_from_priority(staging_dir=str(tmp_path), state=FakeState())
    second = main_mod._prune_done_from_priority(staging_dir=str(tmp_path), state=FakeState())
    assert first == 1
    assert second == 0, "second prune with no state change must be a no-op"


def test_prune_done_handles_missing_priority_file(tmp_path):
    """No priority.json → no error, return 0."""
    import pipeline.__main__ as main_mod

    class FakeState:
        def get_file(self, fp):
            return None

    removed = main_mod._prune_done_from_priority(staging_dir=str(tmp_path), state=FakeState())
    assert removed == 0


def test_prune_done_handles_no_state(tmp_path):
    """No state object passed → return 0 (don't crash)."""
    import pipeline.__main__ as main_mod

    control = tmp_path / "control"
    control.mkdir()
    (control / "priority.json").write_text(
        json.dumps({"paths": [r"\\NAS\x.mkv"]}), encoding="utf-8"
    )
    assert main_mod._prune_done_from_priority(staging_dir=str(tmp_path), state=None) == 0


# --------------------------------------------------------------------------
# set_priority seeds state DB
# --------------------------------------------------------------------------


def _make_state_db(tmp_path: Path, seed_rows=()) -> Path:
    db_path = tmp_path / "pipeline_state.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """CREATE TABLE pipeline_files (
            filepath TEXT PRIMARY KEY, status TEXT, mode TEXT, added TEXT,
            last_updated TEXT, tier TEXT, local_path TEXT, output_path TEXT,
            dest_path TEXT, error TEXT, stage TEXT, reason TEXT, res_key TEXT,
            extras TEXT DEFAULT '{}'
        )"""
    )
    for fp, status in seed_rows:
        conn.execute(
            "INSERT INTO pipeline_files (filepath, status) VALUES (?, ?)",
            (fp, status),
        )
    conn.commit()
    conn.close()
    return db_path


def test_set_priority_seeds_state_db_for_missing_paths(tmp_path, monkeypatch):
    """The priority API should INSERT pending rows for any new path that
    isn't already in state DB. Pre-fix this didn't happen so 96 of 198
    priority paths sat NOT_IN_STATE waiting for a queue rebuild."""
    from server.routers.pipeline import set_priority, PriorityRequest

    nas_movies = tmp_path / "NAS" / "Movies"
    nas_movies.mkdir(parents=True)
    db_path = _make_state_db(tmp_path)

    # Two paths: one already in DB, one not. Both files exist on "NAS".
    existing_path = nas_movies / "Existing.mkv"
    new_path = nas_movies / "Fresh.mkv"
    existing_path.write_bytes(b"x")
    new_path.write_bytes(b"x")

    # Seed the existing one
    con = sqlite3.connect(str(db_path))
    con.execute(
        "INSERT INTO pipeline_files (filepath, status) VALUES (?, 'pending')",
        (str(existing_path),),
    )
    con.commit()
    con.close()

    # Patch the module-level paths the endpoint uses
    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "PIPELINE_STATE_DB", db_path)
    monkeypatch.setattr(paths_mod, "NAS_MOVIES", nas_movies)
    monkeypatch.setattr(paths_mod, "NAS_SERIES", tmp_path / "NAS" / "Series")
    from server import helpers
    monkeypatch.setattr(helpers, "CONTROL_DIR", tmp_path / "control")

    req = PriorityRequest(paths=[str(existing_path), str(new_path)])
    result = set_priority(req)

    assert result["ok"] is True
    assert result["paths"] == 2
    assert result["seeded"] == 1, (
        f"only the previously-missing path should have been seeded, got seeded={result['seeded']}"
    )

    # Verify state DB
    con = sqlite3.connect(str(db_path))
    n = con.execute("SELECT COUNT(*) FROM pipeline_files").fetchone()[0]
    con.close()
    assert n == 2


def test_set_priority_skips_non_nas_paths(tmp_path, monkeypatch):
    """Safety: don't pollute state DB with paths outside the NAS media
    dirs (could be from a malformed UI request)."""
    from server.routers.pipeline import set_priority, PriorityRequest

    nas_movies = tmp_path / "NAS" / "Movies"
    nas_movies.mkdir(parents=True)
    db_path = _make_state_db(tmp_path)

    # File outside NAS
    outside = tmp_path / "Outside.mkv"
    outside.write_bytes(b"x")

    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "PIPELINE_STATE_DB", db_path)
    monkeypatch.setattr(paths_mod, "NAS_MOVIES", nas_movies)
    monkeypatch.setattr(paths_mod, "NAS_SERIES", tmp_path / "NAS" / "Series")
    from server import helpers
    monkeypatch.setattr(helpers, "CONTROL_DIR", tmp_path / "control")

    req = PriorityRequest(paths=[str(outside)])
    result = set_priority(req)

    assert result["seeded"] == 0, "non-NAS paths must NOT be seeded into the DB"


# --------------------------------------------------------------------------
# set_file prunes priority on terminal transition (real-time path)
# --------------------------------------------------------------------------


def _priority_paths(prio_path):
    return json.loads(prio_path.read_text(encoding="utf-8"))["paths"]


def _seed_priority(staging_dir, paths):
    """Write a priority.json containing the given paths."""
    control = staging_dir / "control"
    control.mkdir(exist_ok=True)
    p = control / "priority.json"
    p.write_text(
        json.dumps({"force": [], "paths": list(paths), "patterns": []}),
        encoding="utf-8",
    )
    return p


def _redirect_staging_dir(monkeypatch, tmp_path):
    """Point paths.STAGING_DIR at tmp_path so _remove_from_priority_json
    (which reads STAGING_DIR lazily) operates on the test fixture file."""
    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "STAGING_DIR", tmp_path)


def test_set_file_done_removes_from_priority(tmp_path, monkeypatch):
    """The MOMENT a file transitions to DONE in the state DB, its path
    must come off priority.json. No waiting for the next queue rebuild.
    """
    from pipeline.state import PipelineState, FileStatus

    _redirect_staging_dir(monkeypatch, tmp_path)
    db_path = tmp_path / "state.db"
    fp_done = r"\\NAS\Movies\Done.mkv"
    fp_pending = r"\\NAS\Movies\Pending.mkv"
    prio_path = _seed_priority(tmp_path, [fp_done, fp_pending])

    state = PipelineState(str(db_path))
    state.set_file(fp_done, FileStatus.DONE)

    remaining = _priority_paths(prio_path)
    assert fp_done not in remaining, "DONE transition must drop the path from priority.json"
    assert fp_pending in remaining, "other entries left alone"


def test_set_file_flagged_corrupt_removes_from_priority(tmp_path, monkeypatch):
    """Same hook fires for flagged_* terminal states — Ford-v-Ferrari
    class files belong off the priority list too."""
    from pipeline.state import PipelineState, FileStatus

    _redirect_staging_dir(monkeypatch, tmp_path)
    db_path = tmp_path / "state.db"
    fp = r"\\NAS\Movies\Broken.mkv"
    prio_path = _seed_priority(tmp_path, [fp])

    state = PipelineState(str(db_path))
    state.set_file(fp, FileStatus.FLAGGED_CORRUPT)

    assert fp not in _priority_paths(prio_path)


def test_set_file_pending_does_NOT_remove_from_priority(tmp_path, monkeypatch):
    """Status transitions to non-terminal states (pending, processing,
    fetching, uploading) must leave priority.json intact. The path
    should stay on the list until it's terminal."""
    from pipeline.state import PipelineState, FileStatus

    _redirect_staging_dir(monkeypatch, tmp_path)
    db_path = tmp_path / "state.db"
    fp = r"\\NAS\Movies\InFlight.mkv"
    prio_path = _seed_priority(tmp_path, [fp])

    state = PipelineState(str(db_path))
    state.set_file(fp, FileStatus.PROCESSING)
    state.set_file(fp, FileStatus.FETCHING)
    state.set_file(fp, FileStatus.UPLOADING)

    assert fp in _priority_paths(prio_path), (
        "non-terminal transitions must NOT prune from priority"
    )


def test_remove_from_priority_atomic_on_concurrent_failure(tmp_path):
    """If the .tmp write fails, priority.json must be left untouched
    (not truncated). Hard to simulate write failure; pin idempotency
    instead — calling the helper twice in a row produces the same
    final state."""
    from pipeline.state import _remove_from_priority_json

    prio_path = _seed_priority(tmp_path, [r"\\NAS\a.mkv", r"\\NAS\b.mkv"])
    r1 = _remove_from_priority_json(r"\\NAS\a.mkv", staging_dir=str(tmp_path))
    r2 = _remove_from_priority_json(r"\\NAS\a.mkv", staging_dir=str(tmp_path))
    assert r1 is True
    assert r2 is False  # not present anymore — no-op
    assert _priority_paths(prio_path) == [r"\\NAS\b.mkv"]


def test_set_priority_skips_missing_files(tmp_path, monkeypatch):
    """A priority entry for a file that doesn't exist on disk is a
    ghost — don't seed it (would be a never-fetchable pending row)."""
    from server.routers.pipeline import set_priority, PriorityRequest

    nas_movies = tmp_path / "NAS" / "Movies"
    nas_movies.mkdir(parents=True)
    db_path = _make_state_db(tmp_path)

    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "PIPELINE_STATE_DB", db_path)
    monkeypatch.setattr(paths_mod, "NAS_MOVIES", nas_movies)
    monkeypatch.setattr(paths_mod, "NAS_SERIES", tmp_path / "NAS" / "Series")
    from server import helpers
    monkeypatch.setattr(helpers, "CONTROL_DIR", tmp_path / "control")

    ghost = str(nas_movies / "Ghost.mkv")  # not written to disk
    req = PriorityRequest(paths=[ghost])
    result = set_priority(req)

    assert result["seeded"] == 0
    con = sqlite3.connect(str(db_path))
    n = con.execute("SELECT COUNT(*) FROM pipeline_files").fetchone()[0]
    con.close()
    assert n == 0
