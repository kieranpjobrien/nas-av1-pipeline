"""Pin the 2026-05-13 requeue counter-reset.

User-initiated requeue means "the issue is fixed, give it a clean
shot". Pre-fix, requeue preserved the breaker counters from the
previous attempt. A file at ``compliance_refuse_count=2`` would sit
ONE cycle from terminal even after the underlying bug was fixed —
the no_elevated_breaker_counters invariant fired forever and the
user couldn't clear it without poking the DB by hand.

The Wild Robot, Heads of State, From Russia with Love all tripped
this overnight 2026-05-13.

Post-fix: both ``/api/file/requeue`` and ``/api/files/requeue-batch``
reset ``compliance_refuse_count`` and ``integrity_failure_count`` to
0 alongside the existing ``force_reencode=True`` flag.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest


def _make_db(tmp_path, rows):
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
    for fp, status, extras in rows:
        conn.execute(
            "INSERT INTO pipeline_files (filepath, status, extras) VALUES (?, ?, ?)",
            (fp, status, json.dumps(extras)),
        )
    conn.commit()
    conn.close()
    return db_path


def _read_extras(db_path, fp):
    con = sqlite3.connect(str(db_path))
    cur = con.execute("SELECT extras FROM pipeline_files WHERE filepath=?", (fp,))
    r = cur.fetchone()
    con.close()
    return json.loads(r[0] or "{}") if r else None


def test_single_requeue_clears_breaker_counters(tmp_path, monkeypatch):
    """The Wild Robot case: row at refuse_count=2. Requeue must reset
    to 0 so the file gets a clean three-attempt budget."""
    from server.routers.files import file_requeue

    nas_movies = tmp_path / "NAS" / "Movies"
    nas_movies.mkdir(parents=True)
    fp = nas_movies / "wild.mkv"
    fp.write_bytes(b"x")
    db_path = _make_db(tmp_path, [
        (str(fp), "error", {"compliance_refuse_count": 2, "integrity_failure_count": 1}),
    ])

    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "PIPELINE_STATE_DB", db_path)
    monkeypatch.setattr(paths_mod, "NAS_MOVIES", nas_movies)
    monkeypatch.setattr(paths_mod, "NAS_SERIES", tmp_path / "NAS" / "Series")

    result = file_requeue({"path": str(fp)})
    assert result["ok"] is True

    extras = _read_extras(db_path, str(fp))
    assert extras["compliance_refuse_count"] == 0
    assert extras["integrity_failure_count"] == 0
    assert extras["force_reencode"] is True


def test_batch_requeue_clears_breaker_counters(tmp_path, monkeypatch):
    """Same reset applies to the bulk path."""
    from server.routers.files import files_requeue_batch

    nas_movies = tmp_path / "NAS" / "Movies"
    nas_movies.mkdir(parents=True)
    files = []
    rows = []
    for name, refuse in [("a.mkv", 2), ("b.mkv", 5), ("c.mkv", 0)]:
        fp = nas_movies / name
        fp.write_bytes(b"x")
        files.append(str(fp))
        rows.append((str(fp), "error", {"compliance_refuse_count": refuse}))
    db_path = _make_db(tmp_path, rows)

    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "PIPELINE_STATE_DB", db_path)
    monkeypatch.setattr(paths_mod, "NAS_MOVIES", nas_movies)
    monkeypatch.setattr(paths_mod, "NAS_SERIES", tmp_path / "NAS" / "Series")

    result = files_requeue_batch({"paths": files})
    assert result["queued"] == 3
    for fp in files:
        extras = _read_extras(db_path, fp)
        assert extras["compliance_refuse_count"] == 0, (
            f"{os.path.basename(fp)} counter not reset: {extras}"
        )
        assert extras["force_reencode"] is True


def test_requeue_clears_stale_prep_done(tmp_path, monkeypatch):
    """The 2026-05-14 bug: prepare_for_encode short-circuits past the
    new local-strip + source-integrity flow when ``prep_done=True``
    is cached on the row. Any Given Sunday hit this — pre-architecture
    prep_done from an earlier attempt let the encoder skip the new
    strip step, foreign subs survived, post-encode PREP MISS.

    Fix: requeue clears both ``prep_done`` and ``prep_data`` so the
    next encode always re-runs the full prep flow."""
    from server.routers.files import file_requeue

    nas_movies = tmp_path / "NAS" / "Movies"
    nas_movies.mkdir(parents=True)
    fp = nas_movies / "stale-prep.mkv"
    fp.write_bytes(b"x")
    db_path = _make_db(tmp_path, [
        (str(fp), "error", {
            "compliance_refuse_count": 2,
            "prep_done": True,
            "prep_data": {"actual_input": "/tmp/old.mkv", "output_path": "/tmp/out.mkv"},
        }),
    ])

    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "PIPELINE_STATE_DB", db_path)
    monkeypatch.setattr(paths_mod, "NAS_MOVIES", nas_movies)
    monkeypatch.setattr(paths_mod, "NAS_SERIES", tmp_path / "NAS" / "Series")

    file_requeue({"path": str(fp)})
    extras = _read_extras(db_path, str(fp))

    assert extras.get("prep_done") is False, (
        "stale prep_done must be cleared so the next encode re-runs the "
        "full prep flow (local strip + source-integrity probe)"
    )
    assert "prep_data" not in extras, (
        "stale prep_data must be removed too — encoder shouldn't read a "
        "cached actual_input that points at a file that no longer exists"
    )


def test_requeue_preserves_other_extras(tmp_path, monkeypatch):
    """Counter reset is targeted — other extras (encode_params_used,
    detected_audio, etc.) must survive so the next encode reuses the
    work already done in qualify."""
    from server.routers.files import file_requeue

    nas_movies = tmp_path / "NAS" / "Movies"
    nas_movies.mkdir(parents=True)
    fp = nas_movies / "x.mkv"
    fp.write_bytes(b"x")
    db_path = _make_db(tmp_path, [
        (str(fp), "error", {
            "compliance_refuse_count": 2,
            "encode_params_used": {"cq": 22, "content_grade": "default"},
            "detected_audio": [{"codec": "eac3", "language": "eng"}],
            "duration_seconds": 7200,
        }),
    ])

    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "PIPELINE_STATE_DB", db_path)
    monkeypatch.setattr(paths_mod, "NAS_MOVIES", nas_movies)
    monkeypatch.setattr(paths_mod, "NAS_SERIES", tmp_path / "NAS" / "Series")

    file_requeue({"path": str(fp)})
    extras = _read_extras(db_path, str(fp))

    # Counter reset
    assert extras["compliance_refuse_count"] == 0
    # Other extras preserved
    assert extras["encode_params_used"]["cq"] == 22
    assert extras["detected_audio"][0]["language"] == "eng"
    assert extras["duration_seconds"] == 7200
