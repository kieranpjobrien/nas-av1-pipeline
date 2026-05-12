"""Pin the 2026-05-13 ``set_file(DONE)`` self-scrub.

Three files (A Bronx Tale, Snatch, Shaun of the Dead) tripped the
``no_done_with_error_reason`` invariant because they finished with
``status=done`` but their ``reason`` field still carried the text from
the previous attempt:

  reason="reset 2026-05-12: error was '[WinError 59]' ..."
  error="[WinError 59] An unexpected network error occurred"

That's a Rule-1 violation surface — the DONE row looks like it lied
about success. Root cause: ``set_file`` preserved direct columns from
the previous row when the caller didn't pass them, so old ``reason``
and ``error`` leaked across the success transition.

Post-fix: ``set_file(DONE)``:
  * Clears ``error`` and ``stage`` by default (kwargs can override).
  * Scrubs the ``reason`` field IF the prior reason was
    failure-flavoured (contains error / fail / winerror / stuck /
    reset / refuse). A legitimate audit reason set by the encoder
    (e.g. "compression 18.4%") is left alone.
  * Resets breaker counters (``compliance_refuse_count``,
    ``integrity_failure_count``) — a successful encode means the
    file is no longer one cycle from terminal.
"""

from __future__ import annotations

import json
import sqlite3
import pytest

from pipeline.state import PipelineState, FileStatus


def _open(tmp_path):
    return PipelineState(str(tmp_path / "state.db"))


def _row(state, fp):
    cur = state._conn.execute(
        "SELECT status, reason, error, stage, extras FROM pipeline_files "
        "WHERE filepath = ?", (fp,))
    return cur.fetchone()


def test_done_scrubs_stale_winerror_reason(tmp_path, monkeypatch):
    """The exact A Bronx Tale / Snatch shape — reason carries the
    previous WinError-59 text from a reset. DONE must clear it."""
    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "STAGING_DIR", tmp_path)
    state = _open(tmp_path)
    fp = r"\\NAS\Movies\A Bronx Tale.mkv"
    # Seed: a row in error state with the WinError-flavoured reason
    state.set_file(fp, FileStatus.ERROR,
                   reason="reset 2026-05-12: error was '[WinError 59]' bla",
                   error="[WinError 59] an unexpected network error occurred")
    # Now mark DONE without passing reason
    state.set_file(fp, FileStatus.DONE)

    r = _row(state, fp)
    assert r["status"] == "done"
    assert r["reason"] is None, (
        f"stale failure reason must be scrubbed, got {r['reason']!r}"
    )
    assert r["error"] is None, "error column must be cleared on DONE"


def test_done_preserves_caller_provided_reason(tmp_path, monkeypatch):
    """If the encoder DOES pass a reason= kwarg (e.g. audit history),
    it must NOT be wiped — the caller's value wins."""
    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "STAGING_DIR", tmp_path)
    state = _open(tmp_path)
    fp = r"\\NAS\Movies\Good.mkv"
    state.set_file(fp, FileStatus.PROCESSING)
    state.set_file(fp, FileStatus.DONE, reason="compression 18.4%; encoded clean")

    r = _row(state, fp)
    assert r["reason"] == "compression 18.4%; encoded clean"


def test_done_keeps_legit_audit_reason_when_no_caller_override(tmp_path, monkeypatch):
    """If the previous row has a non-failure reason (legit audit
    history), DONE should keep it. We only scrub failure-flavoured
    text, not all reason values."""
    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "STAGING_DIR", tmp_path)
    state = _open(tmp_path)
    fp = r"\\NAS\Movies\Cleaned.mkv"
    state.set_file(fp, FileStatus.PROCESSING, reason="renamed for clean filename")
    state.set_file(fp, FileStatus.DONE)

    r = _row(state, fp)
    assert r["reason"] == "renamed for clean filename", (
        f"legit non-failure reason must survive, got {r['reason']!r}"
    )


def test_done_clears_stage(tmp_path, monkeypatch):
    """A DONE row at stage='verify' is misleading — verify is an
    in-flight stage. Stage must clear on terminal transition."""
    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "STAGING_DIR", tmp_path)
    state = _open(tmp_path)
    fp = r"\\NAS\Movies\Stage.mkv"
    state.set_file(fp, FileStatus.PROCESSING, stage="verify")
    state.set_file(fp, FileStatus.DONE)
    assert _row(state, fp)["stage"] is None


def test_done_resets_breaker_counters(tmp_path, monkeypatch):
    """A file that previously had refuse_count=2 (one cycle from
    terminal) successfully encoding should reset the counter. Otherwise
    a future single failure trips the breaker even though the file
    just succeeded — making the counter cumulative, not consecutive."""
    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "STAGING_DIR", tmp_path)
    state = _open(tmp_path)
    fp = r"\\NAS\Movies\Recovered.mkv"
    # File that had 2 prior compliance refuses
    state.set_file(fp, FileStatus.ERROR, compliance_refuse_count=2)
    state.set_file(fp, FileStatus.DONE)

    r = _row(state, fp)
    extras = json.loads(r["extras"] or "{}")
    assert extras.get("compliance_refuse_count") == 0, (
        f"counters must reset on successful encode, got {extras.get('compliance_refuse_count')}"
    )
    assert extras.get("integrity_failure_count") == 0


def test_error_transition_preserves_reason(tmp_path, monkeypatch):
    """Negative case: ERROR transitions do NOT scrub. The reason that
    explains the error is exactly what the user needs to debug."""
    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "STAGING_DIR", tmp_path)
    state = _open(tmp_path)
    fp = r"\\NAS\Movies\Sad.mkv"
    state.set_file(fp, FileStatus.ERROR,
                   reason="ffmpeg rc=137",
                   error="OOM killed by host")

    r = _row(state, fp)
    assert r["reason"] == "ffmpeg rc=137"
    assert r["error"] == "OOM killed by host"
