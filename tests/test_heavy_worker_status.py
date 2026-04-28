"""Tests for the heavy gap_filler worker status surfacing.

The 2026-04-29 incident: ``SERVER_SSH_HOST`` was unset, the heavy worker
was silently skipped, and 1,264 files sat queued for ~14 hours with no
visible signal beyond a single INFO log line per pass.

The fix surfaces this in three places:
  1. orchestrator writes ``heavy_worker_state.json`` every pass
  2. tools.invariants.check_heavy_worker_running fails HIGH when blocked
  3. admin.get_health exposes the state via /api/health for the dashboard

These tests cover (1) reading the state file format and (2) the invariant.
The orchestrator-side write path is a thin os.replace + json.dump and is
covered by the round-trip in test_orchestrator_writes_status.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import invariants


@pytest.fixture
def staging(tmp_path: Path, monkeypatch) -> Path:
    """Point the invariant module at a tmp staging dir."""
    monkeypatch.setattr(invariants, "STAGING_DIR", tmp_path)
    return tmp_path


def _write_state(staging: Path, **fields) -> Path:
    state_path = staging / "heavy_worker_state.json"
    state_path.write_text(json.dumps(fields), encoding="utf-8")
    return state_path


def test_passes_when_state_file_missing(staging):
    """Pre-first-pass: no state file yet -> pass with skipped message."""
    result = invariants.check_heavy_worker_running()
    assert result.passed is True
    assert "skipped" in result.message.lower()


def test_passes_when_configured_and_running(staging):
    _write_state(staging, configured=True, queued_count=42, blocked=False, host="nas")
    result = invariants.check_heavy_worker_running()
    assert result.passed is True
    assert result.severity == "HIGH"
    assert "enabled" in result.message


def test_passes_when_idle_and_queue_empty(staging):
    """SERVER not set is fine if there's nothing to do."""
    _write_state(staging, configured=False, queued_count=0, blocked=False, host=None)
    result = invariants.check_heavy_worker_running()
    assert result.passed is True
    assert "idle" in result.message


def test_fails_high_when_blocked_with_queue(staging):
    """The 2026-04-29 condition: not configured + non-empty queue -> HIGH fail."""
    _write_state(staging, configured=False, queued_count=1264, blocked=True, host=None)
    result = invariants.check_heavy_worker_running()
    assert result.passed is False
    assert result.severity == "HIGH"
    assert "1264" in result.message
    assert "SERVER_SSH_HOST" in result.message


def test_passes_when_state_file_corrupt(staging):
    """Malformed JSON shouldn't crash the invariant battery — return passing skip."""
    state_path = staging / "heavy_worker_state.json"
    state_path.write_text("not valid json {", encoding="utf-8")
    result = invariants.check_heavy_worker_running()
    assert result.passed is True
    assert "unreadable" in result.message.lower() or "skipped" in result.message.lower()


def test_invariant_battery_includes_check(staging):
    """The new check is wired into _invariant_runners so it actually runs."""
    runners = invariants._invariant_runners(skip_ssh=True)
    names = [getattr(r, "__name__", "") for r in runners]
    # Lambdas show up as "<lambda>" — the bare function shows as check_heavy_worker_running.
    assert "check_heavy_worker_running" in names
