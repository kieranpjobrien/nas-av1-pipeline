"""Tests for pipeline/process_registry.py — persistent registry + reaper for pipeline processes."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import psutil
import pytest

from pipeline import process_registry as pr_module
from pipeline.process_registry import ProcessRegistry


def _find_dead_pid(max_tries: int = 10_000) -> int:
    """Return a PID that is currently NOT alive on this machine.

    Walks backwards from a high PID number; on any OS, the odds of a
    specific high number being in use are tiny.
    """
    for candidate in range(2**15 - 1, 2**15 - max_tries - 1, -1):
        if candidate > 0 and not psutil.pid_exists(candidate):
            return candidate
    raise RuntimeError("no dead PID found in tested range")


def _write_raw(path: Path, entries: list[dict[str, Any]]) -> None:
    """Directly write the registry JSON without going through ProcessRegistry."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(entries, f)


@pytest.fixture()
def registry_path(tmp_path: Path) -> Path:
    return tmp_path / "process_registry.json"


class TestRegisterDeregister:
    """Context manager adds and removes entries atomically."""

    def test_register_and_deregister(self, registry_path: Path) -> None:
        """Entry visible inside the ``with`` block, gone after exit."""
        reg = ProcessRegistry(registry_path, heartbeat_secs=60)
        with reg.register("scanner", ["python", "-m", "tools.scanner"]):
            active = reg.list_active()
            assert len(active) == 1
            assert active[0]["role"] == "scanner"
            assert active[0]["pid"] == os.getpid()
            assert active[0]["cmd"] == ["python", "-m", "tools.scanner"]
        # Exited -> empty
        assert reg.list_active() == []


class TestDuplicateRole:
    """Same role cannot be registered twice while one is live."""

    def test_register_refuses_duplicate_live_process(self, registry_path: Path) -> None:
        """A second register() for a role already held by a live PID raises."""
        # Plant an entry for the current (live) PID under role "scanner".
        ctime = psutil.Process(os.getpid()).create_time()
        _write_raw(
            registry_path,
            [
                {
                    "role": "scanner",
                    "pid": os.getpid(),
                    "cmd": ["python"],
                    "started_at": time.time(),
                    "create_time": ctime,
                    "last_heartbeat": time.time(),
                }
            ],
        )
        reg = ProcessRegistry(registry_path, heartbeat_secs=60)
        with pytest.raises(RuntimeError, match="already active"):
            with reg.register("scanner", ["python"]):
                pass  # pragma: no cover

    def test_register_accepts_if_prior_pid_dead(self, registry_path: Path) -> None:
        """Stale entry with a dead PID doesn't block a new registration."""
        dead_pid = _find_dead_pid()
        _write_raw(
            registry_path,
            [
                {
                    "role": "scanner",
                    "pid": dead_pid,
                    "cmd": ["python"],
                    "started_at": time.time() - 3600,
                    "create_time": time.time() - 3600,
                    "last_heartbeat": time.time() - 3600,
                }
            ],
        )
        reg = ProcessRegistry(registry_path, heartbeat_secs=60)
        with reg.register("scanner", ["python"]):
            active = reg.list_active()
            assert len(active) == 1
            assert active[0]["pid"] == os.getpid()


class TestReconcile:
    """Dead-PID and PID-recycling entries are removed on reconcile."""

    def test_reconcile_removes_dead_pids(self, registry_path: Path) -> None:
        """Entries whose PID is not alive are dropped."""
        dead_pid = _find_dead_pid()
        _write_raw(
            registry_path,
            [
                {
                    "role": "ghost",
                    "pid": dead_pid,
                    "cmd": ["python"],
                    "started_at": time.time() - 3600,
                    "create_time": time.time() - 3600,
                    "last_heartbeat": time.time() - 3600,
                }
            ],
        )
        reg = ProcessRegistry(registry_path, heartbeat_secs=60)
        removed = reg.reconcile()
        assert removed == ["ghost"]
        assert reg.list_active() == []

    def test_reconcile_detects_pid_recycling(self, registry_path: Path) -> None:
        """Stored create_time that doesn't match psutil's -> removed."""
        # Current process is alive, but we lie about its create_time.
        real_ctime = psutil.Process(os.getpid()).create_time()
        _write_raw(
            registry_path,
            [
                {
                    "role": "recycled",
                    "pid": os.getpid(),
                    "cmd": ["python"],
                    "started_at": time.time() - 3600,
                    "create_time": real_ctime - 999_999.0,  # wildly wrong
                    "last_heartbeat": time.time() - 3600,
                }
            ],
        )
        reg = ProcessRegistry(registry_path, heartbeat_secs=60)
        removed = reg.reconcile()
        assert removed == ["recycled"]
        assert reg.list_active() == []


class TestHeartbeat:
    """Background heartbeat thread advances last_heartbeat over time."""

    def test_heartbeat_updates_last_heartbeat(self, registry_path: Path) -> None:
        """After the heartbeat interval elapses, last_heartbeat is newer than started_at."""
        # heartbeat_secs=1 so a real wait is feasible without huge test runtime.
        reg = ProcessRegistry(registry_path, heartbeat_secs=1)
        with reg.register("hb", ["python"]):
            entry_at_start = reg.list_active()[0]
            started_hb = entry_at_start["last_heartbeat"]
            # Sleep long enough for the heartbeat thread to fire at least once.
            time.sleep(2.5)
            entry_later = reg.list_active()[0]
            assert entry_later["last_heartbeat"] > started_hb


class TestKillStale:
    """kill_stale terminates processes whose heartbeat is too old."""

    def test_kill_stale_terminates_old_heartbeats(self, registry_path: Path) -> None:
        """Entry with ancient heartbeat -> Process.terminate() called, entry removed."""
        dead_pid = _find_dead_pid()
        ancient = time.time() - 10_000
        _write_raw(
            registry_path,
            [
                {
                    "role": "ancient",
                    "pid": dead_pid,
                    "cmd": ["python"],
                    "started_at": ancient,
                    "create_time": ancient,
                    "last_heartbeat": ancient,
                },
                {
                    "role": "fresh",
                    "pid": os.getpid(),
                    "cmd": ["python"],
                    "started_at": time.time(),
                    "create_time": psutil.Process(os.getpid()).create_time(),
                    "last_heartbeat": time.time(),
                },
            ],
        )
        reg = ProcessRegistry(registry_path, heartbeat_secs=60)

        # We want to assert terminate() was called. The ancient entry's PID
        # is dead so psutil.pid_exists(dead_pid) is False, which skips the
        # terminate() path. Force pid_exists to return True for the dead PID
        # and patch Process to a spy.
        with patch.object(pr_module.psutil, "pid_exists", return_value=True):
            with patch.object(pr_module.psutil, "Process") as mock_process:
                killed = reg.kill_stale(max_age_secs=120)

        # Exactly the one stale PID should have been killed.
        assert killed == [dead_pid]
        # terminate() was called on the Process wrapper returned for dead_pid.
        mock_process.assert_called_with(dead_pid)
        mock_process.return_value.terminate.assert_called_once()
        # The stale entry is gone; the fresh one remains.
        remaining_roles = [e["role"] for e in reg.list_active()]
        assert remaining_roles == ["fresh"]

    def test_kill_stale_returns_empty_when_all_fresh(self, registry_path: Path) -> None:
        """No old heartbeats -> nothing killed, entries untouched."""
        _write_raw(
            registry_path,
            [
                {
                    "role": "fresh",
                    "pid": os.getpid(),
                    "cmd": ["python"],
                    "started_at": time.time(),
                    "create_time": psutil.Process(os.getpid()).create_time(),
                    "last_heartbeat": time.time(),
                },
            ],
        )
        reg = ProcessRegistry(registry_path, heartbeat_secs=60)
        killed = reg.kill_stale(max_age_secs=120)
        assert killed == []
        assert len(reg.list_active()) == 1


class TestInit:
    """Constructor guards."""

    def test_heartbeat_secs_must_be_positive(self, registry_path: Path) -> None:
        with pytest.raises(ValueError):
            ProcessRegistry(registry_path, heartbeat_secs=0)
