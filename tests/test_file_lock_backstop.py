"""_file_lock must never wedge, and must not steal from a live owner too early.

Two failure modes, both real:
  * Pre-2026-07-25: the 60s age cutoff ran even when the owner pid was ALIVE, so
    a live writer lost its lock mid-write and two writers shared one .tmp path.
  * 2026-07-25 (my regression): "never break a live owner" was absolute, so the
    agents.registry lock sat held for 11.5 HOURS by a live pid. Every heartbeat
    timed out, and the supervisor then failed to BOOT — reconcile() raised
    TimeoutError. A pid can be recycled onto an unrelated process, or an owner
    can wedge while holding the lock.
"""

import os
import time

import pytest

from tools.report_lock import _LIVE_OWNER_STALE_SECS, _file_lock


def _plant(lock_path, pid, age_secs):
    lock_path.write_text(str(pid), encoding="utf-8")
    past = time.time() - age_secs
    os.utime(lock_path, (past, past))


def test_live_owner_is_not_robbed_mid_write(tmp_path):
    """A live owner holding the lock for 90s must NOT lose it — that is the
    two-writers-one-tmp corruption this guard exists to prevent."""
    lock = tmp_path / "x.lock"
    _plant(lock, os.getpid(), 90)  # our own pid: definitely alive
    with pytest.raises(TimeoutError):
        with _file_lock(lock, timeout=2.0):
            pass
    assert lock.exists(), "a live owner's lock must survive"


def test_live_owner_lock_is_broken_past_the_backstop(tmp_path):
    """But it must not wedge forever — the exact 11.5h outage."""
    lock = tmp_path / "y.lock"
    _plant(lock, os.getpid(), _LIVE_OWNER_STALE_SECS + 60)
    with _file_lock(lock, timeout=5.0):
        assert lock.read_text(encoding="utf-8").strip() == str(os.getpid())


def test_dead_owner_is_reclaimed_immediately(tmp_path):
    """A crashed owner should not cost us the 60s wait."""
    lock = tmp_path / "z.lock"
    _plant(lock, 999_999_999, 1)  # pid that cannot exist
    started = time.monotonic()
    with _file_lock(lock, timeout=5.0):
        pass
    assert time.monotonic() - started < 2.0


def test_corrupt_pid_falls_back_to_short_age_cutoff(tmp_path):
    """Inconclusive pid probe -> the cheap 60s reclaim still applies."""
    lock = tmp_path / "w.lock"
    lock.write_text("not-a-pid", encoding="utf-8")
    past = time.time() - 120
    os.utime(lock, (past, past))
    with _file_lock(lock, timeout=5.0):
        pass
