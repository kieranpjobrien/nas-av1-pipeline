"""Tests for pipeline.gap_fill_lock — cross-process lock for NAS gap-fill ops.

Covers the seven behaviours the spec demands:

    1. Basic acquire/release.
    2. Blocks a concurrent second holder; second acquires once first releases.
    3. Times out cleanly via GapFillLockTimeout.
    4. Shutdown event aborts a waiter.
    5. Dead-PID stale detection breaks the lock and allows new acquire.
    6. Age-based stale detection breaks the lock and allows new acquire.
    7. Lock file contents (pid + role + timestamp) are observable by an
       external reader.

Also: the module refuses recursive acquire from the same thread
(GapFillLockReentrantError). Tested here for completeness because the
semantic decision is explicitly called out in the spec.
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

import pytest

from pipeline.gap_fill_lock import (
    GapFillLockReentrantError,
    GapFillLockTimeout,
    gap_fill_lock,
)


@pytest.fixture()
def lock_path(tmp_path: Path) -> Path:
    """Tmp lock file — must never be the real F:\\AV1_Staging\\control\\gap_fill.lock."""
    return tmp_path / "gap_fill.lock"


def test_lock_acquire_release(lock_path: Path) -> None:
    """Context manager acquires the lock, writes the file, and cleans up on exit."""
    assert not lock_path.exists()
    with gap_fill_lock(role="test", timeout=5.0, lock_path=lock_path):
        assert lock_path.exists(), "lock file must exist while held"
    assert not lock_path.exists(), "lock file must be removed on release"


def test_lock_blocks_second_holder(lock_path: Path) -> None:
    """Two threads contend; the second blocks until the first releases, then acquires."""
    first_acquired = threading.Event()
    first_release = threading.Event()
    second_acquired = threading.Event()
    order: list[str] = []

    def first() -> None:
        with gap_fill_lock(role="first", timeout=10.0, lock_path=lock_path):
            order.append("first_acquired")
            first_acquired.set()
            # Hold until the test signals us to release.
            first_release.wait(timeout=10.0)
            order.append("first_releasing")

    def second() -> None:
        # Wait until first is confirmed holding, otherwise we'd race to acquire first.
        first_acquired.wait(timeout=5.0)
        with gap_fill_lock(role="second", timeout=10.0, lock_path=lock_path):
            order.append("second_acquired")
            second_acquired.set()

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    t2.start()

    assert first_acquired.wait(timeout=5.0), "first never acquired"
    # Give second a brief window to try (and fail) to acquire while first holds.
    time.sleep(0.5)
    assert not second_acquired.is_set(), "second must be blocked while first holds"

    first_release.set()
    t1.join(timeout=5.0)
    assert second_acquired.wait(timeout=5.0), "second never acquired after first released"
    t2.join(timeout=5.0)

    assert order == ["first_acquired", "first_releasing", "second_acquired"]


def test_lock_timeout(lock_path: Path) -> None:
    """Waiter raises GapFillLockTimeout when the holder outlasts the timeout."""
    holder_in = threading.Event()
    holder_release = threading.Event()

    def holder() -> None:
        with gap_fill_lock(role="holder", timeout=10.0, lock_path=lock_path):
            holder_in.set()
            holder_release.wait(timeout=10.0)

    t = threading.Thread(target=holder)
    t.start()
    try:
        assert holder_in.wait(timeout=5.0)
        start = time.monotonic()
        with pytest.raises(GapFillLockTimeout):
            with gap_fill_lock(role="loser", timeout=1.5, lock_path=lock_path):
                pytest.fail("should not have acquired")  # pragma: no cover
        elapsed = time.monotonic() - start
        # Should hit the timeout (~1.5s), not block for 10s. Wide bound to
        # tolerate CI slowness while still catching a "never times out" bug.
        assert 1.0 <= elapsed <= 5.0, f"timeout fired after {elapsed:.2f}s"
    finally:
        holder_release.set()
        t.join(timeout=5.0)


def test_lock_shutdown_event_aborts_wait(lock_path: Path) -> None:
    """Setting the shutdown event while a waiter is blocked aborts it immediately."""
    holder_in = threading.Event()
    holder_release = threading.Event()
    shutdown = threading.Event()
    waiter_result: list[Exception | str] = []

    def holder() -> None:
        with gap_fill_lock(role="holder", timeout=30.0, lock_path=lock_path):
            holder_in.set()
            holder_release.wait(timeout=30.0)

    def waiter() -> None:
        try:
            with gap_fill_lock(
                role="waiter", timeout=30.0, shutdown=shutdown, lock_path=lock_path
            ):
                waiter_result.append("acquired")  # pragma: no cover
        except Exception as e:
            waiter_result.append(e)

    th = threading.Thread(target=holder)
    tw = threading.Thread(target=waiter)
    th.start()
    assert holder_in.wait(timeout=5.0)
    tw.start()

    # Let the waiter enter its poll loop.
    time.sleep(0.3)
    start = time.monotonic()
    shutdown.set()
    tw.join(timeout=5.0)
    elapsed = time.monotonic() - start

    assert not tw.is_alive(), "waiter must exit when shutdown fires"
    assert elapsed < 3.0, f"waiter took too long to abort ({elapsed:.2f}s)"
    assert len(waiter_result) == 1
    assert isinstance(waiter_result[0], GapFillLockTimeout)
    assert "shutdown" in str(waiter_result[0])

    holder_release.set()
    th.join(timeout=5.0)


def _find_dead_pid(max_tries: int = 10_000) -> int:
    """Return a PID that is currently NOT alive on this machine. Mirrors the
    helper in test_process_registry.py — walks high-number PIDs backward."""
    import psutil

    for candidate in range(2**15 - 1, 2**15 - max_tries - 1, -1):
        if candidate > 0 and not psutil.pid_exists(candidate):
            return candidate
    raise RuntimeError("no dead PID found in tested range")


def test_stale_lock_from_dead_pid_broken(lock_path: Path) -> None:
    """A lock file whose owner PID is dead is broken immediately on contention."""
    dead_pid = _find_dead_pid()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(
        json.dumps({
            "pid": dead_pid,
            "role": "ghost",
            "acquired_at": time.time(),
        }),
        encoding="utf-8",
    )
    # Set the mtime to "recent" so age-cutoff cannot take credit for the break.
    now = time.time()
    os.utime(lock_path, (now, now))

    start = time.monotonic()
    with gap_fill_lock(role="new_owner", timeout=5.0, lock_path=lock_path):
        # File should be ours now — metadata overwritten.
        meta = json.loads(lock_path.read_text(encoding="utf-8"))
        assert meta["role"] == "new_owner"
        assert meta["pid"] == os.getpid()
    elapsed = time.monotonic() - start
    # Fast path: dead-PID break should NOT wait the full 5s timeout.
    assert elapsed < 2.0, f"stale-PID break took too long ({elapsed:.2f}s)"


def test_stale_lock_by_age_broken(lock_path: Path) -> None:
    """A lock file older than the age cutoff is broken even without a dead-PID signal."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    # Metadata with pid=0 so the PID-alive probe is inconclusive and the age
    # check is what breaks the lock. This isolates the age-fallback behaviour.
    lock_path.write_text(
        json.dumps({
            "pid": 0,
            "role": "ancient",
            "acquired_at": time.time() - 3600,
        }),
        encoding="utf-8",
    )
    # Age 15 minutes — exceeds the 10-minute default cutoff.
    old = time.time() - (15 * 60)
    os.utime(lock_path, (old, old))

    start = time.monotonic()
    with gap_fill_lock(role="new_owner", timeout=5.0, lock_path=lock_path):
        meta = json.loads(lock_path.read_text(encoding="utf-8"))
        assert meta["role"] == "new_owner"
    elapsed = time.monotonic() - start
    assert elapsed < 2.0, f"age break took too long ({elapsed:.2f}s)"


def test_lock_file_contents_observable(lock_path: Path) -> None:
    """External reader can parse pid + role + timestamp out of the lock file."""
    before = time.time()
    with gap_fill_lock(role="observability_test", timeout=5.0, lock_path=lock_path):
        # Simulate an external reader (e.g. `cat gap_fill.lock`).
        raw = lock_path.read_text(encoding="utf-8")
        meta = json.loads(raw)
        assert meta["pid"] == os.getpid()
        assert meta["role"] == "observability_test"
        assert isinstance(meta["acquired_at"], float)
        # Timestamp should be sensible — between "before" and now+1.
        assert before <= meta["acquired_at"] <= time.time() + 1.0


def test_lock_reentrant_from_same_thread_rejected(lock_path: Path) -> None:
    """Same thread attempting to nest raises GapFillLockReentrantError.

    The spec explicitly calls out this semantic decision ("don't nest;
    raise if a role tries to acquire while already holding"). If it
    silently re-entered, a future refactor that introduced nested gap-fill
    ops could deadlock against a different holder in production.
    """
    with gap_fill_lock(role="outer", timeout=5.0, lock_path=lock_path):
        with pytest.raises(GapFillLockReentrantError):
            with gap_fill_lock(role="inner", timeout=1.0, lock_path=lock_path):
                pytest.fail("should not nest")  # pragma: no cover
