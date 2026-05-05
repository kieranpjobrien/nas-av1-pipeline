"""Regression tests for the encode output-growth watchdog.

The 2026-05-05 incident: Any Given Sunday's corrupt EBML container made
ffmpeg spin in error-recovery for 7.5 hours, output file frozen at
729 MB, while the existing wall-clock deadline (10x duration) never
fired because the deadline check ran INSIDE the stdout-readline loop
and a stdout-silent process blocked the loop on readline() forever.

Fix: a separate daemon-thread watchdog that polls the output file
size and kills ffmpeg if it stops growing for ``encode_output_stall_secs``
(default 180s). Independent of the stdout reader — catches hangs the
old deadline missed.

These tests use a fake Popen-like object so we don't need a real
ffmpeg subprocess. The watchdog only touches:
  * process.poll() — to detect natural exit
  * process.kill() — when stalling
  * os.path.getsize(output_path) — to track growth
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline.full_gamut import _output_growth_watchdog


class _FakeProcess:
    """Minimal Popen stand-in — tracks kill() calls and reports a poll()
    return value. ``alive`` flag controls poll() (None = alive, 0 = exited)."""

    def __init__(self, alive: bool = True):
        self._alive = alive
        self.kill_calls = 0

    def poll(self):
        return None if self._alive else 0

    def kill(self):
        self.kill_calls += 1
        self._alive = False


def test_watchdog_kills_after_stall(tmp_path: Path):
    """The canonical case from the 2026-05-05 incident: output file
    exists but never grows. Watchdog kills after stall_secs."""
    out_path = tmp_path / "stuck.mkv"
    out_path.write_bytes(b"x" * 100)  # initial size 100 bytes, never grows
    proc = _FakeProcess(alive=True)
    stop = threading.Event()

    t = threading.Thread(
        target=_output_growth_watchdog,
        args=(proc, str(out_path), stop, 0.5, 0.1),  # 500ms stall, 100ms poll
    )
    t.start()
    t.join(timeout=3.0)

    assert not t.is_alive(), "watchdog should have exited after kill"
    assert proc.kill_calls == 1, "watchdog should have called process.kill() exactly once"


def test_watchdog_does_not_kill_a_growing_file(tmp_path: Path):
    """Healthy encode: file grows steadily. Watchdog must not kill."""
    out_path = tmp_path / "growing.mkv"
    out_path.write_bytes(b"")
    proc = _FakeProcess(alive=True)
    stop = threading.Event()

    # Grow the file 100 bytes every 50ms in a side thread, for 1 second.
    def grow():
        for _ in range(20):
            time.sleep(0.05)
            with out_path.open("ab") as f:
                f.write(b"x" * 100)

    grower = threading.Thread(target=grow, daemon=True)
    grower.start()

    watchdog = threading.Thread(
        target=_output_growth_watchdog,
        args=(proc, str(out_path), stop, 0.4, 0.1),  # 400ms stall, 100ms poll
    )
    watchdog.start()
    grower.join(timeout=2.0)
    # Stop the watchdog now that growth simulation is done
    stop.set()
    watchdog.join(timeout=2.0)

    assert proc.kill_calls == 0, "watchdog must not kill a healthy growing encode"


def test_watchdog_exits_when_process_terminates_on_its_own(tmp_path: Path):
    """If ffmpeg exits naturally, watchdog must observe that and stop
    polling — not call kill() (the process is already gone)."""
    out_path = tmp_path / "done.mkv"
    out_path.write_bytes(b"x" * 1000)
    proc = _FakeProcess(alive=False)  # already exited
    stop = threading.Event()

    t = threading.Thread(
        target=_output_growth_watchdog,
        args=(proc, str(out_path), stop, 0.5, 0.05),
    )
    t.start()
    t.join(timeout=2.0)

    assert not t.is_alive()
    assert proc.kill_calls == 0


def test_watchdog_tolerates_missing_output_file_at_start(tmp_path: Path):
    """First-frame latency: ffmpeg may take a few seconds to write the
    initial output. Watchdog must not crash on a missing file — it
    treats it as size 0 and starts the stall timer."""
    out_path = tmp_path / "not_yet.mkv"
    # Don't create it — file is missing
    proc = _FakeProcess(alive=True)
    stop = threading.Event()

    t = threading.Thread(
        target=_output_growth_watchdog,
        args=(proc, str(out_path), stop, 0.3, 0.05),
    )
    t.start()
    t.join(timeout=2.0)

    # File never appeared, so the file-doesn't-exist branch returns size 0
    # for both consecutive polls — no growth → kill after stall_secs.
    assert proc.kill_calls == 1


def test_watchdog_stops_when_event_is_set(tmp_path: Path):
    """The encoder caller signals the watchdog to stop after the stdout
    loop exits (success path). Watchdog must respond to the event without
    killing the process."""
    out_path = tmp_path / "ok.mkv"
    out_path.write_bytes(b"x" * 100)
    proc = _FakeProcess(alive=True)
    stop = threading.Event()

    t = threading.Thread(
        target=_output_growth_watchdog,
        args=(proc, str(out_path), stop, 60.0, 0.1),  # long stall — won't fire
    )
    t.start()
    time.sleep(0.2)
    stop.set()
    t.join(timeout=1.0)

    assert not t.is_alive(), "watchdog should respect stop_event"
    assert proc.kill_calls == 0, "watchdog must not kill when stopped cleanly"


def test_watchdog_kill_failure_doesnt_propagate(tmp_path: Path):
    """If process.kill() raises (e.g. process already gone, OS race),
    the watchdog should log and exit cleanly — not propagate the exception
    up the daemon thread (which would silently stop the watchdog and
    nobody would notice)."""
    out_path = tmp_path / "stuck.mkv"
    out_path.write_bytes(b"x" * 100)

    raising_proc = MagicMock()
    raising_proc.poll.return_value = None
    raising_proc.kill.side_effect = OSError("process gone")

    stop = threading.Event()
    t = threading.Thread(
        target=_output_growth_watchdog,
        args=(raising_proc, str(out_path), stop, 0.3, 0.05),
    )
    t.start()
    t.join(timeout=2.0)

    assert not t.is_alive()
    raising_proc.kill.assert_called_once()
