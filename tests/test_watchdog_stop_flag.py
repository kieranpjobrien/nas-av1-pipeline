"""Pin the 2026-05-12 watchdog rewrite — one-shot launcher, never restarts.

Pre-2026-05-12 ``tools.pipeline_watchdog`` was an auto-respawner: launched
the pipeline, watched for non-zero exit, slept 30 s, relaunched, up to 20
times. That defeated the discipline contract — killing the supervisor just
caused a respawn 30 s later. Ford v Ferrari ran 10 corrupt encodes in 9
days because of this loop.

Post-fix the wrapper:
  * launches the command exactly once
  * logs the exit code + wall time
  * exits with the inner command's return code
  * refuses to launch if ``F:\\AV1_Staging\\control\\stop`` exists

These tests pin all of that.
"""

from __future__ import annotations

import sys
from pathlib import Path

import tools.pipeline_watchdog as wd


def test_wrapper_runs_command_exactly_once(monkeypatch, tmp_path):
    """No loop. The wrapper invokes subprocess.run once and returns."""
    log = tmp_path / "watchdog.log"
    call_count = {"n": 0}

    class FakeProc:
        returncode = 1  # non-zero — pre-fix this would have triggered a restart

    def fake_run(cmd, check=False):
        call_count["n"] += 1
        return FakeProc()

    monkeypatch.setattr(wd.subprocess, "run", fake_run)
    monkeypatch.setattr(sys, "argv", [
        "pipeline_watchdog",
        "--log", str(log),
        "--", "echo", "hi",
    ])

    rc = wd.main()
    assert call_count["n"] == 1, (
        f"wrapper must invoke the command exactly once (got {call_count['n']}) — "
        "no auto-restart on crash"
    )
    # Surface the inner exit code so the caller knows the launch failed
    assert rc == 1, f"wrapper should propagate inner rc=1, got {rc}"


def test_stop_flag_blocks_launch(monkeypatch, tmp_path):
    """If the stop flag exists, the wrapper does not invoke the command at all."""
    log = tmp_path / "watchdog.log"
    invoked = []

    def fake_run(*a, **kw):
        invoked.append(a)
        raise AssertionError("wrapper invoked subprocess despite stop flag")

    monkeypatch.setattr(wd.subprocess, "run", fake_run)
    monkeypatch.setattr(sys, "argv", [
        "pipeline_watchdog",
        "--log", str(log),
        "--", "echo", "should-not-run",
    ])

    # Pretend the canonical stop flag exists.
    original_exists = Path.exists

    def _exists(self):
        if str(self).replace("/", "\\").lower().endswith(r"control\stop"):
            return True
        return original_exists(self)

    monkeypatch.setattr(Path, "exists", _exists)

    rc = wd.main()
    assert rc == 0, f"wrapper must exit clean (0) when stop flag present, got {rc}"
    assert invoked == [], "wrapper must NOT invoke subprocess when stop flag present"


def test_no_auto_restart_constants_exist():
    """Defence in depth: the source must NOT contain the old auto-restart
    constants. If anyone reintroduces ``--max-restarts`` or ``--backoff-secs``
    this test fires."""
    import inspect

    src = inspect.getsource(wd)
    assert "--max-restarts" not in src, (
        "auto-restart reintroduced — watchdog must be a one-shot launcher only"
    )
    assert "--backoff-secs" not in src, (
        "backoff reintroduced — watchdog must be a one-shot launcher only"
    )
    # And the stop flag path must still be the documented one
    assert r"F:\AV1_Staging\control\stop" in src, (
        "watchdog stop flag path moved — update the user-facing halt docs"
    )


def test_clean_exit_returns_zero(monkeypatch, tmp_path):
    """rc=0 from inner command means clean — wrapper returns 0."""
    log = tmp_path / "watchdog.log"

    class FakeProc:
        returncode = 0

    monkeypatch.setattr(wd.subprocess, "run", lambda *a, **kw: FakeProc())
    monkeypatch.setattr(sys, "argv", [
        "pipeline_watchdog", "--log", str(log), "--", "echo", "ok",
    ])
    rc = wd.main()
    assert rc == 0
