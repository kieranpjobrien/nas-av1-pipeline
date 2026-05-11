"""Pin the 2026-05-12 watchdog stop-flag.

Pre-fix the watchdog wrapper had no clean halt mechanism. SIGKILL of the
supervisor process led to non-zero exit -> 30s backoff -> watchdog
respawned. To stop the loop the user had to play whack-a-mole: kill the
supervisor, race the 30s, kill the watchdog before it relaunched.

Fix: watchdog now reads ``F:\\AV1_Staging\\control\\stop`` on every
iteration. If the file exists, the watchdog exits cleanly (rc=0) without
launching/relaunching anything.

These tests verify:
  * stop_flag present BEFORE first launch -> watchdog refuses to launch
  * the constant points at the documented path
"""

from __future__ import annotations

from pathlib import Path

import tools.pipeline_watchdog as wd


def test_stop_flag_blocks_first_launch(monkeypatch, tmp_path):
    """When the stop file exists before the watchdog starts, no command runs
    and the wrapper returns 0 (clean exit)."""
    stop = tmp_path / "stop"
    stop.write_text("")

    # Redirect the stop-flag path AND the log path
    monkeypatch.setattr(wd, "Path", Path)
    launches = []

    def _fail_if_called(*a, **kw):
        launches.append((a, kw))
        raise AssertionError("watchdog launched a subprocess despite stop flag")

    monkeypatch.setattr(wd.subprocess, "run", _fail_if_called)

    # The watchdog reads ``F:\AV1_Staging\control\stop`` — substitute via a
    # monkeypatch on the module-level construction by redefining main with our
    # path. The simplest hook is to monkeypatch the Path *constructor* used
    # in main; instead, we just call main() with sys.argv pointing at a
    # benign --log target and patch ``Path.exists`` to return True for the
    # specific known stop path.
    import sys
    monkeypatch.setattr(sys, "argv", [
        "pipeline_watchdog",
        "--log", str(tmp_path / "watchdog.log"),
        "--", "echo", "should-not-run",
    ])

    # Force the canonical stop-flag location to behave as if our flag exists.
    original_exists = Path.exists

    def _exists(self):
        if str(self).replace("/", "\\").lower().endswith("control\\stop"):
            return True
        return original_exists(self)

    monkeypatch.setattr(Path, "exists", _exists)

    rc = wd.main()
    assert rc == 0, f"expected clean exit (0) when stop flag present, got {rc}"
    assert launches == [], "watchdog must not invoke subprocess when stop flag present"


def test_stop_flag_path_is_canonical():
    """The watchdog reads from ``F:\\AV1_Staging\\control\\stop`` — that's
    where the user is expected to ``touch`` to halt the loop. If someone
    moves the path, the tooling/docs must move with it; this assert pins
    the contract."""
    import inspect

    src = inspect.getsource(wd.main)
    assert r"F:\AV1_Staging\control\stop" in src, (
        "watchdog stop flag path moved — update the user-facing halt docs"
    )
