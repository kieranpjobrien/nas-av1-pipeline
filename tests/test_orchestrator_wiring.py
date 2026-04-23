"""Regression tests for the circuit-breaker + process-registry + per-job-UUID wiring.

These cover the overnight 2026-04-23 NAS meltdown pattern:

  * Heavy workers kept firing gap_fill at a dying NAS — now a breaker
    opens after 5 consecutive failures and makes the worker wait.
  * Scanner / pipeline / mux processes from crashed sessions stayed
    in the registry forever — now reconcile() removes them at startup.
  * _ssh_docker's timeout-recovery pkill'd every mkvmerge in the
    container, murdering other workers' jobs — now the kill is scoped
    to the specific job via an NASCLEANUP_JOB=<uuid> env tag.

If any of these regress, the next overnight run will eat another 60+
files. Don't disable without a replacement.
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import psutil
import pytest

from pipeline import circuit_breaker as cb_module
from pipeline.circuit_breaker import CircuitBreaker
from pipeline.process_registry import ProcessRegistry


# =============================================================================
# 1. Circuit breaker opens heavy_worker on repeated failures
# =============================================================================


class TestHeavyWorkerCircuitBreaker:
    """Reproduce the heavy_worker loop's breaker behaviour in isolation.

    The real orchestrator._gap_filler_worker builds a queue + spawns
    threads; wiring that up here would make the test flaky. Instead we
    invoke the exact same pattern — wait_if_open / run / record — to
    prove the breaker opens after 5 failures and the worker stops
    dispatching.
    """

    def test_breaker_opens_after_5_consecutive_failures(self) -> None:
        """5 back-to-back gap_fill failures -> breaker OPEN, worker paused."""
        # Same construction arguments that orchestrator._gap_filler_worker uses.
        breaker = CircuitBreaker(threshold=5, cooldown_secs=300, name="heavy_worker.NAS")

        # Mock gap_fill-like call that always raises (simulates NAS SSH death).
        def fake_gap_fill() -> bool:
            raise RuntimeError("ssh: connect to host refused")

        # The orchestrator's actual loop is:
        #   breaker.wait_if_open(shutdown=...)
        #   try: success = gap_fill(...)
        #   except: success = False
        #   breaker.record(success)
        # We replicate it 5 times and assert the breaker ends up OPEN.
        for _ in range(5):
            # On iteration 1..4 the breaker is CLOSED so wait_if_open returns
            # immediately; iteration 5 also runs (the 5th failure is what
            # flips it to OPEN, not a future iteration).
            assert not breaker.is_open() or breaker.consecutive_failures() >= 5
            try:
                fake_gap_fill()
                success = True
            except Exception:
                success = False
            breaker.record(success)

        assert breaker.is_open() is True
        assert breaker.state() == "open"
        assert breaker.consecutive_failures() == 5

    def test_wait_if_open_blocks_further_dispatches(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Once OPEN, wait_if_open spins (polls) — worker does not proceed."""
        # Use a fake monotonic so we can control cooldown without real waits.
        fake_now = [10_000.0]
        monkeypatch.setattr(cb_module.time, "monotonic", lambda: fake_now[0])

        breaker = CircuitBreaker(threshold=5, cooldown_secs=300, name="heavy_worker.SRV")
        for _ in range(5):
            breaker.record(False)
        assert breaker.is_open()

        # Call wait_if_open with a tiny poll_secs; shutdown is_set=True, so it
        # raises CircuitBreakerOpen instead of blocking forever — that's how
        # the orchestrator heavy_worker exits cleanly at shutdown.
        import threading

        shutdown = threading.Event()
        shutdown.set()
        with pytest.raises(cb_module.CircuitBreakerOpen):
            breaker.wait_if_open(poll_secs=0.05, shutdown=shutdown)

        # If cooldown elapses, the breaker moves to HALF_OPEN on observation.
        fake_now[0] += 301.0
        # is_open() triggers the state transition.
        assert breaker.is_open() is False
        assert breaker.state() == "half_open"


# =============================================================================
# 2. ProcessRegistry reconciles at startup
# =============================================================================


def _find_dead_pid(max_tries: int = 10_000) -> int:
    """Return a PID that is currently not in use."""
    for candidate in range(2**15 - 1, 2**15 - max_tries - 1, -1):
        if candidate > 0 and not psutil.pid_exists(candidate):
            return candidate
    raise RuntimeError("no dead PID found in tested range")


class TestProcessRegistryStartupReconcile:
    """Dead registry entries from crashed sessions are reaped on reconcile()."""

    def test_startup_reconcile_reaps_dead_pid_entry(self, tmp_path: Path) -> None:
        """Plant a dead-PID entry, call reconcile(), assert it's gone."""
        registry_path = tmp_path / "agents.registry.json"
        dead_pid = _find_dead_pid()

        # Plant a "crashed scanner" entry that never cleaned up.
        registry_path.parent.mkdir(parents=True, exist_ok=True)
        planted: list[dict[str, Any]] = [
            {
                "role": "scanner",
                "pid": dead_pid,
                "cmd": ["python", "-m", "tools.scanner"],
                "started_at": time.time() - 7200,
                "create_time": time.time() - 7200,
                "last_heartbeat": time.time() - 7200,
            }
        ]
        with open(registry_path, "w", encoding="utf-8") as f:
            json.dump(planted, f)

        # Simulate what `pipeline/__main__.py::main` does at startup.
        registry = ProcessRegistry(registry_path)
        dead = registry.reconcile()

        # The ghost is gone.
        assert dead == ["scanner"]
        assert registry.list_active() == []

    def test_reconcile_preserves_live_entries(self, tmp_path: Path) -> None:
        """A live registration for a currently-running PID must NOT be reaped."""
        import os

        registry_path = tmp_path / "agents.registry.json"
        registry = ProcessRegistry(registry_path, heartbeat_secs=60)
        with registry.register("pipeline", ["python", "-m", "pipeline"]):
            # Now run reconcile — the current process is alive, so our own
            # entry must survive.
            dead = registry.reconcile()
            assert dead == []
            active = registry.list_active()
            assert len(active) == 1
            assert active[0]["role"] == "pipeline"
            assert active[0]["pid"] == os.getpid()

    def test_reconcile_handles_mixed_live_and_dead(self, tmp_path: Path) -> None:
        """Mixed dead + live entries — only the dead one is reaped."""
        import os

        registry_path = tmp_path / "agents.registry.json"
        dead_pid = _find_dead_pid()
        live_ctime = psutil.Process(os.getpid()).create_time()

        planted: list[dict[str, Any]] = [
            {
                "role": "scanner",
                "pid": dead_pid,
                "cmd": ["python", "-m", "tools.scanner"],
                "started_at": time.time() - 3600,
                "create_time": time.time() - 3600,
                "last_heartbeat": time.time() - 3600,
            },
            {
                "role": "pipeline",
                "pid": os.getpid(),
                "cmd": ["python", "-m", "pipeline"],
                "started_at": time.time(),
                "create_time": live_ctime,
                "last_heartbeat": time.time(),
            },
        ]
        with open(registry_path, "w", encoding="utf-8") as f:
            json.dump(planted, f)

        registry = ProcessRegistry(registry_path)
        dead = registry.reconcile()

        assert dead == ["scanner"]
        remaining = registry.list_active()
        assert [e["role"] for e in remaining] == ["pipeline"]


# =============================================================================
# 3. SSH per-job UUID kills only that job's process tree
# =============================================================================


class TestSshPerJobUuid:
    """_ssh_docker must inject NASCLEANUP_JOB=<uuid> and use it in the kill."""

    def test_docker_prefix_injects_env_for_plain_docker(self) -> None:
        """'docker exec CONTAINER' -> 'docker exec -e NASCLEANUP_JOB=X CONTAINER'."""
        from pipeline.nas_worker import _docker_prefix_with_env

        result = _docker_prefix_with_env("docker exec mkvworker", "NASCLEANUP_JOB", "abc123")
        assert result == "docker exec -e NASCLEANUP_JOB=abc123 mkvworker"

    def test_docker_prefix_injects_env_for_sudo_docker(self) -> None:
        """'sudo /path/to/docker exec CONTAINER' is also handled."""
        from pipeline.nas_worker import _docker_prefix_with_env

        result = _docker_prefix_with_env(
            "sudo /usr/local/bin/docker exec mkvworker",
            "NASCLEANUP_JOB",
            "abc123",
        )
        assert result == "sudo /usr/local/bin/docker exec -e NASCLEANUP_JOB=abc123 mkvworker"

    def test_timeout_recovery_kills_only_tagged_job(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """On TimeoutExpired, recovery ssh command contains our UUID, not bare pkill."""
        from pipeline.nas_worker import _ssh_docker

        # Capture subprocess.run calls — first is the failing ssh, second is
        # the recovery ssh.
        calls: list[dict[str, Any]] = []

        def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
            calls.append({"cmd": cmd, "kwargs": kwargs})
            # First call is the actual docker exec — time it out.
            if len(calls) == 1:
                raise subprocess.TimeoutExpired(cmd=cmd, timeout=1)
            # Second call is the recovery — return a completed process.
            rv = MagicMock()
            rv.returncode = 0
            rv.stdout = ""
            rv.stderr = ""
            return rv

        monkeypatch.setattr("pipeline.nas_worker.subprocess.run", fake_run)

        machine = {
            "host": "nas.test",
            "docker_prefix": "docker exec mkvworker",
            "label": "NAS",
        }
        with pytest.raises(subprocess.TimeoutExpired):
            _ssh_docker(machine, "mkvmerge", ["-o", "/media/out.mkv", "/media/in.mkv"], timeout=1)

        # Two calls: the original + the recovery kill.
        assert len(calls) == 2

        # The first call: the ssh command line includes the docker prefix
        # with our NASCLEANUP_JOB env flag injected.
        original_cmd = calls[0]["cmd"]
        joined = " ".join(original_cmd)
        assert "-e NASCLEANUP_JOB=" in joined

        # Extract the UUID from the original command.
        import re

        match = re.search(r"NASCLEANUP_JOB=([a-f0-9]+)", joined)
        assert match is not None
        uuid_used = match.group(1)
        assert len(uuid_used) == 32  # uuid4.hex is 32 chars

        # The recovery call: must reference the SAME UUID, not bare pkill.
        recovery_cmd = calls[1]["cmd"]
        recovery_joined = " ".join(recovery_cmd)
        assert uuid_used in recovery_joined
        # Must NOT be the old broad `pkill -9 mkvmerge` that killed everyone.
        assert "pkill -9 'mkvmerge'" not in recovery_joined
        assert "pkill -9 mkvmerge" not in recovery_joined
        # Must filter on /proc/<pid>/environ — that's the targeted-kill shape.
        assert "environ" in recovery_joined

    def test_each_call_gets_unique_uuid(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two consecutive _ssh_docker calls must NOT share a UUID.

        If they did, a timeout on job B could kill a still-running job A.
        """
        from pipeline.nas_worker import _ssh_docker

        captured_uuids: list[str] = []

        def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
            import re

            joined = " ".join(cmd)
            match = re.search(r"NASCLEANUP_JOB=([a-f0-9]+)", joined)
            if match:
                captured_uuids.append(match.group(1))
            rv = MagicMock()
            rv.returncode = 0
            rv.stdout = ""
            rv.stderr = ""
            return rv

        monkeypatch.setattr("pipeline.nas_worker.subprocess.run", fake_run)

        machine = {"host": "nas.test", "docker_prefix": "docker exec mkvworker", "label": "NAS"}
        _ssh_docker(machine, "mkvmerge", ["-o", "a.mkv", "in.mkv"])
        _ssh_docker(machine, "mkvmerge", ["-o", "b.mkv", "in.mkv"])

        assert len(captured_uuids) == 2
        assert captured_uuids[0] != captured_uuids[1]
