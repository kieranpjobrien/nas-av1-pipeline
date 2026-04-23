"""ProcessManager class and process configuration for managed subprocesses."""

import os
import subprocess
import sys
import threading
from collections import deque
from pathlib import Path

from server.helpers import STAGING_DIR, drop_file, remove_file

_PROJECT_ROOT = str(Path(__file__).parent.parent)

PROCESS_CONFIGS: dict[str, dict] = {
    "scanner": {
        "cmd": [sys.executable, "-m", "tools.scanner"],
        "cwd": _PROJECT_ROOT,
    },
    "pipeline": {
        "cmd": [sys.executable, "-m", "pipeline"],
        "cwd": _PROJECT_ROOT,
    },
    "gap_filler": {
        "cmd": [sys.executable, "-m", "pipeline", "--gap-filler-only"],
        "cwd": _PROJECT_ROOT,
    },
    "strip_tags": {
        "cmd": [sys.executable, "-m", "tools.maintain", "clean-names", "--execute", "--movies"],
        "cwd": _PROJECT_ROOT,
    },
    "duplicates": {
        "cmd": [sys.executable, "-m", "tools.duplicates", "--delete", "--execute"],
        "cwd": _PROJECT_ROOT,
    },
    "subtitles": {
        "cmd": [sys.executable, "-m", "tools.subtitles"],
        "cwd": _PROJECT_ROOT,
    },
    "integrity": {
        "cmd": [sys.executable, "-m", "tools.integrity", "--from-state", "--workers", "1"],
        "cwd": _PROJECT_ROOT,
    },
    "plex_sync": {
        "cmd": [
            sys.executable,
            "-c",
            "import subprocess, sys; "
            "print('Triggering Plex library scan...', flush=True); "
            "subprocess.run([sys.executable, '-m', 'tools.plex_metadata', 'scan']); "
            "print('Running metadata audit...', flush=True); "
            "subprocess.run([sys.executable, '-m', 'tools.plex_metadata', 'audit', '--json', "
            f"r'{STAGING_DIR / 'plex_audit.json'}']); "
            "print('Applying rules...', flush=True); "
            "subprocess.run([sys.executable, '-m', 'tools.plex_metadata', 'apply-rules', '--execute']); "
            "print('Plex sync complete.', flush=True)",
        ],
        "cwd": _PROJECT_ROOT,
    },
    "detect_languages": {
        "cmd": [
            sys.executable,
            "-m",
            "tools.detect_languages",
            "--workers",
            "6",
            "--apply",
            "--min-confidence",
            "0.85",
        ],
        "cwd": _PROJECT_ROOT,
    },
    "detect_languages_whisper": {
        "cmd": [sys.executable, "-m", "tools.detect_languages", "--whisper", "--apply", "--min-confidence", "0.85"],
        "cwd": _PROJECT_ROOT,
    },
    "detect_languages_retry": {
        "cmd": [
            sys.executable,
            "-m",
            "tools.detect_languages",
            "--whisper",
            "--retry-unresolved",
            "--apply",
            "--min-confidence",
            "0.75",
        ],
        "cwd": _PROJECT_ROOT,
    },
    "detect_languages_spotcheck": {
        "cmd": [sys.executable, "-m", "tools.detect_languages", "--spot-check", "200"],
        "cwd": _PROJECT_ROOT,
    },
    "apply_languages": {
        "cmd": [sys.executable, "-m", "tools.detect_languages", "--apply", "--min-confidence", "0.85"],
        "cwd": _PROJECT_ROOT,
    },
    "tmdb_enrich": {
        "cmd": [sys.executable, "-m", "tools.tmdb", "--enrich-and-apply"],
        "cwd": _PROJECT_ROOT,
    },
    "tmdb_apply": {
        "cmd": [sys.executable, "-m", "tools.tmdb", "--apply"],
        "cwd": _PROJECT_ROOT,
    },
    "rewatchables": {
        "cmd": [sys.executable, "-m", "tools.rewatchables"],
        "cwd": _PROJECT_ROOT,
    },
}

VALID_PROCESS_NAMES: set[str] = set(PROCESS_CONFIGS.keys())

STOP_TIMEOUT = 15  # seconds to wait for graceful stop before terminate


class ProcessManager:
    """Manages subprocess lifecycle for pipeline tools."""

    def __init__(self) -> None:
        self._procs: dict[str, subprocess.Popen] = {}
        self._logs: dict[str, deque] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._lock = threading.Lock()

    def _reader(self, name: str, proc: subprocess.Popen) -> None:
        """Background thread that reads stdout lines into the log buffer."""
        buf = self._logs[name]
        try:
            for line in iter(proc.stdout.readline, ""):
                if not line:
                    break
                buf.append(line.rstrip("\n"))
        except (ValueError, OSError):
            pass

    def start(self, name: str) -> dict:
        """Start a named process if not already running."""
        cfg = PROCESS_CONFIGS.get(name)
        if not cfg:
            raise ValueError(f"Unknown process: {name}")
        with self._lock:
            existing = self._procs.get(name)
            if existing and existing.poll() is None:
                return {"ok": False, "error": f"{name} is already running (pid {existing.pid})"}
            creation_flags = 0
            if sys.platform == "win32":
                creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
            proc = subprocess.Popen(
                cfg["cmd"],
                cwd=cfg["cwd"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                creationflags=creation_flags,
            )
            self._procs[name] = proc
            self._logs[name] = deque(maxlen=500)
            t = threading.Thread(target=self._reader, args=(name, proc), daemon=True)
            t.start()
            self._threads[name] = t
            return {"ok": True, "pid": proc.pid}

    def stop(self, name: str) -> dict:
        """Gracefully stop a running process, terminating after timeout."""
        with self._lock:
            proc = self._procs.get(name)
            if not proc or proc.poll() is not None:
                return {"ok": False, "error": f"{name} is not running"}
        if name == "pipeline":
            drop_file("pause_all.json", {"type": "all"})
            try:
                proc.wait(timeout=STOP_TIMEOUT)
                remove_file("pause_all.json")
                return {"ok": True, "method": "graceful"}
            except subprocess.TimeoutExpired:
                proc.terminate()
                remove_file("pause_all.json")
                return {"ok": True, "method": "terminated"}
        else:
            proc.terminate()
            return {"ok": True, "method": "terminated"}

    def force_kill(self, name: str) -> dict:
        """Kill any OS process matching this pipeline command, even if not started by us.

        Uses psutil to enumerate processes — wmic was the original approach but is
        deprecated/absent in Windows 11. psutil works cross-platform and is already
        a dependency.
        """
        import psutil

        cfg = PROCESS_CONFIGS.get(name)
        if not cfg:
            raise ValueError(f"Unknown process: {name}")

        # The module name to search for (argument immediately after "-m",
        # e.g. "pipeline", "tools.scanner", "tools.maintain").
        try:
            mi = cfg["cmd"].index("-m")
            module_flag = cfg["cmd"][mi + 1]
        except (ValueError, IndexError):
            return {"ok": False, "error": "Cannot identify process command"}

        killed: list[int] = []
        my_pid = os.getpid()
        try:
            for proc_info in psutil.process_iter(["pid", "name", "cmdline"]):
                info = proc_info.info
                if info["pid"] == my_pid:
                    continue
                pname = (info.get("name") or "").lower()
                if "python" not in pname:
                    continue
                cmdline = info.get("cmdline") or []
                # Require both "-m" and the module_flag to match — avoids killing
                # unrelated python processes that happen to mention the module name.
                if "-m" not in cmdline or module_flag not in cmdline:
                    continue
                try:
                    proc_info.terminate()
                    killed.append(info["pid"])
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except Exception as e:
            return {"ok": False, "error": f"psutil scan failed: {e}"}

        # Also clean up our tracked process if it matches
        with self._lock:
            proc = self._procs.get(name)
            if proc and proc.poll() is None:
                proc.terminate()
                if proc.pid not in killed:
                    killed.append(proc.pid)

        if not killed:
            return {"ok": False, "error": f"No {name} process found"}
        return {"ok": True, "killed": killed}

    def status(self, name: str) -> dict:
        """Get the current status of a named process."""
        proc = self._procs.get(name)
        if not proc:
            return {"status": "idle", "pid": None, "exit_code": None}
        code = proc.poll()
        if code is None:
            return {"status": "running", "pid": proc.pid, "exit_code": None}
        if code == 0:
            return {"status": "finished", "pid": proc.pid, "exit_code": 0}
        return {"status": "error", "pid": proc.pid, "exit_code": code}

    def get_logs(self, name: str, last_n: int = 50) -> list[str]:
        """Return the last N log lines for a process."""
        buf = self._logs.get(name, deque())
        return list(buf)[-last_n:]
