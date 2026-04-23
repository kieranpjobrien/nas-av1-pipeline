"""Persistent process registry for detecting and reaping ghost python processes.

Addresses the 2026-04-24 incident where ``strip_tags`` and scanner processes
from previous sessions were still hammering the NAS after their parent shell
had been closed. Each pipeline entry point registers itself with a role
name on startup; on next session start a reconcile() call removes entries
whose PID is dead (the process crashed or was killed) and can identify
entries whose PID has been recycled by a different process since the
entry was written.

Storage is a single JSON file on disk, written atomically via tmp +
``os.replace`` and guarded by the same file-lock primitive used for
``media_report.json`` (``tools.report_lock._file_lock``).

Entry shape::

    {
      "role": "scanner",
      "pid": 12345,
      "cmd": ["python", "-m", "tools.scanner"],
      "started_at": 1714012345.678,
      "create_time": 1714012344.901,   # psutil.Process.create_time(), used to guard PID recycling
      "last_heartbeat": 1714012355.678,
    }
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from types import TracebackType
from typing import Any

import psutil

from tools.report_lock import _file_lock

_LOG = logging.getLogger(__name__)


def _read_entries(path: Path) -> list[dict[str, Any]]:
    """Read the JSON registry at ``path``. Missing/corrupt files return []."""
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError, OSError) as e:
        _LOG.warning(
            "process registry %s unreadable (%s: %s); treating as empty",
            path,
            type(e).__name__,
            str(e)[:120],
        )
        return []
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for entry in data:
        if isinstance(entry, dict):
            out.append(entry)
    return out


def _write_entries(path: Path, entries: list[dict[str, Any]]) -> None:
    """Atomic tmp + os.replace write of the registry."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2)
    os.replace(tmp, str(path))


def _current_create_time(pid: int) -> float | None:
    """Return psutil.Process(pid).create_time(), or None if the process doesn't exist."""
    try:
        return float(psutil.Process(pid).create_time())
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def _pid_matches_entry(entry: dict[str, Any]) -> bool:
    """True if the entry's PID is alive AND its create_time still matches.

    Guards against PID recycling: on Linux and Windows the OS reuses PIDs
    of dead processes, and a registry entry written two weeks ago for
    PID 12345 might now point at an unrelated shell session.
    """
    pid = int(entry.get("pid", 0))
    if pid <= 0 or not psutil.pid_exists(pid):
        return False
    stored_ctime = entry.get("create_time")
    if stored_ctime is None:
        # Old-style entry without a create_time — fall back to pid_exists only.
        return True
    current = _current_create_time(pid)
    if current is None:
        return False
    # Compare with a small tolerance; psutil returns float seconds.
    return abs(current - float(stored_ctime)) < 1.0


class _Registration:
    """Internal handle returned by ``ProcessRegistry.register``.

    Acts as a context manager. On enter, inserts the entry and starts a
    background heartbeat thread. On exit, signals the heartbeat to stop,
    joins it, and removes the entry.
    """

    def __init__(self, registry: ProcessRegistry, role: str, cmd: list[str]) -> None:
        self._registry = registry
        self._role = role
        self._cmd = list(cmd)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._pid = os.getpid()

    def __enter__(self) -> _Registration:
        self._registry._insert(self._role, self._pid, self._cmd)
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"process_registry_heartbeat[{self._role}]",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._registry._heartbeat_secs + 2)
        self._registry._remove(self._role, self._pid)

    def _heartbeat_loop(self) -> None:
        """Periodically bump ``last_heartbeat`` until stopped."""
        interval = max(1, self._registry._heartbeat_secs)
        while not self._stop.wait(timeout=interval):
            try:
                self._registry._touch_heartbeat(self._role, self._pid)
            except Exception as e:  # noqa: BLE001 - heartbeat must never crash
                _LOG.warning("heartbeat update failed for role=%s pid=%d: %s", self._role, self._pid, e)


class ProcessRegistry:
    """Persistent registry of running pipeline processes.

    Guards against two failure modes:
      * Ghost processes from previous sessions still holding resources.
      * Duplicate starts of the same role (e.g. two scanners racing over
        the media_report lock).
    """

    def __init__(self, path: Path, heartbeat_secs: int = 30) -> None:
        """Initialise a registry backed by a JSON file at ``path``.

        Args:
            path: JSON file holding the registry. Will be created on demand.
            heartbeat_secs: Interval between background heartbeat writes.

        Raises:
            ValueError: If ``heartbeat_secs`` is not positive.
        """
        if heartbeat_secs <= 0:
            raise ValueError("heartbeat_secs must be > 0")
        self._path = Path(path)
        self._heartbeat_secs = heartbeat_secs
        # Lock file lives next to the registry file.
        self._lock_path = Path(str(self._path) + ".lock")

    # ------------------------------------------------------------------
    # Internal mutations (each acquires the file lock)
    # ------------------------------------------------------------------

    def _insert(self, role: str, pid: int, cmd: list[str]) -> None:
        """Add an entry for (role, pid, cmd). Caller must have reconciled first."""
        with _file_lock(self._lock_path):
            entries = _read_entries(self._path)
            # Reject if another live entry exists for this role.
            for e in entries:
                if e.get("role") == role and _pid_matches_entry(e):
                    raise RuntimeError(
                        f"process registry: role '{role}' already active with pid={e.get('pid')}"
                    )
            # Drop any stale/dead entries for this role.
            entries = [e for e in entries if e.get("role") != role]
            now = time.time()
            ctime = _current_create_time(pid)
            entries.append(
                {
                    "role": role,
                    "pid": pid,
                    "cmd": cmd,
                    "started_at": now,
                    "create_time": ctime,
                    "last_heartbeat": now,
                }
            )
            _write_entries(self._path, entries)

    def _remove(self, role: str, pid: int) -> None:
        """Remove the entry for (role, pid) if present."""
        with _file_lock(self._lock_path):
            entries = _read_entries(self._path)
            kept = [e for e in entries if not (e.get("role") == role and int(e.get("pid", 0)) == pid)]
            if len(kept) != len(entries):
                _write_entries(self._path, kept)

    def _touch_heartbeat(self, role: str, pid: int) -> None:
        """Update ``last_heartbeat`` on our entry."""
        with _file_lock(self._lock_path):
            entries = _read_entries(self._path)
            changed = False
            for e in entries:
                if e.get("role") == role and int(e.get("pid", 0)) == pid:
                    e["last_heartbeat"] = time.time()
                    changed = True
                    break
            if changed:
                _write_entries(self._path, entries)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def reconcile(self) -> list[str]:
        """Drop entries whose PID is dead or has been recycled.

        Compares each entry's stored ``create_time`` against the current
        ``psutil.Process.create_time()`` for that PID. Any mismatch means
        the PID now belongs to a different process; the entry is removed.

        Returns:
            List of role names whose entries were removed.
        """
        removed: list[str] = []
        with _file_lock(self._lock_path):
            entries = _read_entries(self._path)
            kept: list[dict[str, Any]] = []
            for e in entries:
                if _pid_matches_entry(e):
                    kept.append(e)
                else:
                    role = str(e.get("role", "<unknown>"))
                    removed.append(role)
                    _LOG.info(
                        "process registry: removing stale entry role=%s pid=%s",
                        role,
                        e.get("pid"),
                    )
            if removed:
                _write_entries(self._path, kept)
        return removed

    def register(self, role: str, cmd: list[str]) -> _Registration:
        """Return a context manager that registers this process for ``role``.

        On ``__enter__``:
          * Runs ``reconcile`` implicitly by way of ``_pid_matches_entry``
            on the duplicate-check path.
          * Raises ``RuntimeError`` if another live process is already
            registered for this role.
          * Adds an entry with the current PID, cmd, and timestamps.
          * Spawns a daemon thread that refreshes ``last_heartbeat`` on
            an interval of ``heartbeat_secs``.

        On ``__exit__``:
          * Stops the heartbeat thread.
          * Removes the entry.

        Args:
            role: Logical name (e.g. "scanner", "orchestrator.heavy_worker").
            cmd: Command-line args of the current process, for observability.

        Returns:
            A context manager object. The value yielded is unused.
        """
        return _Registration(self, role, cmd)

    def list_active(self) -> list[dict[str, Any]]:
        """Return the registry contents (copy; callers may not mutate)."""
        with _file_lock(self._lock_path):
            return [dict(e) for e in _read_entries(self._path)]

    def kill_stale(self, max_age_secs: int = 120) -> list[int]:
        """Terminate entries whose last_heartbeat is older than ``max_age_secs``.

        Uses ``psutil.Process.terminate()`` (SIGTERM on POSIX, TerminateProcess
        on Windows). Does NOT escalate to ``kill()`` — that's a decision for
        the caller; we want to give the process a chance to clean up.

        Args:
            max_age_secs: Maximum acceptable heartbeat age. Entries older than
                this are terminated and removed.

        Returns:
            List of PIDs that were terminated.
        """
        killed: list[int] = []
        now = time.time()
        with _file_lock(self._lock_path):
            entries = _read_entries(self._path)
            kept: list[dict[str, Any]] = []
            for e in entries:
                last_hb = float(e.get("last_heartbeat", 0.0))
                age = now - last_hb
                if age > max_age_secs:
                    pid = int(e.get("pid", 0))
                    if pid > 0 and psutil.pid_exists(pid):
                        try:
                            psutil.Process(pid).terminate()
                            killed.append(pid)
                            _LOG.warning(
                                "process registry: terminated stale process role=%s pid=%d age=%.1fs",
                                e.get("role"),
                                pid,
                                age,
                            )
                        except (psutil.NoSuchProcess, psutil.AccessDenied) as ex:
                            _LOG.warning(
                                "process registry: could not terminate pid=%d: %s",
                                pid,
                                ex,
                            )
                    # Drop the entry whether or not terminate() succeeded.
                else:
                    kept.append(e)
            if len(kept) != len(entries):
                _write_entries(self._path, kept)
        return killed
