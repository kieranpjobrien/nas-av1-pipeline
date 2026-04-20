"""File-based lock for safe concurrent access to media_report.json.

Multiple tools (scanner, language detection, TMDb enrichment, pipeline)
can read and write the report. This lock prevents lost writes when two
processes try to update it simultaneously.

Usage:
    from tools.report_lock import read_report, write_report

    report = read_report()        # reads under lock
    # ... modify report ...
    write_report(report)          # writes atomically under lock
"""

import json
import os
import time
from contextlib import contextmanager
from pathlib import Path

from paths import MEDIA_REPORT, MEDIA_REPORT_LOCK


def _pid_alive(pid: int) -> bool:
    """True if a process with this PID is currently running. Best-effort, cross-platform."""
    if pid <= 0:
        return False
    try:
        import psutil
        return psutil.pid_exists(pid)
    except ImportError:
        pass
    # Fallback without psutil — os.kill with signal 0 is a POSIX probe; on Windows
    # Python maps it to OpenProcess with no access check, which returns success on
    # any running PID. It's not perfect (PIDs can be recycled) but it's a good
    # cheap liveness check.
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


@contextmanager
def _file_lock(lock_path: Path, timeout: float = 120.0):
    """Simple file-based mutex. Creates a lock file, yields, removes it.

    Uses atomic create (os.open with O_CREAT|O_EXCL) so two processes
    can't both acquire the lock.

    Stale detection (in priority order):
      1. If the PID stored in the lock file is no longer running → break immediately.
         Fast, accurate, handles crashed processes that didn't clean up.
      2. Else if the lock is older than 60 seconds → break it.
         Fallback for when PID probe fails or the owner is hung.
    """
    lock_str = str(lock_path)
    deadline = time.monotonic() + timeout

    while True:
        try:
            fd = os.open(lock_str, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            break
        except FileExistsError:
            # (1) PID-alive check — fastest path for crashed owners.
            try:
                with open(lock_str, "r") as f:
                    owner_pid = int(f.read().strip() or "0")
                if not _pid_alive(owner_pid):
                    os.remove(lock_str)
                    continue
            except (OSError, ValueError):
                pass

            # (2) Age fallback — 60s is long enough for any legitimate write-and-rename
            # cycle (even a 50 MB JSON) but short enough to self-heal quickly.
            try:
                age = time.time() - os.path.getmtime(lock_str)
                if age > 60:
                    os.remove(lock_str)
                    continue
            except OSError:
                pass

            if time.monotonic() > deadline:
                raise TimeoutError(f"Could not acquire lock {lock_str} within {timeout}s")
            time.sleep(0.5)

    try:
        yield
    finally:
        try:
            os.remove(lock_str)
        except OSError:
            pass


def read_report() -> dict:
    """Read media_report.json under lock."""
    with _file_lock(MEDIA_REPORT_LOCK):
        with open(MEDIA_REPORT, "r", encoding="utf-8") as f:
            return json.load(f)


def write_report(report: dict) -> None:
    """Write media_report.json atomically under lock."""
    with _file_lock(MEDIA_REPORT_LOCK):
        tmp = str(MEDIA_REPORT) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        os.replace(tmp, str(MEDIA_REPORT))


def patch_report(fn) -> None:
    """Read report, apply fn(report) in-place, write back. All under one lock."""
    with _file_lock(MEDIA_REPORT_LOCK):
        with open(MEDIA_REPORT, "r", encoding="utf-8") as f:
            report = json.load(f)
        fn(report)
        tmp = str(MEDIA_REPORT) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        os.replace(tmp, str(MEDIA_REPORT))
