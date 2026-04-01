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


@contextmanager
def _file_lock(lock_path: Path, timeout: float = 120.0):
    """Simple file-based mutex. Creates a lock file, yields, removes it.

    Uses atomic create (os.open with O_CREAT|O_EXCL) so two processes
    can't both acquire the lock. Stale locks older than 10 minutes are
    broken automatically.
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
            # Check for stale lock (older than 10 minutes)
            try:
                age = time.time() - os.path.getmtime(lock_str)
                if age > 600:
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
