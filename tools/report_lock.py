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


_EMPTY_REPORT = {"files": [], "scan_date": "", "total_files": 0}


def _last_good_path() -> Path:
    """Path to the rolling backup of the last successfully-written report."""
    return Path(str(MEDIA_REPORT) + ".last_good")


class ReportCorruptError(Exception):
    """Raised when media_report.json is unreadable AND no backup is available.

    The cascade-of-loss bug (2026-04-29): a single corrupt write made
    ``_read_or_empty`` return an empty skeleton; subsequent ``patch_report``
    calls then patched into that empty dict and wrote it back — wiping
    every file from the report in a single round-trip. Now corruption
    fails LOUD when no backup is present, so callers see the failure
    instead of silently overwriting good data with empty.
    """


def _try_load(path: Path) -> dict | None:
    """Best-effort JSON load. Returns the dict on success, None on any failure.

    Validates structural sanity (must be a dict with a ``files`` list)
    so partially-truncated JSON that happens to parse but isn't a real
    report doesn't get treated as good.
    """
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError, OSError):
        return None
    if not isinstance(data, dict):
        return None
    if "files" not in data or not isinstance(data["files"], list):
        return None
    return data


def _read_or_empty() -> dict:
    """Read media_report.json, falling back to ``.last_good`` if the primary
    is missing or corrupt. Raises :class:`ReportCorruptError` only when both
    are unusable.

    Why the rollback: the previous behaviour was to silently return an
    empty skeleton on any read failure. That worked the first time
    corruption happened, but the next ``patch_report`` cycle then wrote
    the empty dict back to disk — destroying every file entry in one
    atomic step. The 2026-04-29 incident wiped 8,679 files this way.
    """
    primary = _try_load(MEDIA_REPORT)
    if primary is not None:
        return primary

    # Primary is missing or invalid. Try the backup before giving up.
    backup = _last_good_path()
    fallback = _try_load(backup)
    if fallback is not None:
        import logging
        logging.warning(
            f"media_report.json unreadable; restored from {backup.name} "
            f"({len(fallback.get('files', []))} files)"
        )
        return fallback

    # Genuinely fresh state — nothing on disk yet.
    if not Path(MEDIA_REPORT).exists() and not backup.exists():
        return dict(_EMPTY_REPORT)

    # Otherwise: corrupt + no recoverable backup. Fail loud, not silent.
    raise ReportCorruptError(
        f"media_report.json is unreadable and {backup.name} has no usable "
        f"backup. Refusing to return an empty report (would cascade-wipe "
        f"on the next write). Investigate before continuing."
    )


def _atomic_write_with_backup(path: Path, report: dict) -> None:
    """Atomically write ``report`` to ``path`` AND maintain a ``.last_good`` copy.

    Sequence:
      1. Validate the in-memory report (must be a dict with ``files`` list)
         to refuse to write a known-bad shape.
      2. Write to ``<path>.tmp`` and fsync.
      3. If ``<path>`` is currently valid, copy it to ``<path>.last_good``
         BEFORE replacing — so the backup tracks the previous good state.
      4. ``os.replace`` the tmp into place atomically.

    Step 3 is the key change: it gives ``_read_or_empty`` something to fall
    back to if the next read finds corruption. Without this, the user has
    no recovery point.
    """
    if not isinstance(report, dict):
        raise ValueError(
            f"refusing to write malformed report (got {type(report).__name__}, "
            f"expected dict): would cascade-wipe on next read"
        )
    if not isinstance(report.get("files"), list):
        raise ValueError(
            f"refusing to write malformed report (files key is "
            f"{type(report.get('files')).__name__}, expected list)"
        )

    tmp = str(path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        f.flush()
        try:
            os.fsync(f.fileno())
        except OSError:
            # fsync isn't available everywhere; not fatal
            pass

    # Promote current → backup BEFORE replacing, only if current is valid.
    # Otherwise we'd just back up the corruption.
    if Path(path).exists():
        current = _try_load(Path(path))
        if current is not None:
            try:
                # Use a temp+rename to keep .last_good's update atomic too
                backup = _last_good_path()
                backup_tmp = str(backup) + ".tmp"
                with open(backup_tmp, "w", encoding="utf-8") as bf:
                    json.dump(current, bf, indent=2, ensure_ascii=False)
                _replace_with_retry(backup_tmp, str(backup))
            except OSError:
                # Backup write failure is non-fatal — primary write proceeds
                pass

    _replace_with_retry(tmp, str(path))


def _replace_with_retry(src: str, dst: str, attempts: int = 6, base_delay: float = 0.05) -> None:
    """``os.replace`` with retry on Windows sharing violations.

    The 2026-04-29 lang-detect pass produced ~3 ``[WinError 5] Access is
    denied`` failures per 1000 writes — Defender scanning the file, the
    dashboard cache re-reading right at swap time, or another reader briefly
    holding a handle. Each failure dropped one file's detection update. Retry
    with backoff handles all three classes (transient handle held elsewhere)
    without pretending every error is transient: after exhausting attempts
    we re-raise so genuine permission problems surface loud.
    """
    last_err: OSError | None = None
    for i in range(attempts):
        try:
            os.replace(src, dst)
            return
        except PermissionError as e:
            last_err = e
            time.sleep(base_delay * (2 ** i))
    if last_err is not None:
        raise last_err


def read_report() -> dict:
    """Read media_report.json under lock.

    Falls back to ``.last_good`` if the primary is corrupt. Raises
    :class:`ReportCorruptError` if neither is recoverable.
    """
    with _file_lock(MEDIA_REPORT_LOCK):
        return _read_or_empty()


def write_report(report: dict) -> None:
    """Write media_report.json atomically under lock.

    Maintains a ``.last_good`` backup of the prior good state so a
    subsequent corruption can be recovered transparently.
    """
    with _file_lock(MEDIA_REPORT_LOCK):
        _atomic_write_with_backup(MEDIA_REPORT, report)


def patch_report(fn) -> None:
    """Read report, apply fn(report) in-place, write back. All under one lock."""
    with _file_lock(MEDIA_REPORT_LOCK):
        report = _read_or_empty()
        fn(report)
        _atomic_write_with_backup(MEDIA_REPORT, report)
