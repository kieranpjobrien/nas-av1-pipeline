"""Cross-process exclusive lock for serialising NAS gap-fill operations.

Three tools currently drive SSH + Docker + mkvmerge against the Synology
NAS: ``pipeline.gap_filler``, ``pipeline.orchestrator`` (heavy_worker) and
``tools.mux_external_subs``. When two run concurrently, Synology disk I/O
saturates and mkvmerge returns rc=137 (OOM-kill / SIGKILL from the kernel
memory pressure guard). The ``full_gamut`` AV1 encode path is SMB-bound
and does NOT conflict — only the "heavy mkvmerge on NAS disk" class does.

This module provides a single primitive — :func:`gap_fill_lock` — that
serialises that class to one holder at a time.

Reuses the pattern from :func:`tools.report_lock._file_lock`:
atomic-create via ``os.open(..., O_CREAT | O_EXCL)``, PID-alive + age-based
stale detection, context manager interface. Differences from ``_file_lock``:

* Writes pid + role + timestamp to the lock file as JSON so an external
  reader (or a human cat'ing the file) can see who holds it.
* Logs INFO on acquire/release and once-on-block, never per-poll.
* Honours an optional ``shutdown`` :class:`threading.Event` so the waiter
  exits cleanly on Ctrl-C.
* Refuses recursive acquire from the same process+thread — gap-fill
  operations are not meant to nest, and silent recursion would hide a
  logic bug.

Typical usage::

    from pipeline.gap_fill_lock import gap_fill_lock, GapFillLockTimeout

    with gap_fill_lock(role="gap_filler", timeout=600.0, shutdown=event):
        result = remote_strip_and_mux(...)
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

from paths import STAGING_DIR

_LOG = logging.getLogger(__name__)

# Lock file path. Env-var overridable so tests (and alternate staging
# layouts) can point elsewhere without patching.
_DEFAULT_LOCK_PATH = Path(os.environ.get(
    "GAP_FILL_LOCK_PATH",
    str(STAGING_DIR / "control" / "gap_fill.lock"),
))

# Fallback age cutoff used when PID-alive probe is unavailable/inconclusive.
# 10 minutes matches the largest legitimate mkvmerge hold we've measured
# (huge Blu-ray remux rewrites). Anything older is almost certainly a
# crashed owner that never cleaned up.
_STALE_AGE_SECS = 600.0

# Tracks (process, thread) pairs that currently hold the lock, so we can
# reject recursive acquire from the same thread. Keyed by a tuple of
# (os.getpid(), threading.get_ident()) to the role that took it — purely
# in-process bookkeeping; the cross-process guarantee comes from the file.
_in_process_holders: dict[tuple[int, int], str] = {}
_in_process_lock = threading.Lock()


class GapFillLockTimeout(TimeoutError):
    """Raised when :func:`gap_fill_lock` cannot acquire within ``timeout``.

    Also raised (with message ``"shutdown"``) when a supplied
    :class:`threading.Event` is set while we're still waiting — the waiter
    must abort quickly so the outer worker loop can exit.
    """


class GapFillLockReentrantError(RuntimeError):
    """Raised when the same (process, thread) tries to nest gap_fill_lock.

    Recursive acquire is almost always a bug: it means one gap-fill op is
    invoking another, which would have deadlocked if the lock were held
    by a different process. Making it loud (rather than silently
    re-entering) surfaces the bug before it hides in production.
    """


def _pid_alive(pid: int) -> bool:
    """True if a process with this PID is currently running.

    Prefers ``psutil.pid_exists`` when available (project already depends on
    psutil via :mod:`pipeline.process_registry`). Falls back to ``os.kill(pid, 0)``
    — a POSIX liveness probe that Python maps to ``OpenProcess`` on Windows.
    Fallback is imperfect because PIDs can be recycled, but it's adequate
    as a second line of defence behind the age cutoff.
    """
    if pid <= 0:
        return False
    try:
        import psutil
        return psutil.pid_exists(pid)
    except ImportError:
        pass
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_lock_metadata(lock_path: Path) -> dict:
    """Return the JSON body of the lock file, or {} on any read/parse error.

    Callers treat empty dict as "no useful info", which triggers the age-based
    stale check rather than the (faster) PID-alive probe.
    """
    try:
        with open(lock_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        pass
    return {}


def _write_lock_metadata(fd: int, role: str) -> None:
    """Write pid + role + acquisition timestamp to the (already-open) lock fd.

    Kept separate from the atomic-create to keep that step absolutely minimal —
    the only way ``O_CREAT | O_EXCL`` can succeed for two processes
    simultaneously is kernel bugs, so we prioritise "file exists" as the
    synchronisation signal and write metadata immediately afterwards.
    """
    body = json.dumps({
        "pid": os.getpid(),
        "role": role,
        "acquired_at": time.time(),
    }).encode("utf-8")
    os.write(fd, body)


def _try_break_stale(lock_path: Path) -> bool:
    """Attempt to remove the lock file if its owner is dead/stale.

    Returns True if the lock file was removed (caller may retry the acquire),
    False if the lock appears legitimately held.

    Priority order:
      1. PID-alive probe — fastest and most accurate. If the stored PID is
         gone, the owner crashed without cleanup; break immediately.
      2. Age cutoff — fallback for when the metadata is missing/corrupt or
         psutil is unavailable. ``_STALE_AGE_SECS`` is tuned wider than
         realistic hold time (10 min vs. typical 30s-5min).
    """
    meta = _read_lock_metadata(lock_path)
    pid = int(meta.get("pid", 0))

    # (1) PID check — if we have a pid and it's dead, break.
    if pid > 0 and not _pid_alive(pid):
        try:
            os.remove(lock_path)
            _LOG.warning(
                "gap_fill_lock: breaking stale lock (owner pid=%d role=%s is dead)",
                pid,
                meta.get("role", "<unknown>"),
            )
            return True
        except FileNotFoundError:
            # Someone else broke it first — that's fine, retry acquire.
            return True
        except OSError as e:
            _LOG.warning("gap_fill_lock: failed to remove stale lock: %s", e)
            return False

    # (2) Age cutoff.
    try:
        age = time.time() - os.path.getmtime(lock_path)
    except OSError:
        return False
    if age > _STALE_AGE_SECS:
        try:
            os.remove(lock_path)
            _LOG.warning(
                "gap_fill_lock: breaking stale lock by age (owner pid=%s role=%s age=%.0fs > %.0fs)",
                meta.get("pid"),
                meta.get("role", "<unknown>"),
                age,
                _STALE_AGE_SECS,
            )
            return True
        except FileNotFoundError:
            return True
        except OSError as e:
            _LOG.warning("gap_fill_lock: failed to remove age-stale lock: %s", e)
            return False

    return False


@contextmanager
def gap_fill_lock(
    role: str,
    timeout: float = 600.0,
    shutdown: Optional[threading.Event] = None,
    lock_path: Optional[Path] = None,
) -> Iterator[None]:
    """Acquire the cross-process gap-fill lock.

    Args:
        role: Short human label for the caller (e.g. ``"gap_filler"``,
            ``"mux_external_subs"``). Written to the lock file and logs.
        timeout: Maximum wait before raising :class:`GapFillLockTimeout`.
            600s (10 min) is the "something's truly wrong" threshold —
            legitimate mkvmerge holds are 30s-5min.
        shutdown: Optional event. If set while we're waiting, the waiter
            aborts with ``GapFillLockTimeout("shutdown")``.
        lock_path: Override the lock file location. Primarily for tests.

    Raises:
        GapFillLockTimeout: Timeout elapsed or shutdown event fired.
        GapFillLockReentrantError: Same thread is already holding the lock.

    Yields:
        None — the lock is held for the duration of the ``with`` block.
    """
    path = lock_path if lock_path is not None else _DEFAULT_LOCK_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_str = str(path)

    holder_key = (os.getpid(), threading.get_ident())
    with _in_process_lock:
        if holder_key in _in_process_holders:
            raise GapFillLockReentrantError(
                f"gap_fill_lock already held by this thread "
                f"(existing role={_in_process_holders[holder_key]!r}, "
                f"attempted role={role!r})"
            )

    deadline = time.monotonic() + timeout
    acquired_at: Optional[float] = None
    logged_blocked = False

    while True:
        try:
            fd = os.open(lock_str, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            # Stale-detect first — a dead holder should not block anyone.
            if _try_break_stale(path):
                continue

            if shutdown is not None and shutdown.is_set():
                raise GapFillLockTimeout("shutdown")

            if not logged_blocked:
                meta = _read_lock_metadata(path)
                _LOG.info(
                    "gap_fill_lock: %s waiting — held by pid=%s role=%s",
                    role,
                    meta.get("pid", "?"),
                    meta.get("role", "?"),
                )
                logged_blocked = True

            if time.monotonic() >= deadline:
                raise GapFillLockTimeout(
                    f"gap_fill_lock: {role} could not acquire within {timeout:.0f}s "
                    f"(lock={lock_str})"
                )

            # Poll every 1s. Short enough to be responsive to shutdown; long
            # enough not to hammer the filesystem.
            if shutdown is not None:
                if shutdown.wait(timeout=1.0):
                    raise GapFillLockTimeout("shutdown")
            else:
                time.sleep(1.0)
            continue

        # Atomic create succeeded — write metadata and bail out of the loop.
        try:
            _write_lock_metadata(fd, role)
        finally:
            os.close(fd)
        acquired_at = time.monotonic()
        with _in_process_lock:
            _in_process_holders[holder_key] = role
        _LOG.info("gap_fill_lock: %s acquired (pid=%d)", role, os.getpid())
        break

    try:
        yield
    finally:
        with _in_process_lock:
            _in_process_holders.pop(holder_key, None)
        held_for = time.monotonic() - (acquired_at or time.monotonic())
        try:
            os.remove(lock_str)
        except OSError:
            # If the lock file is already gone (e.g. another tool broke it
            # after judging us stale), that's a degenerate state but not
            # worth crashing the caller over — the work is done either way.
            _LOG.warning(
                "gap_fill_lock: lock file already gone at release (role=%s, held=%.1fs)",
                role,
                held_for,
            )
        else:
            _LOG.info(
                "gap_fill_lock: %s released (held %.1fs)", role, held_for
            )
