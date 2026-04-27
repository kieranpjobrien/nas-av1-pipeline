"""Watchdog wrapper that keeps the pipeline alive across Python crashes.

The pipeline can be killed by a Python segfault (we saw a 0xc0000005 access
violation in python314.dll on 2026-04-27 22:47 — likely a faster-whisper /
CUDA / ffmpeg interaction) without writing any final log line. A naked
``uv run python -m pipeline --resume`` invocation just dies silently and the
encode queue stops draining for hours. This wrapper loops on that command,
detects abnormal exit, backs off, and relaunches.

Usage:
    uv run python -m tools.pipeline_watchdog [--max-restarts N] [--backoff-secs S]
        [--log F:\\AV1_Staging\\watchdog.log]

Defaults: max-restarts=20 over the lifetime of this wrapper; 30s backoff
between restarts. Clean exits (exit code 0, or KeyboardInterrupt /
SIGTERM) terminate the wrapper too — restarts only on crash.

Logs each launch + exit to both stderr AND ``--log`` (default
``F:\\AV1_Staging\\watchdog.log``) so a sweep of recent crashes is easy to
audit later.
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def _log(msg: str, log_path: Path) -> None:
    line = f"{datetime.now().isoformat(timespec='seconds')}  {msg}"
    print(line, file=sys.stderr, flush=True)
    try:
        with log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass  # log file unwritable — stderr still has it


def _is_clean_exit(returncode: int) -> bool:
    """Return True if the exit code looks like a deliberate shutdown.

    On Windows, signal-based termination produces large negative codes
    (e.g. -SIGTERM = -15). Code 0 = success. Anything else = abnormal.
    """
    if returncode == 0:
        return True
    if sys.platform != "win32":
        # Caller used Ctrl+C or similar — let them stop.
        if returncode in (-signal.SIGINT, -signal.SIGTERM):
            return True
    else:
        # Windows: STATUS_CONTROL_C_EXIT is 0xC000013A (negative when sign-extended)
        if returncode in (-1073741510, 1073741510, 3221225786):
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.pipeline_watchdog",
        description="Loop on the pipeline command, restart on crash.",
    )
    parser.add_argument(
        "--max-restarts",
        type=int,
        default=20,
        help="Give up after this many crashes in a row (default 20)",
    )
    parser.add_argument(
        "--backoff-secs",
        type=float,
        default=30.0,
        help="Pause between crash and restart (default 30s)",
    )
    parser.add_argument(
        "--log",
        type=str,
        default=r"F:\AV1_Staging\watchdog.log",
        help="Watchdog event log path",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command to run (default: uv run python -m pipeline --resume)",
    )
    args = parser.parse_args()

    log_path = Path(args.log)
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass

    cmd = args.command or ["uv", "run", "python", "-m", "pipeline", "--resume"]

    _log(f"watchdog start: cmd={cmd}", log_path)
    crashes = 0
    while True:
        launch_t0 = time.monotonic()
        _log(f"launching pipeline (crash count={crashes}/{args.max_restarts})", log_path)
        try:
            proc = subprocess.run(cmd, check=False)
            rc = proc.returncode
        except KeyboardInterrupt:
            _log("KeyboardInterrupt — clean watchdog exit", log_path)
            return 0
        except Exception as e:  # noqa: BLE001
            _log(f"watchdog itself crashed: {e!r}", log_path)
            return 2

        wall = time.monotonic() - launch_t0
        if _is_clean_exit(rc):
            _log(f"pipeline exited cleanly (rc={rc}, ran {wall:.0f}s) — watchdog stopping", log_path)
            return 0

        crashes += 1
        _log(
            f"pipeline crashed (rc={rc}, ran {wall:.0f}s) — backoff {args.backoff_secs:.0f}s "
            f"then restart [{crashes}/{args.max_restarts}]",
            log_path,
        )
        if crashes >= args.max_restarts:
            _log("max restarts reached, watchdog giving up", log_path)
            return 1
        time.sleep(args.backoff_secs)


if __name__ == "__main__":
    sys.exit(main())
