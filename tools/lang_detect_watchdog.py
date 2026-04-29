"""Watchdog wrapper for ``tools.detect_languages`` runs.

The 2026-04-29 morning whisper batch died silently after ~19 minutes. The
state file said "running: true" for the next 9.5 hours while no actual
progress was being made — no log activity, no progress-file mtime
updates, just a dead process that the dashboard still reported as live.

This wrapper protects against that:

  * Loops on the inner command. On non-zero exit, sleeps and retries.
  * Wallclock-stall detector: if ``lang_detect_state.json`` mtime hasn't
    advanced for ``--stall-secs`` seconds AND the inner process is still
    alive, it's hung. Kill it and restart.
  * Stops only on (a) a clean exit (rc=0) OR (b) max-restarts reached.
  * Logs every launch + exit + stall to both stderr and a watchdog log
    so we can audit silent deaths after the fact.

Usage:
    uv run python -m tools.lang_detect_watchdog \\
        --max-restarts 5 \\
        --stall-secs 600 \\
        --log F:\\AV1_Staging\\lang_detect_watchdog.log \\
        -- --whisper --apply
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


def _log(msg: str, log_path: Path | None = None) -> None:
    line = f"{datetime.now().isoformat(timespec='seconds')}  {msg}"
    print(line, file=sys.stderr, flush=True)
    if log_path is not None:
        try:
            with log_path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass


def _state_mtime(state_path: Path) -> float | None:
    """Return mtime of the progress state file, or None if missing."""
    try:
        return state_path.stat().st_mtime
    except OSError:
        return None


def _is_clean_exit(returncode: int) -> bool:
    """Return True if the exit code looks like deliberate shutdown."""
    if returncode == 0:
        return True
    if sys.platform == "win32":
        # STATUS_CONTROL_C_EXIT and friends
        if returncode in (-1073741510, 1073741510, 3221225786):
            return True
    else:
        if returncode in (-signal.SIGINT, -signal.SIGTERM):
            return True
    return False


def _terminate_tree(proc: subprocess.Popen) -> None:
    """Best-effort kill of a process and its descendants (Windows-aware)."""
    if proc.poll() is not None:
        return
    if sys.platform == "win32":
        # Use taskkill /T (tree) /F (force) — gentle kill won't take child workers
        subprocess.run(
            ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
            capture_output=True, timeout=15,
        )
    else:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.lang_detect_watchdog",
        description="Loop tools.detect_languages with crash + stall recovery.",
    )
    parser.add_argument("--max-restarts", type=int, default=10, help="cap on restarts (default 10)")
    parser.add_argument("--backoff-secs", type=float, default=15.0, help="pause between restarts")
    parser.add_argument(
        "--stall-secs", type=float, default=600.0,
        help="kill+restart if lang_detect_state.json mtime hasn't advanced this long (default 10 min)",
    )
    parser.add_argument(
        "--state-file", type=str,
        default=r"F:\AV1_Staging\lang_detect_state.json",
        help="progress state file to watch for stall detection",
    )
    parser.add_argument(
        "--log", type=str,
        default=r"F:\AV1_Staging\lang_detect_watchdog.log",
        help="watchdog event log",
    )
    parser.add_argument(
        "passthrough", nargs=argparse.REMAINDER,
        help="args after `--` are passed to tools.detect_languages",
    )
    args = parser.parse_args()

    log_path = Path(args.log)
    state_path = Path(args.state_file)

    # Strip the leading `--` if present (argparse REMAINDER preserves it)
    extra = args.passthrough[:]
    if extra and extra[0] == "--":
        extra = extra[1:]

    cmd = ["uv", "run", "python", "-m", "tools.detect_languages", *extra]

    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass

    _log(f"watchdog start: cmd={cmd}", log_path)

    crashes = 0
    while True:
        if crashes >= args.max_restarts:
            _log(f"max restarts ({args.max_restarts}) reached — giving up", log_path)
            return 1

        launch_t = time.monotonic()
        _log(f"launching detect_languages (attempt {crashes + 1}/{args.max_restarts})", log_path)

        # Inherit env, but ensure WHISPER_FORCE_CPU=1 so we never compete with NVENC.
        env = os.environ.copy()
        env.setdefault("WHISPER_FORCE_CPU", "1")

        # Use Popen so we can poll + kill on stall. subprocess.run won't let us.
        try:
            proc = subprocess.Popen(cmd, env=env)
        except KeyboardInterrupt:
            _log("KeyboardInterrupt before launch — clean exit", log_path)
            return 0
        except Exception as e:  # noqa: BLE001
            _log(f"failed to launch: {e!r}", log_path)
            return 2

        # Stall detector: poll mtime of state file. If unchanged for stall_secs
        # AND the process is still alive, it's hung.
        last_mtime = _state_mtime(state_path)
        last_change = time.monotonic()

        try:
            while True:
                if proc.poll() is not None:
                    break  # process exited on its own — fall through to rc check
                time.sleep(15)  # check cadence
                cur_mtime = _state_mtime(state_path)
                now = time.monotonic()
                if cur_mtime != last_mtime:
                    last_mtime = cur_mtime
                    last_change = now
                elif now - last_change >= args.stall_secs:
                    _log(
                        f"STALL: state file mtime unchanged for {int(now - last_change)}s "
                        f"(threshold {int(args.stall_secs)}s) — killing tree",
                        log_path,
                    )
                    _terminate_tree(proc)
                    proc.wait(timeout=30)
                    rc = proc.returncode
                    crashes += 1
                    _log(f"stall-kill rc={rc} after {int(now - launch_t)}s — backoff", log_path)
                    time.sleep(args.backoff_secs)
                    break  # break inner stall loop, continue outer restart loop
            else:
                rc = 0  # unreachable but keeps type checker happy
        except KeyboardInterrupt:
            _log("KeyboardInterrupt — terminating inner process", log_path)
            _terminate_tree(proc)
            proc.wait(timeout=30)
            return 0
        except Exception as e:  # noqa: BLE001
            _log(f"watchdog loop crashed: {e!r}", log_path)
            _terminate_tree(proc)
            return 2

        # Process exited (cleanly or otherwise). Check returncode.
        if proc.poll() is None:
            # Unreachable: we broke out via stall but didn't actually exit. Guard anyway.
            continue

        rc = proc.returncode
        wall = time.monotonic() - launch_t
        if _is_clean_exit(rc):
            _log(f"clean exit rc={rc} after {int(wall)}s — watchdog stopping", log_path)
            return 0

        crashes += 1
        _log(
            f"detect_languages exited rc={rc} after {int(wall)}s — "
            f"backoff {int(args.backoff_secs)}s [{crashes}/{args.max_restarts}]",
            log_path,
        )
        time.sleep(args.backoff_secs)


if __name__ == "__main__":
    sys.exit(main())
