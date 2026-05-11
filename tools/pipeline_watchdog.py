"""One-shot pipeline launcher with crash logging.

Pre-2026-05-12 this wrapper auto-respawned the pipeline up to 20 times
with a 30 s backoff. That defeated the discipline contract: the pipeline
should NEVER start without an explicit human (or Claude) launch. The
auto-respawn behaviour is what let Ford v Ferrari run 10 corrupt encodes
in 9 days — every kill of the supervisor just made the watchdog spin
it back up.

This file now does ONE thing: launch the pipeline command once, log the
exit code + wall time to ``F:\\AV1_Staging\\watchdog.log``, then exit.
No loop, no backoff, no respawn. If you want the pipeline running, you
launch it; if it crashes you decide whether to launch it again.

Usage:
    uv run python -m tools.pipeline_watchdog [--log PATH] [-- CMD ARGS...]

The ``--`` separator lets you pass arbitrary commands; default is
``uv run python -m pipeline --resume``.
"""
from __future__ import annotations

import argparse
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


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.pipeline_watchdog",
        description="Run the pipeline once and log the exit. Never auto-restarts.",
    )
    parser.add_argument(
        "--log",
        type=str,
        default=r"F:\AV1_Staging\watchdog.log",
        help="Event log path (default F:\\AV1_Staging\\watchdog.log)",
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

    # Stop-flag — if this file exists the wrapper refuses to launch. This is
    # the inverse safety: even if something automated tries to invoke the
    # wrapper, the stop flag halts it without launching anything.
    stop_flag = Path(r"F:\AV1_Staging\control\stop")
    if stop_flag.exists():
        _log(f"stop flag present at {stop_flag} — refusing to launch", log_path)
        return 0

    cmd = args.command or ["uv", "run", "python", "-m", "pipeline", "--resume"]
    _log(f"launch: cmd={cmd}", log_path)
    t0 = time.monotonic()
    try:
        proc = subprocess.run(cmd, check=False)
        rc = proc.returncode
    except KeyboardInterrupt:
        _log("KeyboardInterrupt", log_path)
        return 0
    except Exception as e:  # noqa: BLE001
        _log(f"launcher crashed: {e!r}", log_path)
        return 2

    wall = time.monotonic() - t0
    tag = "clean" if rc == 0 else "CRASH"
    _log(f"{tag} exit rc={rc} ran={wall:.0f}s — wrapper exiting (no auto-restart)", log_path)
    return rc if rc != 0 else 0


if __name__ == "__main__":
    sys.exit(main())
