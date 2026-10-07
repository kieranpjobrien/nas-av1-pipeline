"""Keep the dashboard up, and record what happened when it isn't.

The server died silently on five consecutive days (2026-10-03 .. 10-07). Each
time the log simply stopped after serving a normal request: no traceback, no
Windows Application crash event, 70 GB RAM free. It is being killed rather than
crashing, and the old `start /b` launcher discarded the exit code, so five
deaths produced no evidence.

This does two jobs:

  * **restart it** - the dashboard is also the pipeline's process supervisor,
    so leaving it down loses the ability to start/stop jobs;
  * **record the circumstances** - uptime at death, how long it had been up,
    system memory, and whether the pipeline was alive at the same moment. A
    pattern across several deaths is worth more than another silent restart.

**This must never be extended to restart the pipeline.** ``pipeline_watchdog``
used to auto-respawn the encoder up to 20 times with a backoff, and that is what
let Ford v Ferrari produce 10 corrupt encodes over 9 days - every kill of the
supervisor just span it back up. The encoder starts only on an explicit human
launch. Restarting the dashboard is safe by comparison because it encodes
nothing: its startup handler schedules the periodic *scanner* and nothing else,
so a restart cannot resurrect an encode. Check that still holds
(``server/__init__.py`` startup handler) before widening what this touches.

Deliberately NOT a scheduled task; hand-started like everything else here:

    Start-Process explorer.exe -ArgumentList "D:\\MediaProject\\run_watchdog.cmd"
"""

from __future__ import annotations

import datetime
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

HEALTH = "http://127.0.0.1:8000/api/health"
LAUNCHER = r"D:\MediaProject\run_dashboard.cmd"
JOURNAL = r"F:\AV1_Staging\dashboard_deaths.json"
CHECK_SECS = 60

# A heartbeat written on EVERY poll, not just on failure.
#
# 2026-10-07: the dashboard died at 06:44 and the watchdog died with it, in the
# same instant - both launched through explorer -> cmd. Because the watchdog
# only logged failures, all its log proved was "alive at 05:56, dead by 06:44",
# and because the whole cmd tree went down together the launcher's exit-code
# echo never ran either. Six deaths, no evidence.
#
# A line per minute costs nothing and makes the last line the time of death, to
# within CHECK_SECS. The NAS copy matters because it is readable from the plex
# box: whatever kills processes on this machine cannot touch a file over there,
# and the Windows firewall blocks polling the dashboard inbound from the LAN.
HEARTBEAT_LOCAL = r"F:\AV1_Staging\watchdog_heartbeat.txt"
HEARTBEAT_NAS = r"\\KieranNAS\Media\.heartbeat\windows_dashboard.txt"
# Two consecutive failures before acting: a single timeout while the server is
# busy serving the 22 MB media-report is not a death.
FAILURES_BEFORE_RESTART = 2


def healthy() -> bool:
    try:
        with urllib.request.urlopen(HEALTH, timeout=15) as r:
            return r.status == 200
    except (urllib.error.URLError, OSError, TimeoutError):
        return False


def snapshot() -> dict:
    """What was true at the moment it went down."""
    out: dict = {"when": datetime.datetime.now().isoformat(timespec="seconds")}
    try:
        ps = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "$o=Get-CimInstance Win32_OperatingSystem;"
                "$p=Get-CimInstance Win32_Process -Filter \"Name LIKE 'python%' OR Name='ffmpeg.exe'\";"
                "[pscustomobject]@{freeMB=[int]($o.FreePhysicalMemory/1KB);"
                "uptimeH=[math]::Round(((Get-Date)-$o.LastBootUpTime).TotalHours,1);"
                "procs=@($p | ForEach-Object { $_.Name })} | ConvertTo-Json -Compress",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        out.update(json.loads(ps.stdout or "{}"))
    except Exception as exc:  # noqa: BLE001 - diagnostics must never kill the watchdog
        out["snapshot_error"] = f"{type(exc).__name__}: {exc}"
    return out


def record(entry: dict) -> None:
    hist = []
    if os.path.exists(JOURNAL):
        try:
            with open(JOURNAL, encoding="utf-8") as fh:
                hist = json.load(fh)
        except Exception:  # noqa: BLE001 - a corrupt journal must not stop the restart
            hist = []
    hist.append(entry)
    tmp = JOURNAL + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(hist[-200:], fh, indent=2)
    os.replace(tmp, JOURNAL)


def beat(state: str) -> None:
    """Timestamp every poll, locally and on the NAS. Last line = time of death."""
    line = f"{datetime.datetime.now().isoformat(timespec='seconds')} dashboard={state}\n"
    for path in (HEARTBEAT_LOCAL, HEARTBEAT_NAS):
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line)
        except OSError:
            pass  # a NAS blip must never stop the watchdog watching


def main() -> int:
    print(f"watchdog up, polling {HEALTH} every {CHECK_SECS}s", flush=True)
    fails = 0
    up_since = time.time() if healthy() else None
    while True:
        if healthy():
            beat("up")
            if up_since is None:
                up_since = time.time()
            fails = 0
        else:
            beat("DOWN")
            fails += 1
            print(f"health check failed ({fails}/{FAILURES_BEFORE_RESTART})", flush=True)
            if fails >= FAILURES_BEFORE_RESTART:
                entry = snapshot()
                entry["up_for_hours"] = round((time.time() - up_since) / 3600, 2) if up_since else None
                record(entry)
                print(f"restarting dashboard; it had been up {entry['up_for_hours']}h", flush=True)
                subprocess.run(["explorer.exe", LAUNCHER], capture_output=True)
                time.sleep(45)
                up_since = time.time() if healthy() else None
                fails = 0
        time.sleep(CHECK_SECS)


if __name__ == "__main__":
    sys.exit(main())
