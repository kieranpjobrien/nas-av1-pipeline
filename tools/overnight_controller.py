"""Overnight GPU hand-off orchestrator.

The debloat (reclaim) and the AV1 pipeline both use NVENC; rule 9b forbids two
concurrent NVENC encodes (BSOD). This controller sequences them safely so the
hero-stat converts get done overnight without ever running two GPU encoders:

  1. Pause the debloat (drop pause_reclaim.json — it stops BETWEEN films).
  2. Wait until NVENC is genuinely idle (debloat's current film finished).
  3. Launch the pipeline (AV1 converts + per-file EAC-3/subs/metadata/filenames).
  4. Watch the pipeline queue until it drains (no pending/active rows).
  5. Resume the debloat (remove pause_reclaim.json).

Runs detached; logs to overnight_controller.log. Safe to leave unattended:
it never starts the pipeline until the GPU is idle, so the two never overlap.
"""
import json
import os
import subprocess
import sqlite3
import time

STAGING = "F:/AV1_Staging"
LOG = f"{STAGING}/overnight_controller.log"
PAUSE = f"{STAGING}/control/pause_reclaim.json"
STATE_DB = f"{STAGING}/pipeline_state.db"
DETACHED = 0x00000008  # DETACHED_PROCESS (Windows) — pipeline survives this controller


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    with open(LOG, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")
    print(line, flush=True)


def nvenc_util() -> int:
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=utilization.encoder", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10,
        ).stdout.strip().splitlines()
        return int(out[0]) if out else 0
    except Exception:  # noqa: BLE001
        return 0


def pipeline_running() -> bool:
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "(Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
             "Where-Object { $_.CommandLine -match '-m pipeline' } | Measure-Object).Count"],
            capture_output=True, text=True, timeout=20).stdout.strip()
        return int(out or "0") > 0
    except Exception:  # noqa: BLE001
        return False


def active_rows() -> int:
    con = sqlite3.connect(STATE_DB, timeout=10)
    try:
        return con.execute(
            "SELECT COUNT(*) FROM pipeline_files WHERE status IN "
            "('pending','qualifying','fetching','processing','uploading')").fetchone()[0]
    finally:
        con.close()


def main() -> None:
    log("=== overnight controller start ===")

    # 1. pause debloat
    os.makedirs(os.path.dirname(PAUSE), exist_ok=True)
    with open(PAUSE, "w", encoding="utf-8") as fh:
        json.dump({"type": "reclaim", "by": "overnight-controller"}, fh)
    log("paused debloat (dropped pause_reclaim.json); waiting for current film to finish")

    # 2. wait for NVENC genuinely idle (3 consecutive idle samples ~= 90s)
    idle = 0
    for _ in range(240):  # up to 2h safety cap
        nv = nvenc_util()
        idle = idle + 1 if nv <= 3 else 0
        if idle == 1 or idle >= 3:
            log(f"nvenc={nv}% idle_streak={idle}")
        if idle >= 3:
            break
        time.sleep(30)
    log("GPU idle confirmed — debloat is parked")

    # 3. launch the pipeline (detached, inherits env incl. RADARR/SONARR)
    if pipeline_running():
        log("pipeline already running — not launching a second instance")
    else:
        with open(f"{STAGING}/pipeline.log", "a", encoding="utf-8") as plog:
            p = subprocess.Popen(
                ["D:/MediaProject/.venv/Scripts/python.exe", "-m", "pipeline", "--resume"],
                cwd="D:/MediaProject", stdout=plog, stderr=subprocess.STDOUT,
                creationflags=DETACHED)
        log(f"pipeline launched pid={p.pid}")

    # 4. let it build the queue, then watch until drained
    time.sleep(150)
    drained = 0
    while drained < 6:  # ~12 min of continuous zero-active before we call it done
        try:
            n = active_rows()
        except Exception as e:  # noqa: BLE001
            log(f"active_rows error: {e}")
            n = -1
        drained = drained + 1 if n == 0 else 0
        log(f"pipeline active_rows={n} drained_streak={drained}")
        time.sleep(120)
    log("pipeline queue drained — hero-stat converts complete")

    # 5. resume debloat
    if os.path.exists(PAUSE):
        os.remove(PAUSE)
    log("resumed debloat (removed pause_reclaim.json)")
    log("=== overnight controller done ===")


if __name__ == "__main__":
    main()
