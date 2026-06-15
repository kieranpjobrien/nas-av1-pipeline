"""De-bloat reclaim status — surfaces the standalone reclaim tool's progress.

The in-place VMAF-gated de-bloat reclaim (``tools/reclaim_debloat.py``) runs
outside the pipeline supervisor and writes its own ledger + logs under the
staging dir. The dashboard's "Reclaimed" KPI only sums the pipeline's HEVC->AV1
history, so this endpoint reads the de-bloat ledger directly to make the
months-long reclaim effort visible.

Routes:
    GET /api/reclaim -> {reclaimed, saved_gb, flagged, purged_gb, running,
                         in_progress, recent[], flagged_list[]}
"""
import json
import os
import re
import time

from fastapi import APIRouter

from paths import STAGING_DIR

router = APIRouter()

_LEDGER = os.path.join(str(STAGING_DIR), "reclaim_ledger.json")
_LOG = os.path.join(str(STAGING_DIR), "reclaim.log")
_PURGE_LOG = os.path.join(str(STAGING_DIR), "reclaim_purge.log")

# Phases a film passes through while actively being worked (no terminal status yet).
_ACTIVE_PHASES = {"risk", "gate", "encoding", "uploading", "moving_original", "renaming"}


def _load_ledger() -> dict:
    try:
        with open(_LEDGER, encoding="utf-8") as f:
            data = json.load(f)
        # Drop the swap-mechanics test entry if it's ever lingering.
        return {k: v for k, v in data.items() if "dummy" not in k.lower()}
    except Exception:
        return {}


def _sum_from_log(path: str, pattern: str) -> float:
    try:
        with open(path, encoding="utf-8") as f:
            txt = f.read()
    except Exception:
        return 0.0
    return sum(float(m) for m in re.findall(pattern, txt))


def _log_fresh(within_s: int = 300) -> bool:
    """True if reclaim.log was written in the last `within_s` seconds."""
    try:
        return (time.time() - os.path.getmtime(_LOG)) < within_s
    except OSError:
        return False


def _work_fresh(within_s: int = 180) -> bool:
    """True if the reclaim scratch dir was touched recently. The log goes quiet
    during a ~1h encode, but the encode writes out.mkv (and the gate writes its
    clips) continuously — so this catches a live batch mid-encode (rule 14)."""
    work = os.path.join(str(STAGING_DIR), "reclaim")
    try:
        newest = max((os.path.getmtime(os.path.join(work, f)) for f in os.listdir(work)), default=0.0)
        return (time.time() - newest) < within_s
    except OSError:
        return False


@router.get("/api/reclaim")
def reclaim_status() -> dict:
    led = _load_ledger()
    reclaimed = [v for v in led.values() if v.get("status") == "reclaimed"]
    flagged = [v for v in led.values() if v.get("status") in ("gate_failed", "skipped_highrisk")]
    in_prog = next(
        (v for v in led.values() if v.get("status") is None and v.get("phase") in _ACTIVE_PHASES),
        None,
    )
    recent = list(reversed(reclaimed))[:12]  # ledger preserves processing order

    return {
        # GB banked is summed from the append-only log so it's cumulative across runs
        # (the ledger only stores post-encode size, not the original).
        "saved_gb": round(_sum_from_log(_LOG, r"RECLAIMED \(\d+\) saved ([\d.]+)GB"), 1),
        "reclaimed": len(reclaimed),
        "flagged": len(flagged),
        "purged_gb": round(_sum_from_log(_PURGE_LOG, r"freed ([\d.]+)GB"), 1),
        "running": bool(in_prog) and (_log_fresh() or _work_fresh()),
        "in_progress": {"name": in_prog.get("name"), "phase": in_prog.get("phase")} if in_prog else None,
        "recent": [
            {"name": v.get("name"), "vmaf": v.get("vmaf"), "new_gb": v.get("new_gb"), "cap": v.get("cap")}
            for v in recent
        ],
        "flagged_list": [
            {"name": v.get("name"), "status": v.get("status"), "grain": v.get("grain"), "vmaf": v.get("vmaf")}
            for v in flagged[:20]
        ],
    }
