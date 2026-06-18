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
from server.helpers import drop_file, remove_file

router = APIRouter()

_LEDGER = os.path.join(str(STAGING_DIR), "reclaim_ledger.json")
_LOG = os.path.join(str(STAGING_DIR), "reclaim.log")
_PURGE_LOG = os.path.join(str(STAGING_DIR), "reclaim_purge.log")
_PAUSE = os.path.join(str(STAGING_DIR), "control", "pause_reclaim.json")
_INFLIGHT = os.path.join(str(STAGING_DIR), "reclaim", "inflight.json")

# Phases a film passes through while actively being worked (no terminal status yet).
_ACTIVE_PHASES = {"risk", "gate", "encoding", "uploading", "moving_original", "renaming"}
_TERMINAL = {"reclaimed", "gate_failed", "skipped_highrisk", "skipped_error", "skipped_probefail", "swap_error"}

# The candidate pool (films+series, non-treasured growers) is expensive to build
# (reads media_report + scans the state DB), so cache it ~60s for the polled endpoint.
_cand_cache: dict = {"ts": 0.0, "fps": None}


def _candidate_fps() -> list:
    now = time.time()
    if _cand_cache["fps"] is None or now - _cand_cache["ts"] > 60:
        try:
            from tools.reclaim_debloat import candidates

            _cand_cache["fps"] = [c["fp"] for c in candidates()]
            _cand_cache["ts"] = now
        except Exception:
            _cand_cache["fps"] = _cand_cache.get("fps") or []
    return _cand_cache["fps"] or []


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


def _load_inflight() -> dict:
    try:
        with open(_INFLIGHT, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


@router.get("/api/reclaim")
def reclaim_status() -> dict:
    led = _load_ledger()
    reclaimed = [v for v in led.values() if v.get("status") == "reclaimed"]
    flagged = [v for v in led.values() if v.get("status") in ("gate_failed", "skipped_highrisk")]
    in_prog_kv = next(
        ((k, v) for k, v in led.items() if v.get("status") is None and v.get("phase") in _ACTIVE_PHASES),
        None,
    )
    inflight = _load_inflight()
    recent = list(reversed(reclaimed))[:12]  # ledger preserves processing order
    term_fps = {k for k, v in led.items() if v.get("status") in _TERMINAL}
    cand_fps = _candidate_fps()
    total = len(cand_fps)
    remaining = sum(1 for fp in cand_fps if fp not in term_fps)

    return {
        # GB banked is summed from the append-only log so it's cumulative across runs
        # (the ledger only stores post-encode size, not the original).
        "saved_gb": round(_sum_from_log(_LOG, r"RECLAIMED \(\d+\) saved ([\d.]+)GB"), 1),
        "reclaimed": len(reclaimed),
        "flagged": len(flagged),
        "candidates_total": total,
        "remaining": remaining,
        "purged_gb": round(_sum_from_log(_PURGE_LOG, r"freed ([\d.]+)GB"), 1),
        "running": bool(in_prog_kv) and (_log_fresh() or _work_fresh()),
        "paused": os.path.exists(_PAUSE),
        "in_progress": (
            {
                "name": in_prog_kv[1].get("name"),
                "phase": in_prog_kv[1].get("phase"),
                "cap": in_prog_kv[1].get("cap"),
                # live progress lives in a separate file (not the ledger); match by filepath
                "progress_pct": inflight.get("progress_pct") if inflight.get("fp") == in_prog_kv[0] else None,
                "speed": inflight.get("speed") if inflight.get("fp") == in_prog_kv[0] else None,
                "eta_s": inflight.get("eta_s") if inflight.get("fp") == in_prog_kv[0] else None,
            }
            if in_prog_kv
            else None
        ),
        "recent": [
            {"name": v.get("name"), "vmaf": v.get("vmaf"), "new_gb": v.get("new_gb"), "cap": v.get("cap")}
            for v in recent
        ],
        "flagged_list": [
            {"name": v.get("name"), "status": v.get("status"), "grain": v.get("grain"), "vmaf": v.get("vmaf")}
            for v in flagged[:20]
        ],
    }


@router.post("/api/reclaim/pause")
def reclaim_pause() -> dict:
    """Pause the de-bloat reclaim between films (never mid-encode). The tool
    checks this control file at each film boundary."""
    drop_file("pause_reclaim.json", {"type": "reclaim"})
    return {"ok": True, "paused": True}


@router.post("/api/reclaim/resume")
def reclaim_resume() -> dict:
    remove_file("pause_reclaim.json")
    return {"ok": True, "paused": False}
