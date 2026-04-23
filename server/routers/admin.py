"""Admin, health, GPU, config, history, and diagnostic endpoints.

Routes:
    GET  /api/health                 - system health dashboard
    GET  /api/gpu                    - GPU utilisation stats
    GET  /api/config                 - pipeline config (defaults + overrides)
    PUT  /api/config                 - set config overrides
    GET  /api/dismissed/{section}    - dismissed items for a UI section
    PUT  /api/dismissed/{section}    - set dismissed items
    GET  /api/mkvpropedit-available  - check mkvpropedit availability
    GET  /api/history                - encode history entries
    GET  /api/history/summary        - aggregated history stats and forecast
    GET  /api/plex-audit             - last Plex metadata audit results
"""

import json
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Request

from server.helpers import (
    CONFIG_OVERRIDES_FILE,
    CONTROL_DIR,
    DISMISSED_DIR,
    HISTORY_FILE,
    STAGING_DIR,
    _get_pipeline_state,
    read_json_safe,
    write_json_safe,
)

router = APIRouter()

# --- GPU monitoring ---

_gpu_cache: dict = {}
_gpu_cache_time: float = 0


def _query_gpu() -> dict:
    """Query nvidia-smi for GPU stats. Cached for 3 seconds."""
    global _gpu_cache, _gpu_cache_time
    now = time.monotonic()
    if now - _gpu_cache_time < 3 and _gpu_cache:
        return _gpu_cache
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=utilization.gpu,utilization.encoder,memory.used,memory.total,"
                "temperature.gpu,power.draw,name",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return {"available": False}
        parts = [p.strip() for p in result.stdout.strip().split(",")]
        if len(parts) < 7:
            return {"available": False}
        _gpu_cache = {
            "available": True,
            "gpu_util": int(parts[0]),
            "encoder_util": int(parts[1]),
            "mem_used_mb": int(parts[2]),
            "mem_total_mb": int(parts[3]),
            "temp_c": int(parts[4]),
            "power_w": float(parts[5]),
            "name": parts[6],
        }
        _gpu_cache_time = now
        return _gpu_cache
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        return {"available": False}


@router.get("/api/gpu")
def get_gpu() -> dict:
    """Return current GPU utilisation stats."""
    return _query_gpu()


# --- Host stats (CPU/memory/network/staging disk) ---

_net_prev: dict = {}


@router.get("/api/host-stats")
def get_host_stats() -> dict:
    """Return live host CPU/memory/network + staging disk usage for the telemetry strip."""
    import shutil as _shutil

    try:
        import psutil
    except ImportError:
        return {"available": False, "reason": "psutil not installed"}

    # psutil.cpu_percent with interval=None returns cumulative since last call — we rely on the
    # frontend polling cadence (every ~1.5s) to produce meaningful per-tick deltas.
    cpu_pct = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory()

    # Network throughput: diff against the last snapshot we kept.
    now = time.monotonic()
    io = psutil.net_io_counters()
    prev = _net_prev.get("snap")
    net_mbps = None
    if prev:
        dt = now - prev["t"]
        if dt > 0:
            rx_per_s = (io.bytes_recv - prev["rx"]) / dt
            tx_per_s = (io.bytes_sent - prev["tx"]) / dt
            net_mbps = round((rx_per_s + tx_per_s) / (1024 * 1024), 1)
    _net_prev["snap"] = {"t": now, "rx": io.bytes_recv, "tx": io.bytes_sent}

    try:
        disk = _shutil.disk_usage(str(STAGING_DIR))
        staging_free_gb = round(disk.free / (1024**3), 2)
        staging_used_gb = round(disk.used / (1024**3), 2)
        staging_total_gb = round(disk.total / (1024**3), 2)
    except OSError:
        staging_free_gb = staging_used_gb = staging_total_gb = None

    cpu_temp = None
    try:
        temps = psutil.sensors_temperatures() if hasattr(psutil, "sensors_temperatures") else {}
        for probe in ("coretemp", "k10temp", "cpu_thermal", "acpitz"):
            if probe in temps and temps[probe]:
                cpu_temp = round(temps[probe][0].current)
                break
    except Exception:
        pass

    return {
        "available": True,
        "cpu_pct": round(cpu_pct, 1),
        "cpu_temp_c": cpu_temp,
        "cpu_count": psutil.cpu_count(logical=True),
        "mem_used_gb": round(mem.used / (1024**3), 2),
        "mem_total_gb": round(mem.total / (1024**3), 2),
        "mem_pct": mem.percent,
        "net_mbps": net_mbps,
        "staging_used_gb": staging_used_gb,
        "staging_free_gb": staging_free_gb,
        "staging_total_gb": staging_total_gb,
    }


# --- Health dashboard ---

_health_cache: dict = {}
_health_cache_time: float = 0


@router.get("/api/health")
def get_health(request: Request) -> dict:
    """Return system health dashboard data."""
    global _health_cache, _health_cache_time
    import shutil as _shutil

    now = time.monotonic()
    if now - _health_cache_time < 10 and _health_cache:
        return _health_cache

    import os

    from paths import NAS_MOVIES, NAS_SERIES

    try:
        nas_movies_ok = os.path.exists(str(NAS_MOVIES)) and os.access(str(NAS_MOVIES), os.R_OK)
    except OSError:
        nas_movies_ok = False
    try:
        nas_series_ok = os.path.exists(str(NAS_SERIES)) and os.access(str(NAS_SERIES), os.R_OK)
    except OSError:
        nas_series_ok = False

    try:
        disk = _shutil.disk_usage(str(STAGING_DIR))
        staging_free_gb = round(disk.free / (1024**3), 1)
        staging_total_gb = round(disk.total / (1024**3), 1)
    except OSError:
        staging_free_gb = 0
        staging_total_gb = 0

    ffmpeg_version = "unknown"
    try:
        result = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            first_line = result.stdout.split("\n")[0]
            raw = first_line.split("version ")[-1].split(" ")[0] if "version " in first_line else first_line
            ffmpeg_version = raw.split("-")[0] if "-" in raw else raw
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    gpu = _query_gpu()
    pm = request.app.state.pm
    pipeline_status = pm.status("pipeline")

    nas_ssh = os.environ.get("NAS_SSH_HOST", "")
    server_ssh = os.environ.get("SERVER_SSH_HOST", "")

    _health_cache = {
        "nas_movies_reachable": nas_movies_ok,
        "nas_series_reachable": nas_series_ok,
        "staging_free_gb": staging_free_gb,
        "staging_total_gb": staging_total_gb,
        "ffmpeg_version": ffmpeg_version,
        "gpu_name": gpu.get("name", "N/A"),
        "gpu_temp_c": gpu.get("temp_c"),
        "gpu_available": gpu.get("available", False),
        "pipeline_status": pipeline_status.get("status", "idle"),
        "pipeline_pid": pipeline_status.get("pid"),
        "python_version": sys.version.split()[0],
        "nas_ssh_configured": bool(nas_ssh),
        "server_ssh_configured": bool(server_ssh),
        "nas_ssh_host": nas_ssh or None,
        "server_ssh_host": server_ssh or None,
    }
    _health_cache_time = now
    return _health_cache


# --- Deep health / invariants ---

_health_deep_cache: dict = {}
_health_deep_cache_time: float = 0


@router.get("/api/health-deep")
def get_health_deep() -> dict:
    """Run the invariant battery and return structured results.

    Cached for 60s. Responses include ``generated_at`` so the UI can show
    the age of the last scan ("as of 23s ago"). The invariants themselves
    live in ``tools.invariants`` and cover the 2026-04-23 incident class:
    AV1 files with zero audio, DONE rows paired with deferred reasons,
    stale tmp files on the NAS, ghost python processes, etc.
    """
    global _health_deep_cache, _health_deep_cache_time
    now = time.monotonic()
    if _health_deep_cache and (now - _health_deep_cache_time) < 60:
        return _health_deep_cache

    from tools.invariants import run_all_invariants

    results = run_all_invariants(skip_ssh=False)
    _health_deep_cache = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "all_green": all(r.passed for r in results),
        "any_critical": any(not r.passed and r.severity == "CRITICAL" for r in results),
        "checks": [asdict(r) for r in results],
    }
    _health_deep_cache_time = now
    return _health_deep_cache


# --- Config management ---


@router.get("/api/config")
def get_config() -> dict:
    """Return current config (defaults + overrides)."""
    from pipeline.config import DEFAULT_CONFIG, build_config

    overrides = read_json_safe(CONFIG_OVERRIDES_FILE) or {}
    merged = build_config(overrides)
    return {
        "defaults": DEFAULT_CONFIG,
        "overrides": overrides,
        "effective": merged,
    }


@router.put("/api/config")
def set_config(body: dict) -> dict:
    """Write config overrides. Pipeline reads these on next file."""
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    write_json_safe(CONFIG_OVERRIDES_FILE, body)
    return {"ok": True}


# --- Dismissed items ---


@router.get("/api/dismissed/{section}")
def get_dismissed(section: str) -> dict:
    """Return dismissed items for a UI section."""
    path = DISMISSED_DIR / f"{section}.json"
    data = read_json_safe(path)
    return data or {"paths": []}


@router.put("/api/dismissed/{section}")
def set_dismissed(section: str, body: dict) -> dict:
    """Set dismissed items for a UI section."""
    DISMISSED_DIR.mkdir(parents=True, exist_ok=True)
    path = DISMISSED_DIR / f"{section}.json"
    write_json_safe(path, {"paths": body.get("paths", [])})
    return {"ok": True}


# --- mkvpropedit ---


@router.get("/api/mkvpropedit-available")
def mkvpropedit_available() -> dict:
    """Check whether mkvpropedit is installed and accessible."""
    from tools.detect_languages import _find_mkvpropedit

    found = _find_mkvpropedit()
    return {"available": found is not None, "path": found}


# --- Encode history ---


def _read_history(days: int = 0, limit: int = 0) -> list[dict]:
    """Read encode history JSONL, optionally filtering by recency."""
    if not HISTORY_FILE.exists():
        return []
    entries: list[dict] = []
    cutoff = None
    if days > 0:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if cutoff and entry.get("timestamp", "") < cutoff:
                    continue
                entries.append(entry)
    except OSError:
        return []
    if limit > 0:
        entries = entries[-limit:]
    return entries


@router.get("/api/history")
def get_history(days: int = 0, limit: int = 500) -> dict:
    """Return encode history entries."""
    return {"entries": _read_history(days=days, limit=limit)}


@router.get("/api/history/summary")
def get_history_summary() -> dict:
    """Aggregated history stats: per-day totals, per-tier averages, forecast."""
    entries = _read_history()
    if not entries:
        return {"days": [], "tiers": {}, "totals": {}, "forecast": None}

    by_day: dict[str, dict] = {}
    by_tier: dict[str, dict] = {}
    total_input = 0
    total_output = 0
    total_saved = 0
    total_time = 0.0

    for e in entries:
        day = e.get("timestamp", "")[:10]
        if day not in by_day:
            by_day[day] = {"count": 0, "saved_bytes": 0, "input_bytes": 0, "output_bytes": 0, "encode_time_secs": 0}
        d = by_day[day]
        d["count"] += 1
        d["saved_bytes"] += e.get("saved_bytes", 0)
        d["input_bytes"] += e.get("input_bytes", 0)
        d["output_bytes"] += e.get("output_bytes", 0)
        d["encode_time_secs"] += e.get("encode_time_secs", 0)

        tier = e.get("res_key", "unknown")
        if tier not in by_tier:
            by_tier[tier] = {
                "count": 0,
                "saved_bytes": 0,
                "input_bytes": 0,
                "output_bytes": 0,
                "encode_time_secs": 0,
                "total_compression_ratio": 0,
            }
        t = by_tier[tier]
        t["count"] += 1
        t["saved_bytes"] += e.get("saved_bytes", 0)
        t["input_bytes"] += e.get("input_bytes", 0)
        t["output_bytes"] += e.get("output_bytes", 0)
        t["encode_time_secs"] += e.get("encode_time_secs", 0)
        t["total_compression_ratio"] += e.get("compression_ratio", 0)

        total_input += e.get("input_bytes", 0)
        total_output += e.get("output_bytes", 0)
        total_saved += e.get("saved_bytes", 0)
        total_time += e.get("encode_time_secs", 0)

    for tier in by_tier.values():
        n = tier["count"]
        if n > 0:
            tier["avg_compression_ratio"] = round(tier.pop("total_compression_ratio") / n, 3)
            tier["avg_encode_time_secs"] = round(tier["encode_time_secs"] / n, 1)
        else:
            tier.pop("total_compression_ratio", None)

    days_list = sorted(by_day.items())
    forecast = None
    if len(days_list) >= 2:
        recent = days_list[-7:]
        avg_per_day = sum(d["count"] for _, d in recent) / len(recent)
        avg_saved_per_day = sum(d["saved_bytes"] for _, d in recent) / len(recent)

        state_data = _get_pipeline_state()
        if state_data and "files" in state_data:
            remaining = sum(
                1
                for f in state_data["files"].values()
                if f.get("status") not in ("verified", "replaced", "skipped", "error")
            )
            if avg_per_day > 0 and remaining > 0:
                days_remaining = remaining / avg_per_day
                est_date = datetime.now() + timedelta(days=days_remaining)
                forecast = {
                    "remaining_files": remaining,
                    "avg_files_per_day": round(avg_per_day, 1),
                    "avg_saved_per_day_gb": round(avg_saved_per_day / (1024**3), 2),
                    "est_completion_date": est_date.strftime("%Y-%m-%d"),
                    "est_days_remaining": round(days_remaining, 1),
                }

    return {
        "days": [{"date": d, **v} for d, v in sorted(by_day.items())],
        "tiers": by_tier,
        "totals": {
            "entries": len(entries),
            "input_bytes": total_input,
            "output_bytes": total_output,
            "saved_bytes": total_saved,
            "encode_time_secs": round(total_time, 1),
        },
        "forecast": forecast,
    }


# --- Plex audit ---


@router.get("/api/plex-audit")
def get_plex_audit() -> dict:
    """Read the last Plex metadata audit results."""
    audit_path = STAGING_DIR / "plex_audit.json"
    data = read_json_safe(audit_path)
    if data is None:
        return {"sections": [], "message": "No audit data. Run Plex Metadata Audit from Controls."}
    return data
