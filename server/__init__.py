"""
AV1 Pipeline Dashboard Server
==============================
FastAPI app that serves the pipeline dashboard frontend and provides
API endpoints for monitoring pipeline progress and managing control files.

Usage:
    python -m server
    uv run uvicorn server:app --host 0.0.0.0 --port 8000
"""

import asyncio
import json
import os
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from paths import STAGING_DIR, MEDIA_REPORT

# Derived paths
CONTROL_DIR = STAGING_DIR / "control"
STATE_FILE = STAGING_DIR / "pipeline_state.json"
HISTORY_FILE = STAGING_DIR / "encode_history.jsonl"
FRONTEND_DIST = Path(__file__).parent.parent / "frontend" / "dist"


# --- Models ---

class PauseRequest(BaseModel):
    type: str  # "all" | "fetch" | "encode"

class PathListRequest(BaseModel):
    paths: list[str]

class GentleRequest(BaseModel):
    paths: dict = {}
    patterns: dict = {}
    default_offset: int = 0

class ReencodeRequest(BaseModel):
    files: dict = {}
    patterns: dict = {}

class KeywordListRequest(BaseModel):
    keywords: list[str]


# --- Control file helpers ---

def drop_file(name: str, data: dict | None = None) -> Path:
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    path = CONTROL_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        if data:
            json.dump(data, f, indent=2)
    return path


def remove_file(name: str) -> None:
    path = CONTROL_DIR / name
    if path.exists():
        path.unlink()


def file_exists(name: str) -> bool:
    return (CONTROL_DIR / name).exists()


def get_pause_state() -> str:
    if (STAGING_DIR / "PAUSE").exists():
        return "paused_all"
    for name, ptype in [
        ("pause_all.json", "paused_all"),
        ("pause_fetch.json", "paused_fetch"),
        ("pause_encode.json", "paused_encode"),
    ]:
        if file_exists(name):
            return ptype
    pause_path = CONTROL_DIR / "pause.json"
    if pause_path.exists():
        try:
            data = json.loads(pause_path.read_text())
            t = data.get("type", "all")
            return f"paused_{t}" if t != "all" else "paused_all"
        except Exception:
            return "paused_all"
    return "running"


def clear_all_pauses() -> None:
    for name in ["pause.json", "pause_all.json", "pause_fetch.json", "pause_encode.json"]:
        remove_file(name)
    pause_path = STAGING_DIR / "PAUSE"
    if pause_path.exists():
        pause_path.unlink()


def read_json_safe(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_json_safe(path: Path, data: dict | list) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


# --- Process Manager ---

PROCESS_CONFIGS = {
    "scanner": {
        "cmd": [sys.executable, "-m", "tools.scanner"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "pipeline": {
        "cmd": [sys.executable, "-m", "pipeline", "--resume"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "strip_tags": {
        "cmd": [sys.executable, "-m", "tools.strip_tags", "--execute", "--movies"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "duplicates": {
        "cmd": [sys.executable, "-m", "tools.duplicates", "--delete", "--execute"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "subtitles": {
        "cmd": [sys.executable, "-m", "tools.subtitles"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "integrity": {
        "cmd": [sys.executable, "-m", "tools.integrity", "--from-state"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "plex_scan": {
        "cmd": [sys.executable, "-c",
                "from tools.strip_tags import _trigger_plex_scan; _trigger_plex_scan()"],
        "cwd": str(Path(__file__).parent.parent),
    },
}

STOP_TIMEOUT = 15  # seconds to wait for graceful stop before terminate


class ProcessManager:
    def __init__(self) -> None:
        self._procs: dict[str, subprocess.Popen] = {}
        self._logs: dict[str, deque] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._lock = threading.Lock()

    def _reader(self, name: str, proc: subprocess.Popen) -> None:
        buf = self._logs[name]
        try:
            for line in iter(proc.stdout.readline, ""):
                if not line:
                    break
                buf.append(line.rstrip("\n"))
        except (ValueError, OSError):
            pass

    def start(self, name: str) -> dict:
        cfg = PROCESS_CONFIGS.get(name)
        if not cfg:
            raise ValueError(f"Unknown process: {name}")
        with self._lock:
            existing = self._procs.get(name)
            if existing and existing.poll() is None:
                return {"ok": False, "error": f"{name} is already running (pid {existing.pid})"}
            creation_flags = 0
            if sys.platform == "win32":
                creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
            proc = subprocess.Popen(
                cfg["cmd"],
                cwd=cfg["cwd"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                creationflags=creation_flags,
            )
            self._procs[name] = proc
            self._logs[name] = deque(maxlen=500)
            t = threading.Thread(target=self._reader, args=(name, proc), daemon=True)
            t.start()
            self._threads[name] = t
            return {"ok": True, "pid": proc.pid}

    def stop(self, name: str) -> dict:
        with self._lock:
            proc = self._procs.get(name)
            if not proc or proc.poll() is not None:
                return {"ok": False, "error": f"{name} is not running"}
        if name == "pipeline":
            drop_file("pause_all.json", {"type": "all"})
            try:
                proc.wait(timeout=STOP_TIMEOUT)
                remove_file("pause_all.json")
                return {"ok": True, "method": "graceful"}
            except subprocess.TimeoutExpired:
                proc.terminate()
                remove_file("pause_all.json")
                return {"ok": True, "method": "terminated"}
        else:
            proc.terminate()
            return {"ok": True, "method": "terminated"}

    def force_kill(self, name: str) -> dict:
        """Kill any OS process matching this pipeline command, even if not started by us."""
        import signal
        cfg = PROCESS_CONFIGS.get(name)
        if not cfg:
            raise ValueError(f"Unknown process: {name}")

        # The module name to search for (e.g. "-m pipeline" or "-m tools.scanner")
        module_flag = cfg["cmd"][-1] if "-m" in cfg["cmd"] else None
        if not module_flag:
            return {"ok": False, "error": "Cannot identify process command"}

        killed = []
        try:
            # Use tasklist /v to find python processes, then filter by command line
            result = subprocess.run(
                ["wmic", "process", "where", "name='python.exe'", "get",
                 "processid,commandline", "/format:csv"],
                capture_output=True, text=True, timeout=10,
            )
            my_pid = os.getpid()
            for line in result.stdout.strip().splitlines():
                if module_flag not in line:
                    continue
                # CSV format: Node,CommandLine,ProcessId
                parts = line.strip().split(",")
                if len(parts) < 3:
                    continue
                try:
                    pid = int(parts[-1])
                except ValueError:
                    continue
                if pid == my_pid:
                    continue  # don't kill the dashboard
                try:
                    os.kill(pid, signal.SIGTERM)
                    killed.append(pid)
                except OSError:
                    pass
        except Exception as e:
            return {"ok": False, "error": str(e)}

        # Also clean up our tracked process if it matches
        with self._lock:
            proc = self._procs.get(name)
            if proc and proc.poll() is None:
                proc.terminate()
                if proc.pid not in killed:
                    killed.append(proc.pid)

        if not killed:
            return {"ok": False, "error": f"No {name} process found"}
        return {"ok": True, "killed": killed}

    def status(self, name: str) -> dict:
        proc = self._procs.get(name)
        if not proc:
            return {"status": "idle", "pid": None, "exit_code": None}
        code = proc.poll()
        if code is None:
            return {"status": "running", "pid": proc.pid, "exit_code": None}
        if code == 0:
            return {"status": "finished", "pid": proc.pid, "exit_code": 0}
        return {"status": "error", "pid": proc.pid, "exit_code": code}

    def get_logs(self, name: str, last_n: int = 50) -> list[str]:
        buf = self._logs.get(name, deque())
        return list(buf)[-last_n:]


pm = ProcessManager()


# --- App ---

app = FastAPI(title="AV1 Pipeline Dashboard")


# -- Read-only endpoints --

@app.get("/api/pipeline")
def get_pipeline():
    data = read_json_safe(STATE_FILE)
    if data is None:
        return {"status": "no_state", "message": "Pipeline hasn't run yet"}
    return data


@app.get("/api/media-report")
def get_media_report():
    data = read_json_safe(MEDIA_REPORT)
    if data is None:
        raise HTTPException(404, "media_report.json not found")
    return data


@app.get("/api/control/status")
def get_control_status():
    return {
        "pause_state": get_pause_state(),
        "has_skip": file_exists("skip.json"),
        "has_priority": file_exists("priority.json"),
        "has_gentle": file_exists("gentle.json"),
        "has_reencode": file_exists("reencode.json"),
    }


@app.get("/api/control/skip")
def get_skip():
    data = read_json_safe(CONTROL_DIR / "skip.json")
    return data or {"paths": []}


@app.get("/api/control/priority")
def get_priority():
    data = read_json_safe(CONTROL_DIR / "priority.json")
    return data or {"paths": []}


@app.get("/api/control/gentle")
def get_gentle():
    data = read_json_safe(CONTROL_DIR / "gentle.json")
    return data or {"paths": {}, "patterns": {}, "default_offset": 0}


@app.get("/api/control/reencode")
def get_reencode():
    data = read_json_safe(CONTROL_DIR / "reencode.json")
    return data or {"files": {}, "patterns": {}}


@app.put("/api/control/reencode")
def set_reencode(req: ReencodeRequest):
    drop_file("reencode.json", {"files": req.files, "patterns": req.patterns})
    return {"ok": True, "count": len(req.files), "pattern_count": len(req.patterns)}


@app.get("/api/control/custom-tags")
def get_custom_tags():
    data = read_json_safe(CONTROL_DIR / "custom_tags.json")
    return data or {"keywords": []}


@app.put("/api/control/custom-tags")
def set_custom_tags(req: KeywordListRequest):
    clean = list(dict.fromkeys(k.strip() for k in req.keywords if k.strip()))
    drop_file("custom_tags.json", {"keywords": clean})
    return {"ok": True, "count": len(clean)}


# -- Write endpoints --

@app.post("/api/control/pause")
def pause_pipeline(req: PauseRequest):
    clear_all_pauses()
    type_map = {
        "all": ("pause_all.json", {"type": "all"}),
        "fetch": ("pause_fetch.json", {"type": "fetch_only"}),
        "encode": ("pause_encode.json", {"type": "encode_only"}),
    }
    if req.type not in type_map:
        raise HTTPException(400, f"Invalid pause type: {req.type}")
    name, data = type_map[req.type]
    drop_file(name, data)
    return {"ok": True, "pause_state": get_pause_state()}


@app.post("/api/control/resume")
def resume_pipeline():
    clear_all_pauses()
    return {"ok": True, "pause_state": "running"}


@app.put("/api/control/skip")
def set_skip(req: PathListRequest):
    drop_file("skip.json", {"paths": req.paths})
    return {"ok": True, "count": len(req.paths)}


@app.put("/api/control/priority")
def set_priority(req: PathListRequest):
    drop_file("priority.json", {"paths": req.paths})
    return {"ok": True, "count": len(req.paths)}


@app.put("/api/control/gentle")
def set_gentle(req: GentleRequest):
    drop_file("gentle.json", {
        "paths": req.paths,
        "patterns": req.patterns,
        "default_offset": req.default_offset,
    })
    return {"ok": True}


@app.post("/api/pipeline/reset-errors")
def reset_errors():
    data = read_json_safe(STATE_FILE)
    if data is None:
        raise HTTPException(404, "Pipeline state not found")
    files = data.get("files", {})
    reset_count = 0
    for path, info in files.items():
        if (info.get("status") or "").lower() in ("error", "failed"):
            info["status"] = "pending"
            info["last_updated"] = datetime.now().isoformat()
            info.pop("error", None)
            info.pop("stage", None)
            reset_count += 1
    if reset_count > 0:
        stats = data.get("stats", {})
        stats["errors"] = max(0, stats.get("errors", 0) - reset_count)
        data["last_updated"] = datetime.now().isoformat()
        STATE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return {"ok": True, "reset": reset_count}


# -- Process management endpoints --

VALID_PROCESS_NAMES = set(PROCESS_CONFIGS.keys())


@app.get("/api/process/{name}/status")
def get_process_status(name: str):
    if name not in VALID_PROCESS_NAMES:
        raise HTTPException(404, f"Unknown process: {name}")
    return pm.status(name)


@app.post("/api/process/{name}/start")
def start_process(name: str):
    if name not in VALID_PROCESS_NAMES:
        raise HTTPException(404, f"Unknown process: {name}")
    result = pm.start(name)
    if not result["ok"]:
        raise HTTPException(409, result["error"])
    return result


@app.post("/api/process/{name}/stop")
def stop_process(name: str):
    if name not in VALID_PROCESS_NAMES:
        raise HTTPException(404, f"Unknown process: {name}")
    result = pm.stop(name)
    if not result["ok"]:
        raise HTTPException(409, result["error"])
    return result


@app.post("/api/process/{name}/kill")
def kill_process(name: str):
    if name not in VALID_PROCESS_NAMES:
        raise HTTPException(404, f"Unknown process: {name}")
    result = pm.force_kill(name)
    if not result["ok"]:
        raise HTTPException(409, result["error"])
    return result


@app.get("/api/process/{name}/logs")
def get_process_logs(name: str, last_n: int = 50):
    if name not in VALID_PROCESS_NAMES:
        raise HTTPException(404, f"Unknown process: {name}")
    return {"lines": pm.get_logs(name, last_n)}


# -- Dismissed items (persisted in staging dir) --

DISMISSED_DIR = STAGING_DIR / "dismissed"


@app.get("/api/dismissed/{section}")
def get_dismissed(section: str):
    path = DISMISSED_DIR / f"{section}.json"
    data = read_json_safe(path)
    return data or {"paths": []}


@app.put("/api/dismissed/{section}")
def set_dismissed(section: str, body: dict):
    DISMISSED_DIR.mkdir(parents=True, exist_ok=True)
    path = DISMISSED_DIR / f"{section}.json"
    write_json_safe(path, {"paths": body.get("paths", [])})
    return {"ok": True}


# -- GPU monitoring --

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
            ["nvidia-smi",
             "--query-gpu=utilization.gpu,utilization.encoder,memory.used,memory.total,"
             "temperature.gpu,power.draw,name",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5,
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


@app.get("/api/gpu")
def get_gpu():
    return _query_gpu()


# -- Health dashboard --

_health_cache: dict = {}
_health_cache_time: float = 0


@app.get("/api/health")
def get_health():
    global _health_cache, _health_cache_time
    import shutil as _shutil

    now = time.monotonic()
    if now - _health_cache_time < 10 and _health_cache:
        return _health_cache

    from paths import NAS_MOVIES, NAS_SERIES

    # NAS reachability
    try:
        nas_movies_ok = os.path.exists(str(NAS_MOVIES)) and os.access(str(NAS_MOVIES), os.R_OK)
    except OSError:
        nas_movies_ok = False
    try:
        nas_series_ok = os.path.exists(str(NAS_SERIES)) and os.access(str(NAS_SERIES), os.R_OK)
    except OSError:
        nas_series_ok = False

    # Staging disk
    try:
        disk = _shutil.disk_usage(str(STAGING_DIR))
        staging_free_gb = round(disk.free / (1024**3), 1)
        staging_total_gb = round(disk.total / (1024**3), 1)
    except OSError:
        staging_free_gb = 0
        staging_total_gb = 0

    # FFmpeg version
    ffmpeg_version = "unknown"
    try:
        result = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            first_line = result.stdout.split("\n")[0]
            raw = first_line.split("version ")[-1].split(" ")[0] if "version " in first_line else first_line
            # Trim to just the version number (e.g. "8.0.1" from "8.0.1-full_build-www.gyan.dev")
            ffmpeg_version = raw.split("-")[0] if "-" in raw else raw
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # GPU info (reuse cached)
    gpu = _query_gpu()

    # Pipeline process status
    pipeline_status = pm.status("pipeline")

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
    }
    _health_cache_time = now
    return _health_cache


# -- File detail --

@app.get("/api/file-detail")
def get_file_detail(path: str):
    """Cross-reference media report + pipeline state for a single file."""
    result = {"path": path, "media": None, "pipeline": None}

    # Look up in media report
    report_data = read_json_safe(MEDIA_REPORT)
    if report_data:
        norm = os.path.normpath(path).lower()
        for entry in report_data.get("files", []):
            if os.path.normpath(entry.get("filepath", "")).lower() == norm:
                result["media"] = entry
                break

    # Look up in pipeline state
    state_data = read_json_safe(STATE_FILE)
    if state_data and "files" in state_data:
        result["pipeline"] = state_data["files"].get(path)

    return result


# -- Config management --

CONFIG_OVERRIDES_FILE = CONTROL_DIR / "config_overrides.json"


@app.get("/api/config")
def get_config():
    """Return current config (defaults + overrides)."""
    from pipeline.config import DEFAULT_CONFIG, build_config

    overrides = read_json_safe(CONFIG_OVERRIDES_FILE) or {}
    merged = build_config(overrides)

    return {
        "defaults": DEFAULT_CONFIG,
        "overrides": overrides,
        "effective": merged,
    }


@app.put("/api/config")
def set_config(body: dict):
    """Write config overrides. Pipeline reads these on next file."""
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    write_json_safe(CONFIG_OVERRIDES_FILE, body)
    return {"ok": True}


# -- Encode history --

def _read_history(days: int = 0, limit: int = 0) -> list[dict]:
    """Read encode history JSONL, optionally filtering by recency."""
    if not HISTORY_FILE.exists():
        return []
    entries = []
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


@app.get("/api/history")
def get_history(days: int = 0, limit: int = 500):
    return {"entries": _read_history(days=days, limit=limit)}


@app.get("/api/history/summary")
def get_history_summary():
    """Aggregated history stats: per-day totals, per-tier averages, forecast."""
    entries = _read_history()
    if not entries:
        return {"days": [], "tiers": {}, "totals": {}, "forecast": None}

    # Per-day aggregation
    by_day: dict[str, dict] = {}
    by_tier: dict[str, dict] = {}
    total_input = 0
    total_output = 0
    total_saved = 0
    total_time = 0.0

    for e in entries:
        day = e.get("timestamp", "")[:10]
        if day not in by_day:
            by_day[day] = {"count": 0, "saved_bytes": 0, "input_bytes": 0, "output_bytes": 0,
                           "encode_time_secs": 0}
        d = by_day[day]
        d["count"] += 1
        d["saved_bytes"] += e.get("saved_bytes", 0)
        d["input_bytes"] += e.get("input_bytes", 0)
        d["output_bytes"] += e.get("output_bytes", 0)
        d["encode_time_secs"] += e.get("encode_time_secs", 0)

        tier = e.get("res_key", "unknown")
        if tier not in by_tier:
            by_tier[tier] = {"count": 0, "saved_bytes": 0, "input_bytes": 0, "output_bytes": 0,
                             "encode_time_secs": 0, "total_compression_ratio": 0}
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

    # Compute per-tier averages
    for tier in by_tier.values():
        n = tier["count"]
        if n > 0:
            tier["avg_compression_ratio"] = round(tier.pop("total_compression_ratio") / n, 3)
            tier["avg_encode_time_secs"] = round(tier["encode_time_secs"] / n, 1)
        else:
            tier.pop("total_compression_ratio", None)

    # Forecast: use recent daily average to estimate completion
    days_list = sorted(by_day.items())
    forecast = None
    if len(days_list) >= 2:
        recent = days_list[-7:]  # last 7 active days
        avg_per_day = sum(d["count"] for _, d in recent) / len(recent)
        avg_saved_per_day = sum(d["saved_bytes"] for _, d in recent) / len(recent)

        # Load pipeline state to get remaining count
        state_data = read_json_safe(STATE_FILE)
        if state_data and "files" in state_data:
            remaining = sum(
                1 for f in state_data["files"].values()
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


# -- WebSocket for live updates --

class ConnectionManager:
    """Manage WebSocket connections for live pipeline updates."""

    def __init__(self):
        self.connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.connections:
            self.connections.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.connections:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


ws_manager = ConnectionManager()
_ws_state_mtime: float = 0


@app.websocket("/api/ws")
async def websocket_endpoint(ws: WebSocket):
    global _ws_state_mtime
    await ws_manager.connect(ws)
    try:
        # Send initial state
        state_data = read_json_safe(STATE_FILE)
        if state_data:
            await ws.send_json({"type": "pipeline", "data": state_data})

        gpu_data = _query_gpu()
        await ws.send_json({"type": "gpu", "data": gpu_data})

        control_data = {
            "pause_state": get_pause_state(),
            "has_skip": file_exists("skip.json"),
            "has_priority": file_exists("priority.json"),
            "has_gentle": file_exists("gentle.json"),
            "has_reencode": file_exists("reencode.json"),
        }
        await ws.send_json({"type": "control", "data": control_data})

        # Poll loop — push updates when state changes
        gpu_tick = 0
        while True:
            # Check for client messages (keepalive / close)
            try:
                await asyncio.wait_for(ws.receive_text(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break

            # Pipeline state — check mtime
            try:
                mtime = STATE_FILE.stat().st_mtime if STATE_FILE.exists() else 0
            except OSError:
                mtime = 0
            if mtime != _ws_state_mtime:
                _ws_state_mtime = mtime
                data = read_json_safe(STATE_FILE)
                if data:
                    await ws.send_json({"type": "pipeline", "data": data})

            # GPU stats every 5 ticks (~5 seconds)
            gpu_tick += 1
            if gpu_tick >= 5:
                gpu_tick = 0
                await ws.send_json({"type": "gpu", "data": _query_gpu()})

            # Control status every tick
            new_control = {
                "pause_state": get_pause_state(),
                "has_skip": file_exists("skip.json"),
                "has_priority": file_exists("priority.json"),
                "has_gentle": file_exists("gentle.json"),
                "has_reencode": file_exists("reencode.json"),
            }
            if new_control != control_data:
                control_data = new_control
                await ws.send_json({"type": "control", "data": control_data})

    except WebSocketDisconnect:
        pass
    finally:
        ws_manager.disconnect(ws)


# -- Static file serving (built frontend) --

if FRONTEND_DIST.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIST), html=True), name="frontend")


def run():
    """Entry point for `[project.scripts] dashboard = server:run`."""
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
