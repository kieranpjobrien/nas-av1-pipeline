"""
AV1 Pipeline Dashboard Server
==============================
FastAPI app that serves the pipeline dashboard frontend and provides
API endpoints for monitoring pipeline progress and managing control files.

Usage:
    python -m server
    uv run uvicorn server:app --host 0.0.0.0 --port 8000
"""

import json
import subprocess
import sys
import threading
from collections import deque
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from paths import STAGING_DIR, MEDIA_REPORT

# Derived paths
CONTROL_DIR = STAGING_DIR / "control"
STATE_FILE = STAGING_DIR / "pipeline_state.json"
FRONTEND_DIST = Path(__file__).parent.parent / "frontend" / "dist"


# --- Models ---

class PauseRequest(BaseModel):
    type: str  # "all" | "fetch" | "encode"

class PathListRequest(BaseModel):
    paths: list[str]

class GentleRequest(BaseModel):
    overrides: dict


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
                creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP
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
    return data or {"overrides": {}}


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
    drop_file("gentle.json", {"overrides": req.overrides})
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


@app.get("/api/process/{name}/logs")
def get_process_logs(name: str, last_n: int = 50):
    if name not in VALID_PROCESS_NAMES:
        raise HTTPException(404, f"Unknown process: {name}")
    return {"lines": pm.get_logs(name, last_n)}


# -- Static file serving (built frontend) --

if FRONTEND_DIST.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIST), html=True), name="frontend")


def run():
    """Entry point for `[project.scripts] dashboard = server:run`."""
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
