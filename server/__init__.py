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

from paths import STAGING_DIR, MEDIA_REPORT, PIPELINE_STATE_DB

# Derived paths
CONTROL_DIR = STAGING_DIR / "control"
STATE_FILE = STAGING_DIR / "pipeline_state.json"  # legacy, kept for migration detection
HISTORY_FILE = STAGING_DIR / "encode_history.jsonl"
FRONTEND_DIST = Path(__file__).parent.parent / "frontend" / "dist"


def _get_pipeline_state() -> dict | None:
    """Read pipeline state from SQLite, returning the same dict shape as the old JSON.

    Falls back to the JSON file if the DB doesn't exist yet (pre-migration).
    """
    db_path = str(PIPELINE_STATE_DB)
    if os.path.exists(db_path):
        try:
            from pipeline.state import get_db, PipelineState
            state = PipelineState(db_path)
            data = state.data
            state.close()
            return data
        except Exception:
            pass
    # Fallback to JSON
    return read_json_safe(STATE_FILE)


def _get_state_db():
    """Get a raw SQLite connection for direct queries (reset-errors, compact, etc.)."""
    from pipeline.state import get_db
    return get_db(str(PIPELINE_STATE_DB))


# --- Models ---

class PauseRequest(BaseModel):
    type: str  # "all" | "fetch" | "encode"

class PathListRequest(BaseModel):
    paths: list[str]

class PriorityRequest(BaseModel):
    force: list[str] = []
    paths: list[str] = []
    patterns: list[str] = []

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
        "cmd": [sys.executable, "-m", "pipeline", "--no-gap-filler"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "gap_filler": {
        "cmd": [sys.executable, "-m", "pipeline", "--gap-filler-only"],
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
        "cmd": [sys.executable, "-m", "tools.integrity", "--from-state", "--workers", "1"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "plex_scan": {
        "cmd": [sys.executable, "-c",
                "from tools.strip_tags import _trigger_plex_scan; _trigger_plex_scan()"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "plex_metadata": {
        "cmd": [sys.executable, "-m", "tools.plex_metadata", "audit",
                "--json", str(STAGING_DIR / "plex_audit.json")],
        "cwd": str(Path(__file__).parent.parent),
    },
    "plex_apply_rules": {
        "cmd": [sys.executable, "-m", "tools.plex_metadata", "apply-rules", "--execute"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "strip_subs": {
        "cmd": [sys.executable, "-m", "tools.strip_subs", "--execute"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "mux_subs": {
        "cmd": [sys.executable, "-m", "tools.mux_external_subs", "--execute"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "detect_languages": {
        "cmd": [sys.executable, "-m", "tools.detect_languages", "--workers", "6", "--apply", "--min-confidence", "0.85"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "detect_languages_whisper": {
        "cmd": [sys.executable, "-m", "tools.detect_languages", "--whisper", "--apply", "--min-confidence", "0.85"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "detect_languages_spotcheck": {
        "cmd": [sys.executable, "-m", "tools.detect_languages", "--spot-check", "200"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "apply_languages": {
        "cmd": [sys.executable, "-m", "tools.detect_languages", "--apply", "--min-confidence", "0.85"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "tmdb_enrich": {
        "cmd": [sys.executable, "-m", "tools.tmdb", "--enrich-and-apply"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "tmdb_apply": {
        "cmd": [sys.executable, "-m", "tools.tmdb", "--apply"],
        "cwd": str(Path(__file__).parent.parent),
    },
    "rewatchables": {
        "cmd": [sys.executable, "-m", "tools.rewatchables"],
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
    data = _get_pipeline_state()
    if data is None:
        return {"status": "no_state", "message": "Pipeline hasn't run yet"}
    return data


@app.get("/api/media-report")
def get_media_report():
    data = read_json_safe(MEDIA_REPORT)
    if data is None:
        raise HTTPException(404, "media_report.json not found")
    return data


@app.get("/api/library-completion")
def get_library_completion():
    """True library completion: AV1 video + EAC-3 audio + English-only subs."""
    data = read_json_safe(MEDIA_REPORT)
    if data is None:
        raise HTTPException(404, "media_report.json not found")

    files = data.get("files", [])
    total = len(files)
    keep_langs = {"eng", "en", "english", "und", ""}

    counts = {
        "total": total,
        "av1": 0,
        "eac3_done": 0,
        "subs_done": 0,
        "fully_done": 0,
        "needs_video": 0,
        "needs_audio": 0,
        "needs_subs": 0,
        "quick_wins_audio": [],  # AV1 files needing only audio fix
        "quick_wins_subs": [],   # AV1 files needing only sub strip
    }

    for f in files:
        fp = f.get("filepath", "")
        is_av1 = f.get("video", {}).get("codec_raw") == "av1"

        audio_codec_ok = all(
            (a.get("codec_raw") or a.get("codec", "")).lower() in ("eac3", "e-ac-3")
            for a in f.get("audio_streams", [])
        ) if f.get("audio_streams") else True

        # Audio clean: only English/und/original tracks remain (no foreign dubs)
        audio_streams = f.get("audio_streams", [])
        audio_clean = all(
            i == 0 or (a.get("language") or a.get("detected_language") or "und").lower().strip() in keep_langs
            for i, a in enumerate(audio_streams)
        ) if audio_streams else True

        audio_ok = audio_codec_ok and audio_clean

        subs_ok = all(
            (s.get("language") or s.get("detected_language") or "und").lower().strip() in keep_langs
            for s in f.get("subtitle_streams", [])
        )

        if is_av1:
            counts["av1"] += 1
        else:
            counts["needs_video"] += 1

        if is_av1 and audio_ok:
            counts["eac3_done"] += 1
        elif is_av1:
            counts["needs_audio"] += 1
            if subs_ok:
                counts["quick_wins_audio"].append(fp)

        if is_av1 and subs_ok:
            counts["subs_done"] += 1
        elif is_av1:
            counts["needs_subs"] += 1
            if audio_ok:
                counts["quick_wins_subs"].append(fp)

        if is_av1 and audio_ok and subs_ok:
            counts["fully_done"] += 1

    counts["pct_video"] = round(100 * counts["av1"] / total, 1) if total else 0
    counts["pct_audio"] = round(100 * counts["eac3_done"] / total, 1) if total else 0
    counts["pct_subs"] = round(100 * counts["subs_done"] / total, 1) if total else 0
    counts["pct_done"] = round(100 * counts["fully_done"] / total, 1) if total else 0
    counts["quick_wins_audio_count"] = len(counts["quick_wins_audio"])
    counts["quick_wins_subs_count"] = len(counts["quick_wins_subs"])
    del counts["quick_wins_audio"]
    del counts["quick_wins_subs"]

    # Tier breakdown from media report (persistent, doesn't reset on pipeline restart)
    tiers: dict[str, dict] = {}
    for f in files:
        codec = f.get("video", {}).get("codec_raw", "?")
        codec_name = f.get("video", {}).get("codec", codec)
        res = f.get("video", {}).get("resolution_class", "?")
        is_av1 = codec == "av1"

        a_streams = f.get("audio_streams", [])
        a_ok = all(
            (a.get("codec_raw") or a.get("codec", "")).lower() in ("eac3", "e-ac-3")
            for a in a_streams
        ) if a_streams else True
        a_clean = all(
            i == 0 or (a.get("language") or a.get("detected_language") or "und").lower().strip() in keep_langs
            for i, a in enumerate(a_streams)
        ) if a_streams else True
        s_ok = all(
            (s.get("language") or s.get("detected_language") or "und").lower().strip() in keep_langs
            for s in f.get("subtitle_streams", [])
        )

        if is_av1 and a_ok and a_clean and s_ok:
            tier = "Done"
        elif not is_av1:
            tier = f"{codec_name} {res}"
        elif not a_ok:
            tier = "Audio remux (AV1)"
        else:
            tier = "Cleanup remux (AV1)"

        if tier not in tiers:
            tiers[tier] = {"total": 0, "done": 0}
        tiers[tier]["total"] += 1
        if tier == "Done":
            tiers[tier]["done"] += 1

    counts["tiers"] = [
        {"name": name, "total": t["total"], "done": t["done"]}
        for name, t in sorted(tiers.items(), key=lambda x: (-x[1]["total"]))
    ]

    return counts


@app.get("/api/duplicates")
def get_duplicates():
    """Find duplicate files using title+duration matching with quality scoring."""
    from collections import defaultdict
    from tools.duplicates import find_title_duration_dupes, score_file, pick_best

    data = read_json_safe(MEDIA_REPORT)
    if data is None:
        raise HTTPException(404, "media_report.json not found")
    files = data.get("files", [])
    file_lookup = {f["filepath"]: f for f in files}

    raw_dupes = find_title_duration_dupes(files, same_dir=True)
    if not raw_dupes:
        return {"groups": [], "total_groups": 0, "total_dupes": 0, "wasted_gb": 0}

    # Group by group_id
    by_group = defaultdict(list)
    for r in raw_dupes:
        by_group[r["group_id"]].append(r)

    groups = []
    total_wasted = 0
    for gid, rows in sorted(by_group.items()):
        full_records = [file_lookup[r["filepath"]] for r in rows if r["filepath"] in file_lookup]
        if len(full_records) < 2:
            continue
        keeper, deletions = pick_best(full_records)
        keeper_path = keeper["filepath"]
        wasted = sum(d.get("file_size_gb", 0) for d in deletions)
        total_wasted += wasted

        members = []
        for rec in full_records:
            members.append({
                "filepath": rec["filepath"],
                "filename": rec.get("filename", os.path.basename(rec["filepath"])),
                "file_size_gb": rec.get("file_size_gb", 0),
                "duration_seconds": rec.get("duration_seconds", 0),
                "codec": rec.get("video", {}).get("codec", ""),
                "resolution": rec.get("video", {}).get("resolution_class", ""),
                "score": score_file(rec),
                "keep": rec["filepath"] == keeper_path,
            })
        members.sort(key=lambda m: -m["score"])
        groups.append({
            "group_id": gid,
            "title": rows[0].get("normalized_title", ""),
            "members": members,
            "wasted_gb": round(wasted, 3),
        })

    groups.sort(key=lambda g: -g["wasted_gb"])
    return {
        "groups": groups,
        "total_groups": len(groups),
        "total_dupes": sum(len(g["members"]) - 1 for g in groups),
        "wasted_gb": round(total_wasted, 2),
    }


class DeleteFileRequest(BaseModel):
    path: str


@app.post("/api/file/delete")
def delete_file(req: DeleteFileRequest):
    """Delete a single file. Only allows paths within NAS media directories."""
    from paths import NAS_MOVIES, NAS_SERIES

    norm = os.path.normpath(req.path)
    nas_movies = os.path.normpath(str(NAS_MOVIES))
    nas_series = os.path.normpath(str(NAS_SERIES))

    if not (norm.startswith(nas_movies) or norm.startswith(nas_series)):
        raise HTTPException(403, "Path is outside NAS media directories")

    if not os.path.exists(norm):
        raise HTTPException(404, "File not found")

    try:
        os.remove(norm)
        return {"ok": True, "deleted": req.path}
    except OSError as e:
        raise HTTPException(500, f"Delete failed: {e}")


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
    return data or {"force": [], "paths": [], "patterns": []}


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
def set_priority(req: PriorityRequest):
    # Preserve keys not explicitly provided by merging with current data
    current = read_json_safe(CONTROL_DIR / "priority.json") or {}
    merged = {
        "force": req.force if req.force else current.get("force", []),
        "paths": req.paths,
        "patterns": req.patterns if req.patterns else current.get("patterns", []),
    }
    drop_file("priority.json", merged)
    return {"ok": True, "force": len(merged["force"]), "paths": len(merged["paths"]),
            "patterns": len(merged["patterns"])}


class ForceRequest(BaseModel):
    path: str
    action: str = "add"  # "add" | "remove"


@app.post("/api/control/priority/force")
def toggle_force(req: ForceRequest):
    """Add or remove a single file from the force-priority tier."""
    current = read_json_safe(CONTROL_DIR / "priority.json") or {}
    force = current.get("force", [])
    norm = os.path.normpath(req.path).lower()

    if req.action == "add":
        if not any(os.path.normpath(p).lower() == norm for p in force):
            force.insert(0, req.path)
    elif req.action == "remove":
        force = [p for p in force if os.path.normpath(p).lower() != norm]

    current["force"] = force
    current.setdefault("paths", [])
    current.setdefault("patterns", [])
    drop_file("priority.json", current)
    return {"ok": True, "forced": req.action == "add", "force_count": len(force)}


@app.post("/api/quick-wins")
def quick_wins():
    """Bulk-force AV1 files needing audio or cleanup work to the front of the pipeline queue."""
    data = read_json_safe(MEDIA_REPORT)
    if data is None:
        raise HTTPException(404, "media_report.json not found")

    keep_langs = {"eng", "en", "english", "und", ""}
    files = data.get("files", [])
    paths = []
    for f in files:
        if f.get("video", {}).get("codec_raw") != "av1":
            continue
        audio_streams = f.get("audio_streams", [])
        audio_codec_ok = all(
            (a.get("codec_raw") or a.get("codec", "")).lower() in ("eac3", "e-ac-3")
            for a in audio_streams
        ) if audio_streams else True
        audio_clean = all(
            i == 0 or (a.get("language") or a.get("detected_language") or "und").lower().strip() in keep_langs
            for i, a in enumerate(audio_streams)
        ) if audio_streams else True
        subs_ok = all(
            (s.get("language") or s.get("detected_language") or "und").lower().strip() in keep_langs
            for s in f.get("subtitle_streams", [])
        )
        if not (audio_codec_ok and audio_clean and subs_ok):
            paths.append(f["filepath"])

    if not paths:
        return {"ok": True, "added": 0, "message": "No audio quick wins found"}

    current = read_json_safe(CONTROL_DIR / "priority.json") or {}
    force = current.get("force", [])
    existing = {os.path.normpath(p).lower() for p in force}
    added = 0
    for p in paths:
        if os.path.normpath(p).lower() not in existing:
            force.append(p)
            added += 1

    current["force"] = force
    current.setdefault("paths", [])
    current.setdefault("patterns", [])
    drop_file("priority.json", current)
    return {"ok": True, "added": added, "total_force": len(force)}


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
    """Reset all error entries to pending. Writes directly to SQLite —
    the pipeline process sees the changes immediately, no restart needed."""
    try:
        conn = _get_state_db()
        now = datetime.now().isoformat()
        cursor = conn.execute(
            "UPDATE pipeline_files SET status = 'pending', error = NULL, stage = NULL, "
            "last_updated = ? WHERE status IN ('error', 'failed')", (now,)
        )
        reset_count = cursor.rowcount
        if reset_count > 0:
            # Update stats
            row = conn.execute("SELECT data FROM pipeline_stats WHERE id = 1").fetchone()
            if row:
                stats = json.loads(row[0])
                stats["errors"] = max(0, stats.get("errors", 0) - reset_count)
                conn.execute("UPDATE pipeline_stats SET data = ? WHERE id = 1", (json.dumps(stats),))
        conn.commit()
        conn.close()
        return {"ok": True, "reset": reset_count}
    except Exception as e:
        raise HTTPException(500, f"Failed to reset errors: {e}")


@app.post("/api/pipeline/force-accept")
def force_accept(req: dict):
    """Override duration mismatch for a specific file and requeue for replace.

    Body: {"path": "\\\\KieranNAS\\..."}
    Sets skip_duration_check=True and resets status to uploaded so the pipeline
    re-verifies (and this time ignores the duration delta).
    """
    path = req.get("path")
    if not path:
        raise HTTPException(400, "path required")
    try:
        conn = _get_state_db()
        row = conn.execute("SELECT status, extras FROM pipeline_files WHERE filepath = ?", (path,)).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, f"No state entry for {path}")
        if row["status"] != "error":
            conn.close()
            raise HTTPException(400, f"File is not in error state (status={row['status']})")
        now = datetime.now().isoformat()
        extras = json.loads(row["extras"]) if row["extras"] else {}
        extras["skip_duration_check"] = True
        conn.execute(
            "UPDATE pipeline_files SET status = 'uploaded', error = NULL, stage = NULL, "
            "last_updated = ?, extras = ? WHERE filepath = ?",
            (now, json.dumps(extras), path)
        )
        conn.commit()
        conn.close()
        return {"ok": True, "path": path}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Failed to force-accept: {e}")


@app.post("/api/pipeline/compact")
def compact_state():
    """Remove REPLACED and SKIPPED entries from pipeline state."""
    try:
        conn = _get_state_db()
        cursor = conn.execute(
            "DELETE FROM pipeline_files WHERE status IN ('replaced', 'skipped')"
        )
        removed = cursor.rowcount
        remaining = conn.execute("SELECT COUNT(*) FROM pipeline_files").fetchone()[0]
        if removed > 0:
            row = conn.execute("SELECT data FROM pipeline_stats WHERE id = 1").fetchone()
            if row:
                stats = json.loads(row[0])
                stats["archived_count"] = stats.get("archived_count", 0) + removed
                conn.execute("UPDATE pipeline_stats SET data = ? WHERE id = 1", (json.dumps(stats),))
        conn.commit()
        conn.close()
        return {"ok": True, "removed": removed, "remaining": remaining}
    except Exception as e:
        raise HTTPException(500, f"Failed to compact: {e}")


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


@app.get("/api/mkvpropedit-available")
def mkvpropedit_available():
    from tools.detect_languages import _find_mkvpropedit
    found = _find_mkvpropedit()
    return {"available": found is not None, "path": found}


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
    state_data = _get_pipeline_state()
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


class VmafRequest(BaseModel):
    path: str
    duration: int = 30


@app.post("/api/vmaf/check")
def vmaf_check(req: VmafRequest):
    """Run VMAF quality check on a completed encode."""
    # Look up the file in pipeline state to find source/encoded paths
    state_data = _get_pipeline_state()
    if not state_data or "files" not in state_data:
        raise HTTPException(404, "Pipeline state not found")

    file_info = state_data["files"].get(req.path)
    if not file_info:
        raise HTTPException(404, f"File not in pipeline state: {req.path}")

    status = file_info.get("status", "")
    if status not in ("verified", "replaced"):
        raise HTTPException(400, f"File not in terminal state: {status}")

    # Source = original NAS path, encoded = dest_path or final_path
    source = req.path
    encoded = file_info.get("final_path") or file_info.get("dest_path")
    if not encoded:
        raise HTTPException(400, "No encoded path found in state")

    # Check for cached result
    vmaf_dir = STAGING_DIR / "vmaf_results"
    safe_name = Path(encoded).stem.replace(" ", "_")[:80]
    cached = vmaf_dir / f"{safe_name}.json"
    if cached.exists():
        data = read_json_safe(cached)
        if data:
            return data

    # Run VMAF (synchronous — can take a couple minutes)
    try:
        from tools.vmaf import run_vmaf
        result = run_vmaf(source, encoded, duration=req.duration)
        if "error" in result:
            raise HTTPException(500, result["error"])
        return result
    except ImportError:
        raise HTTPException(500, "VMAF tool not available")
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/plex-audit")
def get_plex_audit():
    """Read the last Plex metadata audit results."""
    audit_path = STAGING_DIR / "plex_audit.json"
    data = read_json_safe(audit_path)
    if data is None:
        return {"sections": [], "message": "No audit data. Run Plex Metadata Audit from Controls."}
    return data


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
        state_data = _get_pipeline_state()
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
        state_data = _get_pipeline_state()
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

            # Pipeline state — check DB mtime
            try:
                db_path = str(PIPELINE_STATE_DB)
                mtime = os.path.getmtime(db_path) if os.path.exists(db_path) else 0
            except OSError:
                mtime = 0
            if mtime != _ws_state_mtime:
                _ws_state_mtime = mtime
                data = _get_pipeline_state()
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
