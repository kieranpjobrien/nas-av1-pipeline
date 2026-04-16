"""Pipeline control and state endpoints.

Routes:
    GET  /api/pipeline              - current pipeline state
    GET  /api/control/status        - pause/skip/priority status
    GET  /api/control/skip          - current skip list
    PUT  /api/control/skip          - set skip list
    GET  /api/control/priority      - current priority config
    PUT  /api/control/priority      - set priority config
    GET  /api/control/force-list    - current force stack
    POST /api/control/priority/force - add/remove single force entry
    POST /api/control/pause         - pause pipeline
    POST /api/control/resume        - resume pipeline
    GET  /api/control/gentle        - current gentle config
    PUT  /api/control/gentle        - set gentle config
    GET  /api/control/reencode      - current reencode config
    PUT  /api/control/reencode      - set reencode config
    GET  /api/control/custom-tags   - current custom tag keywords
    PUT  /api/control/custom-tags   - set custom tag keywords
    POST /api/pipeline/reset-errors - reset error entries to pending
    POST /api/pipeline/compact      - remove replaced/skipped entries
    POST /api/pipeline/force-accept - override duration mismatch
    POST /api/quick-wins            - bulk-force AV1 audio quick wins
"""

import json
import os
from datetime import datetime

from fastapi import APIRouter, HTTPException

from paths import MEDIA_REPORT
from pipeline.config import KEEP_LANGS
from server.helpers import (
    CONTROL_DIR,
    _get_pipeline_state,
    _get_state_db,
    clear_all_pauses,
    drop_file,
    file_exists,
    get_pause_state,
    read_json_safe,
)
from server.models import (
    ForceRequest,
    GentleRequest,
    KeywordListRequest,
    PathListRequest,
    PauseRequest,
    PriorityRequest,
    ReencodeRequest,
)

router = APIRouter()


@router.get("/api/pipeline")
def get_pipeline() -> dict:
    """Return the current pipeline state."""
    data = _get_pipeline_state()
    if data is None:
        return {"status": "no_state", "message": "Pipeline hasn't run yet"}
    return data


@router.get("/api/control/status")
def get_control_status() -> dict:
    """Return the current control file status summary."""
    return {
        "pause_state": get_pause_state(),
        "has_skip": file_exists("skip.json"),
        "has_priority": file_exists("priority.json"),
        "has_gentle": file_exists("gentle.json"),
        "has_reencode": file_exists("reencode.json"),
    }


@router.get("/api/control/skip")
def get_skip() -> dict:
    """Return the current skip list."""
    data = read_json_safe(CONTROL_DIR / "skip.json")
    return data or {"paths": []}


@router.put("/api/control/skip")
def set_skip(req: PathListRequest) -> dict:
    """Set the skip list."""
    drop_file("skip.json", {"paths": req.paths})
    return {"ok": True, "count": len(req.paths)}


@router.get("/api/control/priority")
def get_priority() -> dict:
    """Return the current priority configuration."""
    data = read_json_safe(CONTROL_DIR / "priority.json")
    return data or {"force": [], "paths": [], "patterns": []}


@router.put("/api/control/priority")
def set_priority(req: PriorityRequest) -> dict:
    """Set the priority queue configuration."""
    current = read_json_safe(CONTROL_DIR / "priority.json") or {}
    merged = {
        "force": req.force if req.force else current.get("force", []),
        "paths": req.paths,
        "patterns": req.patterns if req.patterns else current.get("patterns", []),
    }
    drop_file("priority.json", merged)
    return {
        "ok": True,
        "force": len(merged["force"]),
        "paths": len(merged["paths"]),
        "patterns": len(merged["patterns"]),
    }


@router.get("/api/control/force-list")
def get_force_list() -> dict:
    """Get the current force stack (LIFO order)."""
    current = read_json_safe(CONTROL_DIR / "priority.json") or {}
    force = current.get("force", [])
    items = [{"filepath": fp, "filename": os.path.basename(fp)} for fp in force]
    return {"items": items, "count": len(items)}


@router.post("/api/control/priority/force")
def toggle_force(req: ForceRequest) -> dict:
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


@router.post("/api/control/pause")
def pause_pipeline(req: PauseRequest) -> dict:
    """Pause the pipeline by type (all, fetch, encode)."""
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


@router.post("/api/control/resume")
def resume_pipeline() -> dict:
    """Resume a paused pipeline."""
    clear_all_pauses()
    return {"ok": True, "pause_state": "running"}


@router.get("/api/control/gentle")
def get_gentle() -> dict:
    """Return the current gentle encoding configuration."""
    data = read_json_safe(CONTROL_DIR / "gentle.json")
    return data or {"paths": {}, "patterns": {}, "default_offset": 0}


@router.put("/api/control/gentle")
def set_gentle(req: GentleRequest) -> dict:
    """Set gentle encoding offsets."""
    drop_file(
        "gentle.json",
        {
            "paths": req.paths,
            "patterns": req.patterns,
            "default_offset": req.default_offset,
        },
    )
    return {"ok": True}


@router.get("/api/control/reencode")
def get_reencode() -> dict:
    """Return the current re-encode configuration."""
    data = read_json_safe(CONTROL_DIR / "reencode.json")
    return data or {"files": {}, "patterns": {}}


@router.put("/api/control/reencode")
def set_reencode(req: ReencodeRequest) -> dict:
    """Set re-encode targets."""
    drop_file("reencode.json", {"files": req.files, "patterns": req.patterns})
    return {"ok": True, "count": len(req.files), "pattern_count": len(req.patterns)}


@router.get("/api/control/custom-tags")
def get_custom_tags() -> dict:
    """Return the current custom tag keywords."""
    data = read_json_safe(CONTROL_DIR / "custom_tags.json")
    return data or {"keywords": []}


@router.put("/api/control/custom-tags")
def set_custom_tags(req: KeywordListRequest) -> dict:
    """Set custom tag keywords for strip_tags."""
    clean = list(dict.fromkeys(k.strip() for k in req.keywords if k.strip()))
    drop_file("custom_tags.json", {"keywords": clean})
    return {"ok": True, "count": len(clean)}


@router.post("/api/pipeline/reset-errors")
def reset_errors() -> dict:
    """Reset all error entries to pending in the pipeline state DB."""
    try:
        conn = _get_state_db()
        now = datetime.now().isoformat()
        cursor = conn.execute(
            "UPDATE pipeline_files SET status = 'pending', error = NULL, stage = NULL, "
            "last_updated = ? WHERE status IN ('error', 'failed')",
            (now,),
        )
        reset_count = cursor.rowcount
        if reset_count > 0:
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


@router.post("/api/pipeline/force-accept")
def force_accept(req: dict) -> dict:
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
            (now, json.dumps(extras), path),
        )
        conn.commit()
        conn.close()
        return {"ok": True, "path": path}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Failed to force-accept: {e}")


@router.post("/api/pipeline/compact")
def compact_state() -> dict:
    """Remove REPLACED and SKIPPED entries from pipeline state."""
    try:
        conn = _get_state_db()
        cursor = conn.execute("DELETE FROM pipeline_files WHERE status IN ('replaced', 'skipped')")
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


@router.post("/api/quick-wins")
def quick_wins() -> dict:
    """Bulk-force AV1 files needing audio or cleanup work to the front of the pipeline queue."""
    data = read_json_safe(MEDIA_REPORT)
    if data is None:
        raise HTTPException(404, "media_report.json not found")

    files = data.get("files", [])
    paths = []
    for f in files:
        if f.get("video", {}).get("codec_raw") != "av1":
            continue
        audio_streams = f.get("audio_streams", [])
        audio_codec_ok = (
            all((a.get("codec_raw") or a.get("codec", "")).lower() in ("eac3", "e-ac-3") for a in audio_streams)
            if audio_streams
            else True
        )
        audio_clean = (
            all(
                i == 0 or (a.get("language") or a.get("detected_language") or "und").lower().strip() in KEEP_LANGS
                for i, a in enumerate(audio_streams)
            )
            if audio_streams
            else True
        )
        subs_ok = all(
            (s.get("language") or s.get("detected_language") or "und").lower().strip() in KEEP_LANGS
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
