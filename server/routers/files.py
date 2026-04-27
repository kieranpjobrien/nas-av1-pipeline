"""File operations endpoints: rename, delete, detail, and duplicates.

Routes:
    POST /api/file/rename   - rename a file on the NAS
    POST /api/file/delete   - delete a single file
    GET  /api/file-detail   - cross-reference media report + pipeline state
    GET  /api/duplicates    - find duplicate files with quality scoring
"""

import os

from fastapi import APIRouter, HTTPException

from paths import MEDIA_REPORT
from server.helpers import _get_pipeline_state, invalidate_report_cache, read_report_cached
from server.models import DeleteFileRequest

router = APIRouter()


@router.post("/api/file/rename")
def rename_file(req: dict) -> dict:
    """Rename a file on the NAS.

    Body: {path, new_name}.
    """
    path = req.get("path")
    new_name = req.get("new_name")
    if not path or not new_name:
        raise HTTPException(400, "path and new_name required")
    if not os.path.exists(path):
        raise HTTPException(404, f"File not found: {path}")

    source_dir = os.path.dirname(path)
    new_path = os.path.join(source_dir, new_name)

    if os.path.exists(new_path):
        raise HTTPException(409, f"Target already exists: {new_name}")

    try:
        os.rename(path, new_path)
        try:
            from pipeline.report import update_entry

            update_entry(new_path, "movie" if "Movies" in new_path else "series")
            # The update_entry call rewrites media_report.json in-process, so
            # the mtime-stat check will catch it next read — but clearing now
            # means this same request's follow-up reads hit a fresh parse
            # instead of a stale cached dict.
            invalidate_report_cache()
        except Exception:
            pass
        return {"ok": True, "old": path, "new": new_path}
    except Exception as e:
        raise HTTPException(500, f"Rename failed: {e}")


@router.post("/api/file/delete")
def delete_file(req: DeleteFileRequest) -> dict:
    """Delete a single file. Only allows paths within NAS media directories.

    Also drops the corresponding pipeline_state row (if any) so the queue
    builder doesn't see a phantom path on the next pass, and invalidates the
    media-report cache so subsequent reads don't return the deleted file.
    """
    from paths import NAS_MOVIES, NAS_SERIES, PIPELINE_STATE_DB

    norm = os.path.normpath(req.path)
    nas_movies = os.path.normpath(str(NAS_MOVIES))
    nas_series = os.path.normpath(str(NAS_SERIES))

    if not (norm.startswith(nas_movies) or norm.startswith(nas_series)):
        raise HTTPException(403, "Path is outside NAS media directories")

    if not os.path.exists(norm):
        raise HTTPException(404, "File not found")

    try:
        os.remove(norm)
    except OSError as e:
        raise HTTPException(500, f"Delete failed: {e}")

    state_dropped = 0
    try:
        import sqlite3 as _sqlite3

        if os.path.exists(PIPELINE_STATE_DB):
            _conn = _sqlite3.connect(PIPELINE_STATE_DB)
            _cur = _conn.cursor()
            _cur.execute("DELETE FROM pipeline_files WHERE filepath = ?", (req.path,))
            state_dropped = _cur.rowcount
            _conn.commit()
            _conn.close()
    except Exception:
        # Disk delete already succeeded; DB cleanup is best-effort. The next
        # scanner run reconciles either way.
        pass

    # Drop the entry from media_report.json so chart / library views don't keep
    # showing the deleted file until the next scanner run. Best-effort — the
    # next scan reconciles regardless.
    report_dropped = False
    try:
        from pipeline.report import remove_entry as _remove_entry

        report_dropped = _remove_entry(req.path)
    except Exception:
        pass

    invalidate_report_cache()

    return {
        "ok": True,
        "deleted": req.path,
        "state_rows_dropped": state_dropped,
        "report_dropped": report_dropped,
    }


@router.get("/api/file/siblings")
def get_file_siblings(path: str) -> dict:
    """List files that sit alongside the given media file (sub sidecars, artwork, nfo, etc.).

    Used by the Library inspector's "Open in file manager" panel.
    """
    from paths import NAS_MOVIES, NAS_SERIES

    norm = os.path.normpath(path)
    nas_movies = os.path.normpath(str(NAS_MOVIES))
    nas_series = os.path.normpath(str(NAS_SERIES))
    if not (norm.startswith(nas_movies) or norm.startswith(nas_series)):
        raise HTTPException(403, "Path is outside NAS media directories")

    parent = os.path.dirname(norm)
    if not os.path.isdir(parent):
        raise HTTPException(404, f"Parent dir not found: {parent}")

    def classify(name: str, full: str) -> str:
        if os.path.isdir(full):
            return "dir"
        low = name.lower()
        if low.endswith((".mkv", ".mp4", ".avi", ".mov", ".m4v", ".webm", ".ts")):
            return "video"
        if low.endswith((".srt", ".ass", ".sub", ".idx", ".vtt", ".ssa")):
            return "sub"
        if low.endswith((".jpg", ".jpeg", ".png", ".webp")):
            return "art"
        if low.endswith((".nfo", ".xml", ".json")):
            return "meta"
        if low.endswith((".txt", ".log")):
            return "text"
        return "other"

    items = []
    try:
        for name in sorted(os.listdir(parent)):
            full = os.path.join(parent, name)
            try:
                is_dir = os.path.isdir(full)
                size = 0 if is_dir else os.path.getsize(full)
            except OSError:
                continue
            items.append(
                {
                    "name": name,
                    "kind": classify(name, full),
                    "is_dir": is_dir,
                    "size_bytes": size,
                    "current": os.path.normpath(full).lower() == norm.lower(),
                }
            )
    except OSError as e:
        raise HTTPException(500, f"Listing failed: {e}")

    return {"parent": parent, "items": items}


@router.get("/api/file-detail")
def get_file_detail(path: str) -> dict:
    """Cross-reference media report + pipeline state for a single file."""
    result: dict = {"path": path, "media": None, "pipeline": None}

    report_data = read_report_cached(MEDIA_REPORT)
    if report_data:
        norm = os.path.normpath(path).lower()
        for entry in report_data.get("files", []):
            if os.path.normpath(entry.get("filepath", "")).lower() == norm:
                result["media"] = entry
                break

    state_data = _get_pipeline_state()
    if state_data and "files" in state_data:
        result["pipeline"] = state_data["files"].get(path)

    return result


@router.get("/api/duplicates")
def get_duplicates() -> dict:
    """Find duplicate files using title+duration matching with quality scoring."""
    from collections import defaultdict

    from tools.duplicates import find_title_duration_dupes, pick_best, score_file

    data = read_report_cached(MEDIA_REPORT)
    if data is None:
        raise HTTPException(404, "media_report.json not found")
    files = data.get("files", [])
    file_lookup = {f["filepath"]: f for f in files}

    raw_dupes = find_title_duration_dupes(files, same_dir=True)
    if not raw_dupes:
        return {"groups": [], "total_groups": 0, "total_dupes": 0, "wasted_gb": 0}

    by_group: dict[str, list] = defaultdict(list)
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
            members.append(
                {
                    "filepath": rec["filepath"],
                    "filename": rec.get("filename", os.path.basename(rec["filepath"])),
                    "file_size_gb": rec.get("file_size_gb", 0),
                    "duration_seconds": rec.get("duration_seconds", 0),
                    "codec": rec.get("video", {}).get("codec", ""),
                    "resolution": rec.get("video", {}).get("resolution_class", ""),
                    "score": score_file(rec),
                    "keep": rec["filepath"] == keeper_path,
                }
            )
        members.sort(key=lambda m: -m["score"])
        groups.append(
            {
                "group_id": gid,
                "title": rows[0].get("normalized_title", ""),
                "members": members,
                "wasted_gb": round(wasted, 3),
            }
        )

    groups.sort(key=lambda g: -g["wasted_gb"])
    return {
        "groups": groups,
        "total_groups": len(groups),
        "total_dupes": sum(len(g["members"]) - 1 for g in groups),
        "wasted_gb": round(total_wasted, 2),
    }
