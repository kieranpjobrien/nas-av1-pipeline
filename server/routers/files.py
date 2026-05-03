"""File operations endpoints: rename, delete, detail, and duplicates.

Routes:
    POST /api/file/rename         - rename a file on the NAS
    POST /api/file/delete         - delete a single file
    POST /api/file/grade-accept   - mark a file as Grade-Optimal (MKV tag override)
    POST /api/file/grade-clear    - clear the Grade-Optimal override
    POST /api/file/cq-override    - set a per-file CQ override
    POST /api/file/cq-clear       - clear the CQ override
    GET  /api/file-detail         - cross-reference media report + pipeline state
    GET  /api/duplicates          - find duplicate files with quality scoring
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


@router.post("/api/file/grade-accept")
def grade_accept(req: DeleteFileRequest) -> dict:
    """Stamp ``GRADE_REVIEW=accepted`` into the MKV's global tags.

    The audit treats this as a hard override: bucket forces to ``optimal``
    regardless of what the CQ-vs-target comparison says. Use this for
    too_high files where the user has manually verified the visible quality
    is fine and they don't want a re-download.

    Also patches ``audit_cq.json`` in-place so the dashboard reflects the
    change without waiting for a full audit re-run.
    """
    from paths import NAS_MOVIES, NAS_SERIES, STAGING_DIR
    from pipeline.grade_review import set_grade_review
    from pipeline.mkv_tags import MkvTagWriteError

    norm = os.path.normpath(req.path)
    nas_movies = os.path.normpath(str(NAS_MOVIES))
    nas_series = os.path.normpath(str(NAS_SERIES))
    if not (norm.startswith(nas_movies) or norm.startswith(nas_series)):
        raise HTTPException(403, "Path is outside NAS media directories")
    if not os.path.exists(norm):
        raise HTTPException(404, "File no longer exists on disk — likely deleted or moved since the last scan")

    try:
        set_grade_review(req.path, "accepted")
    except MkvTagWriteError as e:
        # Bubble the real reason ("not a Matroska file or could not be found",
        # "permission denied", etc.) so the toast is useful instead of generic.
        raise HTTPException(422, f"Cannot write tag: {e}") from e

    # Patch the audit sidecar in place so the user sees the result before
    # the next full audit. The audit reader walks 6,000 files; we don't want
    # to make the user wait or trigger a re-audit on every accept click.
    patched = _patch_audit_sidecar(STAGING_DIR / "audit_cq.json", req.path, "optimal", "accepted")
    return {"ok": True, "filepath": req.path, "review_status": "accepted", "audit_patched": patched}


@router.post("/api/file/grade-clear")
def grade_clear(req: DeleteFileRequest) -> dict:
    """Remove ``GRADE_REVIEW`` and ``GRADE_REVIEW_AT`` from the MKV.

    Use this if a prior accept was wrong — the file goes back through the
    normal CQ-vs-target comparison on the next audit. Sidecar patch
    re-bins the file by re-running the comparison against its stamped CQ.
    """
    from paths import NAS_MOVIES, NAS_SERIES, STAGING_DIR
    from pipeline.grade_review import clear_grade_review
    from pipeline.mkv_tags import MkvTagWriteError

    norm = os.path.normpath(req.path)
    nas_movies = os.path.normpath(str(NAS_MOVIES))
    nas_series = os.path.normpath(str(NAS_SERIES))
    if not (norm.startswith(nas_movies) or norm.startswith(nas_series)):
        raise HTTPException(403, "Path is outside NAS media directories")
    if not os.path.exists(norm):
        raise HTTPException(404, "File no longer exists on disk — likely deleted or moved since the last scan")

    try:
        clear_grade_review(req.path)
    except MkvTagWriteError as e:
        raise HTTPException(422, f"Cannot clear tag: {e}") from e

    # Re-derive the bucket from the stamped CQ so the sidecar is consistent.
    new_bucket = _rebucket_from_sidecar(STAGING_DIR / "audit_cq.json", req.path)
    patched = _patch_audit_sidecar(STAGING_DIR / "audit_cq.json", req.path, new_bucket, None)
    return {"ok": True, "filepath": req.path, "review_status": None, "bucket": new_bucket, "audit_patched": patched}


def _rebucket_from_sidecar(sidecar_path, filepath: str) -> str:
    """Read the file's current_cq + target_cq from the sidecar and re-derive
    its natural bucket (ignoring any review override). Used by grade-clear.
    """
    import json

    try:
        with open(sidecar_path, encoding="utf-8") as f:
            audit = json.load(f)
    except (OSError, json.JSONDecodeError):
        return "unknown"
    for r in audit.get("results", []):
        if r.get("filepath") == filepath:
            cur, tgt = r.get("current_cq"), r.get("target_cq")
            if cur is None:
                return "unknown"
            if cur == tgt:
                return "optimal"
            return "too_low" if cur < tgt else "too_high"
    return "unknown"


def _patch_audit_sidecar(sidecar_path, filepath: str, new_bucket: str, review_status: str | None) -> bool:
    """Update the per-file row + bucket counts in audit_cq.json in place.

    Returns True if a row was patched, False if the file isn't in the audit
    yet (in which case the next full audit will pick it up — not an error).
    """
    import json

    try:
        with open(sidecar_path, encoding="utf-8") as f:
            audit = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False

    results = audit.get("results", [])
    target_row = None
    for r in results:
        if r.get("filepath") == filepath:
            target_row = r
            break
    if target_row is None:
        return False

    old_bucket = target_row.get("bucket")
    if old_bucket == new_bucket and target_row.get("review_status") == review_status:
        return True  # already in the desired state

    target_row["bucket"] = new_bucket
    target_row["review_status"] = review_status

    # Adjust the bucket counts so the hero stat on the dashboard matches.
    buckets = audit.setdefault("buckets", {})
    if old_bucket and old_bucket in buckets and buckets[old_bucket] > 0:
        buckets[old_bucket] = buckets[old_bucket] - 1
    buckets[new_bucket] = buckets.get(new_bucket, 0) + 1

    try:
        with open(sidecar_path, "w", encoding="utf-8") as f:
            json.dump(audit, f, indent=2)
    except OSError:
        return False
    return True


@router.post("/api/file/cq-override")
def cq_override(req: dict) -> dict:
    """Set the per-file CQ override that the encoder will use instead of
    the grade-derived target. Body: ``{path, cq}``.

    The dashboard's "Proposed CQ: [-] N [+]" buttons hit this endpoint
    each time the user nudges. Stored in the pipeline_state.db extras
    blob so it survives restarts and is visible in the audit history.
    """
    from paths import NAS_MOVIES, NAS_SERIES, PIPELINE_STATE_DB
    from pipeline.cq_override import CQ_MAX, CQ_MIN, set_override

    path = req.get("path")
    cq = req.get("cq")
    if not path or not isinstance(cq, int):
        raise HTTPException(400, "path (str) and cq (int) required")

    norm = os.path.normpath(path)
    nas_movies = os.path.normpath(str(NAS_MOVIES))
    nas_series = os.path.normpath(str(NAS_SERIES))
    if not (norm.startswith(nas_movies) or norm.startswith(nas_series)):
        raise HTTPException(403, "Path is outside NAS media directories")

    if not (CQ_MIN <= cq <= CQ_MAX):
        raise HTTPException(400, f"cq must be in [{CQ_MIN}, {CQ_MAX}]; got {cq}")

    try:
        ok = set_override(PIPELINE_STATE_DB, path, cq)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    if not ok:
        raise HTTPException(500, "Failed to persist CQ override")
    return {"ok": True, "filepath": path, "cq_override": cq}


@router.post("/api/file/cq-clear")
def cq_clear(req: dict) -> dict:
    """Remove the per-file CQ override. The encoder falls back to the
    grade-derived target on the next encode."""
    from paths import NAS_MOVIES, NAS_SERIES, PIPELINE_STATE_DB
    from pipeline.cq_override import clear_override

    path = req.get("path")
    if not path:
        raise HTTPException(400, "path required")
    norm = os.path.normpath(path)
    nas_movies = os.path.normpath(str(NAS_MOVIES))
    nas_series = os.path.normpath(str(NAS_SERIES))
    if not (norm.startswith(nas_movies) or norm.startswith(nas_series)):
        raise HTTPException(403, "Path is outside NAS media directories")

    ok = clear_override(PIPELINE_STATE_DB, path)
    if not ok:
        raise HTTPException(500, "Failed to clear CQ override")
    return {"ok": True, "filepath": path}


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
    """Cross-reference media report + pipeline state for a single file.

    Adds a ``cq`` block with the grade-derived target, the user's
    override (if any), and the effective value the encoder would use.
    The dashboard's "Proposed CQ: [-] N [+]" widget reads from here.
    """
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

    # CQ block — proposed target + override + effective. Cheap (no I/O
    # except a single state-DB SELECT for the override) so we can inline
    # it in every file-detail response.
    if result.get("media"):
        try:
            from paths import PIPELINE_STATE_DB
            from pipeline.cq_override import compute_proposed_cq, get_override

            entry = dict(result["media"])
            entry["filepath"] = path  # ensure compute_proposed_cq sees the path
            proposed = compute_proposed_cq(entry)
            override = get_override(PIPELINE_STATE_DB, path)
            result["cq"] = {
                "proposed_cq": proposed["cq"],
                "base_cq": proposed["base_cq"],
                "cq_offset": proposed["cq_offset"],
                "content_grade": proposed["content_grade"],
                "res_key": proposed["res_key"],
                "override": override,
                "effective_cq": override if override is not None else proposed["cq"],
            }
        except Exception:
            # Best-effort — if grade derivation fails (missing TMDb etc.)
            # the UI just doesn't render the proposed-CQ widget.
            pass

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
