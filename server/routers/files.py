"""File operations endpoints: rename, delete, detail, and duplicates.

Routes:
    POST /api/file/rename         - rename a file on the NAS
    POST /api/file/delete         - delete a single file
    POST /api/file/grade-accept   - mark a file as Grade-Optimal (MKV tag override)
    POST /api/file/grade-clear    - clear the Grade-Optimal override
    POST /api/file/cq-override    - set a per-file CQ override
    POST /api/file/cq-clear       - clear the CQ override
    POST /api/file/requeue        - reset a single file to pending so the encoder picks it up
    POST /api/files/requeue-batch - same, for many filepaths at once
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
    from paths import NAS_MOVIES, NAS_SERIES

    path = req.get("path")
    new_name = req.get("new_name")
    if not path or not new_name:
        raise HTTPException(400, "path and new_name required")

    # NAS-membership guard (2026-06-05 security fix). Mirror delete_file:
    # refuse to rename anything outside the NAS media dirs, so a stray/
    # malicious path can't touch arbitrary server files.
    norm = os.path.normpath(path)
    nas_movies = os.path.normpath(str(NAS_MOVIES))
    nas_series = os.path.normpath(str(NAS_SERIES))
    if not (norm.startswith(nas_movies) or norm.startswith(nas_series)):
        raise HTTPException(403, "Path is outside NAS media directories")

    # Sanitise new_name — must be a plain filename, no path separators or
    # traversal. Without this, new_name='../../staging/x' escapes source_dir.
    if (os.sep in new_name or "/" in new_name or new_name.startswith(".")
            or new_name in ("", os.curdir, os.pardir)):
        raise HTTPException(400, "new_name must be a plain filename (no path separators)")

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

    Also patches the per-file ``audit`` field in ``media_report.json`` in
    place so the dashboard reflects the change without waiting for a full
    audit re-run. 2026-05-11: audit lives in media_report now, not a sidecar.
    """
    from paths import NAS_MOVIES, NAS_SERIES
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

    patched = _patch_audit_in_report(req.path, "optimal", "accepted")
    return {"ok": True, "filepath": req.path, "review_status": "accepted", "audit_patched": patched}


@router.post("/api/file/grade-clear")
def grade_clear(req: DeleteFileRequest) -> dict:
    """Remove ``GRADE_REVIEW`` and ``GRADE_REVIEW_AT`` from the MKV.

    Use this if a prior accept was wrong — the file goes back through the
    normal CQ-vs-target comparison on the next audit. In-place patch
    re-bins the file by re-running the comparison against its stamped CQ.
    """
    from paths import NAS_MOVIES, NAS_SERIES
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

    new_bucket = _rebucket_from_report(req.path)
    patched = _patch_audit_in_report(req.path, new_bucket, None)
    return {"ok": True, "filepath": req.path, "review_status": None, "bucket": new_bucket, "audit_patched": patched}


def _rebucket_from_report(filepath: str) -> str:
    """Read the file's current_cq + target_cq from media_report.json's
    per-file ``audit`` field and re-derive its natural bucket (ignoring
    any review override). Used by grade-clear after the user retracts an
    earlier "accept this too-high file" decision.
    """
    from tools.report_lock import read_report

    try:
        rep = read_report()
    except Exception:
        return "unknown"
    for f in rep.get("files", []):
        if f.get("filepath") != filepath:
            continue
        a = f.get("audit") or {}
        cur, tgt = a.get("current_cq"), a.get("target_cq")
        if cur is None:
            return "unknown"
        if cur == tgt:
            return "optimal"
        return "too_low" if cur < tgt else "too_high"
    return "unknown"


def _patch_audit_in_report(filepath: str, new_bucket: str, review_status: str | None) -> bool:
    """Update the per-file ``audit`` blob + the top-level ``audit_summary``
    bucket counts in media_report.json. Single-source-of-truth replacement
    for the old audit_cq.json sidecar patch (2026-05-11 — see commit log
    for why the sidecar was killed). Returns True if a row was patched,
    False if the file isn't in the audit yet (next full audit picks it up).
    """
    from tools.report_lock import patch_report

    flag = {"patched": False}

    def _fn(rep: dict) -> None:
        for f in rep.get("files", []):
            if f.get("filepath") != filepath:
                continue
            audit_blob = f.get("audit")
            if audit_blob is None:
                # Not in the audit yet — let the next full audit run pick it up.
                return
            old_bucket = audit_blob.get("bucket")
            if old_bucket == new_bucket and audit_blob.get("review_status") == review_status:
                flag["patched"] = True  # already in the desired state
                return
            audit_blob["bucket"] = new_bucket
            audit_blob["review_status"] = review_status
            # Adjust top-level bucket counts so the hero stats match.
            summary = rep.setdefault("audit_summary", {})
            buckets = summary.setdefault("buckets", {})
            if old_bucket and buckets.get(old_bucket, 0) > 0:
                buckets[old_bucket] = buckets[old_bucket] - 1
            buckets[new_bucket] = buckets.get(new_bucket, 0) + 1
            flag["patched"] = True
            return

    try:
        patch_report(_fn)
    except Exception:
        return False
    return flag["patched"]


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


@router.post("/api/file/requeue")
def file_requeue(req: dict) -> dict:
    """Reset a single file's pipeline_state row to ``pending`` so the
    orchestrator picks it up on its next queue-build pass.

    Used by the Inspector's "Queue for re-encode" button. Body:
    ``{path, reason?}``. The reason is stored in the row's ``error``
    column (which the orchestrator clears on success) so the user can
    see in audit history *why* the file got requeued.

    Idempotent: if the file is already pending or in flight, this
    no-ops gracefully.
    """
    import json as _json
    import sqlite3 as _sqlite3

    from paths import NAS_MOVIES, NAS_SERIES, PIPELINE_STATE_DB

    path = req.get("path")
    if not path:
        raise HTTPException(400, "path required")
    norm = os.path.normpath(path)
    nas_movies = os.path.normpath(str(NAS_MOVIES))
    nas_series = os.path.normpath(str(NAS_SERIES))
    if not (norm.startswith(nas_movies) or norm.startswith(nas_series)):
        raise HTTPException(403, "Path is outside NAS media directories")
    if not os.path.exists(norm):
        raise HTTPException(404, "File no longer exists on disk")

    reason = (req.get("reason") or "manual requeue from dashboard")[:200]
    # Allow the user to override the circuit breaker. Without this flag we
    # refuse to revive a ``flagged_corrupt`` row — that's the bug that let
    # Ford v Ferrari run for 10 attempts (the breaker kept transitioning
    # the row to flagged_corrupt and the unguarded requeue button kept
    # flipping it back to pending). The user has to consciously opt in by
    # passing ``{"force_flagged": true}`` after re-acquiring the source.
    force_flagged = bool(req.get("force_flagged") or False)

    try:
        con = _sqlite3.connect(str(PIPELINE_STATE_DB))
        cur = con.cursor()
        row = cur.execute(
            "SELECT status, extras FROM pipeline_files WHERE filepath = ?", (path,)
        ).fetchone()
        if row is None:
            # File was never in the queue — insert a pending row so it gets picked up.
            # Stamp force_reencode so the orchestrator re-encodes even if the file
            # is already AV1 (categorise_entry would otherwise route it to
            # gap_filler/skip and the user's queue action would be a no-op).
            cur.execute(
                "INSERT INTO pipeline_files (filepath, status, extras, reason) "
                "VALUES (?, 'pending', ?, ?)",
                (path, _json.dumps({"force_reencode": True}), reason),
            )
            new_status = "pending"
        else:
            cur_status = (row[0] or "").lower()
            # Skip files already in flight — flipping status under a live encode
            # corrupts the pipeline_state schema invariants (rule 8 / 11).
            if cur_status in ("processing", "encoding", "fetching", "uploading"):
                con.close()
                raise HTTPException(
                    409, f"File is currently {cur_status}; wait for it to finish or fail."
                )
            # Circuit-breaker guard — refuse to revive a flagged_corrupt row
            # without explicit force. The breaker exists to halt a loop; if
            # the dashboard can silently undo it the breaker is a fiction.
            if cur_status == "flagged_corrupt" and not force_flagged:
                con.close()
                raise HTTPException(
                    409,
                    "File is flagged_corrupt (circuit breaker hit). "
                    "Re-acquire the source and pass force_flagged=true to retry.",
                )
            # Preserve any extras (encode_params_used, detected_audio, ...) so
            # the next encode reuses them. Add force_reencode=true on top so
            # categorise_entry routes AV1 files to full_gamut instead of skip.
            # 2026-05-13: reset breaker counters too — user-initiated retry
            # means "give it a clean shot, the issue is fixed". Otherwise a
            # file at refuse_count=2 sits one cycle from terminal forever,
            # the no_elevated_breaker_counters invariant fires forever, and
            # the user has no way to clear it without poking the DB by hand.
            try:
                extras = _json.loads(row[1] or "{}")
            except (TypeError, ValueError, _json.JSONDecodeError):
                extras = {}
            extras["force_reencode"] = True
            extras["compliance_refuse_count"] = 0
            extras["integrity_failure_count"] = 0
            # Clear prep cache: prepare_for_encode short-circuits past
            # the new prep flow when prep_done=True. Stale prep_done
            # from a pre-architectural-fix attempt (2026-05-12 / earlier)
            # means the new local-strip / source-integrity steps NEVER
            # run — Any Given Sunday hit this at 01:29:02 (PREP MISS,
            # 5 foreign subs survived). Force a fresh prep on every
            # requeue.
            extras["prep_done"] = False
            extras.pop("prep_data", None)
            cur.execute(
                "UPDATE pipeline_files SET status='pending', stage=NULL, error=NULL, "
                "reason=?, extras=? WHERE filepath = ?",
                (reason, _json.dumps(extras), path),
            )
            new_status = "pending (was " + cur_status + ")"
        con.commit()
        con.close()
    except _sqlite3.Error as e:
        raise HTTPException(500, f"State DB error: {e}") from e

    return {"ok": True, "filepath": path, "status": new_status}


@router.post("/api/files/requeue-batch")
def files_requeue_batch(req: dict) -> dict:
    """Bulk-requeue many files in a single transaction.

    Body: ``{paths: [str, ...], reason?: str}``.

    Used by the Library page's bulk-select bar. Each path is validated
    individually; failures are collected per-path so a single bad row
    doesn't abort the whole batch. Returns a summary with ok/failed
    counts and per-path detail for the failures.
    """
    import json as _json
    import sqlite3 as _sqlite3

    from paths import NAS_MOVIES, NAS_SERIES, PIPELINE_STATE_DB

    paths = req.get("paths") or []
    if not isinstance(paths, list) or not paths:
        raise HTTPException(400, "paths must be a non-empty list of strings")
    if len(paths) > 5000:
        # Discipline: a 5k+ batch usually means a UI bug shipped a "select all"
        # that fired across the entire library. Fail loud rather than silently
        # mutating thousands of rows.
        raise HTTPException(400, "batch too large (>5000); split into smaller batches")
    reason = (req.get("reason") or "bulk requeue from dashboard")[:200]
    # See file_requeue: bulk requeue must also refuse flagged_corrupt rows
    # by default so the user can't accidentally revive the entire breaker
    # cohort with a "select all + requeue" click.
    force_flagged = bool(req.get("force_flagged") or False)

    nas_movies = os.path.normpath(str(NAS_MOVIES))
    nas_series = os.path.normpath(str(NAS_SERIES))

    queued = 0
    skipped: list[dict] = []
    try:
        con = _sqlite3.connect(str(PIPELINE_STATE_DB))
        cur = con.cursor()
        for raw_path in paths:
            if not isinstance(raw_path, str) or not raw_path:
                skipped.append({"path": raw_path, "reason": "invalid path"})
                continue
            norm = os.path.normpath(raw_path)
            if not (norm.startswith(nas_movies) or norm.startswith(nas_series)):
                skipped.append({"path": raw_path, "reason": "outside NAS media dirs"})
                continue
            if not os.path.exists(norm):
                skipped.append({"path": raw_path, "reason": "file missing"})
                continue
            row = cur.execute(
                "SELECT status, extras FROM pipeline_files WHERE filepath = ?", (raw_path,)
            ).fetchone()
            if row is None:
                # Stamp force_reencode so already-AV1 files don't get silently
                # routed to gap_filler/skip in categorise_entry (see single-file
                # requeue for the full rationale).
                cur.execute(
                    "INSERT INTO pipeline_files (filepath, status, extras, reason) "
                    "VALUES (?, 'pending', ?, ?)",
                    (raw_path, _json.dumps({"force_reencode": True}), reason),
                )
                queued += 1
                continue
            cur_status = (row[0] or "").lower()
            if cur_status in ("processing", "encoding", "fetching", "uploading"):
                skipped.append({"path": raw_path, "reason": f"in flight ({cur_status})"})
                continue
            if cur_status == "flagged_corrupt" and not force_flagged:
                skipped.append({
                    "path": raw_path,
                    "reason": "flagged_corrupt (breaker hit) — pass force_flagged=true to override",
                })
                continue
            try:
                extras = _json.loads(row[1] or "{}")
            except (TypeError, ValueError, _json.JSONDecodeError):
                extras = {}
            # Same reset as single requeue — see file_requeue for rationale.
            extras["force_reencode"] = True
            extras["compliance_refuse_count"] = 0
            extras["integrity_failure_count"] = 0
            extras["prep_done"] = False
            extras.pop("prep_data", None)
            cur.execute(
                "UPDATE pipeline_files SET status='pending', stage=NULL, error=NULL, "
                "reason=?, extras=? WHERE filepath = ?",
                (reason, _json.dumps(extras), raw_path),
            )
            queued += 1
        con.commit()
        con.close()
    except _sqlite3.Error as e:
        raise HTTPException(500, f"State DB error: {e}") from e

    return {
        "ok": True,
        "queued": queued,
        "skipped": len(skipped),
        "skipped_detail": skipped[:20],  # cap so the response stays small
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
