"""API endpoints for the Flagged pane.

Surfaces files in FLAGGED_FOREIGN_AUDIO / FLAGGED_UNDETERMINED / FLAGGED_MANUAL
with the actions:

* ``delete_redownload``  — delete file + ask Radarr/Sonarr to grab again
                            (uses tools.radarr / tools.sonarr clients)
* ``encode_anyway``      — override the flag and encode with current audio
                            (sets status back to PENDING; queue picks up next pass)
* ``dismiss``            — accept as-is, mark DONE, no further work

Routes:
    GET  /api/flagged           - list flagged files with rationale + detected language
    POST /api/flagged/action    - perform action on one flagged file
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from paths import STAGING_DIR
from pipeline.state import FLAGGED_STATUSES, FileStatus

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/flagged", tags=["flagged"])


_STATE_DB = STAGING_DIR / "pipeline_state.db"


# ---------------------------------------------------------------------------
# GET /api/flagged
# ---------------------------------------------------------------------------


@router.get("")
def list_flagged() -> dict[str, Any]:
    """Return all FLAGGED_* files with the data the UI needs to render actions.

    Joins pipeline_state with the media report (for title/year/library_type/
    detected_language) so the UI gets one cohesive payload.
    """
    flagged_values = tuple(s.value for s in FLAGGED_STATUSES)
    placeholders = ",".join("?" * len(flagged_values))

    conn = sqlite3.connect(str(_STATE_DB), timeout=5)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT filepath, status, reason, mode, stage, last_updated
            FROM pipeline_files
            WHERE status IN ({placeholders})
            ORDER BY last_updated DESC
            """,
            flagged_values,
        ).fetchall()
    finally:
        conn.close()

    # Cross-reference with media report for richer context
    report = _load_report_index()
    items = []
    for r in rows:
        fp = r["filepath"]
        entry = report.get(fp) or {}
        tmdb = entry.get("tmdb") or {}
        audio_streams = entry.get("audio_streams") or []
        # Pick the first audio's detection for display
        first_audio = audio_streams[0] if audio_streams else {}
        items.append({
            "filepath": fp,
            "filename": os.path.basename(fp),
            "status": r["status"],
            "reason": r["reason"] or "",
            "mode": r["mode"] or "",
            "stage": r["stage"] or "",
            "last_updated": r["last_updated"],
            # Media-report context
            "library_type": entry.get("library_type") or "",
            "title": tmdb.get("title") or tmdb.get("name") or "",
            "year": tmdb.get("release_year") or tmdb.get("first_air_year"),
            "original_language": tmdb.get("original_language") or "",
            "audio_language_tag": (first_audio.get("language") or "und"),
            "detected_language": first_audio.get("detected_language") or "",
            "detection_confidence": first_audio.get("detection_confidence") or 0,
            "detection_method": first_audio.get("detection_method") or "",
        })
    return {"count": len(items), "items": items}


# ---------------------------------------------------------------------------
# POST /api/flagged/action
# ---------------------------------------------------------------------------


class FlaggedAction(BaseModel):
    """Action requested by the user on a flagged file.

    Three actions: ``delete_redownload`` (delete + Radarr/Sonarr re-grab),
    ``encode_anyway`` (override, set back to PENDING for normal flow),
    ``dismiss`` (accept as-is, mark DONE).
    """

    filepath: str = Field(..., min_length=1)
    action: str = Field(..., pattern="^(delete_redownload|encode_anyway|dismiss)$")


@router.post("/action")
def flagged_action(req: FlaggedAction) -> dict[str, Any]:
    """Perform the chosen action on a flagged file."""
    fp = req.filepath
    if not os.path.exists(fp):
        # File can be missing for ``delete_redownload`` — Radarr/Sonarr
        # already moved/deleted it, we just need to trigger the search.
        if req.action != "delete_redownload":
            raise HTTPException(404, detail=f"file not found on NAS: {fp}")

    if req.action == "dismiss":
        return _action_dismiss(fp)
    if req.action == "encode_anyway":
        return _action_encode_anyway(fp)
    if req.action == "delete_redownload":
        return _action_delete_redownload(fp)
    raise HTTPException(400, detail=f"unknown action: {req.action}")


# ---------------------------------------------------------------------------
# Action implementations
# ---------------------------------------------------------------------------


def _action_dismiss(filepath: str) -> dict[str, Any]:
    """Accept the flagged file as-is. Status -> DONE."""
    _set_status(
        filepath,
        FileStatus.DONE,
        reason="user-dismissed flag (accepted as-is)",
        mode="flagged_action",
    )
    return {"ok": True, "filepath": filepath, "new_status": "done"}


def _action_encode_anyway(filepath: str) -> dict[str, Any]:
    """Override the flag — set back to PENDING so the queue builder picks it up.

    The qualify pre-check inside full_gamut would normally re-flag this. We
    set ``qualify_override=True`` in the row's ``extras`` JSON (durable —
    survives the stage churn as the file moves through fetching/encoding).
    full_gamut reads it via state.get_file() and skips qualification when
    set, so the user's override actually takes effect.
    """
    # Use the pipeline state machine rather than direct SQL so the kwargs
    # land in the extras JSON correctly. ValueError-guarded by set_file.
    from pipeline.state import PipelineState

    state = PipelineState(str(_STATE_DB))
    try:
        state.set_file(
            filepath,
            FileStatus.PENDING,
            reason="user override: encode anyway",
            mode="flagged_action",
            stage="",  # clear any stale stage
            qualify_override=True,  # → extras JSON, survives stage churn
        )
    finally:
        state.close()
    return {"ok": True, "filepath": filepath, "new_status": "pending"}


def _action_delete_redownload(filepath: str) -> dict[str, Any]:
    """Delete the file and trigger Radarr/Sonarr to find a replacement.

    Routing: ``/Series/`` paths -> Sonarr; ``/Movies/`` paths -> Radarr.
    Falls back to title+year match if path-based location lookup fails.
    """
    # Step 1: delete the file (best-effort — if missing already, that's fine)
    delete_result = "missing"
    if os.path.exists(filepath):
        try:
            os.remove(filepath)
            delete_result = "deleted"
        except OSError as exc:
            raise HTTPException(500, detail=f"delete failed: {exc}")

    # Step 2: figure out which Arr to talk to
    is_series = "/Series/" in filepath.replace("\\", "/")
    title, year = _title_and_year_from_path(filepath)

    arr_result: dict[str, Any] = {"queued": False}
    try:
        if is_series:
            from tools import sonarr

            if sonarr.is_configured():
                # Find best-quality profile (highest id wins as a heuristic
                # — user typically creates Quality+ as the latest profile)
                profiles = sonarr.list_quality_profiles()
                target = _pick_target_profile(profiles)
                if target:
                    arr_result = sonarr.upgrade_via_sonarr(
                        filepath=filepath,
                        title=title,
                        year=year,
                        quality_profile_id=int(target["id"]),
                    )
        else:
            from tools import radarr

            if radarr.is_configured():
                profiles = radarr.list_quality_profiles()
                target = _pick_target_profile(profiles)
                if target:
                    arr_result = radarr.upgrade_via_radarr(
                        filepath=filepath,
                        title=title,
                        year=year,
                        quality_profile_id=int(target["id"]),
                    )
    except Exception as exc:  # noqa: BLE001
        # The delete already happened — surface the Arr failure but don't
        # block. User can retry the search manually in Radarr/Sonarr.
        logger.warning(f"Arr re-grab failed for {filepath}: {exc}")
        arr_result = {"queued": False, "error": str(exc)}

    # Step 3: clear the row from pipeline_state — the file's gone; the next
    # scanner run picks up whatever Radarr/Sonarr fetches.
    conn = sqlite3.connect(str(_STATE_DB), timeout=5)
    try:
        conn.execute("DELETE FROM pipeline_files WHERE filepath = ?", (filepath,))
        conn.commit()
    finally:
        conn.close()

    return {
        "ok": True,
        "filepath": filepath,
        "delete": delete_result,
        "arr": arr_result,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _set_status(
    filepath: str,
    status: FileStatus,
    *,
    reason: str = "",
    mode: str = "",
    stage: str = "",
) -> None:
    """Direct DB write — avoid the pipeline.state lock contention because the
    pipeline may be running and we don't want to wait for a long write txn.
    """
    conn = sqlite3.connect(str(_STATE_DB), timeout=5)
    try:
        conn.execute(
            """
            UPDATE pipeline_files
            SET status = ?, reason = ?, mode = ?, stage = ?, last_updated = ?
            WHERE filepath = ?
            """,
            (status.value, reason, mode, stage, time.time(), filepath),
        )
        conn.commit()
    finally:
        conn.close()


def _load_report_index() -> dict[str, dict]:
    """Return {filepath: report_entry} from media_report.json."""
    import json

    p = STAGING_DIR / "media_report.json"
    if not p.exists():
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            rep = json.load(f)
    except Exception:
        return {}
    return {e.get("filepath"): e for e in rep.get("files", []) if e.get("filepath")}


def _title_and_year_from_path(filepath: str) -> tuple[str, int | None]:
    """Best-effort extract title + year from the filepath (used when we
    can't find the file by path in Radarr/Sonarr)."""
    import re

    name = os.path.basename(filepath)
    stem, _ = os.path.splitext(name)
    m = re.search(r"\((\d{4})\)", stem)
    year = int(m.group(1)) if m else None
    title = re.sub(r"\s*\(\d{4}\).*$", "", stem).strip()
    title = re.sub(r"\s+S\d{2}E\d{2}.*$", "", title).strip()
    return title, year


def _pick_target_profile(profiles: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Pick a sensible target quality profile for the re-grab.

    Preference order: ``Quality+`` if present (auto-created by
    tools.radarr_sonarr_setup), otherwise the highest-id profile (typically
    the most-recently-created one in user setups).
    """
    if not profiles:
        return None
    for p in profiles:
        if p.get("name") == "Quality+":
            return p
    return max(profiles, key=lambda p: p.get("id", 0))
