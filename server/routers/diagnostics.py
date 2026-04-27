"""Diagnostic endpoints — surface anomalies the user can act on from the UI.

Routes:
    GET /api/diagnostics/size-vs-duration
        Per-file points for the size-vs-duration scatter chart. Lets the user
        spot corrupt / sample / truncated files at a glance: low file size for
        a long expected duration is a strong corruption signal.
"""

import json
import os

from fastapi import APIRouter, HTTPException

from paths import MEDIA_REPORT, STAGING_DIR
from server.helpers import _get_pipeline_state, read_report_cached

router = APIRouter()


_WHITELIST_PATH = os.path.join(str(STAGING_DIR), "control", "density_whitelist.json")


def _load_density_whitelist() -> list[str]:
    """Read control/density_whitelist.json. Returns lowercased substring patterns.

    Files whose filepath contains any of these substrings are flagged
    `density_whitelisted=True` in the chart output, so the frontend can exclude
    them from the "Suspicious" set. Used for legitimately well-compressed
    sitcoms / animated shows that fall below the density threshold.
    """
    if not os.path.exists(_WHITELIST_PATH):
        return []
    try:
        with open(_WHITELIST_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return [p.lower() for p in (data.get("patterns") or []) if isinstance(p, str)]
    except Exception:
        return []


@router.get("/api/diagnostics/size-vs-duration")
def size_vs_duration() -> dict:
    """Return one point per file with size + duration for the scatter chart.

    Each point includes:
        filepath:                full path on the NAS
        filename:                basename for display
        size_bytes:              current on-disk size (post-encode for DONE files)
        file_duration_seconds:   what ffprobe found in the file
        tmdb_runtime_seconds:    TMDb's runtime if known (movies only); else null
        duration_seconds:        preferred Y value (TMDb > file)
        duration_source:         "tmdb" | "file" | "none"
        status:                  pipeline state DB status, or null if untracked
        library_type:            "movie" | "series" | ""
        is_av1:                  whether the on-disk file is already AV1

    The chart caller decides which subset to render. Points with
    duration_seconds <= 0 are still returned so the frontend can show them
    as a "no duration" stripe rather than silently dropping data.
    """
    report = read_report_cached(MEDIA_REPORT)
    if report is None:
        raise HTTPException(404, "media_report.json not found")

    state_data = _get_pipeline_state() or {}
    state_files = state_data.get("files", {}) if isinstance(state_data, dict) else {}
    whitelist = _load_density_whitelist()

    points: list[dict] = []
    for entry in report.get("files", []) or []:
        fp = entry.get("filepath") or ""
        if not fp:
            continue

        size_bytes = int(entry.get("file_size_bytes") or 0)
        file_dur = float(entry.get("duration_seconds") or 0.0)

        tmdb = entry.get("tmdb") or {}
        tmdb_runtime_minutes = tmdb.get("runtime_minutes")
        tmdb_dur = float(tmdb_runtime_minutes) * 60.0 if tmdb_runtime_minutes else 0.0

        if tmdb_dur > 0:
            preferred = tmdb_dur
            source = "tmdb"
        elif file_dur > 0:
            preferred = file_dur
            source = "file"
        else:
            preferred = 0.0
            source = "none"

        pipeline_row = state_files.get(fp) or {}

        fp_low = fp.lower()
        density_whitelisted = any(pat in fp_low for pat in whitelist)

        points.append(
            {
                "filepath": fp,
                "filename": entry.get("filename") or os.path.basename(fp),
                "size_bytes": size_bytes,
                "file_duration_seconds": file_dur,
                "tmdb_runtime_seconds": tmdb_dur if tmdb_dur > 0 else None,
                "duration_seconds": preferred,
                "duration_source": source,
                "status": pipeline_row.get("status"),
                "library_type": entry.get("library_type", ""),
                "is_av1": (entry.get("video") or {}).get("codec_raw") == "av1",
                "density_whitelisted": density_whitelisted,
            }
        )

    return {"points": points, "count": len(points), "whitelist_patterns": whitelist}
