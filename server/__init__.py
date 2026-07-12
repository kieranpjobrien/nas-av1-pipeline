"""
AV1 Pipeline Dashboard Server
==============================
FastAPI app that serves the pipeline dashboard frontend and provides
API endpoints for monitoring pipeline progress and managing control files.

Usage:
    python -m server
    uv run uvicorn server:app --host 0.0.0.0 --port 8000
"""

# Defensive re-set of JSONEncoder defaults (2026-05-23). Mirror of the
# pipeline.__main__ guard. Two segfaults today on independent Python
# processes (supervisor 13:19, uvicorn 13:29) — diagnostic captured
# JSONEncoder.key_separator mutated to 'status' in the supervisor.
# Working hypothesis: hardware/driver-level memory corruption flipping
# interned-string pointers. Process-start reset gives a known-good
# baseline.
import json.encoder as _json_encoder
_json_encoder.JSONEncoder.key_separator = ": "
_json_encoder.JSONEncoder.item_separator = ", "

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from server.audit import AuditLogMiddleware
from server.helpers import FRONTEND_DIST
from server.middleware import BearerTokenMiddleware
from server.process_manager import PROCESS_CONFIGS, ProcessManager

# Re-export for backward compatibility
__all__ = ["app", "PROCESS_CONFIGS"]

app = FastAPI(title="AV1 Pipeline Dashboard")

# --- Middleware (order: outermost first, so audit wraps auth) ---
# Audit runs outermost so it records 401/403 responses from the auth layer too.
app.add_middleware(AuditLogMiddleware)
app.add_middleware(BearerTokenMiddleware)

# --- ProcessManager singleton, accessible via app.state.pm ---
pm = ProcessManager()
app.state.pm = pm

# --- Include routers ---
from server.routers import admin, diagnostics, files, flagged, library, pipeline, process, reclaim, upgrades, ws  # noqa: E402

app.include_router(pipeline.router)
app.include_router(process.router)
app.include_router(library.router)
app.include_router(files.router)
app.include_router(admin.router)
app.include_router(upgrades.router)
app.include_router(flagged.router)
app.include_router(diagnostics.router)
app.include_router(reclaim.router)
app.include_router(ws.router)

# --- Periodic incremental scan: keep the dashboard's media_report reliably fresh ---
# The dashboard reads media_report.json, which drifts STALE after encodes and new
# downloads unless the scanner re-runs — the recurring "stats don't match reality"
# problem (root-caused 2026-07-13). This schedules an INCREMENTAL scan (only
# re-probes files whose path+size+mtime changed) on a timer. It writes through the
# same report_lock the pipeline's update_entry uses, so the two coexist safely.
# Interval via DASHBOARD_SCAN_INTERVAL_S (default 1200s = 20 min; set 0 to disable).
import asyncio  # noqa: E402
import logging  # noqa: E402
import os  # noqa: E402

_SCAN_INTERVAL_S = int(os.environ.get("DASHBOARD_SCAN_INTERVAL_S", "1200"))


@app.on_event("startup")
async def _schedule_periodic_scan() -> None:
    if _SCAN_INTERVAL_S <= 0:
        logging.info("Periodic dashboard scan disabled (DASHBOARD_SCAN_INTERVAL_S<=0)")
        return

    async def _loop() -> None:
        while True:
            await asyncio.sleep(_SCAN_INTERVAL_S)
            try:
                if pm.status("scanner").get("status") != "running":
                    pm.start("scanner")
                    logging.info("Periodic dashboard scan triggered (incremental)")
            except Exception:  # noqa: BLE001 — never let a scan blip kill the loop
                logging.exception("Periodic scan trigger failed")

    asyncio.create_task(_loop())
    logging.info("Periodic dashboard scan scheduled every %ss", _SCAN_INTERVAL_S)


# --- Static file serving (built frontend) ---
if FRONTEND_DIST.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIST), html=True), name="frontend")


def run() -> None:
    """Entry point for `[project.scripts] dashboard = server:run`."""
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
