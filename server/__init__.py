"""
AV1 Pipeline Dashboard Server
==============================
FastAPI app that serves the pipeline dashboard frontend and provides
API endpoints for monitoring pipeline progress and managing control files.

Usage:
    python -m server
    uv run uvicorn server:app --host 0.0.0.0 --port 8000
"""

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
from server.routers import admin, files, flagged, library, pipeline, process, upgrades, ws  # noqa: E402

app.include_router(pipeline.router)
app.include_router(process.router)
app.include_router(library.router)
app.include_router(files.router)
app.include_router(admin.router)
app.include_router(upgrades.router)
app.include_router(flagged.router)
app.include_router(ws.router)

# --- Static file serving (built frontend) ---
if FRONTEND_DIST.is_dir():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIST), html=True), name="frontend")


def run() -> None:
    """Entry point for `[project.scripts] dashboard = server:run`."""
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
