"""Process management endpoints for starting, stopping, and monitoring subprocesses.

Routes:
    GET  /api/process/{name}/status - process status
    POST /api/process/{name}/start  - start a process
    POST /api/process/{name}/stop   - gracefully stop a process
    POST /api/process/{name}/kill   - force-kill a process
    GET  /api/process/{name}/logs   - recent log lines
"""

from fastapi import APIRouter, HTTPException, Request

from server.process_manager import VALID_PROCESS_NAMES

router = APIRouter()


def _get_pm(request: Request):
    """Get the ProcessManager singleton from app state."""
    return request.app.state.pm


@router.get("/api/process/{name}/status")
def get_process_status(name: str, request: Request) -> dict:
    """Return the current status of a named process."""
    if name not in VALID_PROCESS_NAMES:
        raise HTTPException(404, f"Unknown process: {name}")
    return _get_pm(request).status(name)


@router.post("/api/process/{name}/start")
def start_process(name: str, request: Request) -> dict:
    """Start a named process."""
    if name not in VALID_PROCESS_NAMES:
        raise HTTPException(404, f"Unknown process: {name}")
    result = _get_pm(request).start(name)
    if not result["ok"]:
        raise HTTPException(409, result["error"])
    return result


@router.post("/api/process/{name}/stop")
def stop_process(name: str, request: Request) -> dict:
    """Gracefully stop a named process."""
    if name not in VALID_PROCESS_NAMES:
        raise HTTPException(404, f"Unknown process: {name}")
    result = _get_pm(request).stop(name)
    if not result["ok"]:
        raise HTTPException(409, result["error"])
    return result


@router.post("/api/process/{name}/kill")
def kill_process(name: str, request: Request) -> dict:
    """Force-kill a named process by finding matching OS processes."""
    if name not in VALID_PROCESS_NAMES:
        raise HTTPException(404, f"Unknown process: {name}")
    result = _get_pm(request).force_kill(name)
    if not result["ok"]:
        raise HTTPException(409, result["error"])
    return result


@router.get("/api/process/{name}/logs")
def get_process_logs(name: str, request: Request, last_n: int = 50) -> dict:
    """Return recent log lines for a named process."""
    if name not in VALID_PROCESS_NAMES:
        raise HTTPException(404, f"Unknown process: {name}")
    return {"lines": _get_pm(request).get_logs(name, last_n)}
