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


# Processes that drive NVENC. Only one may run at a time: the RTX 4080 has dual
# NVENC physically, but two concurrent encodes BSOD'd this box in production
# (rule 9b). The pipeline's own semaphore is a threading.Semaphore — in-process
# only — so it cannot see the reclaim/de-bloat daemon, which runs its own
# av1_nvenc encodes from a separate process. Before 2026-07-25 the dashboard's
# De-bloat "Start" button would happily launch one alongside a live pipeline.
_GPU_PROCESSES = ("pipeline", "reclaim")


def _gpu_conflict(name: str, pm) -> str | None:
    """Return a human-readable reason if starting ``name`` would put a second
    NVENC encoder on the GPU, else None."""
    if name not in _GPU_PROCESSES:
        return None
    other = "reclaim" if name == "pipeline" else "pipeline"

    # 1. Sibling started by this server.
    try:
        if (pm.status(other) or {}).get("status") == "running":
            return f"{other} is already running — one NVENC encode at a time (rule 9b)"
    except Exception:  # noqa: BLE001
        pass

    # 2. Sibling started detached from a terminal (the normal way the pipeline is
    #    run here), so it won't be in the process manager's own dict.
    try:
        from pipeline.process_registry import ProcessRegistry  # noqa: PLC0415

        for entry in ProcessRegistry().list_active() or []:
            if entry.get("role") == other:
                return (
                    f"{other} is running detached (pid={entry.get('pid')}) — "
                    f"one NVENC encode at a time (rule 9b)"
                )
    except Exception:  # noqa: BLE001
        pass

    # 3. Starting reclaim while the pipeline has work in flight. reclaim does not
    #    register itself, so this is the backstop for the direction that matters.
    if name == "reclaim":
        try:
            import sqlite3  # noqa: PLC0415

            from paths import PIPELINE_STATE_DB  # noqa: PLC0415
            from pipeline.state import ACTIVE_STATUSES  # noqa: PLC0415

            conn = sqlite3.connect(str(PIPELINE_STATE_DB), timeout=5)
            try:
                placeholders = ",".join("?" * len(ACTIVE_STATUSES))
                n = conn.execute(
                    f"SELECT COUNT(*) FROM pipeline_files WHERE status IN ({placeholders})",
                    [s.value for s in ACTIVE_STATUSES],
                ).fetchone()[0]
            finally:
                conn.close()
            if n:
                return (
                    f"the convert pipeline has {n} file(s) in flight — "
                    f"one NVENC encode at a time (rule 9b)"
                )
        except Exception:  # noqa: BLE001
            pass
    return None


@router.post("/api/process/{name}/start")
def start_process(name: str, request: Request) -> dict:
    """Start a named process."""
    if name not in VALID_PROCESS_NAMES:
        raise HTTPException(404, f"Unknown process: {name}")
    pm = _get_pm(request)
    conflict = _gpu_conflict(name, pm)
    if conflict:
        raise HTTPException(409, f"Refusing to start {name}: {conflict}")
    result = pm.start(name)
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
