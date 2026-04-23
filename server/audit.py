"""JSONL audit log for server write endpoints.

Appends one JSON object per line to ``<STAGING_DIR>/logs/server_audit.jsonl``
for every POST/PUT/PATCH/DELETE request handled by the FastAPI app. Each
record includes:

    ts, method, path, remote_ip, body_summary, status, duration_ms

The body summary is truncated and bytes are decoded leniently so a malformed
payload cannot crash the middleware.
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from paths import STAGING_DIR

AUDIT_LOG_DIR = STAGING_DIR / "logs"
AUDIT_LOG_PATH = AUDIT_LOG_DIR / "server_audit.jsonl"

_WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_BODY_SUMMARY_LIMIT = 512


def _audit_log_path() -> Path:
    """Resolve the audit log path at call time so tests can override env vars."""
    override = os.environ.get("SERVER_AUDIT_LOG")
    if override:
        return Path(override)
    return AUDIT_LOG_PATH


def _summarise_body(raw: bytes) -> str:
    """Return a short, safe summary of a request body for the audit log."""
    if not raw:
        return ""
    try:
        text = raw.decode("utf-8", errors="replace")
    except Exception:
        return f"<{len(raw)} bytes>"
    if len(text) > _BODY_SUMMARY_LIMIT:
        return text[:_BODY_SUMMARY_LIMIT] + "...<truncated>"
    return text


def _append_record(record: dict) -> None:
    """Append a single JSON record to the audit log, creating the dir if needed."""
    path = _audit_log_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError:
        # Never let audit-log failures break the request.
        pass


class AuditLogMiddleware(BaseHTTPMiddleware):
    """Write a JSONL audit record for every write request."""

    async def dispatch(self, request: Request, call_next):
        if request.method not in _WRITE_METHODS:
            return await call_next(request)

        # Capture the body before handing it to downstream handlers. We stash it
        # back into request.scope so FastAPI can still parse it.
        body = await request.body()

        async def _receive() -> dict:
            return {"type": "http.request", "body": body, "more_body": False}

        request = Request(request.scope, _receive)

        started = time.perf_counter()
        status = 500
        response: Response | None = None
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            client = request.client
            remote_ip = client.host if client else ""
            record = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "method": request.method,
                "path": request.url.path,
                "remote_ip": remote_ip,
                "body_summary": _summarise_body(body),
                "status": status,
                "duration_ms": duration_ms,
            }
            _append_record(record)
