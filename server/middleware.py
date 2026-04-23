"""Simple bearer-token auth for write methods.

Reads DASHBOARD_TOKEN from env. If unset, auth is disabled (dev default).
If set, any POST/PUT/PATCH/DELETE requires Authorization: Bearer <token>.
GET/HEAD/OPTIONS are always unauthenticated (read-only dashboard polling).
"""

import os

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request


class BearerTokenMiddleware(BaseHTTPMiddleware):
    """Reject write requests lacking a valid bearer token when DASHBOARD_TOKEN is set."""

    def __init__(self, app, token: str | None = None) -> None:
        super().__init__(app)
        # Allow the token to be passed explicitly (tests) or pulled from env.
        self.token = token if token is not None else os.environ.get("DASHBOARD_TOKEN", "")

    async def dispatch(self, request: Request, call_next):
        if request.method in {"GET", "HEAD", "OPTIONS"}:
            return await call_next(request)
        if not self.token:
            # Auth disabled (no token configured) — pass through for local dev.
            return await call_next(request)
        header = request.headers.get("authorization", "")
        if not header.startswith("Bearer "):
            return JSONResponse({"detail": "missing bearer token"}, status_code=401)
        if header[7:] != self.token:
            return JSONResponse({"detail": "invalid bearer token"}, status_code=403)
        return await call_next(request)
