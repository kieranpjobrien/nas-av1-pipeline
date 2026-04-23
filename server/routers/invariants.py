"""Deep health endpoint backed by ``tools.invariants``.

The ``/api/health`` endpoint in ``admin.py`` is a surface check (paths
reachable, GPU present, pipeline process up). ``/api/health-deep`` runs
the full invariant battery from ``tools.invariants`` and is what the
Incidents card on the dashboard consumes.

Invariants walk the media_report (~50MB) and state DB; they're cheap but
not free, so responses are memoised for 30 seconds.
"""

import time
from datetime import datetime, timezone

from fastapi import APIRouter

from tools.invariants import run_all

router = APIRouter()

_CACHE_TTL_SECS = 30.0
_cache: dict | None = None
_cache_mono: float = 0.0


def _build_payload() -> dict:
    """Run all invariants and assemble the API response."""
    results = run_all()
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "all_green": all(r.ok for r in results),
        "checks": [r.to_dict() for r in results],
    }


@router.get("/api/health-deep")
def get_health_deep() -> dict:
    """Return the full invariant battery. Cached for 30s."""
    global _cache, _cache_mono
    now = time.monotonic()
    if _cache is not None and (now - _cache_mono) < _CACHE_TTL_SECS:
        return _cache
    _cache = _build_payload()
    _cache_mono = now
    return _cache
