"""Sonarr REST API client — minimal surface for the upgrade recommender.

Mirror of ``tools.radarr`` but for series. Sonarr's /api/v3 endpoints are
similar to Radarr's (it's a fork), but the resource names differ: ``series``
instead of ``movie``, ``SeriesSearch``/``SeasonSearch`` instead of
``MoviesSearch``. Quality-profile management is identical.

Configuration
-------------
Environment variables (read at call time, not import):

    SONARR_URL        e.g. http://192.168.4.43:27483
    SONARR_API_KEY    from Sonarr → Settings → General → Security → API Key
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SONARR_URL_ENV = "SONARR_URL"
SONARR_API_KEY_ENV = "SONARR_API_KEY"


class SonarrNotConfigured(RuntimeError):
    """Raised when SONARR_URL / SONARR_API_KEY aren't set."""


class SonarrError(RuntimeError):
    """Raised when Sonarr returns a non-2xx response."""


def _require_config() -> tuple[str, str]:
    url = (os.environ.get(SONARR_URL_ENV) or "").rstrip("/")
    key = os.environ.get(SONARR_API_KEY_ENV) or ""
    if not url or not key:
        raise SonarrNotConfigured(
            "SONARR_URL and/or SONARR_API_KEY not set."
        )
    return url, key


def _request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    body: dict[str, Any] | list | None = None,
    timeout: float = 15.0,
) -> Any:
    url, key = _require_config()
    full = f"{url}{path}"
    if params:
        full = f"{full}?{urllib.parse.urlencode(params)}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        full,
        method=method,
        data=data,
        headers={
            "X-Api-Key": key,
            "Accept": "application/json",
            **({"Content-Type": "application/json"} if data else {}),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            if not raw:
                return None
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        try:
            body_txt = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            body_txt = ""
        raise SonarrError(f"{e.code} {e.reason} — {method} {path} — {body_txt}") from e
    except urllib.error.URLError as e:
        raise SonarrError(f"connection failed: {method} {full} — {e.reason}") from e


def is_configured() -> bool:
    """True when both env vars are set."""
    return bool(os.environ.get(SONARR_URL_ENV) and os.environ.get(SONARR_API_KEY_ENV))


def list_quality_profiles() -> list[dict[str, Any]]:
    """Return Sonarr's quality profile list, preserving user order."""
    return _request("GET", "/api/v3/qualityprofile") or []


def list_series() -> list[dict[str, Any]]:
    """Return the full series list. Sonarr libraries typically have hundreds,
    not tens of thousands, so pagination isn't needed."""
    return _request("GET", "/api/v3/series") or []


def find_series_by_title_year(title: str, year: int | None = None) -> dict[str, Any] | None:
    """Locate a series by normalised title (+ year if given).

    Series in our media_report are grouped by their on-disk folder name, which
    usually matches Sonarr's ``title`` or ``sortTitle``. If multiple series
    share a title (rare; ``Doctor Who (2005)`` vs ``Doctor Who (1963)``), the
    ``year`` filter disambiguates.
    """
    target = _norm_title(title)
    candidates = []
    for s in list_series():
        if _norm_title(s.get("title", "")) == target:
            candidates.append(s)
        elif _norm_title(s.get("sortTitle", "")) == target:
            candidates.append(s)
    if not candidates:
        return None
    if year is not None:
        # Prefer exact year match
        for c in candidates:
            if c.get("year") == year:
                return c
    # Fallback: first hit
    return candidates[0]


def find_series_by_path(filepath: str) -> dict[str, Any] | None:
    """Locate a series via on-disk path. Sonarr stores the show folder as
    ``path``; we compare the trailing folder name since the mounts differ."""
    norm = _normalise_path(filepath)
    # Extract the show folder from the UNC path: .../Series/<show>/...
    parts = [p for p in norm.split("/") if p]
    try:
        i = parts.index("series")
    except ValueError:
        return None
    if i + 1 >= len(parts):
        return None
    folder = parts[i + 1]
    for s in list_series():
        s_path = _normalise_path(s.get("path") or "")
        if folder in s_path.split("/"):
            return s
    return None


def update_series(series_id: int, *, quality_profile_id: int) -> dict[str, Any]:
    """Change a series' quality profile. PUT /series expects the full object."""
    series = _request("GET", f"/api/v3/series/{series_id}")
    if not series:
        raise SonarrError(f"series id {series_id} not found")
    series["qualityProfileId"] = int(quality_profile_id)
    return _request("PUT", f"/api/v3/series/{series_id}", body=series)


def trigger_search(series_id: int) -> dict[str, Any]:
    """Ask Sonarr to search for missing + upgrade-eligible episodes of this series.

    Uses ``SeriesSearch`` which respects the current quality profile and
    custom-format scores. Better choice than ``EpisodeSearch`` for upgrades —
    it walks every monitored episode and grabs anything that beats the
    current file's score.
    """
    return _request(
        "POST",
        "/api/v3/command",
        body={"name": "SeriesSearch", "seriesId": int(series_id)},
    )


def upgrade_via_sonarr(
    *,
    filepath: str | None,
    title: str,
    year: int | None,
    quality_profile_id: int,
) -> dict[str, Any]:
    """End-to-end: locate series → flip profile → trigger search."""
    series = None
    if filepath:
        series = find_series_by_path(filepath)
    if not series:
        series = find_series_by_title_year(title, year)
    if not series:
        raise SonarrError(
            f"could not locate Sonarr series for '{title}' ({year})."
        )
    series_id = int(series["id"])
    update_series(series_id, quality_profile_id=quality_profile_id)
    cmd = trigger_search(series_id) or {}
    return {
        "ok": True,
        "series_id": series_id,
        "sonarr_title": series.get("title"),
        "sonarr_year": series.get("year"),
        "profile_id": int(quality_profile_id),
        "command_id": cmd.get("id"),
    }


# --------------------------------------------------------------------------
# Normalisation helpers (shared shape with tools.radarr)
# --------------------------------------------------------------------------


def _normalise_path(p: str) -> str:
    s = p.replace("\\", "/").lower()
    return s.rstrip("/")


def _norm_title(s: str) -> str:
    """Strip punctuation + lowercase for fuzzy comparison."""
    import re
    s = s.lower()
    s = re.sub(r"\b(the|a|an)\b", "", s)
    s = re.sub(r"\(\d{4}\)", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s.strip()
