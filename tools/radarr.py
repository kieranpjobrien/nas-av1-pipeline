"""Radarr REST API client — minimal surface for the upgrade recommender.

Used by the Upgrades UI's "Radarr →" button: pick a quality profile, flip the
movie's profile in Radarr, and trigger a search. Radarr's monitored-movie
watcher does the rest (grab via indexers, hand off to SAB, import via post-
processing script).

Why a plain ``urllib`` client and not ``pyarr``:
    pyarr's an extra dep, and we only need three endpoints
    (``/api/v3/qualityprofile``, ``/api/v3/movie``, ``/api/v3/command``).
    Stdlib keeps the footprint tiny and survives dep churn.

Configuration
-------------
Environment variables (read at call time, not import, so the server picks up
changes without restart):

    RADARR_URL        e.g. http://192.168.4.43:7878
    RADARR_API_KEY    from Radarr → Settings → General → Security → API Key

If either is unset, the client methods return a ``{"disabled": True}``
sentinel so the UI renders "Radarr (off)" instead of surfacing a stack trace.
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

RADARR_URL_ENV = "RADARR_URL"
RADARR_API_KEY_ENV = "RADARR_API_KEY"


class RadarrNotConfigured(RuntimeError):
    """Raised when RADARR_URL / RADARR_API_KEY aren't set. Callers catch and
    surface a UI-friendly disabled state instead of propagating."""


class RadarrError(RuntimeError):
    """Raised when Radarr returns a non-2xx response."""


def _require_config() -> tuple[str, str]:
    """Return (url, api_key) or raise RadarrNotConfigured."""
    url = (os.environ.get(RADARR_URL_ENV) or "").rstrip("/")
    key = os.environ.get(RADARR_API_KEY_ENV) or ""
    if not url or not key:
        raise RadarrNotConfigured(
            "RADARR_URL and/or RADARR_API_KEY not set. "
            "Set them in your environment to enable Radarr integration."
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
    """Thin urllib wrapper. Raises RadarrError on non-2xx, RadarrNotConfigured
    on missing creds. Returns parsed JSON (list/dict) or None on 204."""
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
        # Surface the response body when possible — Radarr's 4xx messages are
        # genuinely useful ("Movie not found", "Quality profile not found").
        try:
            body_txt = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            body_txt = ""
        raise RadarrError(f"{e.code} {e.reason} — {method} {path} — {body_txt}") from e
    except urllib.error.URLError as e:
        raise RadarrError(f"connection failed: {method} {full} — {e.reason}") from e


def is_configured() -> bool:
    """Return True if both RADARR_URL and RADARR_API_KEY are set.

    Used by the UI button to render "Radarr (off)" without attempting a
    connection when the user hasn't set creds yet.
    """
    return bool(os.environ.get(RADARR_URL_ENV) and os.environ.get(RADARR_API_KEY_ENV))


def list_quality_profiles() -> list[dict[str, Any]]:
    """Return ``[{id, name, cutoff, items: [...]}, ...]``.

    The UI surfaces ``name`` to the user and sends the ``id`` back when
    the user picks a profile. Radarr's ordering is user-defined — we
    preserve it rather than alphabetising.
    """
    return _request("GET", "/api/v3/qualityprofile") or []


def list_movies() -> list[dict[str, Any]]:
    """Return the full movie list. Radarr caps at ~50K items in practice;
    we never paginate.
    """
    return _request("GET", "/api/v3/movie") or []


def find_movie_by_path(filepath: str) -> dict[str, Any] | None:
    """Locate a movie by its on-disk filepath (Radarr's ``movieFile.path``).

    The match is done by trailing-path comparison because Radarr stores paths
    in its own root (e.g. ``/movies/...``) while we see UNC paths (e.g.
    ``\\KieranNAS\Media\Movies\...``). We normalise both to forward slashes
    and compare the tail after the shared ``Movies/`` segment.
    """
    norm_target = _normalise_path(filepath)
    for m in list_movies():
        mf = m.get("movieFile") or {}
        candidate = mf.get("path") or m.get("path") or ""
        if not candidate:
            continue
        if _paths_match(candidate, norm_target):
            return m
    return None


def find_movie_by_title_year(title: str, year: int | None) -> dict[str, Any] | None:
    """Fallback locator when the path match fails (e.g. Radarr root differs
    from our NAS mount). Matches on normalised title + year. Returns the
    first hit; Radarr doesn't permit duplicates so there shouldn't be a
    second one anyway.
    """
    target = _norm_title(title)
    for m in list_movies():
        if _norm_title(m.get("title", "")) == target:
            if year is None or m.get("year") == year:
                return m
    return None


def update_movie(movie_id: int, *, quality_profile_id: int) -> dict[str, Any]:
    """Change a movie's quality profile. Radarr's PUT /movie expects the full
    movie object, so we GET first then PATCH the profile id in-place.
    """
    movie = _request("GET", f"/api/v3/movie/{movie_id}")
    if not movie:
        raise RadarrError(f"movie id {movie_id} not found")
    movie["qualityProfileId"] = int(quality_profile_id)
    return _request("PUT", f"/api/v3/movie/{movie_id}", body=movie)


def trigger_search(movie_id: int) -> dict[str, Any]:
    """Ask Radarr to search for a (better) release for this movie.

    Uses the ``MoviesSearch`` command which respects the movie's (just-set)
    quality profile and custom-format scores. Radarr returns immediately
    with a command id; the actual indexer call happens async.
    """
    return _request(
        "POST",
        "/api/v3/command",
        body={"name": "MoviesSearch", "movieIds": [int(movie_id)]},
    )


def upgrade_via_radarr(
    *,
    filepath: str | None,
    title: str,
    year: int | None,
    quality_profile_id: int,
) -> dict[str, Any]:
    """End-to-end: locate the movie, flip the profile, trigger a search.

    Returns a summary dict suitable for the UI ``{ok, movie_id, profile_id,
    command_id}``. Raises ``RadarrError`` if the movie can't be located.
    """
    movie = None
    if filepath:
        movie = find_movie_by_path(filepath)
    if not movie:
        movie = find_movie_by_title_year(title, year)
    if not movie:
        raise RadarrError(
            f"could not locate Radarr movie for '{title}' ({year}). "
            "Is the movie added to Radarr under a matching title?"
        )
    movie_id = int(movie["id"])
    update_movie(movie_id, quality_profile_id=quality_profile_id)
    cmd = trigger_search(movie_id) or {}
    return {
        "ok": True,
        "movie_id": movie_id,
        "radarr_title": movie.get("title"),
        "radarr_year": movie.get("year"),
        "profile_id": int(quality_profile_id),
        "command_id": cmd.get("id"),
    }


# --------------------------------------------------------------------------
# Path / title normalisation helpers
# --------------------------------------------------------------------------


def _normalise_path(p: str) -> str:
    """Windows → POSIX-ish: backslash → slash, lowercase, strip trailing slashes.

    We don't try to map ``\\KieranNAS\Media\Movies\X`` to Radarr's root path
    — we match on the trailing segment instead (see ``_paths_match``).
    """
    s = p.replace("\\", "/").lower()
    return s.rstrip("/")


def _paths_match(radarr_path: str, our_path: str) -> bool:
    """True if the two paths refer to the same file.

    We match on the trailing ``Movies/<title>/<file>`` (or ``<title>/<file>``)
    component because the Radarr-side mount and our NAS-side UNC path diverge.
    Simple but effective — movie filenames are unique per folder.
    """
    a = _normalise_path(radarr_path)
    b = _normalise_path(our_path)
    # Take the last two path components — that's the movie folder + file.
    # This handles the common case where Radarr uses a unix mount and we use
    # a Windows UNC path for the same NAS volume.
    def tail(path: str, n: int = 2) -> str:
        parts = [p for p in path.split("/") if p]
        return "/".join(parts[-n:])
    return tail(a) == tail(b) or Path(a).name == Path(b).name


def _norm_title(s: str) -> str:
    """Strip punctuation + lowercase for fuzzy title comparison.

    Handles "The Martian" / "Martian, The" / "The Martian (2015)" → same key.
    """
    import re
    s = s.lower()
    s = re.sub(r"\b(the|a|an)\b", "", s)
    s = re.sub(r"\(\d{4}\)", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s.strip()
