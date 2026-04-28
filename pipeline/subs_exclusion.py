"""Per-title exclusion list for the "needs English subtitles" compliance check.

Some titles legitimately don't need (or can't have) English subtitles:
  - Silent films (Birth of a Nation, Sunrise, …)
  - Kids' shows where the user simply isn't running subs (Puffin Rock, …)
  - Concert films / wordless documentaries

Without an exclusion list, every such file shows up forever as "needs subs"
in the dashboard and forever in Bazarr's wanted queue. The exclusion file
lets the user mark these titles as "subs not required" so they:
  1. Count as compliant for the dashboard "subs_done" stat
  2. Stop appearing in the gap_filler / quick-wins lists
  3. (Bazarr is detached separately via the Bazarr UI / API — pipeline can't see Bazarr state)

Format: ``F:\\AV1_Staging\\control\\subs_optional.json``

    {
      "patterns": [
        "Puffin Rock",
        "Birth of a Nation"
      ]
    }

Matching: case-insensitive substring match against the full filepath.
A pattern of "Puffin Rock" matches every episode under any folder
containing that string (e.g. ``\\\\NAS\\Series\\Puffin Rock\\Season 1\\…``).

Patterns are NOT regexes — keep them simple. If a title needs precise
disambiguation (e.g. distinguishing "Birth of a Nation (1915)" from a
2016 remake), use a longer substring like ``"Birth of a Nation (1915)"``.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from paths import STAGING_DIR

# Default location used by both the orchestrator (which seeds the file via
# PipelineControl) and the dashboard (which reads it for compliance checks).
DEFAULT_PATH: Path = Path(STAGING_DIR) / "control" / "subs_optional.json"

# Cache: re-read the file when its mtime changes. The dashboard polls compliance
# every few seconds; reading the file every call would be wasteful. mtime check
# is a single os.stat per call and gives instant pickup of edits.
_cache: dict = {
    "mtime": 0.0,
    "patterns_lower": (),  # tuple of lowercased patterns
    "expires_at": 0.0,  # also expire after a TTL even if mtime unchanged
}

_TTL_SECONDS = 5.0


def _load_patterns(path: Path | None = None) -> tuple[str, ...]:
    """Return the lowercased patterns from the exclusion file, with mtime caching.

    Missing file or malformed JSON returns an empty tuple — exclusion is a
    user-controlled file, broken state should never crash the dashboard.
    """
    # Read DEFAULT_PATH at call time (not as a parameter default), so tests
    # can monkeypatch the module attribute and have it take effect.
    if path is None:
        path = DEFAULT_PATH
    now = time.monotonic()
    try:
        mtime = path.stat().st_mtime
    except OSError:
        # File doesn't exist (yet) — pipeline.control will seed it on next start.
        return ()

    if mtime == _cache["mtime"] and now < _cache["expires_at"]:
        return _cache["patterns_lower"]

    try:
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
        raw_patterns = data.get("patterns") or []
        patterns_lower = tuple(p.lower() for p in raw_patterns if isinstance(p, str) and p.strip())
    except (OSError, json.JSONDecodeError, AttributeError) as e:
        logging.warning(f"subs_optional.json unreadable ({e}) — treating as empty")
        patterns_lower = ()

    _cache["mtime"] = mtime
    _cache["patterns_lower"] = patterns_lower
    _cache["expires_at"] = now + _TTL_SECONDS
    return patterns_lower


def is_subs_optional(filepath: str, path: Path | None = None) -> bool:
    """Return True if ``filepath`` matches any pattern in subs_optional.json.

    Match is case-insensitive substring against the full filepath. Empty
    filepath always returns False.
    """
    if not filepath:
        return False
    fp_lower = filepath.lower()
    for pat in _load_patterns(path):
        if pat in fp_lower:
            return True
    return False


def reset_cache_for_tests() -> None:
    """Clear the mtime cache. Tests need this between iterations."""
    _cache["mtime"] = 0.0
    _cache["patterns_lower"] = ()
    _cache["expires_at"] = 0.0
