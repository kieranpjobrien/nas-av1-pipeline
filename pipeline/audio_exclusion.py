"""Per-title opt-out of the foreign-audio compliance flag.

Some films are legitimately watched in a language other than TMDb's
``original_language`` and the user is happy with that:
  - Sergio Leone's "Dollars" trilogy — shot MOS, English is the canonical
    track (A Fistful of Dollars, For a Few Dollars More, …)
  - Hong Kong / martial-arts films watched in the English dub
  - Any title where the original-language audio simply isn't obtainable and
    the English track is acceptable to the user

Without an opt-out, ``qualify_file`` flags these ``FLAGGED_FOREIGN_AUDIO``
forever — they never encode and sit in the flagged pane. This list lets the
user mark such titles "audio is fine as-is" so they:
  1. Skip the foreign-audio flag and proceed to AV1 encode
  2. Count toward the dashboard completion stats
  3. Stop reappearing in the flagged / review lists

Format: ``F:\\AV1_Staging\\control\\audio_foreign_ok.json``

    {
      "patterns": [
        "A Fistful of Dollars (1964)",
        "For a Few Dollars More (1965)"
      ]
    }

Matching mirrors subs_exclusion: case-insensitive substring against the full
filepath. Patterns are NOT regexes — keep them simple; use a longer substring
(with year) when a title needs disambiguation.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from paths import STAGING_DIR

DEFAULT_PATH: Path = Path(STAGING_DIR) / "control" / "audio_foreign_ok.json"

_cache: dict = {"mtime": 0.0, "patterns_lower": (), "expires_at": 0.0}
_TTL_SECONDS = 5.0


def _load_patterns(path: Path | None = None) -> tuple[str, ...]:
    """Return the lowercased patterns, with mtime caching. Missing/malformed
    file returns an empty tuple — this is a user-controlled file, a broken
    state must never crash qualification."""
    if path is None:
        path = DEFAULT_PATH
    now = time.monotonic()
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return ()

    if mtime == _cache["mtime"] and now < _cache["expires_at"]:
        return _cache["patterns_lower"]

    try:
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
        raw = data.get("patterns") or []
        patterns_lower = tuple(p.lower() for p in raw if isinstance(p, str) and p.strip())
    except (OSError, json.JSONDecodeError, AttributeError) as e:
        logging.warning(f"audio_foreign_ok.json unreadable ({e}) — treating as empty")
        patterns_lower = ()

    _cache["mtime"] = mtime
    _cache["patterns_lower"] = patterns_lower
    _cache["expires_at"] = now + _TTL_SECONDS
    return patterns_lower


def is_foreign_audio_ok(filepath: str, path: Path | None = None) -> bool:
    """True if ``filepath`` matches any pattern in audio_foreign_ok.json.

    Case-insensitive substring match against the full filepath. Empty
    filepath always returns False."""
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
