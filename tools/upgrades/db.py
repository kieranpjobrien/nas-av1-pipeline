"""SQLite persistence for the upgrade recommender.

Tables
------
upgrade_info
    One row per library file (keyed on filepath). Tracks current media
    state, the best available release discovered by scrapers, and the
    computed upgrade score/reasons.

scraper_cache
    Short-term HTML cache (default TTL 7 days) for scraper responses.
    Keyed on an opaque ``cache_key`` supplied by the scraper.

The DB path defaults to ``paths.STAGING_DIR / 'upgrades.sqlite'`` so it
sits alongside the rest of the pipeline state, but callers can override
it (the test suite does — see ``tests/test_upgrades.py``).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from paths import STAGING_DIR

UPGRADES_DB: Path = STAGING_DIR / "upgrades.sqlite"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS upgrade_info (
    filepath TEXT PRIMARY KEY,
    tmdb_id INTEGER,
    last_checked TEXT,
    title TEXT,
    year INTEGER,
    library_type TEXT,

    current_video_codec TEXT,
    current_video_res TEXT,
    current_audio_codec TEXT,
    current_audio_channels INTEGER,
    current_has_atmos INTEGER,

    best_available_label TEXT,
    best_source_url TEXT,
    has_atmos_available INTEGER,
    has_truehd_available INTEGER,
    has_4k_hdr_available INTEGER,

    upgrade_score INTEGER,
    upgrade_reasons TEXT,
    confidence TEXT
);

CREATE TABLE IF NOT EXISTS scraper_cache (
    cache_key TEXT PRIMARY KEY,
    fetched_at TEXT,
    body TEXT
);

CREATE INDEX IF NOT EXISTS idx_upgrade_score
    ON upgrade_info(upgrade_score DESC);

-- LLM-backed taste scores. One row per (title, year) regardless of how many
-- library files reference that film. seed_version enables auto-rescore when
-- the user edits taste_seeds.json via the UI (stale rows get skipped by the
-- fetch helpers and re-computed on next pass).
CREATE TABLE IF NOT EXISTS taste_scores (
    title         TEXT    NOT NULL,
    year          INTEGER,
    score         INTEGER NOT NULL CHECK (score >= 0 AND score <= 10),
    rationale     TEXT    NOT NULL,
    model         TEXT    NOT NULL,
    seed_version  INTEGER NOT NULL,
    scored_at     REAL    NOT NULL,
    PRIMARY KEY (title, year)
);

CREATE INDEX IF NOT EXISTS idx_taste_score
    ON taste_scores(score DESC);
"""

# Columns of upgrade_info in their insertion order.
_UPGRADE_COLS: tuple[str, ...] = (
    "filepath",
    "tmdb_id",
    "last_checked",
    "title",
    "year",
    "library_type",
    "current_video_codec",
    "current_video_res",
    "current_audio_codec",
    "current_audio_channels",
    "current_has_atmos",
    "best_available_label",
    "best_source_url",
    "has_atmos_available",
    "has_truehd_available",
    "has_4k_hdr_available",
    "upgrade_score",
    "upgrade_reasons",
    "confidence",
)


def connect(path: str | Path = UPGRADES_DB) -> sqlite3.Connection:
    """Open a SQLite connection with WAL + FK + Row factory, and ensure the schema exists.

    Args:
        path: On-disk database path. Parent directory is created if missing.

    Returns:
        A live connection with ``row_factory = sqlite3.Row`` so callers get
        dict-like rows.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def _row_values(row: dict[str, Any]) -> tuple[Any, ...]:
    """Coerce a dict into the tuple order expected by INSERT statements."""
    return tuple(row.get(col) for col in _UPGRADE_COLS)


def upsert(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    """Insert-or-replace a single upgrade_info row.

    Lists / tuples in ``row`` (e.g. ``upgrade_reasons``) are comma-joined
    before persistence to keep the column type TEXT.
    """
    if "filepath" not in row or not row["filepath"]:
        raise ValueError("upsert requires a non-empty 'filepath'")

    data = dict(row)
    reasons = data.get("upgrade_reasons")
    if isinstance(reasons, (list, tuple)):
        data["upgrade_reasons"] = ",".join(str(r) for r in reasons)

    # Normalise bools to 0/1 for the INTEGER columns.
    for col in ("current_has_atmos", "has_atmos_available",
                "has_truehd_available", "has_4k_hdr_available"):
        if col in data and data[col] is not None:
            data[col] = int(bool(data[col]))

    placeholders = ", ".join(["?"] * len(_UPGRADE_COLS))
    cols_csv = ", ".join(_UPGRADE_COLS)
    conn.execute(
        f"INSERT OR REPLACE INTO upgrade_info ({cols_csv}) VALUES ({placeholders})",
        _row_values(data),
    )
    conn.commit()


def read(conn: sqlite3.Connection, filepath: str) -> dict[str, Any] | None:
    """Fetch a single upgrade_info row as a plain dict, or None if absent."""
    cur = conn.execute("SELECT * FROM upgrade_info WHERE filepath = ?", (filepath,))
    row = cur.fetchone()
    return dict(row) if row else None


def iter_top(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    """Return the top-N rows ordered by ``upgrade_score DESC, title ASC``."""
    cur = conn.execute(
        "SELECT * FROM upgrade_info "
        "WHERE upgrade_score IS NOT NULL "
        "ORDER BY upgrade_score DESC, title ASC "
        "LIMIT ?",
        (int(limit),),
    )
    return [dict(r) for r in cur.fetchall()]


def iter_all_paths(conn: sqlite3.Connection) -> Iterable[str]:
    """Yield every filepath currently in upgrade_info (used for dashboard queries)."""
    cur = conn.execute("SELECT filepath FROM upgrade_info")
    for row in cur:
        yield row["filepath"]


# ---------- Scraper cache ----------


def _utc_now_iso() -> str:
    """Return ISO8601 UTC timestamp, second precision."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def cache_get(
    conn: sqlite3.Connection, key: str, ttl_days: int = 7
) -> str | None:
    """Return the cached body for ``key`` if fresh, else None.

    Entries older than ``ttl_days`` are treated as misses (but left on disk
    — the next ``cache_set`` for that key will overwrite them).
    """
    cur = conn.execute(
        "SELECT fetched_at, body FROM scraper_cache WHERE cache_key = ?", (key,)
    )
    row = cur.fetchone()
    if not row:
        return None
    try:
        fetched = datetime.fromisoformat(row["fetched_at"])
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None
    if datetime.now(timezone.utc) - fetched > timedelta(days=ttl_days):
        return None
    return row["body"]


def cache_set(conn: sqlite3.Connection, key: str, body: str) -> None:
    """Store ``body`` under ``key`` with the current UTC timestamp."""
    conn.execute(
        "INSERT OR REPLACE INTO scraper_cache (cache_key, fetched_at, body) "
        "VALUES (?, ?, ?)",
        (key, _utc_now_iso(), body),
    )
    conn.commit()


def row_to_public_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Convert a raw DB row to a JSON-friendly dict (reasons list, bools, etc.)."""
    out = dict(row)
    reasons = out.get("upgrade_reasons") or ""
    out["upgrade_reasons"] = [r for r in reasons.split(",") if r] if isinstance(reasons, str) else list(reasons)
    for col in ("current_has_atmos", "has_atmos_available",
                "has_truehd_available", "has_4k_hdr_available"):
        if col in out and out[col] is not None:
            out[col] = bool(out[col])
    return out


def dump_json(row: dict[str, Any]) -> str:
    """Pretty-print a row as JSON (used by CLI ``show``)."""
    return json.dumps(row_to_public_dict(row), indent=2, default=str)
