"""Shared state, paths, and utility functions for the server package."""

import json
import os
import threading
from pathlib import Path
from typing import Any

from paths import PIPELINE_STATE_DB, STAGING_DIR

# Derived paths
CONTROL_DIR = STAGING_DIR / "control"
STATE_FILE = STAGING_DIR / "pipeline_state.json"  # legacy, kept for migration detection
HISTORY_FILE = STAGING_DIR / "encode_history.jsonl"
FRONTEND_DIST = Path(__file__).parent.parent / "frontend" / "dist"
DISMISSED_DIR = STAGING_DIR / "dismissed"
CONFIG_OVERRIDES_FILE = CONTROL_DIR / "config_overrides.json"


def _get_pipeline_state() -> dict | None:
    """Read pipeline state from SQLite, returning the same dict shape as the old JSON.

    Falls back to the JSON file if the DB doesn't exist yet (pre-migration).
    """
    db_path = str(PIPELINE_STATE_DB)
    if os.path.exists(db_path):
        try:
            from pipeline.state import PipelineState

            state = PipelineState(db_path)
            data = state.data
            state.close()
            return data
        except Exception:
            pass
    # Fallback to JSON
    return read_json_safe(STATE_FILE)


def _get_state_db():
    """Get a raw SQLite connection for direct queries (reset-errors, compact, etc.)."""
    from pipeline.state import get_db

    return get_db(str(PIPELINE_STATE_DB))


def drop_file(name: str, data: dict | None = None) -> Path:
    """Create a control file, optionally writing JSON data to it."""
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    path = CONTROL_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        if data:
            json.dump(data, f, indent=2)
    return path


def remove_file(name: str) -> None:
    """Remove a control file if it exists."""
    path = CONTROL_DIR / name
    if path.exists():
        path.unlink()


def file_exists(name: str) -> bool:
    """Check whether a control file exists."""
    return (CONTROL_DIR / name).exists()


def get_pause_state() -> str:
    """Determine the current pause state of the pipeline."""
    if (STAGING_DIR / "PAUSE").exists():
        return "paused_all"
    for name, ptype in [
        ("pause_all.json", "paused_all"),
        ("pause_fetch.json", "paused_fetch"),
        ("pause_encode.json", "paused_encode"),
    ]:
        if file_exists(name):
            return ptype
    pause_path = CONTROL_DIR / "pause.json"
    if pause_path.exists():
        try:
            data = json.loads(pause_path.read_text())
            t = data.get("type", "all")
            return f"paused_{t}" if t != "all" else "paused_all"
        except Exception:
            return "paused_all"
    return "running"


def clear_all_pauses() -> None:
    """Remove all pause control files."""
    for name in ["pause.json", "pause_all.json", "pause_fetch.json", "pause_encode.json"]:
        remove_file(name)
    pause_path = STAGING_DIR / "PAUSE"
    if pause_path.exists():
        pause_path.unlink()


def read_json_safe(path: Path) -> dict | list | None:
    """Read and parse a JSON file, returning None on any error."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_json_safe(path: Path, data: dict | list) -> None:
    """Atomically write JSON data to a file via tmp-rename."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


# --- mtime-invalidating cache for media_report.json reads ---
#
# Glance polls /api/library-completion every 60s and every call re-parses
# 50 MB of JSON. This cache keeps the parsed dict in memory keyed on
# (filepath, mtime); a mtime change (pipeline finished an encode and called
# update_entry, or the scanner ran) automatically invalidates.
#
# Thread-safe. A cache HIT skips both locks entirely — just a stat + dict
# lookup. A cache MISS takes a per-path loader lock so N concurrent readers
# produce exactly one underlying parse; the losers wait, then hit the cache.
_REPORT_CACHE_LOCK = threading.Lock()
_REPORT_CACHE: dict[str, tuple[float, Any]] = {}
_REPORT_LOADERS: dict[str, threading.Lock] = {}


def _loader_lock(path_str: str) -> threading.Lock:
    """Return a per-path lock used to serialise cold-miss parses."""
    with _REPORT_CACHE_LOCK:
        lock = _REPORT_LOADERS.get(path_str)
        if lock is None:
            lock = threading.Lock()
            _REPORT_LOADERS[path_str] = lock
        return lock


def read_report_cached(path: str | Path) -> dict | None:
    """Return parsed media_report.json, cached by (path, mtime).

    Cache HIT: returns the cached parsed dict (shared instance) in ~0.1 ms —
    just a dict lookup and an ``os.stat`` call. Cache MISS takes the
    per-path loader lock before reading, so racing readers share a single
    50 MB parse. The read itself goes through ``tools.report_lock.read_report``
    which serialises against writers.

    Returns ``None`` if the file does not exist, matching ``read_json_safe``.
    """
    path_str = str(path)
    try:
        mtime = os.path.getmtime(path_str)
    except OSError:
        return None

    with _REPORT_CACHE_LOCK:
        hit = _REPORT_CACHE.get(path_str)
        if hit and hit[0] == mtime:
            return hit[1]

    # Cold miss. Take the per-path loader lock; only one thread parses,
    # the rest wait and then re-check the cache.
    loader = _loader_lock(path_str)
    with loader:
        with _REPORT_CACHE_LOCK:
            hit = _REPORT_CACHE.get(path_str)
            if hit and hit[0] == mtime:
                return hit[1]

        from tools.report_lock import read_report

        data = read_report()

        # Re-stat post-read: if the file was rewritten while we were parsing,
        # key the cache entry against the actual on-disk mtime so the next
        # reader's stat matches.
        try:
            mtime = os.path.getmtime(path_str)
        except OSError:
            return data

        with _REPORT_CACHE_LOCK:
            _REPORT_CACHE[path_str] = (mtime, data)
        return data


def invalidate_report_cache() -> None:
    """Clear the media_report cache. Call after an in-process write."""
    with _REPORT_CACHE_LOCK:
        _REPORT_CACHE.clear()
