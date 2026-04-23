"""Pipeline state management — SQLite-backed state that survives crashes.

Uses SQLite with WAL mode for safe concurrent access from both the pipeline
process and the server process. The API surface is identical to the old
JSON-based implementation so callers don't need to change.
"""

import json
import logging
import sqlite3
import threading
import time
from datetime import datetime
from enum import Enum
from typing import Optional


class FileStatus(str, Enum):
    """Simplified pipeline state machine (6 states).

    PENDING → FETCHING → PROCESSING → UPLOADING → DONE → ERROR

    Each file is owned by one thread start to finish. No handoffs.
    The 'stage' field in the DB tracks which substep is active.
    """

    PENDING = "pending"
    FETCHING = "fetching"
    PROCESSING = "processing"
    UPLOADING = "uploading"
    DONE = "done"
    ERROR = "error"


# Columns stored directly (not in extras JSON) for efficient queries
_DIRECT_COLS = {
    "status",
    "mode",
    "added",
    "last_updated",
    "tier",
    "audio_only",
    "cleanup_strip",
    "local_path",
    "output_path",
    "dest_path",
    "error",
    "stage",
    "reason",
    "res_key",
    "sub_strip",
}


def get_db(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection with WAL mode and sensible defaults.

    check_same_thread=False allows the connection to be used from multiple threads
    (safe because we protect all access with an RLock).
    """
    conn = sqlite3.connect(str(db_path), timeout=60, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")  # 30s wait on lock contention
    conn.row_factory = sqlite3.Row
    return conn


def _init_tables(conn: sqlite3.Connection) -> None:
    """Create tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pipeline_files (
            filepath TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            mode TEXT DEFAULT 'full_gamut',
            added TEXT,
            last_updated TEXT,
            tier TEXT,
            audio_only INTEGER,
            cleanup_strip INTEGER,
            sub_strip INTEGER,
            local_path TEXT,
            output_path TEXT,
            dest_path TEXT,
            error TEXT,
            stage TEXT,
            reason TEXT,
            res_key TEXT,
            extras TEXT DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS pipeline_stats (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            data TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS pipeline_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_files_status ON pipeline_files(status);
    """)
    # Ensure stats row exists
    conn.execute(
        "INSERT OR IGNORE INTO pipeline_stats (id, data) VALUES (1, ?)",
        (
            json.dumps(
                {
                    "total_files": 0,
                    "completed": 0,
                    "skipped": 0,
                    "errors": 0,
                    "bytes_saved": 0,
                    "total_encode_time_secs": 0,
                }
            ),
        ),
    )

    # Add mode column if missing (migration from pre-rewrite schema)
    try:
        conn.execute("ALTER TABLE pipeline_files ADD COLUMN mode TEXT DEFAULT 'full_gamut'")
    except sqlite3.OperationalError:
        pass  # column already exists

    conn.commit()


class PipelineState:
    """Persistent state tracker — SQLite-backed, survives crashes.

    Thread-safe via SQLite's built-in locking. Multiple processes can
    read/write the same database safely via WAL mode.
    """

    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        self._lock = threading.RLock()
        self._conn = get_db(self.db_path)
        _init_tables(self._conn)
        self._stats_cache = None
        self._stats_dirty = False

        count = self._conn.execute("SELECT COUNT(*) FROM pipeline_files").fetchone()[0]
        if count > 0:
            logging.info(f"Loaded state: {count} files tracked")

    def _get_conn(self) -> sqlite3.Connection:
        """Get the thread-local connection. SQLite connections aren't thread-safe,
        but we protect with RLock and use WAL mode for cross-process safety."""
        return self._conn

    def save(self):
        """Flush stats to the database. Always writes since callers mutate the dict directly."""
        with self._lock:
            if self._stats_cache is not None:
                self._conn.execute("UPDATE pipeline_stats SET data = ? WHERE id = 1", (json.dumps(self._stats_cache),))
                self._conn.commit()
                self._stats_dirty = False

    def get_file(self, filepath: str) -> Optional[dict]:
        """Get file entry as a dict, or None if not tracked."""
        with self._lock:
            row = self._conn.execute("SELECT * FROM pipeline_files WHERE filepath = ?", (filepath,)).fetchone()
            if not row:
                return None
            return self._row_to_dict(row)

    def set_file(self, filepath: str, status: FileStatus, **kwargs):
        """Create or update a file entry. Writes to DB immediately.

        Uses INSERT OR REPLACE for simplicity and atomicity — single statement,
        no SELECT+UPDATE race under concurrent thread access.
        """
        # Runtime guard: DONE paired with a "deferred" or "skipped" reason is the
        # anti-pattern that lost 65 files overnight 2026-04-23. The file-on-disk is
        # unchanged but the state row looks complete, so the queue builder excludes
        # it forever. If work is deferred, the correct state is ERROR (so the next
        # queue build retries) or PENDING — never DONE.
        if status == FileStatus.DONE:
            reason = kwargs.get("reason") or ""
            reason_low = str(reason).lower()
            if "defer" in reason_low or "skip" in reason_low:
                raise ValueError(
                    "FileStatus.DONE with deferred/skipped reason is forbidden — "
                    f"use ERROR instead. filepath={filepath!r} reason={reason!r}"
                )

        with self._lock:
            now = datetime.now().isoformat()

            # Read existing entry to preserve fields not being updated
            existing_row = self._conn.execute("SELECT * FROM pipeline_files WHERE filepath = ?", (filepath,)).fetchone()

            all_data = {"status": status.value, "last_updated": now}
            if not existing_row:
                all_data["added"] = now
            all_data.update(kwargs)

            # Build the full row from existing + new data
            direct = {}
            extras = {}
            if existing_row:
                old = dict(existing_row)
                old.pop("filepath", None)
                old_extras = json.loads(old.pop("extras", "{}") or "{}")
                # Start with existing direct cols
                for k in _DIRECT_COLS:
                    if k in old and old[k] is not None:
                        direct[k] = old[k]
                extras = old_extras

            # Apply new values
            for k, v in all_data.items():
                if k in _DIRECT_COLS:
                    if k in ("audio_only", "cleanup_strip", "sub_strip") and v is not None:
                        v = 1 if v else 0
                    direct[k] = v
                else:
                    extras[k] = v

            # Single INSERT OR REPLACE — atomic, no race
            cols = ["filepath"] + list(direct.keys()) + ["extras"]
            placeholders = ", ".join(["?"] * len(cols))
            vals = [filepath] + list(direct.values()) + [json.dumps(extras)]
            try:
                self._conn.execute(
                    f"INSERT OR REPLACE INTO pipeline_files ({', '.join(cols)}) VALUES ({placeholders})", vals
                )
                self._conn.commit()
            except sqlite3.OperationalError as e:
                logging.warning(f"SQLite write retry for {filepath}: {e}")
                time.sleep(0.5)
                self._conn.execute(
                    f"INSERT OR REPLACE INTO pipeline_files ({', '.join(cols)}) VALUES ({placeholders})", vals
                )
                self._conn.commit()

    def get_files_by_status(self, status: FileStatus) -> list[str]:
        """Return list of filepaths matching a status."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT filepath FROM pipeline_files WHERE status = ?", (status.value,)
            ).fetchall()
            return [row[0] for row in rows]

    def all_filepaths(self) -> list[str]:
        """Return every filepath tracked in state, any status.

        Used for startup sweeps (e.g. finding stale .gapfill_tmp.mkv leftovers).
        """
        with self._lock:
            rows = self._conn.execute("SELECT filepath FROM pipeline_files").fetchall()
            return [row[0] for row in rows]

    @property
    def stats(self) -> dict:
        """Get stats dict. Cached in memory, flushed to DB on save()."""
        if self._stats_cache is None:
            with self._lock:
                row = self._conn.execute("SELECT data FROM pipeline_stats WHERE id = 1").fetchone()
                self._stats_cache = (
                    json.loads(row[0])
                    if row
                    else {
                        "total_files": 0,
                        "completed": 0,
                        "skipped": 0,
                        "errors": 0,
                        "bytes_saved": 0,
                        "total_encode_time_secs": 0,
                    }
                )
        return self._stats_cache

    @stats.setter
    def stats(self, value: dict):
        self._stats_cache = value
        self._stats_dirty = True

    def mark_stats_dirty(self):
        """Mark stats as needing a flush. Call after modifying stats dict in-place."""
        self._stats_dirty = True

    def reset_non_terminal(self) -> int:
        """Reset any non-terminal states (from crashed runs) back to pending.

        Returns the number of rows reset.
        """
        with self._lock:
            count = self._conn.execute(
                "UPDATE pipeline_files SET status = ?, stage = NULL, error = NULL WHERE status NOT IN (?, ?)",
                ("pending", "done", "pending"),
            ).rowcount
            self._conn.commit()
            return count

    def remove_ghosts(self, filepaths: list[str]) -> int:
        """Remove 'done' entries where the source file no longer exists.

        Args:
            filepaths: list of filepaths confirmed to not exist on disk.

        Returns the number of ghost entries removed.
        """
        with self._lock:
            for fp in filepaths:
                self._conn.execute("DELETE FROM pipeline_files WHERE filepath = ?", (fp,))
            self._conn.commit()
            return len(filepaths)

    def compact(self) -> int:
        """Remove REPLACED and SKIPPED entries."""
        with self._lock:
            cursor = self._conn.execute("SELECT COUNT(*) FROM pipeline_files WHERE status IN ('replaced', 'skipped')")
            count = cursor.fetchone()[0]
            if count > 0:
                self._conn.execute("DELETE FROM pipeline_files WHERE status IN ('replaced', 'skipped')")
                self._conn.commit()
                self.stats["archived_count"] = self.stats.get("archived_count", 0) + count
                self._stats_dirty = True
                self.save()
                remaining = self._conn.execute("SELECT COUNT(*) FROM pipeline_files").fetchone()[0]
                logging.info(f"Compacted state: removed {count} terminal entries ({remaining} remaining)")
        return count

    def get_all_files(self) -> dict[str, dict]:
        """Return all file entries as a dict keyed by filepath. Used by server API."""
        with self._lock:
            rows = self._conn.execute("SELECT * FROM pipeline_files").fetchall()
            return {row["filepath"]: self._row_to_dict(row) for row in rows}

    def _row_to_dict(self, row: sqlite3.Row) -> dict:
        """Convert a SQLite row to the dict format callers expect."""
        d = dict(row)
        d.pop("filepath", None)
        # Merge extras into the main dict
        extras_raw = d.pop("extras", "{}")
        extras = json.loads(extras_raw) if extras_raw else {}
        d.update(extras)
        # Convert SQLite integers back to booleans
        for bool_col in ("audio_only", "cleanup_strip", "sub_strip"):
            if bool_col in d and d[bool_col] is not None:
                d[bool_col] = bool(d[bool_col])
        # Remove None values for clean output
        return {k: v for k, v in d.items() if v is not None}

    @property
    def data(self) -> dict:
        """Compatibility property — returns the full state as a dict matching the old JSON format.

        Used by server API endpoints that return the full pipeline state, and by
        runner.py for direct iteration. Builds the dict on-demand from SQLite.
        """
        files = self.get_all_files()
        meta_rows = self._conn.execute("SELECT key, value FROM pipeline_meta").fetchall()
        meta = {}
        for row in meta_rows:
            try:
                meta[row[0]] = json.loads(row[1])
            except (json.JSONDecodeError, TypeError):
                meta[row[0]] = row[1]

        return {
            "created": meta.get("created", ""),
            "last_updated": meta.get("last_updated", datetime.now().isoformat()),
            "config": meta.get("config", {}),
            "stats": self.stats,
            "files": files,
        }

    def set_meta(self, key: str, value) -> None:
        """Set a metadata key (created, config, last_updated, etc.)."""
        val_str = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        with self._lock:
            self._conn.execute("INSERT OR REPLACE INTO pipeline_meta (key, value) VALUES (?, ?)", (key, val_str))
            self._conn.commit()

    def close(self):
        """Flush and close the database connection."""
        self.save()
        self._conn.close()
