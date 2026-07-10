"""Pipeline state management — SQLite-backed state that survives crashes.

Uses SQLite with WAL mode for safe concurrent access from both the pipeline
process and the server process. The API surface is identical to the old
JSON-based implementation so callers don't need to change.
"""

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime
from enum import Enum
from typing import Optional


class FileStatus(str, Enum):
    """Pipeline state machine.

    Happy path:
        PENDING → QUALIFYING → QUALIFIED → FETCHING → PROCESSING → UPLOADING → DONE

    Failure / sidetrack paths:
        ─→ ERROR                    transient/structural failure, retried later
        ─→ FLAGGED_FOREIGN_AUDIO    audio language ≠ TMDb original_language
                                    (Bluey dubbed Swedish, Amelie English-dub-only,
                                    etc.) — file is encoded but the user should
                                    delete + Sonarr/Radarr re-grab from the UI
        ─→ FLAGGED_UNDETERMINED     audio is `und` and whisper couldn't
                                    confidently identify it — manual review
        ─→ FLAGGED_MANUAL           other ambiguous cases the qualify stage
                                    surfaces for the user
        ─→ FLAGGED_CORRUPT          ffprobe could not determine the video
                                    codec — file is unreadable / truncated.
                                    User should delete and re-acquire.

    Each file is owned by one thread start to finish; no handoffs. The 'stage'
    field tracks which substep is active. Adding a state here is the first
    step — also update:
      * orchestrator queue-build filters (must include only PENDING + ERROR
        for retry; QUALIFYING/QUALIFIED/FLAGGED_* live in their own pools)
      * frontend status pills (so the new states render correctly)
      * invariants (no_done_with_deferred_reason and friends)
    """

    PENDING = "pending"
    QUALIFYING = "qualifying"
    QUALIFIED = "qualified"
    FETCHING = "fetching"
    PROCESSING = "processing"
    UPLOADING = "uploading"
    DONE = "done"
    ERROR = "error"
    FLAGGED_FOREIGN_AUDIO = "flagged_foreign_audio"
    FLAGGED_UNDETERMINED = "flagged_undetermined"
    FLAGGED_MANUAL = "flagged_manual"
    FLAGGED_CORRUPT = "flagged_corrupt"


# Status groupings used by the orchestrator + UI for rapid filtering.
# Source of truth: any change here MUST also be reflected in the
# is_* helpers below and the SQL clauses that use them.

# Statuses that mean "no more work to do" — the queue builder skips these.
TERMINAL_STATUSES: frozenset[FileStatus] = frozenset({
    FileStatus.DONE,
    FileStatus.FLAGGED_FOREIGN_AUDIO,
    FileStatus.FLAGGED_UNDETERMINED,
    FileStatus.FLAGGED_MANUAL,
    FileStatus.FLAGGED_CORRUPT,
})

# Statuses where a file is mid-flight. The orchestrator reaps these on
# startup (they were stranded by a crash).
ACTIVE_STATUSES: frozenset[FileStatus] = frozenset({
    FileStatus.QUALIFYING,
    FileStatus.FETCHING,
    FileStatus.PROCESSING,
    FileStatus.UPLOADING,
})

# All FLAGGED_* — the UI's Flagged pane queries on this group.
FLAGGED_STATUSES: frozenset[FileStatus] = frozenset({
    FileStatus.FLAGGED_FOREIGN_AUDIO,
    FileStatus.FLAGGED_UNDETERMINED,
    FileStatus.FLAGGED_MANUAL,
    FileStatus.FLAGGED_CORRUPT,
})


def is_flagged(status: str | FileStatus) -> bool:
    """True if ``status`` is any FLAGGED_* state."""
    if isinstance(status, str):
        try:
            status = FileStatus(status)
        except ValueError:
            return False
    return status in FLAGGED_STATUSES


def _remove_from_priority_json(filepath: str, staging_dir: str | None = None) -> bool:
    """Drop ``filepath`` from ``control/priority.json -> paths`` if present.

    Called by ``set_file`` whenever a file transitions to a terminal
    status (done / flagged_*) so the priority list stays a live "still
    to do" view rather than an append-only log.

    Atomic write (``.tmp`` + ``os.replace``) so a concurrent reader
    never sees a truncated priority.json. Best-effort: errors are
    swallowed because the state DB write has already committed; the
    queue-rebuild prune in ``pipeline.__main__._prune_done_from_priority``
    acts as a safety net to catch anything this misses.
    """
    import os as _os
    if staging_dir is None:
        # Import lazily to avoid circular import on module load
        from paths import STAGING_DIR
        staging_dir = str(STAGING_DIR)
    prio_path = _os.path.join(staging_dir, "control", "priority.json")
    if not _os.path.exists(prio_path):
        return False
    try:
        with open(prio_path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False
    paths = data.get("paths") or []
    if filepath not in paths:
        return False
    data["paths"] = [p for p in paths if p != filepath]
    tmp = prio_path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        _os.replace(tmp, prio_path)
    except OSError:
        try:
            if _os.path.exists(tmp):
                _os.remove(tmp)
        except OSError:
            pass
        return False
    return True


def is_terminal(status: str | FileStatus) -> bool:
    """True if ``status`` means 'no more pipeline work needed'."""
    if isinstance(status, str):
        try:
            status = FileStatus(status)
        except ValueError:
            return False
    return status in TERMINAL_STATUSES


# Columns stored directly (not in extras JSON) for efficient queries
_DIRECT_COLS = {
    "status",
    "mode",
    "added",
    "last_updated",
    "tier",
    "local_path",
    "output_path",
    "dest_path",
    "error",
    "stage",
    "reason",
    "res_key",
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
        """Flush stats to the database. Always writes since callers mutate the dict directly.

        Read-back-parse validation (2026-05-19): the pipeline_stats row
        was found corrupted with the same arbitrary-word-separator
        substitution that hit the file-backed JSONs on 2026-05-18. The
        corrupt row killed every finalize_upload's post-replace step
        (``state.stats["completed"] += 1`` → JSON parse on the read →
        ``Expecting ':' delimiter`` ). Validating the just-serialised
        bytes parses before committing means corrupt output never
        reaches the DB.
        """
        with self._lock:
            if self._stats_cache is not None:
                stats_json = json.dumps(self._stats_cache)
                try:
                    json.loads(stats_json)
                except json.JSONDecodeError as je:
                    raise ValueError(
                        f"state.save(): json.dumps(stats) produced invalid "
                        f"JSON ({je}). Refusing to corrupt pipeline_stats. "
                        f"Raw head: {stats_json[:200]!r}"
                    ) from je
                self._conn.execute("UPDATE pipeline_stats SET data = ? WHERE id = 1", (stats_json,))
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
            # On terminal success, scrub stale failure fields from the
            # previous attempt. Otherwise DONE rows keep error= and
            # reason= text from the run that errored before the reset,
            # which trips the no_done_with_error_reason invariant and
            # generally looks like a Rule-1 violation. Callers can still
            # set their own reason — kwargs win over scrub defaults.
            kwargs.setdefault("error", None)
            kwargs.setdefault("stage", None)
            # If the caller did not pass a fresh reason AND the existing
            # row has a stale error-flavoured reason, clear it. We don't
            # blindly wipe — a "compression ratio 18.4%" reason from the
            # actual encode is useful audit history.
            if "reason" not in kwargs:
                kwargs["__scrub_stale_reason"] = True
            # Successful encode means the file is no longer "one cycle
            # from terminal" — reset breaker counters so future failures
            # of an unrelated kind get a fresh consecutive count.
            kwargs.setdefault("integrity_failure_count", 0)
            kwargs.setdefault("compliance_refuse_count", 0)

        # Pop our internal sentinel before normal merge — handled below.
        _scrub_stale_reason = kwargs.pop("__scrub_stale_reason", False)

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

            # Scrub stale failure text on DONE transition. The previous
            # row's ``reason`` often carries forward from an earlier
            # error or manual reset ("reset 2026-05-12: error was
            # WinError 59..."); leaving it on the DONE row tripped the
            # no_done_with_error_reason invariant. Caller can opt out
            # by passing an explicit reason= kwarg (handled by the
            # setdefault in the DONE pre-block).
            if _scrub_stale_reason:
                # Heuristic: if existing reason contains failure-flavoured
                # keywords, clear it. Otherwise leave it alone (might be
                # legit audit history like "compression 18.4%").
                old_reason = (direct.get("reason") or "").lower()
                if any(kw in old_reason for kw in (
                    "error", "fail", "winerror", "stuck", "reset",
                    "compliance unfixed", "refuse", "broken",
                )):
                    direct["reason"] = None

            # Apply new values
            for k, v in all_data.items():
                if k in _DIRECT_COLS:
                    direct[k] = v
                else:
                    extras[k] = v

            # Single INSERT OR REPLACE — atomic, no race
            cols = ["filepath"] + list(direct.keys()) + ["extras"]
            placeholders = ", ".join(["?"] * len(cols))
            # Explicit separators (2026-05-23): see
            # pipeline.orchestrator._write_heavy_worker_status for the
            # incident. JSONEncoder.key_separator gets corrupted from
            # ': ' to a random interned string mid-process. Passing
            # separators explicitly bypasses the class default. This
            # path is called on every state.set_file() — critical to
            # defend.
            extras_json = json.dumps(extras, separators=(",", ": "))

            # Defense-in-depth: validate the extras JSON we're about to
            # commit. The 2026-05-18/19 corruption incident showed
            # arbitrary-word substitution of the JSON ``: `` separator
            # appearing in state extras (and in 4 file-backed JSONs).
            # File writes are now guarded; this validates the SQLite
            # write path too. If json.dumps is somehow producing
            # corrupt output (the smoking-gun scenario), this raises
            # with a stack trace pointing at the caller — the corrupt
            # bytes never reach SQLite.
            try:
                json.loads(extras_json)
            except json.JSONDecodeError as je:
                # Capture the smoking gun. Across 4 recurrences (utf-8,
                # frame, search, status) we've never caught WHY json.dumps
                # produces corrupt output. The hypotheses are:
                #   (a) someone monkey-patched JSONEncoder.key_separator
                #       at class level — would affect all dumps calls.
                #   (b) memory corruption on this box (BSOD history).
                #   (c) a thread mutating the dict mid-dumps.
                # Capture enough state here to discriminate next time.
                import json.encoder as _je
                cls_keysep = getattr(_je.JSONEncoder, "key_separator", "<missing>")
                cls_itemsep = getattr(_je.JSONEncoder, "item_separator", "<missing>")
                inst = _je.JSONEncoder()
                inst_keysep = getattr(inst, "key_separator", "<missing>")
                inst_itemsep = getattr(inst, "item_separator", "<missing>")
                # Retry with explicit separators — if that succeeds while
                # the bare call failed, separator mutation is the smoking gun.
                try:
                    explicit = json.dumps(extras, separators=(",", ":"))
                    explicit_ok = True
                    try:
                        json.loads(explicit)
                        explicit_loads_ok = True
                    except Exception:
                        explicit_loads_ok = False
                    explicit_head = explicit[:200]
                except Exception as ex:
                    explicit_ok = False
                    explicit_loads_ok = False
                    explicit_head = f"<retry raised: {ex!r}>"
                # Truncated repr of the source dict — small primitives only.
                safe_keys = sorted(extras.keys()) if isinstance(extras, dict) else []
                logging.error(
                    "set_file CORRUPTION CAUGHT: %s\n"
                    "  raw head: %r\n"
                    "  class key_sep=%r item_sep=%r\n"
                    "  inst  key_sep=%r item_sep=%r\n"
                    "  explicit-sep retry: dumps_ok=%s loads_ok=%s head=%r\n"
                    "  extras keys=%r",
                    filepath, extras_json[:200],
                    cls_keysep, cls_itemsep,
                    inst_keysep, inst_itemsep,
                    explicit_ok, explicit_loads_ok, explicit_head,
                    safe_keys,
                )
                raise ValueError(
                    f"set_file({filepath!r}): json.dumps(extras) produced "
                    f"invalid JSON ({je}). Refusing to write corrupt extras "
                    f"to state DB. Raw head: {extras_json[:200]!r} | "
                    f"cls key_sep={cls_keysep!r} item_sep={cls_itemsep!r} | "
                    f"explicit-retry dumps_ok={explicit_ok} loads_ok={explicit_loads_ok}"
                ) from je

            vals = [filepath] + list(direct.values()) + [extras_json]
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

        # Terminal transition → prune from priority.json immediately so the
        # list reflects "still to do", not "every add we ever made". The
        # queue-rebuild prune still runs as a safety net; this is the
        # primary real-time path. Best-effort: any failure is swallowed
        # because the DB write has already committed.
        if is_terminal(status):
            try:
                _remove_from_priority_json(filepath)
            except Exception:
                pass

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

    def count_active_with_local(self, statuses: list[str]) -> int:
        """Count rows in given statuses that have a non-empty local_path.

        Direct SQL — no JSON decode of the extras column. Used by the fetch
        worker's prefetch-cap loop, which formerly went through
        ``get_all_files`` and json.loads'd every row's extras on every tick.
        That hotspot triggered a recurring Windows access-violation in the
        Python 3.14 json decoder (2026-05-16 19:01 pipeline stall) — narrowing
        the query to real columns removes the crash surface entirely.
        """
        if not statuses:
            return 0
        placeholders = ",".join("?" * len(statuses))
        with self._lock:
            row = self._conn.execute(
                f"SELECT COUNT(*) FROM pipeline_files "
                f"WHERE status IN ({placeholders}) "
                f"AND local_path IS NOT NULL AND local_path != ''",
                statuses,
            ).fetchone()
        return int(row[0]) if row else 0

    def reset_non_terminal(self) -> int:
        """Reset any non-terminal states (from crashed runs) back to pending.

        TERMINAL_STATUSES (DONE + all FLAGGED_*) are preserved — those are
        deliberate end states the user must action via the UI, not crash
        residue. Earlier versions excluded only DONE/PENDING, which silently
        flipped FLAGGED_* back to PENDING on every pipeline restart and let
        the encode loop re-process audit-flagged files with the wrong audio.

        Also clears stale ``prep_data`` and ``prep_done`` from the extras JSON
        column. Without this, restart after orchestrator-startup cleanup of
        ``F:/AV1_Staging/fetch/`` would leave rows with prep_data pointing at
        deleted local files — the next encode pickup blindly trusts the
        cached path and ffmpeg hits ENOENT (the 2026-04-29 Lost Thing /
        Futurama / Star Wars Rebels stall pattern). A defensive guard in
        ``full_gamut._encode_only`` catches it at runtime; this is the
        preventative fix at reset time.

        Returns the number of rows reset.
        """
        with self._lock:
            preserve = ["pending"] + [s.value for s in TERMINAL_STATUSES]
            placeholders = ",".join("?" * len(preserve))

            # Two-step: status reset + extras scrub. SQLite has no JSON_REMOVE
            # on the version we're targeting reliably, so do extras in Python.
            rows_to_reset = self._conn.execute(
                f"SELECT filepath, status, stage, output_path, extras "
                f"FROM pipeline_files WHERE status NOT IN ({placeholders})",
                preserve,
            ).fetchall()

            count = 0
            for fp, status, stage, output_path, extras_json in rows_to_reset:
                # 2026-05-19 carve-out: UPLOADING rows with stage=pending_upload
                # AND a valid output_path on disk are NOT crash residue — they're
                # finished encodes waiting on a live upload worker that died.
                # Resetting these to pending + clearing output_path forces a
                # full re-encode of work we already have on disk (57 GB / 23
                # files in the canonical case). Leave them as-is so the new
                # upload worker picks them up on its first poll.
                if (
                    status == FileStatus.UPLOADING.value
                    and stage == "pending_upload"
                    and output_path
                    and os.path.exists(output_path)
                ):
                    continue

                try:
                    extras = json.loads(extras_json or "{}")
                except (json.JSONDecodeError, TypeError):
                    extras = {}
                # Drop the in-flight artefacts that don't survive a restart.
                # local_path / output_path can be re-derived; prep_data is
                # the dangerous one because it's an opaque cached blob.
                for k in ("prep_data", "prep_done", "local_path", "output_path"):
                    extras.pop(k, None)
                self._conn.execute(
                    "UPDATE pipeline_files SET status = ?, stage = NULL, error = NULL, "
                    "local_path = NULL, output_path = NULL, extras = ? WHERE filepath = ?",
                    ("pending", json.dumps(extras), fp),
                )
                count += 1
            self._conn.commit()
            return count

    def remove_ghosts(self, filepaths: list[str]) -> int:
        """Remove terminal entries (done / auto-flagged) where the source file no longer exists.

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
        """Convert a SQLite row to the dict format callers expect.

        ``extras`` is a JSON blob carrying derived/cached fields (prep_data,
        fetch timings, etc.). If a crash mid-write leaves it malformed,
        ``json.loads`` would raise and take the whole pipeline down on the
        next restart — exactly what happened on 2026-05-14 07:24, when a
        Python access-violation (0xc0000005) left the Six Feet Under
        S03E07 row with ``"force_reencode"E-AC-3true, ...`` instead of
        valid JSON. Subsequent ``python -m pipeline`` startups crashed
        in ``build_queues`` before reset_non_terminal could run.

        Authoritative state (status, error, paths) lives in real columns,
        so dropping a corrupt extras blob loses only derived data. Log
        loudly and continue rather than letting one poisoned row block
        every other file in the queue.
        """
        d = dict(row)
        filepath = d.pop("filepath", None)
        extras_raw = d.pop("extras", "{}")
        if extras_raw:
            try:
                extras = json.loads(extras_raw)
            except (json.JSONDecodeError, TypeError) as exc:
                logging.error(
                    "Corrupt extras JSON for %s (%s); treating as empty. "
                    "Raw head: %r",
                    filepath,
                    exc,
                    extras_raw[:120],
                )
                extras = {}
        else:
            extras = {}
        d.update(extras)
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
