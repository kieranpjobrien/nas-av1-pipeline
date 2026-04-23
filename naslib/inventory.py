"""SQLite schema and single-writer helpers for the NAS inventory.

This module owns every SQL statement executed against ``inventory.sqlite``.
No other module in :mod:`naslib` is allowed to open a raw connection; they go
through the helpers defined here so the single-writer invariant is preserved:

* :func:`upsert_file` — called only by :mod:`naslib.scan`.
* :func:`insert_plan` / :func:`mark_plan_executed` — called only by
  :mod:`naslib.plan` and :mod:`naslib.run` respectively.
* :func:`read_file` / :func:`iter_pending_plans` — read-only, any caller.

The schema, pragmas, and connection lifecycle live here because they're
load-bearing: WAL mode and ``foreign_keys=ON`` are required for the design,
and any deviation would let multiple writers collide or let stale rows
pretend to be live.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

# ``paths`` lives at the repo root, one level up from this package. Callers
# running via ``python -m naslib`` (which is the supported entrypoint) have the
# repo root on ``sys.path`` already, but an import-time defensive add keeps the
# module importable from tests and ad-hoc scripts.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from paths import STAGING_DIR  # noqa: E402 — deliberate post-path-shim import

from . import SCHEMA_VERSION  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Default on-disk location of the inventory database.
INVENTORY_DB: Path = STAGING_DIR / "inventory.sqlite"

#: Action names recognised by the planner and runner. Keeping these as a
#: literal type union lets mypy catch typos when we call :func:`insert_plan`.
Action = Literal[
    "encode_av1",
    "transcode_audio",
    "mux_sub",
    "rename",
    "tag_tmdb",
    "delete_sidecar",
]

#: Result status values written by :mod:`naslib.run`.
ResultStatus = Literal["ok", "skipped", "refused", "failed"]

#: Library type discriminator.
LibraryType = Literal["movie", "series"]

#: Full schema definition, executed on first connection (``CREATE ... IF NOT
#: EXISTS``) so every process agrees on the shape of the database. Any change
#: to this string requires bumping :data:`naslib.SCHEMA_VERSION`.
SCHEMA_SQL: str = """
CREATE TABLE IF NOT EXISTS files (
    filepath TEXT PRIMARY KEY,
    library_type TEXT,
    size_bytes INTEGER,
    mtime REAL,
    duration_secs REAL,
    video_codec TEXT,
    video_width INTEGER,
    video_height INTEGER,
    video_hdr INTEGER,
    video_bit_depth INTEGER,
    video_bitrate_kbps INTEGER,
    audio_streams TEXT,
    audio_count INTEGER GENERATED ALWAYS AS (
        CASE WHEN audio_streams IS NULL THEN 0
             ELSE json_array_length(audio_streams) END
    ) STORED,
    sub_streams TEXT,
    sub_count INTEGER GENERATED ALWAYS AS (
        CASE WHEN sub_streams IS NULL THEN 0
             ELSE json_array_length(sub_streams) END
    ) STORED,
    external_subs TEXT,
    tmdb TEXT,
    filename_matches_folder INTEGER,
    scanned_at TEXT,
    scan_version INTEGER,
    damage_flag TEXT
);

CREATE INDEX IF NOT EXISTS idx_files_video_codec ON files(video_codec);
CREATE INDEX IF NOT EXISTS idx_files_library_type ON files(library_type);
CREATE INDEX IF NOT EXISTS idx_files_damage_flag
    ON files(damage_flag) WHERE damage_flag IS NOT NULL;

CREATE TABLE IF NOT EXISTS plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filepath TEXT NOT NULL,
    action TEXT NOT NULL,
    params TEXT,
    priority INTEGER DEFAULT 100,
    source_fingerprint TEXT,
    created_at TEXT,
    executed_at TEXT,
    result_status TEXT,
    result_msg TEXT,
    result_output_fingerprint TEXT,
    FOREIGN KEY (filepath) REFERENCES files(filepath)
);

CREATE INDEX IF NOT EXISTS idx_plans_pending
    ON plans(executed_at) WHERE executed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_plans_filepath ON plans(filepath);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AudioStream:
    """One audio stream's metadata as stored in ``files.audio_streams`` JSON."""

    index: int
    codec: str
    language: str
    channels: int
    bitrate_kbps: int | None
    lossless: bool
    detected_language: str | None = None


@dataclass(frozen=True, slots=True)
class SubStream:
    """One subtitle stream's metadata as stored in ``files.sub_streams`` JSON."""

    index: int
    codec: str
    language: str
    title: str
    forced: bool
    hi: bool


@dataclass(frozen=True, slots=True)
class ExternalSub:
    """One external subtitle sidecar as stored in ``files.external_subs`` JSON."""

    filename: str
    language: str
    forced: bool
    hi: bool


@dataclass(slots=True)
class FileRow:
    """A single row of the ``files`` table, decoded into typed form.

    The dataclass mirrors the columns of the ``files`` table one-for-one plus
    the derived ``audio_count`` / ``sub_count`` columns. :func:`read_file`
    returns one of these; :func:`upsert_file` accepts one.
    """

    filepath: str
    library_type: LibraryType | None = None
    size_bytes: int = 0
    mtime: float = 0.0
    duration_secs: float = 0.0
    video_codec: str | None = None
    video_width: int | None = None
    video_height: int | None = None
    video_hdr: bool = False
    video_bit_depth: int | None = None
    video_bitrate_kbps: int | None = None
    audio_streams: list[AudioStream] = field(default_factory=list)
    sub_streams: list[SubStream] = field(default_factory=list)
    external_subs: list[ExternalSub] = field(default_factory=list)
    tmdb: dict[str, Any] | None = None
    filename_matches_folder: bool = True
    scanned_at: str = ""
    scan_version: int = SCHEMA_VERSION
    damage_flag: str | None = None

    @property
    def audio_count(self) -> int:
        """Number of audio streams (matches the generated column)."""
        return len(self.audio_streams)

    @property
    def sub_count(self) -> int:
        """Number of subtitle streams (matches the generated column)."""
        return len(self.sub_streams)


@dataclass(slots=True)
class PlanRow:
    """A single row of the ``plans`` table, decoded into typed form."""

    id: int
    filepath: str
    action: Action
    params: dict[str, Any]
    priority: int
    source_fingerprint: str
    created_at: str
    executed_at: str | None = None
    result_status: ResultStatus | None = None
    result_msg: str | None = None
    result_output_fingerprint: str | None = None


# ---------------------------------------------------------------------------
# Connection lifecycle
# ---------------------------------------------------------------------------


@contextmanager
def connect(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    """Open a connection with WAL + foreign keys enabled, and close on exit.

    Args:
        db_path: Override the default :data:`INVENTORY_DB` location. Primarily
            used by the test suite; production callers should pass ``None``.

    Yields:
        A configured :class:`sqlite3.Connection`. The connection is closed
        automatically when the ``with`` block exits, even on exception.
    """
    target = db_path or INVENTORY_DB
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(target),
        timeout=30.0,
        isolation_level=None,  # we manage transactions explicitly
    )
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.executescript(SCHEMA_SQL)
        _ensure_meta(conn)
        yield conn
    finally:
        conn.close()


def _ensure_meta(conn: sqlite3.Connection) -> None:
    """Write schema-version and installation-time keys to the ``meta`` table."""
    conn.execute(
        "INSERT OR IGNORE INTO meta(key, value) VALUES (?, ?)",
        ("schema_version", str(SCHEMA_VERSION)),
    )
    conn.execute(
        "INSERT OR IGNORE INTO meta(key, value) VALUES (?, ?)",
        ("created_at", _now_iso()),
    )


def _now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(UTC).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# File-row helpers (writers live in scan.py; this module just provides the SQL)
# ---------------------------------------------------------------------------


def upsert_file(conn: sqlite3.Connection, row: FileRow) -> None:
    """Insert or update a ``files`` row.

    This is the ONLY write path into the ``files`` table. Called exclusively
    by :mod:`naslib.scan`. The damage-flag is set by the scanner before the
    row is handed to this function; we do not compute it here so that the
    single-writer contract is clean.

    Args:
        conn: Open connection from :func:`connect`.
        row: Populated :class:`FileRow` — all fields must be set by the caller.
    """
    row.scanned_at = row.scanned_at or _now_iso()
    row.scan_version = row.scan_version or SCHEMA_VERSION
    conn.execute(
        """
        INSERT INTO files (
            filepath, library_type, size_bytes, mtime, duration_secs,
            video_codec, video_width, video_height, video_hdr,
            video_bit_depth, video_bitrate_kbps,
            audio_streams, sub_streams, external_subs,
            tmdb, filename_matches_folder,
            scanned_at, scan_version, damage_flag
        ) VALUES (
            :filepath, :library_type, :size_bytes, :mtime, :duration_secs,
            :video_codec, :video_width, :video_height, :video_hdr,
            :video_bit_depth, :video_bitrate_kbps,
            :audio_streams, :sub_streams, :external_subs,
            :tmdb, :filename_matches_folder,
            :scanned_at, :scan_version, :damage_flag
        )
        ON CONFLICT(filepath) DO UPDATE SET
            library_type = excluded.library_type,
            size_bytes = excluded.size_bytes,
            mtime = excluded.mtime,
            duration_secs = excluded.duration_secs,
            video_codec = excluded.video_codec,
            video_width = excluded.video_width,
            video_height = excluded.video_height,
            video_hdr = excluded.video_hdr,
            video_bit_depth = excluded.video_bit_depth,
            video_bitrate_kbps = excluded.video_bitrate_kbps,
            audio_streams = excluded.audio_streams,
            sub_streams = excluded.sub_streams,
            external_subs = excluded.external_subs,
            tmdb = CASE
                WHEN excluded.tmdb IS NULL THEN files.tmdb
                ELSE excluded.tmdb END,
            filename_matches_folder = excluded.filename_matches_folder,
            scanned_at = excluded.scanned_at,
            scan_version = excluded.scan_version,
            damage_flag = excluded.damage_flag
        """,
        _row_to_params(row),
    )


def read_file(conn: sqlite3.Connection, filepath: str) -> FileRow | None:
    """Fetch one file row by path, or return ``None`` if absent."""
    cur = conn.execute("SELECT * FROM files WHERE filepath = ?", (filepath,))
    raw = cur.fetchone()
    if raw is None:
        return None
    return _row_from_sqlite(raw)


def iter_files(
    conn: sqlite3.Connection,
    *,
    library_type: LibraryType | None = None,
    video_codec: str | None = None,
    damage_only: bool = False,
) -> Iterator[FileRow]:
    """Stream file rows, optionally filtered by library type or codec.

    Args:
        conn: Open connection from :func:`connect`.
        library_type: Filter to ``"movie"`` or ``"series"``. ``None`` returns both.
        video_codec: Filter to a specific normalised codec (``"av1"``, ``"h264"``,
            ``"hevc"``, ...). ``None`` returns all.
        damage_only: If ``True``, return only rows with ``damage_flag`` set.

    Yields:
        One :class:`FileRow` per matching row. Caller must consume the
        iterator within the ``with connect()`` block.
    """
    clauses: list[str] = []
    params: list[Any] = []
    if library_type is not None:
        clauses.append("library_type = ?")
        params.append(library_type)
    if video_codec is not None:
        clauses.append("video_codec = ?")
        params.append(video_codec)
    if damage_only:
        clauses.append("damage_flag IS NOT NULL")
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    cur = conn.execute(f"SELECT * FROM files{where} ORDER BY filepath", params)
    for raw in cur:
        yield _row_from_sqlite(raw)


def delete_file(conn: sqlite3.Connection, filepath: str) -> None:
    """Remove a file row. Called by :mod:`naslib.scan` when a file vanishes."""
    conn.execute("DELETE FROM files WHERE filepath = ?", (filepath,))


def _row_to_params(row: FileRow) -> dict[str, Any]:
    """Convert a :class:`FileRow` to a dict of SQL parameters."""
    return {
        "filepath": row.filepath,
        "library_type": row.library_type,
        "size_bytes": int(row.size_bytes),
        "mtime": float(row.mtime),
        "duration_secs": float(row.duration_secs),
        "video_codec": row.video_codec,
        "video_width": row.video_width,
        "video_height": row.video_height,
        "video_hdr": int(bool(row.video_hdr)),
        "video_bit_depth": row.video_bit_depth,
        "video_bitrate_kbps": row.video_bitrate_kbps,
        "audio_streams": json.dumps([_asdict(a) for a in row.audio_streams]),
        "sub_streams": json.dumps([_asdict(s) for s in row.sub_streams]),
        "external_subs": json.dumps([_asdict(e) for e in row.external_subs]),
        "tmdb": json.dumps(row.tmdb) if row.tmdb is not None else None,
        "filename_matches_folder": int(bool(row.filename_matches_folder)),
        "scanned_at": row.scanned_at,
        "scan_version": int(row.scan_version),
        "damage_flag": row.damage_flag,
    }


def _row_from_sqlite(raw: sqlite3.Row) -> FileRow:
    """Decode a :class:`sqlite3.Row` into a typed :class:`FileRow`."""
    audio = [_audio_from_dict(d) for d in json.loads(raw["audio_streams"] or "[]")]
    subs = [_sub_from_dict(d) for d in json.loads(raw["sub_streams"] or "[]")]
    external = [_external_from_dict(d) for d in json.loads(raw["external_subs"] or "[]")]
    tmdb_raw = raw["tmdb"]
    lib_type = raw["library_type"]
    return FileRow(
        filepath=raw["filepath"],
        library_type=lib_type if lib_type in ("movie", "series") else None,
        size_bytes=int(raw["size_bytes"] or 0),
        mtime=float(raw["mtime"] or 0.0),
        duration_secs=float(raw["duration_secs"] or 0.0),
        video_codec=raw["video_codec"],
        video_width=raw["video_width"],
        video_height=raw["video_height"],
        video_hdr=bool(raw["video_hdr"] or 0),
        video_bit_depth=raw["video_bit_depth"],
        video_bitrate_kbps=raw["video_bitrate_kbps"],
        audio_streams=audio,
        sub_streams=subs,
        external_subs=external,
        tmdb=json.loads(tmdb_raw) if tmdb_raw else None,
        filename_matches_folder=bool(raw["filename_matches_folder"] or 0),
        scanned_at=raw["scanned_at"] or "",
        scan_version=int(raw["scan_version"] or 0),
        damage_flag=raw["damage_flag"],
    )


def _audio_from_dict(data: dict[str, Any]) -> AudioStream:
    """Rehydrate an :class:`AudioStream` from its JSON dict form."""
    return AudioStream(
        index=int(data.get("index", 0)),
        codec=str(data.get("codec", "")),
        language=str(data.get("language", "und")),
        channels=int(data.get("channels", 0)),
        bitrate_kbps=data.get("bitrate_kbps"),
        lossless=bool(data.get("lossless", False)),
        detected_language=data.get("detected_language"),
    )


def _sub_from_dict(data: dict[str, Any]) -> SubStream:
    """Rehydrate a :class:`SubStream` from its JSON dict form."""
    return SubStream(
        index=int(data.get("index", 0)),
        codec=str(data.get("codec", "")),
        language=str(data.get("language", "und")),
        title=str(data.get("title", "")),
        forced=bool(data.get("forced", False)),
        hi=bool(data.get("hi", False)),
    )


def _external_from_dict(data: dict[str, Any]) -> ExternalSub:
    """Rehydrate an :class:`ExternalSub` from its JSON dict form."""
    return ExternalSub(
        filename=str(data.get("filename", "")),
        language=str(data.get("language", "und")),
        forced=bool(data.get("forced", False)),
        hi=bool(data.get("hi", False)),
    )


def _asdict(obj: Any) -> dict[str, Any]:
    """Convert a frozen dataclass to a plain dict for JSON serialisation."""
    return {k: getattr(obj, k) for k in obj.__slots__}


# ---------------------------------------------------------------------------
# Plan-row helpers
# ---------------------------------------------------------------------------


def insert_plan(
    conn: sqlite3.Connection,
    *,
    filepath: str,
    action: Action,
    params: dict[str, Any],
    source_fingerprint: str,
    priority: int = 100,
) -> int:
    """Insert a pending plan row. Returns the new plan id.

    Called only by :mod:`naslib.plan`. The source fingerprint is the staleness
    detector; the runner refuses to execute a plan whose current file
    fingerprint no longer matches this value.

    Args:
        conn: Open connection from :func:`connect`.
        filepath: Absolute path to the target file (must already exist in ``files``).
        action: One of the action names defined in :data:`Action`.
        params: Action-specific parameters, serialised as JSON.
        source_fingerprint: ``"<size_bytes>:<mtime>"`` string captured at plan
            creation time. The runner requires this to still match at execution
            time.
        priority: Smaller numbers execute earlier. Default 100.

    Returns:
        The auto-generated ``plans.id`` of the inserted row.
    """
    cur = conn.execute(
        """
        INSERT INTO plans (filepath, action, params, priority,
                           source_fingerprint, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            filepath,
            action,
            json.dumps(params),
            priority,
            source_fingerprint,
            _now_iso(),
        ),
    )
    plan_id = cur.lastrowid
    assert plan_id is not None  # SQLite always returns a row id for AUTOINCREMENT
    return int(plan_id)


def mark_plan_executed(
    conn: sqlite3.Connection,
    *,
    plan_id: int,
    status: ResultStatus,
    msg: str,
    output_fingerprint: str | None = None,
) -> None:
    """Record the outcome of a plan row. Only called by :mod:`naslib.run`."""
    conn.execute(
        """
        UPDATE plans
           SET executed_at = ?,
               result_status = ?,
               result_msg = ?,
               result_output_fingerprint = ?
         WHERE id = ?
        """,
        (_now_iso(), status, msg, output_fingerprint, plan_id),
    )


def iter_pending_plans(
    conn: sqlite3.Connection,
    *,
    action: Action | None = None,
    limit: int | None = None,
) -> Iterator[PlanRow]:
    """Stream pending plans ordered by priority then id (FIFO within a tier).

    Args:
        conn: Open connection from :func:`connect`.
        action: Optional filter — only yield plans of this action type.
        limit: Optional cap on the number of rows returned.
    """
    clauses = ["executed_at IS NULL"]
    params: list[Any] = []
    if action is not None:
        clauses.append("action = ?")
        params.append(action)
    sql = "SELECT * FROM plans WHERE " + " AND ".join(clauses) + " ORDER BY priority ASC, id ASC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    cur = conn.execute(sql, params)
    for raw in cur:
        yield _plan_from_sqlite(raw)


def read_plan(conn: sqlite3.Connection, plan_id: int) -> PlanRow | None:
    """Fetch a single plan by id."""
    cur = conn.execute("SELECT * FROM plans WHERE id = ?", (plan_id,))
    raw = cur.fetchone()
    return _plan_from_sqlite(raw) if raw is not None else None


def _plan_from_sqlite(raw: sqlite3.Row) -> PlanRow:
    """Decode a :class:`sqlite3.Row` into a typed :class:`PlanRow`."""
    action = raw["action"]
    status = raw["result_status"]
    return PlanRow(
        id=int(raw["id"]),
        filepath=raw["filepath"],
        action=action,
        params=json.loads(raw["params"] or "{}"),
        priority=int(raw["priority"] or 100),
        source_fingerprint=raw["source_fingerprint"] or "",
        created_at=raw["created_at"] or "",
        executed_at=raw["executed_at"],
        result_status=status,
        result_msg=raw["result_msg"],
        result_output_fingerprint=raw["result_output_fingerprint"],
    )


# ---------------------------------------------------------------------------
# Fingerprinting
# ---------------------------------------------------------------------------


def fingerprint_path(filepath: str) -> str:
    """Return the ``"<size>:<mtime>"`` fingerprint for a file on disk.

    This is the staleness detector used by the runner. We deliberately avoid
    hashing the contents — a 20GB MKV on a NAS over SMB would take minutes,
    and the combination of size + mtime catches every realistic change path
    (re-encode, rename, copy-over).

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    st = os.stat(filepath)
    return f"{st.st_size}:{st.st_mtime}"


def fingerprint_or_none(filepath: str) -> str | None:
    """Like :func:`fingerprint_path` but returns ``None`` if the file is gone."""
    try:
        return fingerprint_path(filepath)
    except (FileNotFoundError, OSError):
        return None


# ---------------------------------------------------------------------------
# Meta table helpers
# ---------------------------------------------------------------------------


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Set a value in the ``meta`` table, overwriting any existing entry."""
    conn.execute(
        "INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    """Fetch a value from the ``meta`` table, or ``None`` if not present."""
    cur = conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
    raw = cur.fetchone()
    return raw["value"] if raw is not None else None


def stamp_last_scan(conn: sqlite3.Connection) -> None:
    """Record the current UTC time as the most recent scan timestamp."""
    set_meta(conn, "last_full_scan_at", _now_iso())


# ---------------------------------------------------------------------------
# Transaction helper
# ---------------------------------------------------------------------------


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Wrap a block of writes in a SQLite transaction.

    ``sqlite3`` is in autocommit mode when ``isolation_level=None``, so we
    issue explicit BEGIN/COMMIT here. Used by :mod:`naslib.scan` to amortise
    per-row ``upsert_file`` calls — a full rescan of 20k files completes in
    seconds under one transaction and many minutes without it.
    """
    start = time.monotonic()
    conn.execute("BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
    _ = start  # keep local for profiling hook; not exported
