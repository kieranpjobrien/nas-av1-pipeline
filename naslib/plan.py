"""Diff the inventory against library standards and emit plan rows.

This is the ONLY write path into the :mod:`naslib.inventory.plans` table.
The planner is a pure function over the current ``files`` rows: given what
the library looks like now, it decides what work the runner should do next.

Policy is deliberately narrow and easy to audit:

* **encode_av1** — any file whose video codec is not AV1 and who has at
  least one audio stream. Refuse (never emit a plan for) zero-audio files.
* **transcode_audio** — an AV1 file that still carries a lossless audio
  track. EAC-3 is a far smaller lossy target that plays everywhere.
* **mux_sub** — an AV1 file missing English subtitles when a matching
  external ``.srt`` sidecar exists.
* **rename** — filename does not match its parent folder's title. Informational
  only; we do not auto-rename in the MVP. Reserved for future work.
* **tag_tmdb** — AV1 file with no ``tmdb`` row populated. Reserved; the MVP
  does not emit these automatically.
* **delete_sidecar** — a subtitle sidecar whose language was successfully
  mux'd into the MKV. Reserved; paired with ``mux_sub``.

A plan is **idempotent** by construction: re-running the planner against an
unchanged inventory will not emit a duplicate plan (we skip files that
already have a pending plan of the same action).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .inventory import (
    INVENTORY_DB,
    Action,
    FileRow,
    PlanRow,
    connect,
    fingerprint_or_none,
    insert_plan,
    iter_files,
    iter_pending_plans,
    transaction,
)

# ---------------------------------------------------------------------------
# Policy constants
# ---------------------------------------------------------------------------

#: English-language tokens recognised in audio/sub stream metadata.
ENGLISH_TOKENS: frozenset[str] = frozenset({"eng", "en", "english"})

#: Undetermined-language tokens treated as "probably English" for a library
#: where English is the assumed default. We keep ``und`` out of negative
#: filters but we do NOT treat it as English when deciding whether to mux a
#: sidecar — a confirmed English tag always wins.
UND_TOKENS: frozenset[str] = frozenset({"und", ""})

#: Video codecs that count as "already AV1".
AV1_CODECS: frozenset[str] = frozenset({"av1"})

#: Priority tiers. Smaller = earlier. We want re-encodes to block behind
#: subtitle-muxes and rename-tags because those are near-instant and make
#: the library more usable immediately.
PRIORITY = {
    "delete_sidecar": 10,
    "rename": 20,
    "tag_tmdb": 30,
    "mux_sub": 40,
    "transcode_audio": 60,
    "encode_av1": 100,
}


@dataclass(slots=True)
class PlanStats:
    """Counters returned by :func:`build_plans`."""

    emitted: int = 0
    skipped_existing: int = 0
    refused: int = 0
    considered: int = 0
    by_action: dict[Action, int] | None = None

    def __post_init__(self) -> None:
        """Default-initialise the per-action counter dict."""
        if self.by_action is None:
            self.by_action = {}

    def bump(self, action: Action) -> None:
        """Increment the per-action counter by one."""
        assert self.by_action is not None
        self.by_action[action] = self.by_action.get(action, 0) + 1


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_plans(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
) -> tuple[PlanStats, list[tuple[str, Action, dict[str, Any]]]]:
    """Walk the inventory and emit plan rows for any non-compliant file.

    Args:
        dry_run: If ``True``, compute the plans but do NOT insert them. The
            returned preview list lets the CLI show what would happen.
        db_path: Optional override for the SQLite path (tests).

    Returns:
        ``(stats, preview)`` — ``stats`` is always populated; ``preview``
        lists the ``(filepath, action, params)`` tuples that were (or would
        have been) emitted, in insertion order.
    """
    stats = PlanStats()
    preview: list[tuple[str, Action, dict[str, Any]]] = []

    with connect(db_path or INVENTORY_DB) as conn:
        pending_by_file = _existing_pending_plans(conn)

        rows = list(iter_files(conn))
        stats.considered = len(rows)

        # We accumulate the candidate actions without touching the DB, then
        # open a single transaction to insert them. Keeping plan emission
        # inside one transaction guarantees the planner never leaves the DB
        # in a torn state.
        candidates: list[tuple[FileRow, Action, dict[str, Any]]] = []
        for row in rows:
            for action, params in _candidates_for_file(row):
                if action in pending_by_file.get(row.filepath, set()):
                    stats.skipped_existing += 1
                    continue
                candidates.append((row, action, params))

        if dry_run:
            for row, action, params in candidates:
                preview.append((row.filepath, action, params))
                stats.bump(action)
                stats.emitted += 1
            return stats, preview

        with transaction(conn):
            for row, action, params in candidates:
                # Re-fingerprint at emission time so a later run can spot
                # files that changed between scan and plan. If the file is
                # gone we refuse — there's nothing to plan.
                fp = fingerprint_or_none(row.filepath)
                if fp is None:
                    stats.refused += 1
                    continue
                insert_plan(
                    conn,
                    filepath=row.filepath,
                    action=action,
                    params=params,
                    source_fingerprint=fp,
                    priority=PRIORITY.get(action, 100),
                )
                preview.append((row.filepath, action, params))
                stats.bump(action)
                stats.emitted += 1

    return stats, preview


# ---------------------------------------------------------------------------
# Candidate generation per file
# ---------------------------------------------------------------------------


def _candidates_for_file(row: FileRow) -> list[tuple[Action, dict[str, Any]]]:
    """Emit the list of plan candidates for one file, in priority order.

    The candidate list is deduplicated against pending plans at a higher
    level — this function only cares about the compliance rules.
    """
    out: list[tuple[Action, dict[str, Any]]] = []

    if _needs_av1_encode(row):
        out.append(("encode_av1", _encode_av1_params(row)))
    elif _needs_audio_transcode(row):
        # Only consider audio transcode on an already-AV1 file. An H.264 file
        # needs the full encode, which handles audio as a side-effect.
        out.append(("transcode_audio", _audio_transcode_params(row)))

    mux_params = _needs_sub_mux(row)
    if mux_params is not None:
        out.append(("mux_sub", mux_params))

    return out


# --- encode_av1 --------------------------------------------------------------


def _needs_av1_encode(row: FileRow) -> bool:
    """True iff the file should be re-encoded to AV1.

    Preconditions:

    * Video codec is set and not already AV1.
    * At least one audio stream (else we hard-refuse — destructive).
    * At least one video stream (implicitly, since ``video_codec`` is set).
    """
    if row.video_codec is None or row.video_codec in AV1_CODECS:
        return False
    if row.audio_count < 1:
        # Refuse: zero-audio source would produce a zero-audio output, which
        # is the exact failure mode we were asked to prevent.
        return False
    return True


def _encode_av1_params(row: FileRow) -> dict[str, Any]:
    """Build the params payload for an ``encode_av1`` plan.

    The runner uses these to build the ffmpeg command. We record the keep
    lists explicitly rather than re-deriving them at run time — that way
    the plan is a fully explicit record of intent.
    """
    # Keep every audio stream by default. Stripping is explicitly out of MVP
    # scope; the zero-audio catastrophe from the previous pipeline came from
    # an over-eager strip, and we will not risk repeating it.
    keep_audio = [a.index for a in row.audio_streams]
    # Keep English + forced + unknown-language subs.
    keep_subs = [
        s.index
        for s in row.sub_streams
        if s.language.lower() in ENGLISH_TOKENS or s.language.lower() in UND_TOKENS or s.forced
    ]
    # Also list any external English sidecars so the runner can mux them.
    external = [
        {"filename": e.filename, "language": e.language}
        for e in row.external_subs
        if e.language.lower() in ENGLISH_TOKENS or e.language.lower() in UND_TOKENS
    ]
    return {
        "keep_audio_indices": keep_audio,
        "keep_sub_indices": keep_subs,
        "external_subs": external,
        "is_hdr": bool(row.video_hdr),
        "target_resolution": _resolution_tag(row),
        "library_type": row.library_type,
    }


def _resolution_tag(row: FileRow) -> str:
    """Return a human-readable resolution bucket (used only by the runner)."""
    if row.video_height is None or row.video_width is None:
        return "SD"
    h, w = row.video_height, row.video_width
    if h >= 2100 or w >= 3800:
        return "4K"
    if h >= 1000 or w >= 1900:
        return "1080p"
    if h >= 700 or w >= 1200:
        return "720p"
    if h >= 400:
        return "480p"
    return "SD"


# --- transcode_audio ---------------------------------------------------------


def _needs_audio_transcode(row: FileRow) -> bool:
    """True iff an AV1 file has a lossless audio track worth transcoding."""
    if row.video_codec not in AV1_CODECS:
        return False
    return any(a.lossless for a in row.audio_streams)


def _audio_transcode_params(row: FileRow) -> dict[str, Any]:
    """Build the params payload for a ``transcode_audio`` plan."""
    indices = [a.index for a in row.audio_streams if a.lossless]
    return {
        "indices_to_transcode": indices,
        "expected_audio_count": row.audio_count,
    }


# --- mux_sub -----------------------------------------------------------------


def _needs_sub_mux(row: FileRow) -> dict[str, Any] | None:
    """Return params for a ``mux_sub`` plan, or ``None`` if none is needed.

    We emit a mux plan when:

    * The file has no internal English subtitle track, AND
    * At least one external English sidecar exists.
    """
    has_internal_en = any(s.language.lower() in ENGLISH_TOKENS for s in row.sub_streams)
    if has_internal_en:
        return None
    external_en = [e for e in row.external_subs if e.language.lower() in ENGLISH_TOKENS and not e.hi]
    if not external_en:
        return None
    # Prefer a non-forced, non-HI sidecar.
    first = next(
        (e for e in external_en if not e.forced),
        external_en[0],
    )
    parent = str(Path(row.filepath).parent)
    return {
        "sub_path": str(Path(parent) / first.filename),
        "language": first.language,
        "expected_audio_count": row.audio_count,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _existing_pending_plans(conn: Any) -> dict[str, set[str]]:
    """Return a ``{filepath: {action, ...}}`` map of pending plans."""
    out: dict[str, set[str]] = {}
    for row in iter_pending_plans(conn):
        out.setdefault(row.filepath, set()).add(row.action)
    return out


# ---------------------------------------------------------------------------
# Preview rendering (used by the CLI)
# ---------------------------------------------------------------------------


def describe_plans(preview: list[tuple[str, Action, dict[str, Any]]]) -> str:
    """Render a human-readable summary of a plan preview."""
    if not preview:
        return "no plans would be emitted."
    lines: list[str] = []
    by_action: dict[str, int] = {}
    for _, action, _ in preview:
        by_action[action] = by_action.get(action, 0) + 1
    for name, count in sorted(by_action.items()):
        lines.append(f"  {name}: {count}")
    return "\n".join(lines)


def describe_pending(db_path: Path | None = None) -> str:
    """Return a one-line summary of pending plans grouped by action."""
    by_action: dict[str, int] = {}
    with connect(db_path or INVENTORY_DB) as conn:
        for plan in iter_pending_plans(conn):
            by_action[plan.action] = by_action.get(plan.action, 0) + 1
    if not by_action:
        return "no pending plans."
    return ", ".join(f"{k}={v}" for k, v in sorted(by_action.items()))


def pending_snapshot(db_path: Path | None = None) -> list[PlanRow]:
    """Materialise every pending plan as a list (copies data out of conn)."""
    with connect(db_path or INVENTORY_DB) as conn:
        return list(iter_pending_plans(conn))
