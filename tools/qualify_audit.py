"""Bulk requalification of the existing library.

For every ``status='done'`` file in the pipeline state DB, run the new
qualification pipeline (whisper detection + foreign-audio check).
Files that pass requalify keep their DONE status. Files that fail get
re-marked as FLAGGED_FOREIGN_AUDIO / FLAGGED_UNDETERMINED so they show
up on the dashboard's Flagged pane for the user to action.

Why this exists
---------------
The pre-2026-04-25 code's broken inference heuristic mis-IDed foreign-dub
content (Bluey Swedish, Amelie English-dub-only, Spirited Away English-dub
-only). 5/5 sampled Bluey episodes detected as Swedish. There are likely
hundreds of similar files in the library that have been silently encoded
with the wrong audio.

This script does the audit — once. Going forward, the new qualify worker
catches new files inline.

GPU coordination
----------------
Whisper requires GPU exclusivity (CLAUDE.md rule 9a — NVENC + whisper on
the same chip caused a BSOD). This script will REFUSE to run while the
pipeline encoder is active. Stop the pipeline first:

    POST http://localhost:8002/api/process/pipeline/stop

Then run:

    uv run python -m tools.qualify_audit                    # all files
    uv run python -m tools.qualify_audit --limit 50         # smoke test
    uv run python -m tools.qualify_audit --library-type movie
    uv run python -m tools.qualify_audit --dry-run          # report-only

Restart the pipeline when complete:

    POST http://localhost:8002/api/process/pipeline/start
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import Counter
from typing import Any

from paths import STAGING_DIR
from pipeline.qualify import QualifyOutcome, qualify_file
from pipeline.state import FileStatus, PipelineState

logger = logging.getLogger("qualify_audit")


# ---------------------------------------------------------------------------
# State lookup helpers
# ---------------------------------------------------------------------------


_STATE_DB = STAGING_DIR / "pipeline_state.db"
_REPORT_PATH = STAGING_DIR / "media_report.json"


def _load_report() -> dict[str, dict]:
    """Return {filepath: file_entry} from media_report.json.

    Qualification needs the full media_report fields (audio_streams,
    subtitle_streams, tmdb, etc.), not just the pipeline_state row.
    """
    if not _REPORT_PATH.exists():
        logger.error(f"media_report.json not found at {_REPORT_PATH}")
        return {}
    with open(_REPORT_PATH, encoding="utf-8") as f:
        rep = json.load(f)
    by_path: dict[str, dict] = {}
    for entry in rep.get("files", []) or []:
        fp = entry.get("filepath")
        if fp:
            by_path[fp] = entry
    return by_path


def _select_done_files(
    state: PipelineState,
    library_type: str | None = None,
    limit: int = 0,
) -> list[str]:
    """Return filepaths currently in DONE status, optionally filtered."""
    conn = state._get_conn()
    sql = "SELECT filepath FROM pipeline_files WHERE LOWER(status) = 'done'"
    params: list[Any] = []
    rows = conn.execute(sql, params).fetchall()
    paths = [r[0] for r in rows]
    if library_type:
        # Filter against the report — pipeline_state doesn't always carry library_type
        report = _load_report()
        paths = [p for p in paths if (report.get(p) or {}).get("library_type") == library_type]
    if limit:
        paths = paths[: int(limit)]
    return paths


# ---------------------------------------------------------------------------
# Per-file requalification
# ---------------------------------------------------------------------------


def _requalify_one(
    filepath: str,
    file_entry: dict | None,
    config: dict,
    state: PipelineState,
    *,
    dry_run: bool = False,
) -> str:
    """Requalify one file. Returns the resulting outcome string.

    On non-dry-run, updates the pipeline_state row to reflect the verdict:
      QUALIFIED / NOTHING_TO_DO     -> stays DONE (no DB write, idempotent)
      FLAGGED_FOREIGN               -> FLAGGED_FOREIGN_AUDIO with rationale
      FLAGGED_UND                   -> FLAGGED_UNDETERMINED with rationale
      ERROR                         -> ERROR with rationale
    """
    if file_entry is None:
        # File is in DB as DONE but missing from media_report. Treat as a
        # data-staleness issue, not a flag — leave alone.
        return "skipped_missing_report_entry"

    try:
        result = qualify_file(file_entry, config, use_whisper=True)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"qualify failed for {filepath}: {exc}")
        return "error_qualify_raised"

    name = os.path.basename(filepath)

    if result.outcome == QualifyOutcome.FLAGGED_FOREIGN:
        logger.warning(f"  FLAGGED_FOREIGN: {name} — {result.rationale}")
        if not dry_run:
            state.set_file(
                filepath,
                FileStatus.FLAGGED_FOREIGN_AUDIO,
                mode="qualify_audit",
                stage="requalify",
                reason=result.rationale,
            )
        return "flagged_foreign"

    if result.outcome == QualifyOutcome.FLAGGED_UND:
        logger.warning(f"  FLAGGED_UND: {name} — {result.rationale}")
        if not dry_run:
            state.set_file(
                filepath,
                FileStatus.FLAGGED_UNDETERMINED,
                mode="qualify_audit",
                stage="requalify",
                reason=result.rationale,
            )
        return "flagged_undetermined"

    if result.outcome == QualifyOutcome.ERROR:
        logger.warning(f"  ERROR: {name} — {result.rationale}")
        if not dry_run:
            state.set_file(
                filepath,
                FileStatus.ERROR,
                mode="qualify_audit",
                stage="requalify",
                error=result.rationale,
            )
        return "error_qualify_outcome"

    # QUALIFIED / NOTHING_TO_DO — file is fine, leave DONE.
    return "ok"


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m tools.qualify_audit",
        description="Bulk requalify the existing library — flags foreign-dub files.",
    )
    p.add_argument("--limit", type=int, default=0, help="Process at most N files")
    p.add_argument(
        "--library-type",
        choices=["movie", "series"],
        help="Restrict to one library type",
    )
    p.add_argument("--dry-run", action="store_true", help="Report-only; don't update DB")
    p.add_argument("--verbose", "-v", action="store_true", help="DEBUG logging")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    # Refuse to run if encoder is active (whisper + NVENC = BSOD per rule 9a).
    # This is also enforced at the whisper-load level but raising early saves
    # time on a partial run.
    try:
        import urllib.request

        resp = urllib.request.urlopen(
            "http://localhost:8002/api/process/pipeline/status", timeout=2
        )
        data = json.loads(resp.read())
        if data.get("status") == "running":
            logger.error(
                "Pipeline is currently encoding — stop it before requalifying. "
                "POST http://localhost:8002/api/process/pipeline/stop"
            )
            return 2
    except (ConnectionError, OSError, TimeoutError):
        pass  # API unreachable -> assume nothing's running

    from pipeline.config import build_config

    config = build_config()
    state = PipelineState(str(_STATE_DB))
    try:
        report_by_path = _load_report()
        paths = _select_done_files(state, library_type=args.library_type, limit=args.limit)
        logger.info(
            f"Requalifying {len(paths)} files "
            f"(library_type={args.library_type or 'all'}, dry_run={args.dry_run})"
        )

        outcomes: Counter[str] = Counter()
        t0 = time.monotonic()
        for i, fp in enumerate(paths, start=1):
            entry = report_by_path.get(fp)
            outcome = _requalify_one(fp, entry, config, state, dry_run=args.dry_run)
            outcomes[outcome] += 1
            if i % 25 == 0 or i == len(paths):
                elapsed = time.monotonic() - t0
                rate = i / elapsed if elapsed > 0 else 0
                logger.info(
                    f"  progress: {i}/{len(paths)}  rate={rate:.2f}/s  "
                    f"summary={dict(outcomes)}"
                )

        elapsed = time.monotonic() - t0
        logger.info(f"\nDone in {elapsed:.0f}s. Summary:")
        for k, v in sorted(outcomes.items()):
            logger.info(f"  {k:35s}  {v}")
        return 0
    finally:
        state.close()


if __name__ == "__main__":
    raise SystemExit(main())
