"""Re-queue files that are non-compliant against the current rule set but
were marked terminal in pipeline_state.db under older rules.

Reads ``tools/audit_requeue.json`` (produced by ``tools.audit_compliance``)
and resets the matching ``pipeline_files`` rows from ``done`` (or any
terminal status) back to ``pending`` so the orchestrator's queue refresh
worker picks them up. Fresh ``categorise_entry`` evaluation routes:

  * Non-AV1 → full re-encode (fetch + NVENC + upload)
  * AV1 with foreign subs / non-EAC-3 audio / dirty filename → gap-filler
    (in-place mkvmerge / mkvpropedit / rename via the local_mux backend —
    SMB I/O only, no fetch+upload)

Dry-run by default. Pass ``--apply`` to actually mutate the state DB.

Bucket flags let you re-queue only the cheap-to-fix work first:
  * ``--bucket targeted``  (default)  AV1-non-compliant only — most can be
                                       fixed in-place without fetch+upload
  * ``--bucket full``      Non-AV1 only — these all need full re-encode
  * ``--bucket all``       Both buckets

Also clears the ``extras`` JSON (stale prep_data pointing at long-gone
staging files would otherwise confuse the new run) and any prior
``error`` text.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path

from paths import PIPELINE_STATE_DB

REQUEUE_LIST = Path(__file__).parent / "audit_requeue.json"


def _load_requeue_list() -> dict | None:
    if not REQUEUE_LIST.exists():
        return None
    return json.loads(REQUEUE_LIST.read_text(encoding="utf-8"))


def _bucket_paths(audit: dict, bucket: str) -> list[tuple[str, list[str]]]:
    """Return [(filepath, violations)] for the chosen bucket.

    For ``needs_full_reencode`` we synthesise ``["video_not_av1"]`` since
    the audit only stores violations for the targeted-fix entries.
    """
    out: list[tuple[str, list[str]]] = []
    if bucket in ("targeted", "all"):
        for entry in audit.get("needs_targeted_fix") or []:
            fp = entry.get("filepath")
            vs = entry.get("violations") or []
            if fp:
                out.append((fp, vs))
    if bucket in ("full", "all"):
        for fp in audit.get("needs_full_reencode") or []:
            out.append((fp, ["video_not_av1"]))
    return out


def _bucket_summary(paths: list[tuple[str, list[str]]]) -> Counter:
    """Count violations across the chosen bucket."""
    c: Counter = Counter()
    for _, vs in paths:
        for v in vs:
            c[v] += 1
    return c


def _classify_action(violations: list[str]) -> str:
    """Coarse categorisation of what kind of work the file needs.

    Used to surface to the user which rows will result in expensive
    fetch+upload vs. cheap in-place fixes.
    """
    if "video_not_av1" in violations:
        return "FULL_REENCODE (fetch+NVENC+upload)"
    if "audio_codec_not_eac3" in violations:
        return "AUDIO_TRANSCODE (in-place ffmpeg, no NVENC)"
    if "audio_foreign_language" in violations or "subs_foreign_present" in violations:
        return "TRACK_STRIP (in-place mkvmerge, SMB only)"
    if "subs_english_missing" in violations:
        return "SUB_MUX (mux external sub if available)"
    if "filename_mismatch" in violations:
        return "RENAME (instant, no transfer)"
    if "no_tmdb" in violations:
        return "TMDB_BACKFILL (network, but tiny)"
    return "OTHER"


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-queue non-compliant terminal-state rows.")
    parser.add_argument(
        "--bucket",
        choices=("targeted", "full", "all"),
        default="targeted",
        help=(
            "Which violations to re-queue. 'targeted' = AV1-but-non-compliant only "
            "(in-place fix candidates). 'full' = non-AV1 (full re-encode). "
            "'all' = both. Default: targeted."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually update the state DB. Without this flag, dry-run only.",
    )
    parser.add_argument(
        "--db",
        default=str(PIPELINE_STATE_DB),
        help=f"Path to pipeline_state.db (default: {PIPELINE_STATE_DB})",
    )
    args = parser.parse_args()

    audit = _load_requeue_list()
    if not audit:
        print(
            f"ERROR: {REQUEUE_LIST} not found.\n"
            f"       Run first: uv run python -m tools.audit_compliance --write-control",
            file=sys.stderr,
        )
        return 2

    paths = _bucket_paths(audit, args.bucket)
    if not paths:
        print(f"Nothing in bucket '{args.bucket}'. Nothing to do.")
        return 0

    summary = _bucket_summary(paths)
    print(f"Bucket: {args.bucket}")
    print(f"  Files to re-queue: {len(paths)}")
    print(f"  Violation tallies (a single file may hit multiple):")
    for v, n in summary.most_common():
        print(f"    {v:30s} {n}")
    print()

    # Action classification
    action_counts: Counter = Counter()
    for _, vs in paths:
        action_counts[_classify_action(vs)] += 1
    print(f"  Action types (worst-case per file):")
    for a, n in action_counts.most_common():
        print(f"    {a:50s} {n}")
    print()

    if not args.apply:
        print("DRY RUN — no rows updated. Re-run with --apply to mutate the state DB.")
        return 0

    # ---- Apply ----
    conn = sqlite3.connect(args.db)
    cur = conn.cursor()
    updated = 0
    missing = 0
    skipped_already_pending = 0

    for fp, _vs in paths:
        cur.execute("SELECT status FROM pipeline_files WHERE filepath = ?", (fp,))
        row = cur.fetchone()
        if not row:
            missing += 1
            continue
        status = (row[0] or "").lower()
        if status == "pending":
            skipped_already_pending += 1
            continue
        # Reset to pending and wipe stale extras + any error message.
        # Stale extras can carry prep_data pointing at long-gone staging files
        # (the encoded/ wipe scenario) which would confuse the next run.
        cur.execute(
            "UPDATE pipeline_files SET status='pending', extras='{}', error=NULL, "
            "stage=NULL, reason=NULL WHERE filepath = ?",
            (fp,),
        )
        if cur.rowcount:
            updated += 1
    conn.commit()
    conn.close()

    print(f"Applied:")
    print(f"  Reset to pending           : {updated}")
    print(f"  Already pending (skipped)  : {skipped_already_pending}")
    print(f"  Not in state DB (skipped)  : {missing}")
    print()
    print("Refresh worker polls every 60s — within ~1 min the orchestrator")
    print("will re-evaluate these rows via categorise_entry and dispatch")
    print("most to the gap-filler queue (in-place fix, no fetch+upload).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
