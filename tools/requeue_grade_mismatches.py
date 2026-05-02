"""Re-queue files whose stamped CQ doesn't match the current grade target.

Reads ``F:/AV1_Staging/audit_cq.json`` (produced by ``tools.audit_encode_cq``)
and resets pipeline_state rows so the encoder picks them up fresh.

Default: targets the ``too_low`` and ``unknown`` buckets — these are
files that should be encoded HARDER than they currently are. Re-encoding
the existing AV1 at the new (higher) CQ saves disk space and the AV1->AV1
generation loss at sitcom-grade CQ is imperceptible.

Excludes the ``too_high`` bucket — those are files where the new rule
wants gentler CQ than the current encode. We can't recover lost detail
by re-encoding the existing AV1; the user must decide whether each one
is worth a fresh re-download from source. Use ``--bucket too_high`` if
you've handled the re-download manually and want to mark them for
re-encode against the new (gentler) target.

Dry-run by default. ``--apply`` mutates state DB.

Usage:
  uv run python -m tools.requeue_grade_mismatches              # dry-run
  uv run python -m tools.requeue_grade_mismatches --apply
  uv run python -m tools.requeue_grade_mismatches --bucket all --apply
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path

from paths import PIPELINE_STATE_DB

AUDIT_PATH = Path("F:/AV1_Staging/audit_cq.json")


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-queue files where stamped CQ ≠ grade target.")
    parser.add_argument(
        "--bucket",
        choices=("default", "all", "too_low", "unknown", "too_high"),
        default="default",
        help=(
            "Which audit buckets to re-queue. 'default' = too_low + unknown "
            "(re-encode candidates that save space). 'all' adds too_high. "
            "Single-bucket modes target one only."
        ),
    )
    parser.add_argument("--apply", action="store_true", help="Actually mutate state DB. Default is dry-run.")
    parser.add_argument("--audit", default=str(AUDIT_PATH), help="Audit JSON path")
    parser.add_argument("--db", default=str(PIPELINE_STATE_DB), help="State DB path")
    args = parser.parse_args()

    audit_path = Path(args.audit)
    if not audit_path.exists():
        print(
            f"ERROR: {audit_path} not found.\n"
            f"       Run first: uv run python -m tools.audit_encode_cq",
            file=sys.stderr,
        )
        return 2

    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    results = audit.get("results") or []
    if not results:
        print("Audit has no results — nothing to re-queue.")
        return 0

    # Pick which buckets to act on
    if args.bucket == "default":
        target_buckets = {"too_low", "unknown"}
    elif args.bucket == "all":
        target_buckets = {"too_low", "unknown", "too_high"}
    else:
        target_buckets = {args.bucket}

    targets = [r for r in results if r["bucket"] in target_buckets]
    summary = Counter(r["bucket"] for r in targets)

    print(f"Audit produced {len(results)} entries.")
    print(f"Targeting buckets: {', '.join(sorted(target_buckets))}")
    print(f"Files to re-queue: {len(targets)}")
    for b, n in summary.most_common():
        print(f"  {b:12s} {n}")
    print()

    # Distribution by current_cq -> target_cq for sanity
    delta_dist: Counter = Counter()
    for r in targets:
        if r["bucket"] == "unknown":
            delta_dist[("unknown", r["target_cq"])] += 1
        else:
            delta_dist[(r["current_cq"], r["target_cq"])] += 1
    print("Top current -> target deltas:")
    for (cur, tgt), n in delta_dist.most_common(8):
        print(f"  {cur} -> {tgt}: {n} files")
    print()

    if not args.apply:
        print("DRY RUN — no rows touched. Re-run with --apply to mutate the state DB.")
        return 0

    # Apply: reset matched rows to pending. For unknown rows that may not
    # have a state DB row at all, queue refresh will pick them up via
    # categorise_entry on the next pipeline cycle.
    conn = sqlite3.connect(args.db)
    cur = conn.cursor()
    reset = 0
    skipped_no_row = 0
    for r in targets:
        fp = r["filepath"]
        cur.execute("SELECT status FROM pipeline_files WHERE filepath = ?", (fp,))
        row = cur.fetchone()
        if not row:
            skipped_no_row += 1
            continue
        cur.execute(
            "UPDATE pipeline_files SET status='pending', extras='{}', error=NULL, "
            "stage=NULL, reason=NULL WHERE filepath = ?",
            (fp,),
        )
        if cur.rowcount:
            reset += 1
    conn.commit()
    conn.close()

    print(f"Reset to pending: {reset}")
    print(f"Skipped (no state row, will be picked up by queue refresh): {skipped_no_row}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
