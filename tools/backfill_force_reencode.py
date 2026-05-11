"""One-shot backfill: stamp ``force_reencode=true`` on rows that the user
already queued for re-encode via the dashboard, before the 2026-05-07
fix wired the flag through the requeue endpoints.

Background: pre-2026-05-07 the requeue endpoints set ``status='pending'``
but didn't add anything to ``extras``. The orchestrator's
``categorise_entry`` skipped already-AV1 files based on codec alone, so
the user's 1300+ "Queue re-encode" clicks did nothing — the rows sat in
PENDING (or got flipped to DONE by gap_filler "nothing to do") and the
audit kept showing them in the too_low / too_high bucket.

This script identifies rows that show evidence of an explicit user
requeue (``reason LIKE '%requeue%'``) and:

  * for ``status='pending'`` rows — adds ``force_reencode=true`` to the
    existing extras, leaving status alone;
  * for ``status='done'`` or ``status='error'`` rows that came from a
    bulk requeue — resets to ``pending`` AND adds the flag, so the
    orchestrator picks them up on the next queue-build pass.

Idempotent: rows that already have ``force_reencode=true`` are
counted as "already done" and left untouched. Run with ``--dry-run``
first to see what would happen.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=None,
        help="pipeline_state.db path (default paths.PIPELINE_STATE_DB)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing.",
    )
    args = parser.parse_args()

    if args.db is None:
        from paths import PIPELINE_STATE_DB

        db = str(PIPELINE_STATE_DB)
    else:
        db = args.db

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute(
        "SELECT filepath, status, extras FROM pipeline_files "
        "WHERE reason LIKE '%requeue%'"
    ).fetchall()

    plan: list[tuple[str, str, str, dict]] = []  # (filepath, old_status, action, new_extras)
    actions = Counter()
    for r in rows:
        fp = r["filepath"]
        st = (r["status"] or "").lower()
        try:
            extras = json.loads(r["extras"] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            extras = {}

        if extras.get("force_reencode") is True and st == "pending":
            actions["already_flagged"] += 1
            continue

        extras["force_reencode"] = True

        if st == "pending":
            action = "stamp_flag"
        elif st in ("done", "error"):
            action = "reset_and_stamp"
        else:
            # Active (qualifying/fetching/processing/uploading) — leave alone.
            # Touching an in-flight row violates discipline rule 11.
            actions["skipped_in_flight"] += 1
            continue

        plan.append((fp, st, action, extras))
        actions[action] += 1

    print(f"Scanned {len(rows)} rows with reason LIKE '%requeue%'")
    print()
    print("Action breakdown:")
    for k, n in actions.most_common():
        print(f"  {k:25} : {n}")
    print()

    if not plan:
        print("Nothing to backfill.")
        conn.close()
        return 0

    if args.dry_run:
        print("DRY RUN — pass without --dry-run to apply.")
        # Print a sample.
        print()
        print("Sample (first 5):")
        for fp, st, action, _ in plan[:5]:
            print(f"  [{action:18}] {st:8} -> {fp}")
        conn.close()
        return 0

    # Apply.
    stamped = 0
    reset = 0
    for fp, _st, action, extras in plan:
        if action == "stamp_flag":
            cur.execute(
                "UPDATE pipeline_files SET extras = ? WHERE filepath = ?",
                (json.dumps(extras), fp),
            )
            stamped += 1
        elif action == "reset_and_stamp":
            cur.execute(
                "UPDATE pipeline_files SET status='pending', stage=NULL, error=NULL, "
                "extras=? WHERE filepath = ?",
                (json.dumps(extras), fp),
            )
            reset += 1
    conn.commit()
    conn.close()

    print(f"Stamped force_reencode on {stamped} pending rows")
    print(f"Reset + stamped {reset} done/error rows -> pending")
    print()
    print("Next: restart the pipeline so it picks up the flagged rows on its")
    print("next queue-build pass. Audit will reflect the change once the")
    print("re-encodes complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
