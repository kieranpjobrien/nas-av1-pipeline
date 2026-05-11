"""Strip ``force_reencode=true`` from rows whose audit signal was
unreliable (bitrate_inferred). 2026-05-08 incident.

Background: the audit's bitrate-to-CQ inference table for 4K HDR only
has reliable medians at cq=22 and cq=24. Anything below 16,738 kbps
snaps to "cq=24 closest" even when the source is actually at cq=28 or
higher. Re-encoding those files at target cq=22 produced 25-30 GB
GROWTH per file with zero quality gain — 255 of 339 May re-encodes
grew rather than shrank.

This tool removes ``force_reencode`` from extras for any flagged row
whose audit entry (now in media_report.json's per-file ``audit`` field,
not the removed audit_cq.json sidecar) has ``source=bitrate_inferred``.
Rows with ``source=tag`` or ``source=state_db`` keep the flag (those
are high-confidence — the original CQ is known).
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.db is None:
        from paths import PIPELINE_STATE_DB

        db = str(PIPELINE_STATE_DB)
    else:
        db = args.db

    # 2026-05-11: audit lives in media_report.json's per-file ``audit`` field,
    # not the removed audit_cq.json sidecar.
    from tools.report_lock import read_report

    rep = read_report()
    audit = {
        f["filepath"]: f["audit"]
        for f in rep.get("files", [])
        if f.get("audit")
    }

    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(
        "SELECT filepath, status, extras FROM pipeline_files "
        "WHERE extras LIKE '%force_reencode%true%'"
    )
    rows = cur.fetchall()

    # Strip ALL bitrate_inferred flags. May data shows the inference is
    # unreliable across both classes:
    #   * 4K HDR feature films (target cq=22): 42 grew avg +12.8 GB / 0 shrank
    #   * Sitcom/animation (target cq=30):    213 grew avg +4 GB / 64 shrank
    # tag/state_db source is reliable (we have the actual original CQ
    # from the MKV ENCODER tag or the encode_params_used record), so
    # those flags are kept — those re-encodes go in the correct
    # direction.

    plan: list[tuple[str, dict]] = []
    counts = Counter()
    for fp, _st, ex in rows:
        try:
            extras = json.loads(ex or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            extras = {}
        a = audit.get(fp)
        if not a:
            counts["no_audit_entry"] += 1
            continue
        src = a.get("source")
        if src == "bitrate_inferred":
            extras["force_reencode"] = False
            plan.append((fp, extras))
            counts["bitrate_inferred_strip"] += 1
        elif src in ("tag", "state_db"):
            counts["high_confidence_keep"] += 1
        else:
            counts["other_skip"] += 1

    print(f"Flagged rows scanned: {len(rows)}")
    for k, n in counts.most_common():
        print(f"  {k}: {n}")
    print()

    if not plan:
        print("Nothing to strip.")
        conn.close()
        return 0

    if args.dry_run:
        print(f"DRY RUN — would strip flag from {len(plan)} rows.")
        print("Sample (5):")
        for fp, _ in plan[:5]:
            print(f"  {fp}")
        conn.close()
        return 0

    for fp, extras in plan:
        cur.execute(
            "UPDATE pipeline_files SET extras = ? WHERE filepath = ?",
            (json.dumps(extras), fp),
        )
    conn.commit()
    conn.close()
    print(f"Stripped force_reencode from {len(plan)} bitrate-inferred rows.")
    print("High-confidence (tag / state_db) flags preserved.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
