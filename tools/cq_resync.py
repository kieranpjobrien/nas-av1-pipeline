"""Flip every DONE/REPLACED AV1 row whose current_cq != target_cq back
to PENDING with ``force_reencode=true`` so the pipeline re-encodes
them at the current grade-rule target.

Why this exists
---------------
Pre-2026-05-21 the pipeline's compliance check (qualify + categorise_entry)
gated on codec + audio config + sub config. It did NOT check
current_cq vs target_cq. Result: an AV1 file encoded at CQ 30 under
the old policy stayed DONE forever even after the tv_animation grade
rule shifted the target to CQ 37. Operator's rule (re-stated 2026-05-21):
"if they're too low then they're not done — that needs to be stopped."

The forward fix lives in:
  * ``pipeline.__main__.categorise_entry`` — routes off-target AV1 to full_gamut.
  * ``pipeline.full_gamut`` — qualify NOTHING_TO_DO short-circuit also
    refuses to mark DONE when cur != tgt.

This script handles the BACKWARD migration: the existing DONE-off-target
rows that the queue builder will otherwise keep skipping (queue builder
short-circuits on terminal state before categorise_entry ever runs).

Dry-run by default. ``--apply`` writes.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from paths import MEDIA_REPORT, PIPELINE_STATE_DB


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true", help="Actually write (default: dry-run)")
    parser.add_argument("--bucket", choices=["all", "confident", "inferred"],
                        default="all",
                        help="Restrict to confident buckets (too_low/too_high only) "
                             "or to inferred_uncertain only. Default: all off-target.")
    args = parser.parse_args()

    rep = json.loads(Path(MEDIA_REPORT).read_text(encoding="utf-8"))
    by_path = {f["filepath"]: f for f in rep.get("files", [])}

    con = sqlite3.connect(str(PIPELINE_STATE_DB))
    con.row_factory = sqlite3.Row

    rows = con.execute(
        "SELECT filepath, status, extras FROM pipeline_files "
        "WHERE status IN ('done', 'replaced')"
    ).fetchall()

    candidates: list[tuple[str, int, int, str]] = []  # (filepath, cur, tgt, bucket)
    for r in rows:
        fp = r["filepath"]
        f = by_path.get(fp)
        if not f:
            continue
        a = f.get("audit") or {}
        cur, tgt = a.get("current_cq"), a.get("target_cq")
        bucket = a.get("bucket") or "?"
        if cur is None or tgt is None or cur == tgt:
            continue
        if args.bucket == "confident" and bucket not in ("too_low", "too_high"):
            continue
        if args.bucket == "inferred" and bucket != "inferred_uncertain":
            continue
        candidates.append((fp, cur, tgt, bucket))

    print(f"DONE/REPLACED rows whose cur != tgt (bucket filter={args.bucket}): {len(candidates)}")
    by_bucket: dict[str, int] = {}
    for _fp, _c, _t, b in candidates:
        by_bucket[b] = by_bucket.get(b, 0) + 1
    for b, n in sorted(by_bucket.items(), key=lambda x: -x[1]):
        print(f"  {b:24s} {n:5d}")

    if not args.apply:
        print("\nFirst 10 examples:")
        for fp, cur, tgt, b in candidates[:10]:
            bn = fp.rsplit("\\", 1)[-1].rsplit("/", 1)[-1]
            print(f"  cur={cur:2d} tgt={tgt:2d} [{b:18s}]  {bn}")
        print("\n(dry-run — pass --apply to flip these rows)")
        return 0

    # Apply: status -> pending, set force_reencode=true in extras, clear stage/prep fields
    print("\nApplying...")
    updated = 0
    for fp, cur, tgt, _b in candidates:
        row = con.execute("SELECT extras FROM pipeline_files WHERE filepath = ?", (fp,)).fetchone()
        try:
            extras = json.loads(row["extras"] or "{}")
        except Exception:
            extras = {}
        # Wipe prep-cached state so the next prep pass redoes the work,
        # and stamp force_reencode so the qualify short-circuit cannot
        # silently mark this DONE again before the new CQ-adherence
        # guards take effect (belt and braces — the guards alone would
        # be enough, but this is one-shot migration code, and a stuck
        # force_reencode flag is harmless: full_gamut clears it on
        # successful DONE).
        extras["force_reencode"] = True
        extras.pop("prep_done", None)
        extras.pop("prep_data", None)
        extras["cq_resync"] = {"from_cur": cur, "to_tgt": tgt}
        ex_json = json.dumps(extras)
        # Read-back-parse guard (matches state.set_file's defense).
        json.loads(ex_json)
        con.execute(
            "UPDATE pipeline_files SET status = ?, stage = NULL, error = NULL, "
            "reason = ?, extras = ? WHERE filepath = ?",
            ("pending", f"cq_resync: cur={cur} tgt={tgt} — flipped from done", ex_json, fp),
        )
        updated += 1

    con.commit()
    con.close()
    print(f"\nFlipped {updated} rows to pending with force_reencode=true.")
    print("Next queue-build will pick them up; categorise_entry's CQ-adherence")
    print("guard routes them to full_gamut for re-encode at target_cq.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
