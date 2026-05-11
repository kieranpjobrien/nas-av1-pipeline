"""One-off: pick 30 candidate AV1 files in 1-10 GB range and write them
to control/priority.json's paths list so the queue bumps them to the
front. Also captures a pre-encode snapshot for post-run comparison.

Run: ``uv run python -m tools.overnight_test_setup``
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from paths import PIPELINE_STATE_DB  # noqa: E402

BACKSLASH = chr(92)


def main() -> int:
    # 2026-05-11: audit lives in media_report.json's per-file ``audit`` field
    # (the audit_cq.json sidecar was removed — it drifted from media_report
    # and caused the dashboard to bulk-requeue already-encoded files).
    from tools.report_lock import read_report

    rep = read_report()
    audit = {
        f["filepath"]: f["audit"]
        for f in rep.get("files", [])
        if f.get("audit")
    }

    con = sqlite3.connect(str(PIPELINE_STATE_DB))
    cur = con.cursor()
    cur.execute(
        "SELECT filepath, status, extras FROM pipeline_files "
        "WHERE extras LIKE '%force_reencode%true%' AND status='pending'"
    )
    flagged = list(cur.fetchall())
    con.close()

    candidates: list[tuple[int, str, dict]] = []
    for fp, _st, _ex in flagged:
        a = audit.get(fp, {})
        if a.get("bucket") != "too_low":
            continue
        if a.get("source") not in ("tag", "state_db"):
            continue
        if not os.path.exists(fp):
            continue
        sz = os.path.getsize(fp)
        if 1_000_000_000 <= sz <= 10_000_000_000:
            candidates.append((sz, fp, a))

    candidates.sort()
    print(f"In-range candidates (1-10 GB, force_reencode=true, too_low+state_db): {len(candidates)}")

    buckets: dict[str, list] = {"1-2": [], "2-4": [], "4-7": [], "7-10": []}
    for sz, fp, a in candidates:
        if sz < 2_000_000_000:
            buckets["1-2"].append((sz, fp, a))
        elif sz < 4_000_000_000:
            buckets["2-4"].append((sz, fp, a))
        elif sz < 7_000_000_000:
            buckets["4-7"].append((sz, fp, a))
        else:
            buckets["7-10"].append((sz, fp, a))

    print()
    for k, v in buckets.items():
        print(f"  {k} GB pool: {len(v)}")

    chosen: list[tuple[int, str, dict]] = []
    target_per_bucket = 8
    for items in buckets.values():
        if len(items) <= target_per_bucket:
            chosen.extend(items)
        else:
            step = max(1, len(items) // target_per_bucket)
            chosen.extend(items[::step][:target_per_bucket])

    print()
    print(f"Chosen: {len(chosen)} files")
    print()
    print("Files (sorted by size):")
    for sz, fp, a in sorted(chosen):
        name = fp.split(BACKSLASH)[-1][:55]
        print(
            f"  {sz/1024**3:5.2f} GB  cq {a['current_cq']} -> {a['target_cq']:2}  "
            f"grade={a['grade']:18}  {name}"
        )

    prio_path = "F:/AV1_Staging/control/priority.json"
    try:
        with open(prio_path, encoding="utf-8") as f:
            prio = json.load(f)
    except (OSError, json.JSONDecodeError):
        prio = {}
    prio["paths"] = [fp for _, fp, _ in chosen]
    prio.setdefault("force", [])
    prio.setdefault("patterns", [])
    with open(prio_path, "w", encoding="utf-8") as f:
        json.dump(prio, f, indent=2)

    snapshot = {
        "created_at": "2026-05-08T21:30:00",
        "description": "Pre-encode snapshot for overnight 30-file shrink test",
        "files": [
            {
                "filepath": fp,
                "pre_encode_size_bytes": sz,
                "pre_encode_size_gb": round(sz / 1024**3, 3),
                "audit_current_cq": a["current_cq"],
                "audit_target_cq": a["target_cq"],
                "grade": a["grade"],
                "res_key": a["res_key"],
            }
            for sz, fp, a in chosen
        ],
    }
    with open("F:/AV1_Staging/overnight_test_snapshot.json", "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)

    print()
    print(f"priority.json updated with {len(chosen)} priority paths")
    print(f"pre-encode snapshot saved: F:/AV1_Staging/overnight_test_snapshot.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
