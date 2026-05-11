"""Compare the overnight snapshot to the current state of disk + state DB +
encode_history. Report which of the 24 priority files have completed,
how much they shrank (or grew — we hope nothing grew), and the totals.

Run: ``uv run python -m tools.overnight_test_report``
Output is also written to F:/AV1_Staging/overnight_test_morning_report.txt
so it survives Claude session restarts.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from paths import PIPELINE_STATE_DB  # noqa: E402

BACKSLASH = chr(92)


def _fmt_gb(b: int) -> str:
    return f"{b / 1024**3:.2f} GB"


def main() -> int:
    snapshot_path = "F:/AV1_Staging/overnight_test_snapshot.json"
    out_path = "F:/AV1_Staging/overnight_test_morning_report.txt"
    history_path = "F:/AV1_Staging/encode_history.jsonl"

    if not os.path.exists(snapshot_path):
        print(f"snapshot missing: {snapshot_path}")
        return 1

    with open(snapshot_path, encoding="utf-8") as f:
        snap = json.load(f)

    # Build a filepath -> latest history entry map (encode_history.jsonl
    # is append-only; latest wins).
    history_by_path: dict[str, dict] = {}
    with open(history_path, encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            fp = r.get("filepath")
            if fp and r.get("timestamp", "") >= snap["created_at"]:
                history_by_path[fp] = r

    # State DB lookup
    con = sqlite3.connect(str(PIPELINE_STATE_DB))
    cur = con.cursor()

    rows: list[dict] = []
    completed = 0
    grew = 0
    shrank = 0
    pending = 0
    in_flight = 0
    error = 0
    total_pre = 0
    total_post = 0
    total_saved = 0

    for entry in snap["files"]:
        fp = entry["filepath"]
        pre_size = entry["pre_encode_size_bytes"]
        total_pre += pre_size

        # State DB status
        cur.execute("SELECT status, stage, error FROM pipeline_files WHERE filepath = ?", (fp,))
        st_row = cur.fetchone()
        status = st_row[0] if st_row else None
        stage = st_row[1] if st_row else None
        err = st_row[2] if st_row else None

        # Disk size now
        try:
            disk_size = os.path.getsize(fp) if os.path.exists(fp) else None
        except OSError:
            disk_size = None

        # History entry from this overnight session?
        h = history_by_path.get(fp)

        rec = {
            "filename": fp.split(BACKSLASH)[-1],
            "filepath": fp,
            "pre_size": pre_size,
            "disk_size": disk_size,
            "history_entry": h,
            "state_status": status,
            "state_stage": stage,
            "state_error": err,
            "pre_cq": entry["audit_current_cq"],
            "target_cq": entry["audit_target_cq"],
            "grade": entry["grade"],
        }

        if h:
            completed += 1
            post_size = h["output_bytes"]
            saved = pre_size - post_size  # positive = shrank
            total_post += post_size
            total_saved += saved
            rec["post_size"] = post_size
            rec["saved"] = saved
            rec["used_cq"] = (h.get("encode_params") or {}).get("cq")
            rec["used_grade"] = (h.get("encode_params") or {}).get("content_grade")
            rec["encode_minutes"] = round((h.get("encode_time_secs") or 0) / 60, 1)
            if saved > 0:
                shrank += 1
            else:
                grew += 1
        elif status == "error":
            error += 1
            total_post += disk_size or pre_size
        elif status in ("processing", "encoding", "fetching", "uploading", "qualifying"):
            in_flight += 1
            total_post += disk_size or pre_size
        else:
            pending += 1
            total_post += disk_size or pre_size

        rows.append(rec)

    con.close()

    # Build the report
    lines: list[str] = []
    lines.append(f"OVERNIGHT TEST REPORT — generated {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"Snapshot taken: {snap['created_at']}")
    lines.append(f"Files in test: {len(snap['files'])}")
    lines.append("")
    lines.append("=" * 80)
    lines.append("SUMMARY")
    lines.append("=" * 80)
    lines.append(f"  completed:  {completed:3} ({shrank} shrank, {grew} grew)")
    lines.append(f"  in_flight:  {in_flight:3}")
    lines.append(f"  pending:    {pending:3}")
    lines.append(f"  error:      {error:3}")
    lines.append("")
    lines.append(f"  total pre-size:   {_fmt_gb(total_pre)}")
    lines.append(f"  total post-size:  {_fmt_gb(total_post)}  (live + completed)")
    lines.append(
        f"  total saved:      {'+' if total_saved >= 0 else ''}{_fmt_gb(total_saved)}  "
        f"({100 * total_saved / total_pre:.1f}% reduction)"
    )
    lines.append("")
    lines.append("=" * 80)
    lines.append("PER-FILE BREAKDOWN")
    lines.append("=" * 80)
    lines.append("")

    # Sort: completed first (biggest savings first), then in-flight, then pending, then error
    def _sort_key(r: dict) -> tuple:
        if r.get("history_entry"):
            return (0, -(r.get("saved") or 0))
        if r["state_status"] in ("processing", "encoding", "fetching", "uploading", "qualifying"):
            return (1, 0)
        if r["state_status"] == "error":
            return (3, 0)
        return (2, 0)

    for r in sorted(rows, key=_sort_key):
        name = r["filename"][:55]
        if r.get("history_entry"):
            saved_gb = r["saved"] / 1024**3
            ratio = r["post_size"] / r["pre_size"] if r["pre_size"] else 0
            arrow = "→"
            tag = "[SHRANK]" if r["saved"] > 0 else "[GREW]  "
            lines.append(
                f"  {tag} {_fmt_gb(r['pre_size']):>9} {arrow} {_fmt_gb(r['post_size']):>9}  "
                f"{'+' if saved_gb >= 0 else ''}{saved_gb:6.2f} GB  "
                f"ratio={ratio:.3f}  cq {r['pre_cq']}{arrow}{r.get('used_cq','?')}  "
                f"({r['encode_minutes']:.0f}m)  {name}"
            )
        elif r["state_status"] in ("processing", "encoding", "fetching", "uploading", "qualifying"):
            lines.append(
                f"  [LIVE]   {_fmt_gb(r['pre_size']):>9}              "
                f"status={r['state_status']:12} stage={r['state_stage'] or '-'}  {name}"
            )
        elif r["state_status"] == "error":
            err_short = (r["state_error"] or "")[:60]
            lines.append(
                f"  [ERROR]  {_fmt_gb(r['pre_size']):>9}              "
                f"err={err_short}  {name}"
            )
        else:
            lines.append(
                f"  [WAIT]   {_fmt_gb(r['pre_size']):>9}              "
                f"status={r['state_status'] or 'unknown'}  {name}"
            )

    lines.append("")
    if completed > 0:
        avg_ratio = total_post / total_pre if total_pre else 0
        verdict = (
            "✅ PASS — re-encodes are shrinking files in the right direction"
            if total_saved > 0 and grew == 0
            else (
                "⚠️  PARTIAL — most shrank but some grew (check the [GREW] entries above)"
                if shrank > grew
                else "❌ FAIL — files grew on average. Investigate."
            )
        )
        lines.append(verdict)
        lines.append(f"  Avg compression ratio across completed: {avg_ratio:.3f}")

    report = "\n".join(lines)
    print(report)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\nReport saved to: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
