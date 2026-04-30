"""Compliance audit: walk media_report.json, run the dashboard's compliance
check on every entry, and report what's still non-compliant against the
"100% on every metric" goal.

The pipeline's existing queue builder (``pipeline.__main__.categorise_entry``)
catches non-AV1 files for full re-encode and AV1-with-gaps for gap-fill. But
the gap-detection logic in ``analyse_gaps`` doesn't flag every compliance
violation surfaced by ``server.routers.library._compliance_for_entry`` —
notably foreign INTERNAL subs on already-AV1 files. So the queue builder
declares those files "skip" while the dashboard correctly flags them as
non-compliant. This audit closes that loop:

  * Read media_report.json
  * Run _compliance_for_entry per file (the same check the dashboard uses)
  * Bucket into ``compliant``, ``needs_full_reencode`` (non-AV1), and
    ``needs_targeted_fix`` (AV1 but compliance violation)
  * Print per-violation counts so the user can decide what to action

Read-only by default. ``--write-control`` will emit a control file the
pipeline can pick up (separate, gated step — not run automatically).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from paths import MEDIA_REPORT


def _load_compliance_check():
    """Late import: the server router pulls in FastAPI etc., which is
    overkill for a CLI audit. Wrap so the import error is human-readable."""
    try:
        from server.routers.library import _compliance_for_entry
    except ImportError as e:
        print(f"ERROR: could not import compliance checker: {e}", file=sys.stderr)
        print("       Run from the repo root with: uv run python -m tools.audit_compliance", file=sys.stderr)
        sys.exit(2)
    return _compliance_for_entry


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit media_report.json for compliance gaps.")
    parser.add_argument("--report", default=str(MEDIA_REPORT), help="Path to media_report.json")
    parser.add_argument(
        "--write-control",
        action="store_true",
        help=(
            "Write tools/audit_requeue.json with the non-compliant filepaths. "
            "Read-only without this flag."
        ),
    )
    parser.add_argument(
        "--limit-show",
        type=int,
        default=10,
        help="How many sample filepaths to print per violation bucket (default 10)",
    )
    args = parser.parse_args()

    _compliance_for_entry = _load_compliance_check()

    report_path = Path(args.report)
    if not report_path.exists():
        print(f"ERROR: report not found at {report_path}", file=sys.stderr)
        return 1

    with report_path.open(encoding="utf-8") as f:
        report = json.load(f)

    files = report.get("files", [])
    total = len(files)
    if total == 0:
        print("Report has zero files. Nothing to audit.")
        return 0

    compliant = 0
    needs_full_reencode: list[str] = []  # non-AV1
    needs_targeted_fix: list[tuple[str, list[str]]] = []  # AV1 but failing one+ rules
    violation_counter: Counter[str] = Counter()

    for entry in files:
        try:
            res = _compliance_for_entry(entry)
        except Exception as e:
            # Don't let one weird entry kill the audit.
            violation_counter["audit_check_failed"] += 1
            continue

        violations = res.get("violations") or []
        if not violations:
            compliant += 1
            continue

        for v in violations:
            violation_counter[v] += 1

        fp = entry.get("filepath", "")
        if not res.get("is_av1"):
            needs_full_reencode.append(fp)
        else:
            needs_targeted_fix.append((fp, violations))

    # ---------- Report ----------
    pct_compliant = 100 * compliant / total if total else 0
    print(f"Library: {total} files")
    print(f"  Fully compliant      : {compliant} ({pct_compliant:.1f}%)")
    print(f"  Non-AV1 (re-encode)  : {len(needs_full_reencode)}")
    print(f"  AV1 but non-compliant: {len(needs_targeted_fix)} (targeted fix)")
    print()

    print("Violation breakdown (a single file can hit multiple):")
    for v, n in violation_counter.most_common():
        print(f"  {v:30s} {n}")
    print()

    # Sample paths for the AV1-non-compliant bucket — that's the surprising one
    if needs_targeted_fix:
        print(f"AV1-but-non-compliant samples (first {args.limit_show}):")
        for fp, vs in needs_targeted_fix[: args.limit_show]:
            short = fp[-70:] if len(fp) > 70 else fp
            print(f"  {short}")
            print(f"    -> {', '.join(vs)}")
        print()

    if args.write_control:
        out = Path(__file__).parent / "audit_requeue.json"
        payload = {
            "generated_at": report.get("scan_date") or "",
            "total": total,
            "compliant": compliant,
            "needs_full_reencode": needs_full_reencode,
            "needs_targeted_fix": [
                {"filepath": fp, "violations": vs} for fp, vs in needs_targeted_fix
            ],
            "violation_counts": dict(violation_counter),
        }
        out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Wrote re-queue list to {out}")
    else:
        print("(Read-only mode. Re-run with --write-control to emit audit_requeue.json.)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
