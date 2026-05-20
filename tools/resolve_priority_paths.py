"""Re-resolve stale paths in control/priority.json against the current
media_report.json + on-disk state.

Why this exists
---------------
The priority list is a user-curated set of "encode these first" paths.
When files get renamed on disk (bulk_rename_clean for the
``Show (Year) - SXXEYY - Title`` -> ``Show SXXEYY Title`` pattern) or
swapped to a different container (.mp4 -> .mkv from Sonarr re-grab),
the priority.json entries become orphans pointing at paths that no
longer exist. Pre-fix, the pipeline's priority bump silently failed
to match anything for those rows — the user clicked Prioritise and
nothing happened.

Pre-2026-05-21 the remediation was a brute prune: anything not in
state DB or not on disk got deleted from priority.json. That loses
user intent: a Bluey episode renamed from
``Bluey (2018) - S01E02 - Hospital.mkv`` to
``Bluey S01E02 Hospital.mkv`` is the SAME episode, and the user's
"prioritise this" still applies to the new path.

What this does
--------------
For each priority path:

  1. If it exists in media_report.json AND on disk: KEEP (no change).
  2. Try the series-rename heuristic (strip ``(YEAR) -``, ``- `` separators).
     If the new basename appears uniquely in media_report: RESOLVE
     to the new path.
  3. Try the extension-swap heuristic (.mp4↔.mkv). If the alternate
     extension is in media_report: RESOLVE to it.
  4. Otherwise: DROP — the file is genuinely gone (Sonarr removed,
     placeholder for unreleased title, hand-typo).

Writes priority.json atomically with the existing read-back-parse
guard (commit 14c7a29). Dry-run by default; pass ``--apply`` to write.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from paths import MEDIA_REPORT, PIPELINE_STATE_DB, STAGING_DIR


# The two rename patterns the bulk_rename_clean run produced on 2026-05-15.
# Anchored to the basename, not the full path — most renames keep the
# directory unchanged.
_SERIES_RENAME_RE_YEAR = re.compile(
    r"(.+?) \(\d{4}\) - (S\d+E\d+(?:-E\d+)?) - (.+?)(\.\w+)$", re.I
)
_SERIES_RENAME_RE_NOYEAR = re.compile(
    r"(.+?) - (S\d+E\d+(?:-E\d+)?) - (.+?)(\.\w+)$", re.I
)


def _series_rename_target(basename: str) -> str | None:
    """If ``basename`` matches the dash-separated series-rename pattern,
    return the cleaned basename. Else None."""
    m = _SERIES_RENAME_RE_YEAR.match(basename) or _SERIES_RENAME_RE_NOYEAR.match(basename)
    if not m:
        return None
    show, sxxeyy, title, ext = m.groups()
    return f"{show} {sxxeyy} {title}{ext}"


def _ext_swap(filepath: str) -> str | None:
    """If filepath has a .mp4/.mkv extension, return the other one. Else None."""
    base, ext = os.path.splitext(filepath)
    ext_l = ext.lower()
    if ext_l == ".mp4":
        return base + ".mkv"
    if ext_l == ".mkv":
        return base + ".mp4"
    return None


def resolve(prio_paths: list[str], report_paths_by_path: dict[str, dict],
            report_by_basename: dict[str, list[dict]],
            state_by_path: dict[str, str]) -> tuple[list[str], dict]:
    """Returns (new_list, stats).

    new_list preserves order of prio_paths so user-controlled priority
    is unchanged. Each entry is either the original path (kept), a
    resolved replacement (renamed/ext-swapped), or omitted (dropped).
    """
    out: list[str] = []
    stats = {
        "kept": 0,
        "resolved_rename": 0,
        "resolved_ext": 0,
        "dropped_not_found": 0,
        "dropped_done": 0,
        "dropped_flagged": 0,
    }
    terminal_flagged = {
        "flagged_corrupt", "flagged_foreign_audio",
        "flagged_undetermined", "flagged_manual",
    }

    for fp in prio_paths:
        status = state_by_path.get(fp, "")
        on_disk = os.path.exists(fp)

        # Path is alive — keep as-is.
        if on_disk and fp in report_paths_by_path:
            if status == "done":
                stats["dropped_done"] += 1
                continue
            if status in terminal_flagged:
                stats["dropped_flagged"] += 1
                continue
            stats["kept"] += 1
            out.append(fp)
            continue

        # Try series rename (basename -> new basename in same parent).
        bn = os.path.basename(fp)
        new_bn = _series_rename_target(bn)
        if new_bn:
            candidates = report_by_basename.get(new_bn.lower(), [])
            if len(candidates) == 1:
                resolved_path = candidates[0]["filepath"]
                if os.path.exists(resolved_path):
                    rstatus = state_by_path.get(resolved_path, "")
                    if rstatus == "done":
                        stats["dropped_done"] += 1
                        continue
                    if rstatus in terminal_flagged:
                        stats["dropped_flagged"] += 1
                        continue
                    stats["resolved_rename"] += 1
                    out.append(resolved_path)
                    continue

        # Try extension swap.
        alt = _ext_swap(fp)
        if alt and alt in report_paths_by_path and os.path.exists(alt):
            astatus = state_by_path.get(alt, "")
            if astatus == "done":
                stats["dropped_done"] += 1
                continue
            if astatus in terminal_flagged:
                stats["dropped_flagged"] += 1
                continue
            stats["resolved_ext"] += 1
            out.append(alt)
            continue

        # Genuine miss — drop.
        stats["dropped_not_found"] += 1

    return out, stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Actually write priority.json (default: dry-run)")
    parser.add_argument("--prio-path", type=str,
                        default=os.path.join(str(STAGING_DIR), "control", "priority.json"),
                        help="Path to priority.json")
    args = parser.parse_args()

    prio_path = args.prio_path
    if not os.path.exists(prio_path):
        print(f"priority.json not found at {prio_path}", file=sys.stderr)
        return 1

    with open(prio_path, encoding="utf-8") as f:
        prio = json.load(f)
    paths_list = prio.get("paths") or []
    forced_list = prio.get("force") or []
    print(f"priority.json: {len(paths_list)} paths, {len(forced_list)} forced")

    with open(MEDIA_REPORT, encoding="utf-8") as f:
        report = json.load(f)
    by_path = {e["filepath"]: e for e in report.get("files", [])}
    by_basename: dict[str, list[dict]] = {}
    for e in report.get("files", []):
        bn = os.path.basename(e["filepath"]).lower()
        by_basename.setdefault(bn, []).append(e)

    con = sqlite3.connect(str(PIPELINE_STATE_DB))
    state_by_path = {row[0]: row[1] for row in con.execute(
        "SELECT filepath, status FROM pipeline_files"
    )}
    con.close()

    new_paths, stats_paths = resolve(paths_list, by_path, by_basename, state_by_path)
    new_force, stats_force = resolve(forced_list, by_path, by_basename, state_by_path)

    print(f"\n=== paths ({len(paths_list)} -> {len(new_paths)}) ===")
    for k, v in stats_paths.items():
        print(f"  {k}: {v}")
    print(f"\n=== force ({len(forced_list)} -> {len(new_force)}) ===")
    for k, v in stats_force.items():
        print(f"  {k}: {v}")

    if not args.apply:
        print("\n(dry-run — pass --apply to write priority.json)")
        return 0

    out = dict(prio)
    out["paths"] = new_paths
    out["force"] = new_force
    payload = json.dumps(out, indent=2, ensure_ascii=False)
    # Read-back-parse guard before commit.
    json.loads(payload)
    tmp = prio_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(payload)
    os.replace(tmp, prio_path)
    print("\npriority.json written atomically.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
