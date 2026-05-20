"""Bulk-apply ``pipeline.filename.clean_filename`` to every file in the
media report whose current name differs from the cleaned form.

Cleans typical scene/torrent name bloat:
  ``The.West.Wing.S04E21.Life.on.Mars.1080p.BluRay.DTS-HD.MA-BTN.mkv``
becomes
  ``The West Wing S04E21 Life on Mars.mkv``

The cleaner is the same module the scanner already invokes for the
"Clean Filename" hero stat — running it across the whole library is
just batch-applying its suggestions instead of leaving them flagged
for manual review.

Renames are simple ``os.rename`` calls (no content change). The
``pipeline_state.db`` row's filepath column is updated alongside so
the encoder doesn't lose track of in-flight files. ``media_report.json``
is patched in place so the dashboard reflects the new names without a
full rescan.

Defaults to dry-run. ``--apply`` actually performs the rename.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from paths import MEDIA_REPORT, PIPELINE_STATE_DB
from pipeline.filename import clean_filename


def _update_state_filepath(db: str, old_path: str, new_path: str) -> None:
    """Move the pipeline_files row from old_path to new_path. Quiet on miss."""
    try:
        con = sqlite3.connect(db)
        cur = con.cursor()
        cur.execute(
            "UPDATE pipeline_files SET filepath = ? WHERE filepath = ?",
            (new_path, old_path),
        )
        con.commit()
        con.close()
    except sqlite3.Error:
        # Best-effort. The encoder will re-derive the row on next scan.
        pass


def _update_priority_paths(rename_map: dict[str, str]) -> int:
    """Rewrite any old paths in ``control/priority.json`` to their new
    paths. Returns the number of entries updated.

    Pre-2026-05-21 this step didn't exist — bulk_rename_clean updated
    state DB + media_report, but priority.json kept the old paths. The
    operator's "encode these first" list became a 45% orphan-rate
    graveyard pointing at files that no longer existed at those names.
    The pipeline's priority bump silently matched nothing and the
    operator's intent was lost.

    Read-back-parse guard on the write matches the pattern from
    tools.report_lock (commit 14c7a29).
    """
    from paths import STAGING_DIR

    prio_path = Path(STAGING_DIR) / "control" / "priority.json"
    if not prio_path.exists():
        return 0
    try:
        prio = json.loads(prio_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0  # caller's rename succeeded — priority drift is non-fatal

    updated = 0

    def _remap(seq: list) -> list:
        nonlocal updated
        out = []
        for fp in seq or []:
            new = rename_map.get(fp)
            if new is not None:
                out.append(new)
                updated += 1
            else:
                out.append(fp)
        return out

    prio["paths"] = _remap(prio.get("paths") or [])
    prio["force"] = _remap(prio.get("force") or [])
    if updated == 0:
        return 0

    payload = json.dumps(prio, indent=2, ensure_ascii=False)
    try:
        json.loads(payload)  # read-back-parse guard
    except json.JSONDecodeError:
        return 0
    tmp = str(prio_path) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp, str(prio_path))
    except OSError:
        return 0
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true", help="Actually rename (default: dry-run)")
    parser.add_argument("--limit", type=int, default=0, help="Stop after N renames (0 = all)")
    parser.add_argument("--path-contains", default=None, help="Only rename files whose path contains substring")
    args = parser.parse_args()

    rep_path = Path(MEDIA_REPORT)
    rep = json.loads(rep_path.read_text(encoding="utf-8"))
    files = rep.get("files") or []

    candidates: list[tuple[str, str, dict]] = []  # (old_path, new_path, entry)
    for entry in files:
        old_path = entry.get("filepath", "")
        old_name = entry.get("filename", "")
        if not old_path or not old_name:
            continue
        if args.path_contains and args.path_contains.lower() not in old_path.lower():
            continue
        try:
            new_name = clean_filename(old_path, entry.get("library_type", ""))
        except Exception:
            continue
        if not new_name or new_name == old_name:
            continue
        new_path = os.path.join(os.path.dirname(old_path), new_name)
        # Skip if the cleaned name would clobber an existing file
        if os.path.exists(new_path) and os.path.normcase(new_path) != os.path.normcase(old_path):
            print(f"  SKIP (would clobber): {new_name}")
            continue
        candidates.append((old_path, new_path, entry))
        if args.limit and len(candidates) >= args.limit:
            break

    print(f"Candidates: {len(candidates)} (apply={args.apply})")

    if not args.apply:
        print("\n--- First 10 dry-run examples ---")
        for old, new, _ in candidates[:10]:
            print(f"  OLD: {os.path.basename(old)}")
            print(f"  NEW: {os.path.basename(new)}")
        return 0

    renamed = 0
    failed = 0
    rename_map: dict[str, str] = {}
    for old, new, entry in candidates:
        try:
            os.rename(old, new)
        except OSError as e:
            failed += 1
            print(f"  FAIL: {os.path.basename(old)}: {e}")
            continue
        # Update media-report entry in place
        entry["filepath"] = new
        entry["filename"] = os.path.basename(new)
        # Update state DB row if it exists
        _update_state_filepath(str(PIPELINE_STATE_DB), old, new)
        rename_map[old] = new
        renamed += 1
        if renamed % 25 == 0:
            print(f"  Progress: {renamed}/{len(candidates)}")

    # Atomic write of the updated report
    if renamed > 0:
        tmp = rep_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(rep, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, rep_path)

    # Update priority.json to match the rename. Pre-2026-05-21 this was
    # missed and the operator's "encode these first" list drifted into
    # 45% orphan paths after the bulk-rename run.
    prio_updated = _update_priority_paths(rename_map) if rename_map else 0

    print(f"\n=== Summary ===")
    print(f"  Renamed: {renamed}")
    print(f"  Failed:  {failed}")
    print(f"  priority.json entries remapped: {prio_updated}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
