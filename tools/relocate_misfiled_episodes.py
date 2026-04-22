"""Move episode files from show root into their proper Season N folder.

E.g. `/Series/Chuck/Chuck S02E12 ...mkv` → `/Series/Chuck/Season 2/Chuck S02E12 ...mkv`
Creates Season folders as needed. Safe on collisions.
"""
import argparse
import os
import re
import sys
from pathlib import Path

# Force UTF-8 stdout for Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

from paths import NAS_SERIES

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".flv", ".mov", ".ts", ".webm"}
SXXEXX = re.compile(r"S(\d{1,4})E(\d{1,2})", re.IGNORECASE)
SEASON_FOLDER = re.compile(r"Season[\s_]*(\d{1,4})", re.IGNORECASE)


def find_misfiled(root: Path) -> list[dict]:
    """Find video files in show root (not inside a Season N folder)."""
    plan = []
    # Each subdirectory of `root` is a show
    for show_dir in root.iterdir():
        if not show_dir.is_dir():
            continue
        for item in show_dir.iterdir():
            if not item.is_file():
                continue
            if item.suffix.lower() not in VIDEO_EXTS:
                continue
            m = SXXEXX.search(item.stem)
            if not m:
                continue
            season_num = int(m.group(1))
            season_folder = show_dir / f"Season {season_num}"
            new_path = season_folder / item.name
            plan.append({
                "old_path": item,
                "new_path": new_path,
                "show": show_dir.name,
                "season": season_num,
            })
    return plan


def execute(plan: list[dict], dry_run: bool) -> None:
    if not plan:
        print("No misfiled episodes found.")
        return

    # Group by show for printing
    by_show: dict[str, list[dict]] = {}
    for e in plan:
        by_show.setdefault(e["show"], []).append(e)

    print(f"Found {len(plan)} misfiled episodes across {len(by_show)} shows:\n")
    for show, items in by_show.items():
        seasons = sorted({e["season"] for e in items})
        print(f"  {show}: {len(items)} files into Season {seasons}")

    done = 0
    errors = []
    for e in plan:
        old_path = e["old_path"]
        new_path = e["new_path"]
        if new_path.exists():
            errors.append(f"collision: {new_path.name} already exists in Season folder")
            continue
        if dry_run:
            print(f"  MOVE: {old_path} -> {new_path}")
            continue
        try:
            new_path.parent.mkdir(exist_ok=True)
            old_path.rename(new_path)
            done += 1
        except OSError as err:
            errors.append(f"{old_path.name}: {err}")

    action = "Would move" if dry_run else "Moved"
    print(f"\n{action} {done if not dry_run else len(plan)} files. {len(errors)} errors.")
    for err in errors[:10]:
        print(f"  ! {err}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    dry_run = not args.execute
    if dry_run:
        print("DRY RUN (pass --execute to move)\n")

    plan = find_misfiled(Path(str(NAS_SERIES)))
    execute(plan, dry_run)


if __name__ == "__main__":
    main()
