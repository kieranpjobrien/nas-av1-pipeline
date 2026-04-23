"""Anchor-based filename cleaner CLI for series and movie files on NAS.

Thin CLI wrapper around :mod:`pipeline.filename` — the filename-cleaning
logic lives there (single source of truth for both the pipeline and this
CLI). This module provides: NAS walk, rename planning, sidecar rename,
collision detection, stripped-tag reporting, and Plex scan trigger.

Series: keeps title + SxxExx + episode title, strips codec/resolution/group tags.
Movies: keeps title + (year), strips everything after.

Default is dry-run. Pass --execute to actually rename files.
"""

import argparse
import json
import os
import re
from pathlib import Path

from paths import NAS_MOVIES, NAS_SERIES, PLEX_TOKEN, PLEX_URL, STAGING_DIR
from pipeline.filename import (
    _build_tag_regex,
    _load_custom_keywords,
    clean_movie_name,
    clean_series_name,
)

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".flv", ".mov", ".ts", ".webm"}
SIDECAR_EXTS = {".srt", ".ass", ".ssa", ".sub", ".idx", ".vtt", ".nfo", ".smi", ".sup"}


def _find_sidecars(video_path: Path) -> list[Path]:
    """Find .srt/.nfo/etc sharing the video's stem (incl. .lang[.hi] suffixes)."""
    stem = video_path.stem
    results: list[Path] = []
    for f in video_path.parent.iterdir():
        if not f.is_file():
            continue
        if f.suffix.lower() not in SIDECAR_EXTS:
            continue
        if f.stem == stem or f.stem.startswith(stem + "."):
            results.append(f)
    return results


def plan_renames(root: Path, mode: str) -> list[dict]:
    """Walk directory, compute old->new names, detect collisions.

    mode: "series", "movies", or "both"
    Returns list of dicts with keys: old_path, new_path, status
    """
    # Build regex once with any custom keywords
    tag_re = _build_tag_regex(_load_custom_keywords())

    cleaner_funcs = []
    if mode in ("series", "both"):
        cleaner_funcs.append(("series", lambda stem: clean_series_name(stem, tag_re)))
    if mode in ("movies", "both"):
        cleaner_funcs.append(("movies", lambda stem: clean_movie_name(stem, tag_re)))

    plan: list[dict] = []
    target_map: dict[Path, Path] = {}  # new_path -> old_path (first claimer)

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for fn in sorted(filenames):
            old_path = Path(dirpath) / fn
            ext = old_path.suffix.lower()
            if ext not in VIDEO_EXTS:
                continue

            stem = old_path.stem
            new_stem = None
            for _, func in cleaner_funcs:
                result = func(stem)
                if result is not None:
                    new_stem = result
                    break

            if new_stem is None or new_stem == stem:
                continue

            new_path = old_path.with_name(f"{new_stem}{ext}")
            if new_path.exists() and new_path != old_path:
                plan.append({"old_path": old_path, "new_path": new_path, "status": "collision_exists"})
                continue
            if new_path in target_map and target_map[new_path] != old_path:
                plan.append({"old_path": old_path, "new_path": new_path, "status": "collision_batch"})
                continue
            target_map[new_path] = old_path
            plan.append({"old_path": old_path, "new_path": new_path, "status": "rename"})

    return plan


def _extract_stripped_tags(old_stem: str, new_stem: str) -> str:
    """Extract the tag portion that was stripped from a filename."""
    norm_old = re.sub(r"[._]+", " ", old_stem)
    idx = norm_old.lower().find(new_stem.lower())
    stripped = norm_old[idx + len(new_stem):] if idx >= 0 else norm_old[len(new_stem):]
    return stripped.strip(" .-")


def _save_stripped_tags(plan: list[dict]) -> None:
    """Save all unique stripped tag fragments to a JSON file for future reference."""
    tags_file = STAGING_DIR / "control" / "stripped_tags.json"
    tag_fragments: dict[str, int] = {}

    if tags_file.exists():
        try:
            existing = json.loads(tags_file.read_text(encoding="utf-8"))
            tag_fragments = existing.get("tags", {})
        except Exception:
            pass

    for entry in plan:
        if entry["status"] != "rename":
            continue
        stripped = _extract_stripped_tags(entry["old_path"].stem, entry["new_path"].stem)
        if stripped:
            tag_fragments[stripped] = tag_fragments.get(stripped, 0) + 1

    if tag_fragments:
        sorted_tags = dict(sorted(tag_fragments.items(), key=lambda x: -x[1]))
        tags_file.write_text(
            json.dumps({"tags": sorted_tags, "total_unique": len(sorted_tags)}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"\n  Saved {len(sorted_tags)} unique tag patterns to {tags_file}")


def execute_renames(plan: list[dict], dry_run: bool) -> None:
    """Rename files or print preview."""
    renames = [e for e in plan if e["status"] == "rename"]
    collisions = [e for e in plan if e["status"].startswith("collision")]

    if not renames and not collisions:
        print("No files to rename.")
        return

    _save_stripped_tags(plan)

    sidecars_done = 0
    for entry in renames:
        old_path: Path = entry["old_path"]
        new_path: Path = entry["new_path"]
        sidecars = _find_sidecars(old_path) if not dry_run else []
        if dry_run:
            print(f"  {old_path.name}")
            print(f"    -> {new_path.name}")
        else:
            try:
                old_path.rename(new_path)
                print(f"  RENAMED: {old_path.name} -> {new_path.name}")
                for sc in sidecars:
                    new_sc = sc.with_name(new_path.stem + sc.name[len(old_path.stem):])
                    try:
                        if not new_sc.exists():
                            sc.rename(new_sc)
                            sidecars_done += 1
                    except OSError as se:
                        print(f"    sidecar ERR {sc.name}: {se}")
            except OSError as e:
                print(f"  ERROR: {old_path.name}: {e}")
    if not dry_run and sidecars_done:
        print(f"  + {sidecars_done} sidecars renamed alongside")

    if collisions:
        print(f"\n  Skipped ({len(collisions)} collisions):")
        for entry in collisions:
            kind = "target exists" if entry["status"] == "collision_exists" else "batch duplicate"
            print(f"    {entry['old_path'].name} ({kind})")

    action = "Would rename" if dry_run else "Renamed"
    print(f"\n{action} {len(renames)} files. {len(collisions)} skipped.")

    if not dry_run and renames:
        _trigger_plex_scan()


def _trigger_plex_scan() -> None:
    """Trigger a Plex library scan so renamed files are picked up."""
    if not PLEX_URL or not PLEX_TOKEN:
        print("\n  Plex scan skipped (no PLEX_URL/PLEX_TOKEN configured)")
        return

    from urllib.error import URLError
    from urllib.request import Request, urlopen

    try:
        req = Request(f"{PLEX_URL}/library/sections", headers={"X-Plex-Token": PLEX_TOKEN})
        with urlopen(req, timeout=10) as resp:
            body = resp.read().decode()
        sections = re.findall(r'key="(\d+)"', body)
        for section_id in sections:
            req = Request(
                f"{PLEX_URL}/library/sections/{section_id}/refresh",
                headers={"X-Plex-Token": PLEX_TOKEN},
            )
            with urlopen(req, timeout=10):
                pass
        print(f"\n  Triggered Plex library scan ({len(sections)} sections)")
    except (URLError, OSError) as e:
        print(f"\n  Plex scan failed: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Anchor-based filename cleaner for series and movie files")
    parser.add_argument("--execute", action="store_true", help="Actually rename files (default is dry-run)")
    parser.add_argument("--movies", action="store_true", help="Also process movie files (year-based anchor)")
    parser.add_argument("--root", type=str, default=None, help="Custom root directory (overrides NAS paths)")
    args = parser.parse_args()

    dry_run = not args.execute
    if dry_run:
        print("DRY RUN (pass --execute to rename)\n")

    if args.root:
        root = Path(args.root)
        mode = "both" if args.movies else "series"
        print(f"Scanning: {root}")
        execute_renames(plan_renames(root, mode), dry_run)
    else:
        print(f"Scanning series: {NAS_SERIES}")
        execute_renames(plan_renames(NAS_SERIES, "series"), dry_run)
        if args.movies:
            print(f"\nScanning movies: {NAS_MOVIES}")
            execute_renames(plan_renames(NAS_MOVIES, "movies"), dry_run)


if __name__ == "__main__":
    main()
