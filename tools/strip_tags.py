"""Anchor-based filename cleaner for series and movie files on NAS.

Series: keeps title + SxxExx + episode title, strips codec/resolution/group tags.
Movies: keeps title + (year), strips everything after.

Default is dry-run. Pass --execute to actually rename files.
"""

import argparse
import json
import re
from pathlib import Path

from paths import NAS_SERIES, NAS_MOVIES, STAGING_DIR

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".flv", ".mov", ".ts", ".webm"}

# Regex for SxxExx (case-insensitive). Captures season+episode marker.
EPISODE_RE = re.compile(r"(S\d{1,2}\s?E\d{1,2}(?:\s?E\d{1,2})?)", re.IGNORECASE)

# Base tag parts — tokens that signal the end of an episode title.
_BASE_TAG_PARTS = (
    r"(?:19[2-9]\d|20[0-2]\d)(?=[\s.)\-]|$)"  # bare year (Fargo.S02E04.2015.)
    r"|1080p|720p|480p|2160p|4K|UHD"
    r"|WEB[-.]?DL|WEBRip|BluRay|BDRip|HDTV|DVDRip|REMUX"
    r"|NF|AMZN|DSNP|HULU|MAX|HBO|ATVP|PCOK|PMTP"
    r"|x264|x265|H\.?264|H\.?265|HEVC|AVC|AV1|AAC|DDP?5?\.?1|Atmos|DTS"
    r"|REPACK\d*|INTERNAL|PROPER|MULTi"
    r"|POLISH|GERMAN|iTALiAN|FRENCH|SPANISH|NORDiC|DUTCH|SWEDISH"
    r"|FINNISH|DANISH|NORWEGIAN|CZECH|HUNGARIAN|TURKISH|ARABIC"
)

CUSTOM_TAGS_FILE = STAGING_DIR / "control" / "custom_tags.json"


def _load_custom_keywords() -> list[str]:
    """Read custom keywords from control/custom_tags.json if it exists."""
    if not CUSTOM_TAGS_FILE.exists():
        return []
    try:
        data = json.loads(CUSTOM_TAGS_FILE.read_text(encoding="utf-8"))
        return [k for k in data.get("keywords", []) if isinstance(k, str) and k.strip()]
    except Exception:
        return []


def _build_tag_regex(extra_keywords: list[str] | None = None) -> re.Pattern:
    """Compile tag boundary regex, optionally including extra keywords."""
    parts = _BASE_TAG_PARTS
    if extra_keywords:
        escaped = "|".join(re.escape(k) for k in extra_keywords)
        parts = f"{parts}|{escaped}"
    return re.compile(
        rf"(?:\b|(?<=\[))({parts})(?:\b|(?=[\]\-]))",
        re.IGNORECASE,
    )


# Default regex (no custom keywords) for backward compat / direct imports
TAG_BOUNDARY_RE = _build_tag_regex()

# Year pattern for movies: (YYYY) or .YYYY. or space-YYYY-space, range 1920-2029.
MOVIE_YEAR_RE = re.compile(
    r"[\s.(]*((?:19[2-9]\d|20[0-2]\d))[\s.)]*"
)


def _dots_to_spaces(s: str) -> str:
    """Replace dots/underscores with spaces, collapse whitespace."""
    s = re.sub(r"[._]+", " ", s)
    return " ".join(s.split())


def clean_series_name(stem: str, tag_re: re.Pattern = TAG_BOUNDARY_RE) -> str | None:
    """Find SxxExx anchor, keep title + episode title, strip tags.

    Returns cleaned name or None if no SxxExx anchor found.
    """
    m = EPISODE_RE.search(stem)
    if not m:
        return None

    # Title portion: everything before SxxExx, strip trailing year if present
    title = stem[: m.start()]
    title = re.sub(r"[\s.](19[2-9]\d|20[0-2]\d)[\s.]*$", "", title)
    episode_marker = re.sub(r"\s+", "", m.group(1)).upper()

    # After SxxExx: might contain episode title then tags
    after = stem[m.end() :]

    # Find where tags begin
    tag_match = tag_re.search(after)
    if tag_match:
        episode_title = after[: tag_match.start()]
    else:
        # No recognizable tags -- keep everything (rare)
        episode_title = after

    # Clean up each part
    title = _dots_to_spaces(title).strip()
    episode_title = _dots_to_spaces(episode_title).strip()
    # Strip leading/trailing hyphens from episode title
    episode_title = episode_title.strip("- ")

    if episode_title:
        return f"{title} {episode_marker} {episode_title}"
    return f"{title} {episode_marker}"


def clean_movie_name(stem: str, tag_re: re.Pattern = TAG_BOUNDARY_RE) -> str | None:
    """Find year anchor, keep title + (year), strip everything after.

    Returns cleaned name or None if no year anchor found.
    """
    # Find all year candidates; pick the last one that looks like a movie year
    # (sometimes a year appears in the title itself, e.g. "2001 A Space Odyssey")
    matches = list(MOVIE_YEAR_RE.finditer(stem))
    if not matches:
        return None

    # Use the first year that's followed by tags or end-of-string.
    # For most filenames, the first year IS the release year.
    for m in matches:
        year = m.group(1)
        title = stem[: m.start()]
        title = _dots_to_spaces(title).strip()
        if not title:
            continue
        return f"{title} ({year})"

    return None


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

    plan = []
    # Track target paths to detect batch collisions
    target_map: dict[Path, Path] = {}  # new_path -> old_path (first claimer)

    for dirpath, _, filenames in _walk_sorted(root):
        dp = Path(dirpath)
        for fn in sorted(filenames):
            old_path = dp / fn
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

            # Collision: target file already exists on disk
            if new_path.exists() and new_path != old_path:
                plan.append({
                    "old_path": old_path,
                    "new_path": new_path,
                    "status": "collision_exists",
                })
                continue

            # Collision: another file in this batch maps to the same target
            if new_path in target_map and target_map[new_path] != old_path:
                plan.append({
                    "old_path": old_path,
                    "new_path": new_path,
                    "status": "collision_batch",
                })
                continue

            target_map[new_path] = old_path
            plan.append({
                "old_path": old_path,
                "new_path": new_path,
                "status": "rename",
            })

    return plan


def _walk_sorted(root: Path):
    """os.walk equivalent that yields sorted directory entries."""
    import os
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        yield dirpath, dirnames, filenames


def execute_renames(plan: list[dict], dry_run: bool) -> None:
    """Rename files or print preview."""
    renames = [e for e in plan if e["status"] == "rename"]
    collisions = [e for e in plan if e["status"].startswith("collision")]

    if not renames and not collisions:
        print("No files to rename.")
        return

    # Print renames
    for entry in renames:
        old_rel = entry["old_path"].name
        new_rel = entry["new_path"].name
        if dry_run:
            print(f"  {old_rel}")
            print(f"    -> {new_rel}")
        else:
            try:
                entry["old_path"].rename(entry["new_path"])
                print(f"  RENAMED: {old_rel} -> {new_rel}")
            except OSError as e:
                print(f"  ERROR: {old_rel}: {e}")

    # Print collisions
    if collisions:
        print(f"\n  Skipped ({len(collisions)} collisions):")
        for entry in collisions:
            kind = "target exists" if entry["status"] == "collision_exists" else "batch duplicate"
            print(f"    {entry['old_path'].name} ({kind})")

    # Summary
    action = "Would rename" if dry_run else "Renamed"
    print(f"\n{action} {len(renames)} files. {len(collisions)} skipped.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Anchor-based filename cleaner for series and movie files"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually rename files (default is dry-run preview)",
    )
    parser.add_argument(
        "--movies",
        action="store_true",
        help="Also process movie files (year-based anchor)",
    )
    parser.add_argument(
        "--root",
        type=str,
        default=None,
        help="Custom root directory (overrides default NAS paths)",
    )
    args = parser.parse_args()

    dry_run = not args.execute

    if dry_run:
        print("DRY RUN (pass --execute to rename)\n")

    if args.root:
        # Single custom root -- determine mode from flags
        root = Path(args.root)
        mode = "both" if args.movies else "series"
        print(f"Scanning: {root}")
        plan = plan_renames(root, mode)
        execute_renames(plan, dry_run)
    else:
        # Default: always scan series
        print(f"Scanning series: {NAS_SERIES}")
        series_plan = plan_renames(NAS_SERIES, "series")
        execute_renames(series_plan, dry_run)

        if args.movies:
            print(f"\nScanning movies: {NAS_MOVIES}")
            movie_plan = plan_renames(NAS_MOVIES, "movies")
            execute_renames(movie_plan, dry_run)


if __name__ == "__main__":
    main()
