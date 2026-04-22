"""Pair orphaned .srt/.nfo/etc sidecars to their renamed video in the same folder.

Matches by (season, episode) for series files, and by year+closest-stem for movies.
Renames the sidecar so Plex picks it up.
"""
import argparse
import os
import re
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

from paths import NAS_MOVIES, NAS_SERIES

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".flv", ".mov", ".ts", ".webm"}
SIDECAR_EXTS = {".srt", ".ass", ".ssa", ".sub", ".idx", ".vtt", ".nfo", ".smi", ".sup"}
SXXEXX = re.compile(r"S(\d{1,4})E(\d{1,2})", re.IGNORECASE)
YEAR = re.compile(r"\((19[2-9]\d|20[0-2]\d)\)")


def _sidecar_suffix(sidecar_stem: str, video_stem: str) -> str | None:
    """If sidecar starts with video_stem + ".<lang>[.<flag>]", return the trailing
    portion (e.g. ".en", ".en.hi"). Else None.
    """
    if sidecar_stem == video_stem:
        return ""
    if sidecar_stem.startswith(video_stem + "."):
        return sidecar_stem[len(video_stem):]
    return None


def _extract_lang_flags(sidecar_stem: str) -> str:
    """Extract the language + flags suffix from a sidecar stem.

    E.g. `Brooklyn Nine Nine S03E14.en.hi` → `.en.hi`
         `Brooklyn Nine Nine S03E14` → ``
    Detects the last run of short (≤3 char) all-alpha tokens separated by dots.
    """
    parts = sidecar_stem.rsplit(".", 3)
    tail = []
    for part in reversed(parts[1:]):
        if len(part) <= 4 and part.isalpha():
            tail.insert(0, part)
        else:
            break
    if tail:
        return "." + ".".join(tail)
    return ""


def pair_in_folder(folder: Path) -> list[dict]:
    """Return list of {sidecar, new_name, reason} for one folder."""
    videos: list[Path] = []
    sidecars: list[Path] = []
    for item in folder.iterdir():
        if not item.is_file():
            continue
        ext = item.suffix.lower()
        if ext in VIDEO_EXTS:
            videos.append(item)
        elif ext in SIDECAR_EXTS:
            sidecars.append(item)

    if not sidecars or not videos:
        return []

    # Build map: episode-key -> video
    ep_to_video: dict[tuple[str, str], Path] = {}
    for v in videos:
        m = SXXEXX.search(v.stem)
        if m:
            key = (m.group(1).upper(), m.group(2).upper())
            ep_to_video[key] = v

    plans = []
    for sc in sidecars:
        # If the sidecar is already paired (its stem + optional lang suffix
        # matches a video stem exactly), skip it.
        already_ok = False
        for v in videos:
            if _sidecar_suffix(sc.stem, v.stem) is not None:
                already_ok = True
                break
        if already_ok:
            continue

        # Episode-based match (series)
        m = SXXEXX.search(sc.stem)
        if m:
            key = (m.group(1).upper(), m.group(2).upper())
            v = ep_to_video.get(key)
            if v:
                lang_flags = _extract_lang_flags(sc.stem)
                new_stem = v.stem + lang_flags
                new_name = new_stem + sc.suffix
                if new_name != sc.name:
                    plans.append({
                        "sidecar": sc,
                        "new_name": new_name,
                        "reason": f"S{key[0]}E{key[1]} episode match",
                    })
            continue

        # Movie-style (year-based) match — if there's exactly ONE video in folder
        if len(videos) == 1:
            v = videos[0]
            lang_flags = _extract_lang_flags(sc.stem)
            new_stem = v.stem + lang_flags
            new_name = new_stem + sc.suffix
            if new_name != sc.name:
                plans.append({
                    "sidecar": sc,
                    "new_name": new_name,
                    "reason": "single-video folder",
                })

    return plans


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--series", action="store_true")
    ap.add_argument("--movies", action="store_true")
    args = ap.parse_args()
    if not (args.series or args.movies):
        args.series = True
        args.movies = True

    dry_run = not args.execute
    if dry_run:
        print("DRY RUN (pass --execute to rename)\n")

    roots = []
    if args.series:
        roots.append(("series", Path(str(NAS_SERIES))))
    if args.movies:
        roots.append(("movies", Path(str(NAS_MOVIES))))

    grand_plans: list[dict] = []
    for label, root in roots:
        print(f"Scanning {label}: {root}")
        for dirpath, _, _ in os.walk(str(root)):
            dp = Path(dirpath)
            plans = pair_in_folder(dp)
            for p in plans:
                p["folder"] = dp
                grand_plans.append(p)

    print(f"\nFound {len(grand_plans)} orphan sidecars to repair.\n")

    done = 0
    errors = []
    for p in grand_plans:
        sc: Path = p["sidecar"]
        new_sc = sc.with_name(p["new_name"])
        if new_sc.exists():
            if not dry_run:
                errors.append(f"collision: {p['new_name']}")
            continue
        if dry_run:
            print(f"  {sc.name}")
            print(f"    -> {p['new_name']}  ({p['reason']})")
        else:
            try:
                sc.rename(new_sc)
                done += 1
                if done <= 30 or done % 25 == 0:
                    print(f"  {sc.name} -> {p['new_name']}")
            except OSError as e:
                errors.append(f"{sc}: {e}")

    if dry_run:
        print(f"\nWould rename {len(grand_plans)} sidecars.")
    else:
        print(f"\nRenamed {done} sidecars, {len(errors)} errors.")
        for e in errors[:10]:
            print(f"  ! {e}")


if __name__ == "__main__":
    main()
