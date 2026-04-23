"""Unified CLI for small NAS-cleanup maintenance tools.

Subcommands consolidate what used to live in 6 separate scripts:

  clean-names      Anchor-based scene-tag stripper (movies + series).
  normalise        Folder-as-truth pass: diacritics, years, apostrophes,
                   welded words, bare-SxxExx prepending.
  dedupe           Find same-episode duplicates and auto-delete obvious
                   foreign-tag / junk siblings when a clean sibling exists.
  relocate         Move episodes out of show root into Season N folders.
  repair-sidecars  Pair orphaned .srt/.nfo sidecars to their renamed video.
  audit            Library-wide standards compliance report. Pass
                   ``--queue reencode`` to append non-compliant paths to
                   ``control/reencode.json`` so the pipeline picks them up.

Every destructive subcommand defaults to DRY RUN. Pass ``--execute`` to
actually mutate files (``audit --queue reencode`` writes the control file
unconditionally — historical behaviour preserved).

Shared helpers:
  * sidecar scanning supports the legacy CLI sidecar set (adds .nfo/.smi/.sup
    on top of ``pipeline.subs.SCAN_EXTS``)
  * filename regex / anchors come from :mod:`pipeline.filename`
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

# Force UTF-8 stdout/stderr on Windows (Ō / ü / ō in filenames cp1252-crash otherwise).
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

from paths import MEDIA_REPORT, NAS_MOVIES, NAS_SERIES, PLEX_TOKEN, PLEX_URL, STAGING_DIR
from pipeline.config import DEFAULT_CONFIG
from pipeline.filename import (
    SCENE_TAG_RE,
    _build_tag_regex,
    _load_custom_keywords,
    clean_movie_name,
    clean_series_name,
)
from pipeline.streams import is_hi_external, is_hi_internal
from pipeline.subs import SCAN_EXTS

VIDEO_EXTS: frozenset[str] = frozenset(
    {".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".flv", ".mov", ".ts", ".webm"}
)
# Legacy CLI tools care about .nfo/.smi/.sup on top of the subtitle-sidecar
# set covered by pipeline.subs.SCAN_EXTS.
SIDECAR_EXTS: frozenset[str] = frozenset(SCAN_EXTS) | frozenset({".nfo", ".smi", ".sup"})

SXXEXX_RE = re.compile(r"S(\d{1,4})E(\d{1,2})", re.IGNORECASE)
SXXEXX_EXTENDED = re.compile(r"(S(\d{1,4})[\s.]?E(\d{1,2})(?:[\s.]?E\d{1,2})?)", re.IGNORECASE)
XEPS_RE = re.compile(r"(?<![A-Za-z0-9])(\d{1,2})x(\d{1,3})(?![A-Za-z0-9])")
YEAR_RE = re.compile(r"\((19[2-9]\d|20[0-2]\d)\)")

CONTROL_DIR = STAGING_DIR / "control"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _find_sidecars(video_path: Path) -> list[Path]:
    """Return sidecar files whose stem == video stem or starts with ``stem + "."``.

    Supports the legacy CLI sidecar set (adds .nfo/.smi/.sup on top of
    pipeline.subs.SCAN_EXTS). These non-subtitle sidecars are renamed
    alongside the video even though pipeline.subs doesn't look at them.
    """
    stem = video_path.stem
    results: list[Path] = []
    try:
        for f in video_path.parent.iterdir():
            if not f.is_file():
                continue
            if f.suffix.lower() not in SIDECAR_EXTS:
                continue
            if f.stem == stem or f.stem.startswith(stem + "."):
                results.append(f)
    except OSError:
        pass
    return results


def _trigger_plex_scan() -> None:
    """Trigger a Plex library scan. No-op if PLEX_URL/PLEX_TOKEN unset."""
    if not PLEX_URL or not PLEX_TOKEN:
        print("  Plex scan skipped (no PLEX_URL/PLEX_TOKEN)")
        return
    from urllib.error import URLError
    from urllib.request import Request, urlopen

    try:
        req = Request(f"{PLEX_URL}/library/sections", headers={"X-Plex-Token": PLEX_TOKEN})
        with urlopen(req, timeout=10) as resp:
            body = resp.read().decode()
        sections = re.findall(r'key="(\d+)"', body)
        for sid in sections:
            r2 = Request(f"{PLEX_URL}/library/sections/{sid}/refresh", headers={"X-Plex-Token": PLEX_TOKEN})
            with urlopen(r2, timeout=10):
                pass
        print(f"  Triggered Plex scan ({len(sections)} sections)")
    except (URLError, OSError) as e:
        print(f"  Plex scan failed: {e}")


def _walk_roots(include_movies: bool, include_series: bool) -> list[tuple[str, Path]]:
    roots: list[tuple[str, Path]] = []
    if include_series:
        roots.append(("series", Path(str(NAS_SERIES))))
    if include_movies:
        roots.append(("movies", Path(str(NAS_MOVIES))))
    return roots


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _ascii_key(s: str) -> str:
    s = _strip_accents(s)
    s = re.sub(r"[^\w]+", " ", s, flags=re.UNICODE).lower()
    return re.sub(r"\s+", " ", s).strip()


# ---------------------------------------------------------------------------
# clean-names: anchor-based scene-tag strip (from strip_tags.py)
# ---------------------------------------------------------------------------


def _plan_tag_strip(root: Path, mode: str) -> list[dict]:
    """Anchor-based tag-strip plan using :mod:`pipeline.filename` cleaners.

    ``mode`` in ``{"series", "movies", "both"}``. Returns a list of
    ``{old_path, new_path, fixes, status}`` dicts where ``status`` is
    ``"rename"`` or a ``"collision_*"`` marker.
    """
    tag_re = _build_tag_regex(_load_custom_keywords())
    cleaners: list = []
    if mode in ("series", "both"):
        cleaners.append(lambda stem: clean_series_name(stem, tag_re))
    if mode in ("movies", "both"):
        cleaners.append(lambda stem: clean_movie_name(stem, tag_re))
    plan: list[dict] = []
    targets: dict[Path, Path] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for fn in sorted(filenames):
            old_path = Path(dirpath) / fn
            if old_path.suffix.lower() not in VIDEO_EXTS:
                continue
            new_stem = None
            for fn_clean in cleaners:
                result = fn_clean(old_path.stem)
                if result is not None:
                    new_stem = result
                    break
            if new_stem is None or new_stem == old_path.stem:
                continue
            new_path = old_path.with_name(f"{new_stem}{old_path.suffix}")
            status = "rename"
            if new_path.exists() and new_path != old_path:
                status = "collision_exists"
            elif new_path in targets and targets[new_path] != old_path:
                status = "collision_batch"
            else:
                targets[new_path] = old_path
            plan.append({"old_path": old_path, "new_path": new_path, "fixes": ["tag_strip"], "status": status})
    return plan


def _extract_stripped_tags(old_stem: str, new_stem: str) -> str:
    """Extract the tag portion that was stripped from a filename."""
    norm_old = re.sub(r"[._]+", " ", old_stem)
    idx = norm_old.lower().find(new_stem.lower())
    stripped = norm_old[idx + len(new_stem):] if idx >= 0 else norm_old[len(new_stem):]
    return stripped.strip(" .-")


def _save_stripped_tags(plan: list[dict]) -> None:
    """Persist unique stripped tag fragments to ``control/stripped_tags.json``."""
    tags_file = CONTROL_DIR / "stripped_tags.json"
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
        try:
            tags_file.parent.mkdir(parents=True, exist_ok=True)
            tags_file.write_text(
                json.dumps({"tags": sorted_tags, "total_unique": len(sorted_tags)}, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            print(f"\n  Saved {len(sorted_tags)} unique tag patterns to {tags_file}")
        except OSError as e:
            print(f"\n  Could not save stripped tags to {tags_file}: {e}")


# ---------------------------------------------------------------------------
# normalise: parent-folder truth pass (from normalise_filenames.py)
# ---------------------------------------------------------------------------


_SMART_PUNCT = {
    "\u2018": "'", "\u2019": "'",
    "\u201c": '"', "\u201d": '"',
    "\u2013": "-", "\u2014": "-",
    "\uFF1A": " - ", "\uFFFD": "",
}


def _normalise_smart_punct(s: str) -> str:
    for smart, dumb in _SMART_PUNCT.items():
        s = s.replace(smart, dumb)
    return s


def _strip_stray_brackets(stem: str) -> str:
    if stem.count("[") == stem.count("]"):
        return stem
    out: list[str] = []
    depth = 0
    for ch in stem:
        if ch == "[":
            depth += 1
            out.append(ch)
        elif ch == "]":
            if depth > 0:
                depth -= 1
                out.append(ch)
        else:
            out.append(ch)
    result = "".join(out)
    if result.count("[") > result.count("]"):
        pos = 0
        while True:
            idx = result.find("[", pos)
            if idx == -1:
                break
            if "]" not in result[idx + 1:]:
                result = result[:idx] + result[idx + 1:]
            else:
                pos = idx + 1
    return result.strip()


def _normalise_1x_marker(stem: str) -> str:
    def _sub(m: re.Match[str]) -> str:
        return f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}"
    return XEPS_RE.sub(_sub, stem)


def _prepend_show_name(file_stem: str, grandparent: str) -> str | None:
    stripped = file_stem.lstrip(" .-")
    m = SXXEXX_EXTENDED.match(stripped)
    if not m:
        return None
    show = YEAR_RE.sub("", grandparent).strip()
    if not show or show.lower() == "series":
        return None
    return f"{show} {stripped}"


def _restore_from_parent(file_stem: str, source_name: str) -> str | None:
    """Adopt parent's exact title form when ascii-keys match (diacritics, apostrophes)."""
    m = SXXEXX_EXTENDED.search(file_stem)
    if m:
        file_title = file_stem[: m.start()].rstrip(" .-")
        parent_no_year = YEAR_RE.sub("", source_name).strip()
        if _ascii_key(file_title) and _ascii_key(file_title) == _ascii_key(parent_no_year):
            if file_title != parent_no_year:
                return parent_no_year + file_stem[len(file_title):]
        return None
    if YEAR_RE.search(file_stem):
        def _strip_year(s: str) -> str:
            mm = YEAR_RE.search(s)
            return s[: mm.start()].rstrip(" .-") if mm else s
        ft, pt = _strip_year(file_stem).strip(), _strip_year(source_name).strip()
        if _ascii_key(ft) and _ascii_key(ft) == _ascii_key(pt) and ft != pt:
            year_match = YEAR_RE.search(file_stem)
            rest = file_stem[year_match.start():] if year_match else ""
            return f"{pt} {rest}".strip()
    return None


def _adopt_parent_year(file_stem: str, parent_name: str) -> str | None:
    fm, pm = YEAR_RE.search(file_stem), YEAR_RE.search(parent_name)
    if not fm or not pm:
        return None
    fy, py = int(fm.group(1)), int(pm.group(1))
    if fy == py or abs(fy - py) > 1:
        return None
    return file_stem.replace(fm.group(0), pm.group(0), 1)


def _clean_ws(s: str) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^[\s\-\.]+", "", s)
    s = re.sub(r"[\s\-\.]+$", "", s)
    return s


def _compute_normalise_stem(file_stem: str, parent: str, grandparent: str) -> tuple[str, list[str]]:
    """Apply all normalise fixes in order; return ``(new_stem, fixes_applied)``."""
    fixes: list[str] = []
    s = file_stem
    for name, fn in (
        ("smart_punct", _normalise_smart_punct),
        ("stray_brackets", _strip_stray_brackets),
        ("1xNN", _normalise_1x_marker),
    ):
        s2 = fn(s)
        if s2 != s:
            fixes.append(name)
            s = s2
    s2 = _prepend_show_name(s, grandparent)
    if s2 and s2 != s:
        fixes.append("prepend_show")
        s = s2
    source = grandparent if SXXEXX_EXTENDED.search(s) else parent
    s2 = _restore_from_parent(s, source)
    if s2 and s2 != s:
        fixes.append("restore_from_parent")
        s = s2
    s2 = _adopt_parent_year(s, parent)
    if s2 and s2 != s:
        fixes.append("adopt_parent_year")
        s = s2
    s2 = _clean_ws(s)
    if s2 != s:
        fixes.append("cleanup_ws")
        s = s2
    return s, fixes


def _plan_normalise(root: Path) -> list[dict]:
    """Parent-folder truth pass plan."""
    plan: list[dict] = []
    targets: dict[Path, Path] = {}
    for dirpath, _, filenames in os.walk(root):
        dp = Path(dirpath)
        parent = dp.name
        grandparent = dp.parent.name
        for fn in filenames:
            old_path = dp / fn
            if old_path.suffix.lower() not in VIDEO_EXTS:
                continue
            new_stem, fixes = _compute_normalise_stem(old_path.stem, parent, grandparent)
            if not fixes or new_stem == old_path.stem:
                continue
            new_path = old_path.with_name(f"{new_stem}{old_path.suffix}")
            if new_path == old_path:
                continue
            status = "rename"
            if new_path.exists() and new_path != old_path:
                status = "collision_exists"
            elif new_path in targets and targets[new_path] != old_path:
                status = "collision_batch"
            else:
                targets[new_path] = old_path
            plan.append({"old_path": old_path, "new_path": new_path, "fixes": fixes, "status": status})
    return plan


# ---------------------------------------------------------------------------
# Shared rename executor (sidecars first + rollback on video failure)
# ---------------------------------------------------------------------------


def _execute_renames(plan: list[dict], dry_run: bool, save_tags: bool = False) -> dict:
    """Rename videos + their sidecars. Roll back sidecars if video rename fails.

    ``save_tags=True`` also persists the set of stripped fragments to
    ``control/stripped_tags.json`` for future custom-keyword management.
    """
    renames = [e for e in plan if e["status"] == "rename"]
    collisions = [e for e in plan if e["status"].startswith("collision")]

    fix_counts: dict[str, int] = {}
    for e in renames:
        for f in e["fixes"]:
            fix_counts[f] = fix_counts.get(f, 0) + 1

    if save_tags and renames:
        _save_stripped_tags(plan)

    done = 0
    sidecars_done = 0
    errors: list[str] = []
    for e in renames:
        old_path: Path = e["old_path"]
        new_path: Path = e["new_path"]
        old_stem = old_path.stem
        new_stem = new_path.stem
        sidecars = _find_sidecars(old_path)
        fixes_str = ",".join(e["fixes"])

        if dry_run:
            print(f"  [{fixes_str}]")
            print(f"    {old_path.name} -> {new_path.name}")
            for sc in sidecars:
                new_sc_name = new_stem + sc.name[len(old_stem):]
                print(f"      sidecar: {sc.name} -> {new_sc_name}")
            continue

        # Rename sidecars FIRST (so Plex can still pair via fallback if video rename fails).
        planned_sidecars: list[tuple[Path, Path]] = []
        for sc in sidecars:
            new_sc = sc.with_name(new_stem + sc.name[len(old_stem):])
            if new_sc.exists():
                errors.append(f"sidecar collision: {new_sc.name}")
                continue
            planned_sidecars.append((sc, new_sc))

        renamed_sidecars: list[tuple[Path, Path]] = []
        abort = False
        for sc, new_sc in planned_sidecars:
            try:
                sc.rename(new_sc)
                renamed_sidecars.append((sc, new_sc))
            except OSError as se:
                errors.append(f"sidecar {sc.name}: {se}")
                abort = True
                break
        if abort:
            for orig, moved in renamed_sidecars:
                try:
                    moved.rename(orig)
                except OSError:
                    pass
            continue

        try:
            old_path.rename(new_path)
            done += 1
            sidecars_done += len(renamed_sidecars)
            if done <= 50 or done % 25 == 0:
                print(f"  [{fixes_str}] {old_path.name} -> {new_path.name}")
        except OSError as err:
            for orig, moved in renamed_sidecars:
                try:
                    moved.rename(orig)
                except OSError:
                    pass
            errors.append(f"{old_path.name}: {err}")

    action = "Would rename" if dry_run else "Renamed"
    print(f"\n{action} {len(renames) if dry_run else done} files "
          f"(+{sidecars_done} sidecars), {len(collisions)} collisions, {len(errors)} errors.")
    if fix_counts:
        print(f"  Fix breakdown: {fix_counts}")
    for e in collisions[:10]:
        print(f"  ! collision: {e['old_path'].name} ({e['status']})")
    for err in errors[:10]:
        print(f"  ! {err}")
    return {"renamed": done, "sidecars": sidecars_done, "collisions": len(collisions),
            "errors": len(errors), "planned": len(renames)}


def cmd_clean_names(args: argparse.Namespace) -> int:
    """Anchor-based scene-tag stripper (replaces ``tools.strip_tags``).

    Historical behaviour preserved: default scans series only; ``--movies``
    adds movies (both are scanned). ``--root`` overrides with a single
    custom directory processed as "both".
    """
    dry_run = not args.execute
    if dry_run:
        print("DRY RUN (pass --execute to rename)\n")

    totals = {"renamed": 0, "sidecars": 0}

    if args.root:
        root = Path(args.root)
        print(f"Scanning: {root}")
        plan = _plan_tag_strip(root, "both")
        print("\n=== tag-strip ===")
        result = _execute_renames(plan, dry_run, save_tags=True)
        totals["renamed"] += result["renamed"]
        totals["sidecars"] += result["sidecars"]
    else:
        print(f"Scanning series: {NAS_SERIES}")
        plan = _plan_tag_strip(Path(str(NAS_SERIES)), "series")
        print("\n=== tag-strip: series ===")
        result = _execute_renames(plan, dry_run, save_tags=True)
        totals["renamed"] += result["renamed"]
        totals["sidecars"] += result["sidecars"]
        if args.movies:
            print(f"\nScanning movies: {NAS_MOVIES}")
            plan = _plan_tag_strip(Path(str(NAS_MOVIES)), "movies")
            print("\n=== tag-strip: movies ===")
            result = _execute_renames(plan, dry_run, save_tags=True)
            totals["renamed"] += result["renamed"]
            totals["sidecars"] += result["sidecars"]

    if not dry_run and totals["renamed"]:
        print()
        _trigger_plex_scan()
    return 0


def cmd_normalise(args: argparse.Namespace) -> int:
    """Folder-as-truth normalisation pass (replaces ``tools.normalise_filenames``).

    Defaults to scanning both movies and series. Pass ``--series`` or
    ``--movies`` to limit; ``--root`` overrides both with a custom directory.
    """
    dry_run = not args.execute
    if not (args.movies or args.series or args.root):
        args.movies = True
        args.series = True

    if dry_run:
        print("DRY RUN (pass --execute to rename)\n")

    totals = {"renamed": 0, "sidecars": 0}

    if args.root:
        root = Path(args.root)
        print(f"Scanning {root}")
        plan = _plan_normalise(root)
        print("\n=== normalise ===")
        result = _execute_renames(plan, dry_run)
        totals["renamed"] += result["renamed"]
        totals["sidecars"] += result["sidecars"]
    else:
        if args.series:
            print(f"Scanning series: {NAS_SERIES}")
            plan = _plan_normalise(Path(str(NAS_SERIES)))
            print("\n=== normalise: series ===")
            result = _execute_renames(plan, dry_run)
            totals["renamed"] += result["renamed"]
            totals["sidecars"] += result["sidecars"]
        if args.movies:
            print(f"\nScanning movies: {NAS_MOVIES}")
            plan = _plan_normalise(Path(str(NAS_MOVIES)))
            print("\n=== normalise: movies ===")
            result = _execute_renames(plan, dry_run)
            totals["renamed"] += result["renamed"]
            totals["sidecars"] += result["sidecars"]

    if not dry_run and totals["renamed"]:
        print()
        _trigger_plex_scan()
    return 0


# ---------------------------------------------------------------------------
# dedupe
# ---------------------------------------------------------------------------

# Patterns that mark a file as the dirty / foreign variant.
# IMPORTANT: avoid generic words like "Italian", "German" on their own — those
# appear in real episode titles (The White Lotus S02E02 "Italian Dream"). Only
# flag clear scene-tag patterns. Operates on the stem (no .mkv) so ``$`` works.
_DIRTY_MARKERS = re.compile(
    r"(?:^|[\s.])(?:ITA[\s.]+ENG|ENG[\s.]+ITA)(?:[\s.]|$)"
    r"|\bMULTI\b|\bDUAL\b|\bDubbedGerman\b|\bAC3D\b"
    r"|\bBOLUM\b|\bB[ÖO]L[ÜU]M\b"
    r"|\s\d{3}NH?\d*(?:\s|$)"
    r"|\[TAo\s*E\]"
    r"|\(\s*Kappa\s*\)",
    re.IGNORECASE,
)


def _scan_episode_duplicates(root: Path) -> dict[tuple[str, str, str], list[Path]]:
    ep_map: dict[tuple[str, str, str], list[Path]] = defaultdict(list)
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            p = Path(dirpath) / fn
            if p.suffix.lower() not in VIDEO_EXTS:
                continue
            m = SXXEXX_RE.search(p.stem)
            if not m:
                continue
            show = Path(dirpath).parent.name
            if not show or show.lower() == "series":
                show = Path(dirpath).name
            key = (show.lower(), m.group(1).upper(), m.group(2).upper())
            ep_map[key].append(p)
    return {k: v for k, v in ep_map.items() if len(v) > 1}


def _classify_duplicates(paths: list[Path]) -> dict:
    """Return ``{'delete': [...], 'keep': Path, 'review': [...]}``.

    Safe auto-delete only when exactly one clean sibling exists AND the others
    carry dirty scene markers.
    """
    dirty = [p for p in paths if _DIRTY_MARKERS.search(p.stem)]
    clean = [p for p in paths if not _DIRTY_MARKERS.search(p.stem)]
    if clean and dirty and len(clean) == 1:
        return {"delete": dirty, "keep": clean[0], "review": []}
    return {"delete": [], "keep": None, "review": paths}


def cmd_dedupe(args: argparse.Namespace) -> int:
    """Delete obvious foreign/junk duplicate episodes (replaces ``tools.dedupe_episodes``)."""
    dry_run = not args.execute
    dups = _scan_episode_duplicates(Path(str(NAS_SERIES)))
    if not dups:
        print("No duplicate episodes found.")
        return 0

    auto = 0
    review = 0
    freed = 0
    for key, paths in sorted(dups.items()):
        c = _classify_duplicates(paths)
        show, s, ep = key
        if c["delete"]:
            auto += len(c["delete"])
            print(f"\n[{show} S{s}E{ep}]")
            print(f"  KEEP:   {c['keep'].name}")
            for p in c["delete"]:
                sz = p.stat().st_size if p.exists() else 0
                freed += sz
                if dry_run:
                    print(f"  DELETE: {p.name}  ({sz / 1024 / 1024:.1f} MB)")
                else:
                    try:
                        p.unlink()
                        print(f"  DELETED: {p.name}  ({sz / 1024 / 1024:.1f} MB)")
                    except OSError as e:
                        print(f"  ERR delete {p}: {e}")
        else:
            review += len(c["review"])

    print(f"\n=== auto-delete: {auto} files (~{freed / 1024**3:.2f} GB), review: {review} files ===")
    if dry_run:
        print("DRY RUN — pass --execute to delete.")
    return 0


# ---------------------------------------------------------------------------
# relocate
# ---------------------------------------------------------------------------


def _find_misfiled_episodes(root: Path) -> list[dict]:
    """Find videos in show-root that belong in a ``Season N`` folder."""
    plan: list[dict] = []
    for show_dir in root.iterdir():
        if not show_dir.is_dir():
            continue
        for item in show_dir.iterdir():
            if not item.is_file() or item.suffix.lower() not in VIDEO_EXTS:
                continue
            m = SXXEXX_RE.search(item.stem)
            if not m:
                continue
            season_num = int(m.group(1))
            new_path = show_dir / f"Season {season_num}" / item.name
            plan.append({
                "old_path": item, "new_path": new_path,
                "show": show_dir.name, "season": season_num,
            })
    return plan


def cmd_relocate(args: argparse.Namespace) -> int:
    """Move bare-root episodes into Season N folders (replaces ``tools.relocate_misfiled_episodes``)."""
    dry_run = not args.execute
    plan = _find_misfiled_episodes(Path(str(NAS_SERIES)))
    if not plan:
        print("No misfiled episodes found.")
        return 0

    by_show: dict[str, list[dict]] = {}
    for e in plan:
        by_show.setdefault(e["show"], []).append(e)

    print(f"Found {len(plan)} misfiled episodes across {len(by_show)} shows:\n")
    for show, items in by_show.items():
        seasons = sorted({e["season"] for e in items})
        print(f"  {show}: {len(items)} files into Season {seasons}")

    done, sidecars_moved, errors = 0, 0, []
    for e in plan:
        old_path, new_path = e["old_path"], e["new_path"]
        if new_path.exists():
            errors.append(f"collision: {new_path.name}")
            continue
        if dry_run:
            print(f"  MOVE: {old_path} -> {new_path}")
            for sc in _find_sidecars(old_path):
                print(f"    + sidecar: {sc.name}")
            continue
        try:
            new_path.parent.mkdir(parents=True, exist_ok=True)
            for sc in _find_sidecars(old_path):
                new_sc = new_path.parent / (new_path.stem + sc.name[len(old_path.stem):])
                try:
                    if not new_sc.exists():
                        sc.rename(new_sc)
                        sidecars_moved += 1
                except OSError as se:
                    errors.append(f"sidecar {sc.name}: {se}")
            old_path.rename(new_path)
            done += 1
        except OSError as err:
            errors.append(f"{old_path.name}: {err}")

    action = "Would move" if dry_run else "Moved"
    print(f"\n{action} {done if not dry_run else len(plan)} files. "
          f"(+{sidecars_moved} sidecars), {len(errors)} errors.")
    for err in errors[:10]:
        print(f"  ! {err}")
    return 0


# ---------------------------------------------------------------------------
# repair-sidecars
# ---------------------------------------------------------------------------


def _sidecar_suffix(sidecar_stem: str, video_stem: str) -> str | None:
    if sidecar_stem == video_stem:
        return ""
    if sidecar_stem.startswith(video_stem + "."):
        return sidecar_stem[len(video_stem):]
    return None


def _extract_lang_flags(sidecar_stem: str) -> str:
    """Extract the trailing ``.lang[.flag]`` suffix from a sidecar stem."""
    parts = sidecar_stem.rsplit(".", 3)
    tail: list[str] = []
    for part in reversed(parts[1:]):
        if len(part) <= 4 and part.isalpha():
            tail.insert(0, part)
        else:
            break
    return ("." + ".".join(tail)) if tail else ""


def _pair_orphan_sidecars(folder: Path) -> list[dict]:
    videos, sidecars = [], []
    try:
        entries = list(folder.iterdir())
    except OSError:
        return []
    for item in entries:
        if not item.is_file():
            continue
        ext = item.suffix.lower()
        if ext in VIDEO_EXTS:
            videos.append(item)
        elif ext in SIDECAR_EXTS:
            sidecars.append(item)
    if not sidecars or not videos:
        return []

    ep_to_video: dict[tuple[str, str], Path] = {}
    for v in videos:
        m = SXXEXX_RE.search(v.stem)
        if m:
            ep_to_video[(m.group(1).upper(), m.group(2).upper())] = v

    plans: list[dict] = []
    for sc in sidecars:
        if any(_sidecar_suffix(sc.stem, v.stem) is not None for v in videos):
            continue
        m = SXXEXX_RE.search(sc.stem)
        if m:
            key = (m.group(1).upper(), m.group(2).upper())
            v = ep_to_video.get(key)
            if v:
                new_name = v.stem + _extract_lang_flags(sc.stem) + sc.suffix
                if new_name != sc.name:
                    plans.append({"sidecar": sc, "new_name": new_name,
                                  "reason": f"S{key[0]}E{key[1]} match"})
            continue
        if len(videos) == 1:
            v = videos[0]
            new_name = v.stem + _extract_lang_flags(sc.stem) + sc.suffix
            if new_name != sc.name:
                plans.append({"sidecar": sc, "new_name": new_name, "reason": "single-video folder"})
    return plans


def cmd_repair_sidecars(args: argparse.Namespace) -> int:
    """Rename orphan sidecars to match their videos (replaces ``tools.repair_sidecars``)."""
    dry_run = not args.execute
    include_movies = args.movies or not args.series
    include_series = args.series or not args.movies

    grand: list[dict] = []
    for label, root in _walk_roots(include_movies, include_series):
        print(f"Scanning {label}: {root}")
        for dirpath, _, _ in os.walk(str(root)):
            for p in _pair_orphan_sidecars(Path(dirpath)):
                p["folder"] = Path(dirpath)
                grand.append(p)

    print(f"\nFound {len(grand)} orphan sidecars to repair.\n")

    done, errors = 0, []
    for p in grand:
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
        print(f"\nWould rename {len(grand)} sidecars.")
    else:
        print(f"\nRenamed {done} sidecars, {len(errors)} errors.")
        for e in errors[:10]:
            print(f"  ! {e}")
    return 0


# ---------------------------------------------------------------------------
# audit (compliance — replaces tools.compliance)
# ---------------------------------------------------------------------------

TARGET_VIDEO = {"av1"}
TARGET_AUDIO = {"eac3", "opus"}
ENG_LANGS = {"en", "eng", "english"}
UND_LANGS = {"und", "unk", ""}

# Per-language equivalence map for the "original language" audio rule.
# TMDb original_language is ISO 639-1; ffprobe usually emits ISO 639-2/639-3.
ORIG_LANG_EQUIVS: dict[str, set[str]] = {
    "en":  {"en", "eng"},
    "ja":  {"ja", "jpn"},
    "ko":  {"ko", "kor"},
    "zh":  {"zh", "chi", "zho", "cmn", "yue"},
    "cn":  {"zh", "chi", "zho", "cmn", "yue"},
    "fr":  {"fr", "fre", "fra"},
    "de":  {"de", "ger", "deu"},
    "es":  {"es", "spa", "esp"},
    "it":  {"it", "ita"},
    "pt":  {"pt", "por", "pt-br", "pt-pt"},
    "ru":  {"ru", "rus"},
    "sv":  {"sv", "swe"},
    "no":  {"no", "nor", "nob", "nno"},
    "da":  {"da", "dan"},
    "fi":  {"fi", "fin"},
    "nl":  {"nl", "dut", "nld"},
    "pl":  {"pl", "pol"},
    "cs":  {"cs", "cze", "ces"},
    "hu":  {"hu", "hun"},
    "tr":  {"tr", "tur"},
    "ar":  {"ar", "ara"},
    "hi":  {"hi", "hin"},
    "th":  {"th", "tha"},
    "he":  {"he", "heb", "iw"},
    "el":  {"el", "gre", "ell"},
    "fa":  {"fa", "per", "fas"},
    "xx":  set(),
    "zxx": set(),
}


def check_file(entry: dict, config: dict) -> list[str]:
    """Return list of violation strings for a media_report entry (empty = compliant)."""
    violations: list[str] = []
    lossless = {c.lower() for c in config.get("lossless_audio_codecs") or []}

    v = entry.get("video") or {}
    vcodec = (v.get("codec_raw") or v.get("codec") or "").lower()
    if vcodec and vcodec not in TARGET_VIDEO:
        violations.append(f"video codec {vcodec} (target: av1)")

    tmdb = entry.get("tmdb") or {}
    orig_lang = (tmdb.get("original_language") or "").lower().strip()
    keeper_langs = ORIG_LANG_EQUIVS.get(orig_lang, {orig_lang} if orig_lang else set()) | UND_LANGS
    enforce_orig = bool(orig_lang) and orig_lang not in ("xx", "zxx")

    for i, a in enumerate(entry.get("audio_streams") or []):
        codec = (a.get("codec_raw") or a.get("codec", "")).lower().replace("-", "")
        lang = (a.get("language") or a.get("detected_language") or "").lower().strip()
        if codec and codec not in TARGET_AUDIO and codec not in lossless:
            violations.append(f"audio[{i}] codec {a.get('codec_raw') or a.get('codec')}")
        if enforce_orig and lang and lang not in keeper_langs:
            violations.append(f"audio[{i}] language {lang} (original: {orig_lang})")

    int_subs = entry.get("subtitle_streams") or []
    ext_subs = entry.get("external_subtitles") or []
    regular_eng = sum(
        1 for s in int_subs
        if (s.get("language") or s.get("detected_language") or "").lower().strip() in ENG_LANGS
        and not is_hi_internal(s)
    )
    regular_eng += sum(
        1 for s in ext_subs
        if (s.get("language") or "").lower().strip() in ENG_LANGS
        and not is_hi_external(s.get("filename") or "")
    )
    hi_count = sum(1 for s in int_subs if is_hi_internal(s)) + sum(
        1 for s in ext_subs if is_hi_external(s.get("filename") or "")
    )
    foreign = sum(
        1 for s in int_subs
        if (s.get("language") or s.get("detected_language") or "und").lower().strip() not in (ENG_LANGS | UND_LANGS)
    ) + sum(
        1 for s in ext_subs
        if (s.get("language") or "und").lower().strip() not in (ENG_LANGS | UND_LANGS)
    )
    if regular_eng == 0:
        violations.append("sub: missing non-HI English sub")
    elif regular_eng > 1:
        violations.append(f"sub: {regular_eng} English subs (want exactly 1)")
    if hi_count > 0:
        violations.append(f"sub: {hi_count} HI/SDH sub(s) present")
    if foreign > 0:
        violations.append(f"sub: {foreign} foreign sub(s)")

    fname = entry.get("filename") or ""
    if SCENE_TAG_RE.search(fname):
        violations.append(f"filename has scene tags: {fname}")

    if entry.get("library_type") == "movie":
        if not (entry.get("tmdb") and entry["tmdb"].get("tmdb_id")):
            violations.append("no tmdb metadata")

    return violations


def cmd_audit(args: argparse.Namespace) -> int:
    """Library standards compliance audit (replaces ``tools.compliance``).

    Prints a summary and top violation types. With ``--csv PATH`` also writes
    a per-file violations CSV. With ``--queue reencode`` appends non-compliant
    filepaths to ``control/reencode.json`` so the pipeline picks them up
    (historical behaviour: the queue write happens immediately, no dry-run).
    """
    with open(args.report, encoding="utf-8") as f:
        report = json.load(f)
    files = report.get("files", [])
    print(f"Auditing {len(files)} files against library standards...")

    non_compliant: list[tuple[str, list[str]]] = []
    counter: Counter = Counter()
    for entry in files:
        vs = check_file(entry, DEFAULT_CONFIG)
        if not vs:
            continue
        non_compliant.append((entry["filepath"], vs))
        for v in vs:
            key = " ".join(v.split(":")[0].split(" ", 2)[0:2])
            counter[key] += 1
        if args.limit and len(non_compliant) >= args.limit:
            break

    total = len(files)
    compliant = total - len(non_compliant)
    pct = (compliant / total * 100) if total else 0
    print()
    print(f"Compliant:     {compliant:>5} / {total} ({pct:.1f}%)")
    print(f"Non-compliant: {len(non_compliant):>5}")
    print()
    print("Top violation types:")
    for v, n in counter.most_common(10):
        print(f"  {n:>5}  {v}")
    print()

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["filepath", "n_violations", "violations"])
            for path, vs in non_compliant:
                w.writerow([path, len(vs), "; ".join(vs)])
        print(f"CSV: {args.csv}")

    if args.queue == "reencode":
        # reencode.json format: {"files": {path: override_dict, ...}, "patterns": {pattern: override}}.
        # An empty override dict just means "re-queue with default params".
        out = CONTROL_DIR / "reencode.json"
        try:
            existing = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
        except Exception:
            existing = {}
        existing_files = existing.get("files", {})
        if isinstance(existing_files, list):
            existing_files = {p: {} for p in existing_files}
        for path, _vs in non_compliant:
            if path not in existing_files:
                existing_files[path] = {}
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps({"files": existing_files, "patterns": existing.get("patterns", {})}, indent=2),
            encoding="utf-8",
        )
        print(f"Queued {len(non_compliant)} files (total now {len(existing_files)} in reencode.json)")
    return 0


# ---------------------------------------------------------------------------
# CLI plumbing
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tools.maintain",
        description="Unified NAS-cleanup CLI. Destructive subcommands dry-run by default; pass --execute to mutate.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True, metavar="<subcommand>")

    p_clean = sub.add_parser("clean-names", help="Anchor-based scene-tag stripping (replaces strip_tags)")
    p_clean.add_argument("--execute", action="store_true", help="Actually rename files (default: dry-run)")
    p_clean.add_argument("--movies", action="store_true", help="Also scan movies (default: series only)")
    p_clean.add_argument("--root", type=str, default=None, help="Custom root (overrides NAS paths)")
    p_clean.set_defaults(func=cmd_clean_names)

    p_norm = sub.add_parser("normalise", help="Folder-as-truth normalisation pass (replaces normalise_filenames)")
    p_norm.add_argument("--execute", action="store_true")
    p_norm.add_argument("--movies", action="store_true")
    p_norm.add_argument("--series", action="store_true")
    p_norm.add_argument("--root", type=str, default=None)
    p_norm.set_defaults(func=cmd_normalise)

    p_dedup = sub.add_parser("dedupe", help="Find same-episode duplicates; auto-delete obvious junk siblings")
    p_dedup.add_argument("--execute", action="store_true")
    p_dedup.set_defaults(func=cmd_dedupe)

    p_reloc = sub.add_parser("relocate", help="Move misfiled episodes into their Season N folder")
    p_reloc.add_argument("--execute", action="store_true")
    p_reloc.set_defaults(func=cmd_relocate)

    p_rep = sub.add_parser("repair-sidecars", help="Pair orphaned sidecars to their renamed video")
    p_rep.add_argument("--execute", action="store_true")
    p_rep.add_argument("--series", action="store_true")
    p_rep.add_argument("--movies", action="store_true")
    p_rep.set_defaults(func=cmd_repair_sidecars)

    p_aud = sub.add_parser(
        "audit",
        help="Library standards compliance audit. --queue reencode writes control/reencode.json.",
    )
    p_aud.add_argument("--report", type=str, default=str(MEDIA_REPORT))
    p_aud.add_argument("--csv", type=str, default=None, help="Write per-file CSV of violations")
    p_aud.add_argument(
        "--queue",
        choices=["reencode", "print"],
        default="print",
        help="'reencode' appends non-compliant paths to control/reencode.json",
    )
    p_aud.add_argument("--limit", type=int, default=0, help="Stop after N non-compliant files")
    p_aud.set_defaults(func=cmd_audit)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
