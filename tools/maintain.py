"""Unified CLI for small NAS-cleanup maintenance tools.

Subcommands consolidate what used to live in 6 separate scripts:

  clean-names      Rename files — anchor-based tag strip + parent-folder
                   truth pass (merges strip_tags + normalise_filenames).
  dedupe           Find same-episode duplicates and auto-delete obvious
                   foreign-tag / junk siblings when a clean sibling exists.
  relocate         Move episodes out of show root into Season N folder.
  repair-sidecars  Pair orphaned .srt/.nfo sidecars to their renamed video.
  audit            Library-wide standards compliance report.
  queue-reencode   Run audit and append non-compliant paths to
                   control/reencode.json for the pipeline to pick up.

Every subcommand defaults to DRY RUN. Pass ``--execute`` to actually
mutate files.
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

# Force UTF-8 stdout/stderr on Windows (Ō / ü / ō in filenames will cp1252-crash otherwise).
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
    """Return sidecar files whose stem == video stem or starts with stem + "."."""
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
    """Trigger Plex library scan. No-op if PLEX_URL/PLEX_TOKEN unset."""
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


def _load_report() -> dict:
    """Read media_report.json. Returns {} if missing / unreadable."""
    try:
        with open(MEDIA_REPORT, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _walk_roots(include_movies: bool, include_series: bool) -> list[tuple[str, Path]]:
    roots: list[tuple[str, Path]] = []
    if include_series:
        roots.append(("series", Path(str(NAS_SERIES))))
    if include_movies:
        roots.append(("movies", Path(str(NAS_MOVIES))))
    return roots


# ---------------------------------------------------------------------------
# clean-names: anchor strip (phase 1) + parent-truth normalise (phase 2)
# ---------------------------------------------------------------------------


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _ascii_key(s: str) -> str:
    s = _strip_accents(s)
    s = re.sub(r"[^\w]+", " ", s, flags=re.UNICODE).lower()
    return re.sub(r"\s+", " ", s).strip()


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
    """Remove unbalanced `[` / `]` left by partial cleaning."""
    if stem.count("[") == stem.count("]"):
        return stem
    out, depth = [], 0
    for ch in stem:
        if ch == "]" and depth == 0:
            continue  # unmatched close
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
        out.append(ch)
    result = "".join(out)
    # Strip any leftover unmatched opens (no closing `]` later in the string)
    while result.count("[") > result.count("]"):
        idx = result.find("[")
        if idx == -1 or "]" in result[idx + 1:]:
            break
        result = result[:idx] + result[idx + 1:]
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


def _strip_year(s: str) -> str:
    m = YEAR_RE.search(s)
    return s[: m.start()].rstrip(" .-") if m else s


def _restore_from_parent(file_stem: str, source_name: str) -> str | None:
    """Adopt parent's exact title form when ascii-keys match (diacritics, apostrophes)."""
    m = SXXEXX_EXTENDED.search(file_stem)
    if m:
        file_title = file_stem[: m.start()].rstrip(" .-")
        parent_no_year = YEAR_RE.sub("", source_name).strip()
        if (_ascii_key(file_title) and _ascii_key(file_title) == _ascii_key(parent_no_year)
                and file_title != parent_no_year):
            return parent_no_year + file_stem[len(file_title):]
        return None
    if YEAR_RE.search(file_stem):
        ft, pt = _strip_year(file_stem).strip(), _strip_year(source_name).strip()
        if _ascii_key(ft) and _ascii_key(ft) == _ascii_key(pt) and ft != pt:
            m2 = YEAR_RE.search(file_stem)
            rest = file_stem[m2.start():] if m2 else ""
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
    """Phase 2: parent-folder truth. Returns (new_stem, fix_list)."""
    fixes: list[str] = []
    s = file_stem

    def _apply(name: str, result: str | None) -> None:
        nonlocal s
        if result is not None and result != s:
            fixes.append(name)
            s = result

    _apply("smart_punct", _normalise_smart_punct(s))
    _apply("stray_brackets", _strip_stray_brackets(s))
    _apply("1xNN", _normalise_1x_marker(s))
    _apply("prepend_show", _prepend_show_name(s, grandparent))
    _apply("restore_from_parent",
           _restore_from_parent(s, grandparent if SXXEXX_EXTENDED.search(s) else parent))
    _apply("adopt_parent_year", _adopt_parent_year(s, parent))
    _apply("cleanup_ws", _clean_ws(s))
    return s, fixes


def _classify_rename(old_path: Path, new_path: Path, targets: dict[Path, Path]) -> str:
    """Classify a planned rename as rename / collision_exists / collision_batch."""
    if new_path.exists() and new_path != old_path:
        return "collision_exists"
    if new_path in targets and targets[new_path] != old_path:
        return "collision_batch"
    targets[new_path] = old_path
    return "rename"


def _plan_tag_strip(root: Path, mode: str) -> list[dict]:
    """Phase 1: anchor-based tag strip using pipeline.filename."""
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
            new_stem = next((r for r in (fn_c(old_path.stem) for fn_c in cleaners) if r is not None), None)
            if new_stem is None or new_stem == old_path.stem:
                continue
            new_path = old_path.with_name(f"{new_stem}{old_path.suffix}")
            plan.append({
                "old_path": old_path, "new_path": new_path,
                "fixes": ["tag_strip"], "status": _classify_rename(old_path, new_path, targets),
            })
    return plan


def _plan_normalise(root: Path) -> list[dict]:
    """Phase 2: parent-folder truth pass."""
    plan: list[dict] = []
    targets: dict[Path, Path] = {}
    for dirpath, _, filenames in os.walk(root):
        dp = Path(dirpath)
        for fn in filenames:
            old_path = dp / fn
            if old_path.suffix.lower() not in VIDEO_EXTS:
                continue
            new_stem, fixes = _compute_normalise_stem(old_path.stem, dp.name, dp.parent.name)
            if not fixes or new_stem == old_path.stem:
                continue
            new_path = old_path.with_name(f"{new_stem}{old_path.suffix}")
            if new_path == old_path:
                continue
            plan.append({
                "old_path": old_path, "new_path": new_path,
                "fixes": fixes, "status": _classify_rename(old_path, new_path, targets),
            })
    return plan


def _rollback(renamed: list[tuple[Path, Path]]) -> None:
    """Undo a list of (orig, moved) renames. Best-effort; ignores errors."""
    for orig, moved in renamed:
        try:
            moved.rename(orig)
        except OSError:
            pass


def _rename_with_sidecars(old_path: Path, new_path: Path, errors: list[str]) -> int | None:
    """Rename video + its sidecars atomically. Returns sidecar count, or None on failure."""
    old_stem, new_stem = old_path.stem, new_path.stem
    # Rename sidecars FIRST so Plex can still pair via fallback if the video rename fails.
    planned: list[tuple[Path, Path]] = []
    for sc in _find_sidecars(old_path):
        new_sc = sc.with_name(new_stem + sc.name[len(old_stem):])
        if new_sc.exists():
            errors.append(f"sidecar collision: {new_sc.name}")
            continue
        planned.append((sc, new_sc))

    done: list[tuple[Path, Path]] = []
    for sc, new_sc in planned:
        try:
            sc.rename(new_sc)
            done.append((sc, new_sc))
        except OSError as e:
            errors.append(f"sidecar {sc.name}: {e}")
            _rollback(done)
            return None

    try:
        old_path.rename(new_path)
        return len(done)
    except OSError as e:
        _rollback(done)
        errors.append(f"{old_path.name}: {e}")
        return None


def _execute_renames(plan: list[dict], dry_run: bool) -> dict:
    """Rename videos + their sidecars. Rolls back sidecars if video rename fails."""
    renames = [e for e in plan if e["status"] == "rename"]
    collisions = [e for e in plan if e["status"].startswith("collision")]

    fix_counts: dict[str, int] = {}
    for e in renames:
        for f in e["fixes"]:
            fix_counts[f] = fix_counts.get(f, 0) + 1

    done = 0
    sidecars_done = 0
    errors: list[str] = []
    for e in renames:
        old_path, new_path = e["old_path"], e["new_path"]
        fixes_str = ",".join(e["fixes"])

        if dry_run:
            print(f"  [{fixes_str}]")
            print(f"    {old_path.name} -> {new_path.name}")
            for sc in _find_sidecars(old_path):
                print(f"      sidecar: {sc.name} -> {new_path.stem + sc.name[len(old_path.stem):]}")
            continue

        n_sidecars = _rename_with_sidecars(old_path, new_path, errors)
        if n_sidecars is None:
            continue
        done += 1
        sidecars_done += n_sidecars
        if done <= 50 or done % 25 == 0:
            print(f"  [{fixes_str}] {old_path.name} -> {new_path.name}")

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
    dry_run = not args.execute
    include_movies = args.movies or not args.series
    include_series = args.series or not args.movies
    if args.root:
        include_movies = True
        include_series = True

    if dry_run:
        print("DRY RUN (pass --execute to rename)\n")

    totals = {"renamed": 0, "sidecars": 0}
    phases: list[tuple[str, list[dict]]] = []

    if args.root:
        root = Path(args.root)
        print(f"Scanning: {root}")
        if not args.skip_tag_strip:
            phases.append(("tag-strip", _plan_tag_strip(root, "both")))
        if not args.skip_normalise:
            phases.append(("normalise", _plan_normalise(root)))
    else:
        for label, root in _walk_roots(include_movies, include_series):
            print(f"Scanning {label}: {root}")
            mode = "series" if label == "series" else "movies"
            if not args.skip_tag_strip:
                phases.append((f"{label}/tag-strip", _plan_tag_strip(root, mode)))
            if not args.skip_normalise:
                phases.append((f"{label}/normalise", _plan_normalise(root)))

    for phase_name, plan in phases:
        print(f"\n=== phase: {phase_name} ===")
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
    dirty = [p for p in paths if _DIRTY_MARKERS.search(p.stem)]
    clean = [p for p in paths if not _DIRTY_MARKERS.search(p.stem)]
    if clean and dirty and len(clean) == 1:
        return {"delete": dirty, "keep": clean[0], "review": []}
    return {"delete": [], "keep": None, "review": paths}


def cmd_dedupe(args: argparse.Namespace) -> int:
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
# audit + queue-reencode (compliance)
# ---------------------------------------------------------------------------

TARGET_VIDEO = {"av1"}
TARGET_AUDIO = {"eac3", "opus"}
ENG_LANGS = {"en", "eng", "english"}
UND_LANGS = {"und", "unk", ""}

# TMDb ISO 639-1 → all ISO 639-x codes ffprobe might emit.
ORIG_LANG_EQUIVS: dict[str, set[str]] = {k: set(v.split()) for k, v in {
    "en": "en eng", "ja": "ja jpn", "ko": "ko kor",
    "zh": "zh chi zho cmn yue", "cn": "zh chi zho cmn yue",
    "fr": "fr fre fra", "de": "de ger deu", "es": "es spa esp",
    "it": "it ita", "pt": "pt por pt-br pt-pt", "ru": "ru rus",
    "sv": "sv swe", "no": "no nor nob nno", "da": "da dan", "fi": "fi fin",
    "nl": "nl dut nld", "pl": "pl pol", "cs": "cs cze ces", "hu": "hu hun",
    "tr": "tr tur", "ar": "ar ara", "hi": "hi hin", "th": "th tha",
    "he": "he heb iw", "el": "el gre ell", "fa": "fa per fas",
    "xx": "", "zxx": "",
}.items()}


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

    # Unified iteration over internal + external subs — one pass, three counters.
    regular_eng = hi_count = foreign = 0
    for s in entry.get("subtitle_streams") or []:
        lang = (s.get("language") or s.get("detected_language") or "").lower().strip()
        is_hi = is_hi_internal(s)
        if is_hi:
            hi_count += 1
        elif lang in ENG_LANGS:
            regular_eng += 1
        elif lang and lang not in UND_LANGS:
            foreign += 1
    for s in entry.get("external_subtitles") or []:
        lang = (s.get("language") or "").lower().strip()
        is_hi = is_hi_external(s.get("filename") or "")
        if is_hi:
            hi_count += 1
        elif lang in ENG_LANGS:
            regular_eng += 1
        elif lang and lang not in UND_LANGS:
            foreign += 1
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


def _run_audit(report_path: str, limit: int = 0) -> tuple[list[tuple[str, list[str]]], int]:
    """Return (non_compliant_list, total_file_count)."""
    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)
    files = report.get("files", [])
    non_compliant: list[tuple[str, list[str]]] = []
    for entry in files:
        vs = check_file(entry, DEFAULT_CONFIG)
        if vs:
            non_compliant.append((entry["filepath"], vs))
            if limit and len(non_compliant) >= limit:
                break
    return non_compliant, len(files)


def cmd_audit(args: argparse.Namespace) -> int:
    non_compliant, total = _run_audit(args.report, args.limit)
    counter: Counter = Counter()
    for _, vs in non_compliant:
        for v in vs:
            counter[" ".join(v.split(":")[0].split(" ", 2)[0:2])] += 1

    compliant = total - len(non_compliant)
    pct = (compliant / total * 100) if total else 0
    print(f"Auditing {total} files against library standards...")
    print(f"\nCompliant:     {compliant:>5} / {total} ({pct:.1f}%)")
    print(f"Non-compliant: {len(non_compliant):>5}\n\nTop violation types:")
    for v, n in counter.most_common(10):
        print(f"  {n:>5}  {v}")

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["filepath", "n_violations", "violations"])
            for path, vs in non_compliant:
                w.writerow([path, len(vs), "; ".join(vs)])
        print(f"CSV: {args.csv}")
    return 0


def cmd_queue_reencode(args: argparse.Namespace) -> int:
    non_compliant, _ = _run_audit(args.report, args.limit)

    out = CONTROL_DIR / "reencode.json"
    try:
        existing = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    except Exception:
        existing = {}
    existing_files = existing.get("files", {})
    if isinstance(existing_files, list):
        existing_files = {p: {} for p in existing_files}
    for path, _vs in non_compliant:
        existing_files.setdefault(path, {})

    if args.execute:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps({"files": existing_files, "patterns": existing.get("patterns", {})}, indent=2),
            encoding="utf-8",
        )
        print(f"Queued {len(non_compliant)} files (total now {len(existing_files)} in reencode.json)")
    else:
        print(f"DRY RUN — would queue {len(non_compliant)} files "
              f"(total would be {len(existing_files)}). Pass --execute to write.")
    return 0


# ---------------------------------------------------------------------------
# CLI plumbing
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tools.maintain",
        description="Unified NAS-cleanup CLI. All subcommands dry-run by default; pass --execute to mutate.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True, metavar="<subcommand>")

    p_clean = sub.add_parser("clean-names", help="Strip scene tags + normalise filenames against parent folders")
    p_clean.add_argument("--execute", action="store_true", help="Actually rename files (default: dry-run)")
    p_clean.add_argument("--movies", action="store_true", help="Only process movies")
    p_clean.add_argument("--series", action="store_true", help="Only process series")
    p_clean.add_argument("--root", type=str, default=None, help="Custom root (overrides NAS paths)")
    p_clean.add_argument("--skip-tag-strip", action="store_true", help="Skip phase 1 (anchor tag strip)")
    p_clean.add_argument("--skip-normalise", action="store_true", help="Skip phase 2 (parent-truth normalise)")
    p_clean.set_defaults(func=cmd_clean_names)

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

    p_aud = sub.add_parser("audit", help="Library standards compliance audit (report only)")
    p_aud.add_argument("--report", type=str, default=str(MEDIA_REPORT))
    p_aud.add_argument("--csv", type=str, default=None, help="Write per-file CSV of violations")
    p_aud.add_argument("--limit", type=int, default=0, help="Stop after N non-compliant files")
    p_aud.set_defaults(func=cmd_audit)

    p_q = sub.add_parser("queue-reencode", help="Audit + append non-compliant files to control/reencode.json")
    p_q.add_argument("--report", type=str, default=str(MEDIA_REPORT))
    p_q.add_argument("--limit", type=int, default=0)
    p_q.add_argument("--execute", action="store_true", help="Actually write reencode.json (default: dry-run)")
    p_q.set_defaults(func=cmd_queue_reencode)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
