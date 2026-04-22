"""Whole-library filename normalisation pass.

Uses PARENT FOLDER as source of truth for movies (e.g. "Wayne's World 2 (1993)")
and GRANDPARENT for episodes (e.g. "The Office (US)"), since parent/grandparent
folders are typically clean while the filename has been munged by scene releases
or partial cleaning passes.

Fixes in a single sweep:
  A. Bare-SxxExx filenames → prepend show name from grandparent folder
  B. `1x07` episode-marker format → `S01E07`
  C. Stray `[` or `]` characters not part of a proper bracket group
  D. Missing apostrophes vs parent ("Waynes World" → "Wayne's World")
  E. Missing period after abbreviations (Dr, Mr, Mrs, St, Jr) when parent has them
  F. Missing diacritics vs parent ("Shogun" → "Shōgun")
  G. Missing ` - ` subtitle separator ("Pirates... Dead Mans Chest" → parent form)
  H. Filename-year mismatches parent-year — adopts parent's year when only 1-2 off
     (keeps filename year if parent has no year)
  I. Welded words ("Rickand Morty" → "Rick and Morty") — using parent as target
  J. Double spaces, leading/trailing whitespace and hyphens

Default is dry-run. Pass --execute to actually rename files.
"""

import argparse
import re
import sys
import unicodedata
from pathlib import Path

# Windows cp1252 stdout blows up on Ō/ō/etc — force UTF-8 for all prints
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

from paths import NAS_MOVIES, NAS_SERIES, PLEX_TOKEN, PLEX_URL

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".flv", ".mov", ".ts", ".webm"}
# Sidecar suffixes that follow a video file — Plex matches them by stem prefix
SIDECAR_EXTS = {".srt", ".ass", ".ssa", ".sub", ".idx", ".vtt", ".nfo", ".smi", ".sup"}

# SxxExx detector (covers S01E01, S01E01E02, 1x07)
SXXEXX_RE = re.compile(r"(S(\d{1,4})[\s.]?E(\d{1,2})(?:[\s.]?E\d{1,2})?)", re.IGNORECASE)
XEPS_RE = re.compile(r"(?<![A-Za-z0-9])(\d{1,2})x(\d{1,3})(?![A-Za-z0-9])")

YEAR_RE = re.compile(r"\((19[2-9]\d|20[0-2]\d)\)")


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _ascii_key(s: str) -> str:
    """Canonical compare key: strip accents + punctuation + case, collapse spaces."""
    s = _strip_accents(s)
    s = re.sub(r"[^\w]+", " ", s, flags=re.UNICODE).lower()
    return re.sub(r"\s+", " ", s).strip()


def _collapse_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def normalise_1x_marker(stem: str) -> str:
    """Fix B: 1x07 → S01E07."""
    def _sub(m):
        season = int(m.group(1))
        episode = int(m.group(2))
        return f"S{season:02d}E{episode:02d}"
    return XEPS_RE.sub(_sub, stem)


def strip_stray_brackets(stem: str) -> str:
    """Fix C: remove unmatched `[` or `]` left after partial cleaning."""
    # Count opens vs closes
    opens = stem.count("[")
    closes = stem.count("]")
    if opens == closes:
        return stem
    # Remove lone brackets that aren't balanced: find brackets not part of a pair
    out = []
    depth = 0
    for ch in stem:
        if ch == "[":
            depth += 1
            out.append(ch)
        elif ch == "]":
            if depth > 0:
                depth -= 1
                out.append(ch)
            # else: unmatched close, drop it
        else:
            out.append(ch)
    result = "".join(out)
    # Drop trailing unmatched opens
    if result.count("[") > result.count("]"):
        # Strip "[" that aren't followed by a matching "]"
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


def _longest_common_prefix_title(file_stem: str, parent_name: str) -> tuple[str, str]:
    """Return (file_title, parent_title) — the title portions before SxxExx/year.

    For movies parent is "Wayne's World 2 (1993)", file_stem "Waynes World 2 (1993)";
    both get their year suffix stripped for compare.
    """
    def _strip_year(s):
        m = YEAR_RE.search(s)
        return s[: m.start()].rstrip(" .-") if m else s

    return _strip_year(file_stem), _strip_year(parent_name)


def restore_punctuation_from_parent(file_stem: str, parent_name: str) -> str | None:
    """Fixes D, E, F, G, I: when the _ascii_key of file-title matches parent-title,
    adopt parent's exact title form. Keeps rest of file_stem (SxxExx / episode title).
    """
    # Movies: whole stem matches parent name (minus year)
    # Episodes: prefix before SxxExx matches grandparent

    m = SXXEXX_RE.search(file_stem)
    has_year = YEAR_RE.search(file_stem)

    if m:  # series episode
        file_title = file_stem[: m.start()].rstrip(" .-")
        parent_title = parent_name
        # Strip " (YYYY)" from grandparent e.g. "Archer (2009)"
        parent_title_no_year = YEAR_RE.sub("", parent_title).strip()
        if _ascii_key(file_title) and _ascii_key(file_title) == _ascii_key(parent_title_no_year):
            if file_title != parent_title_no_year:
                new_stem = parent_title_no_year + file_stem[m.start() - len(file_stem[: m.start()]) + len(file_title):]
                # Simpler: just replace the prefix
                new_stem = parent_title_no_year + file_stem[len(file_title):]
                return new_stem
        return None

    if has_year:  # movie
        ft, pt = _longest_common_prefix_title(file_stem, parent_name)
        ft_clean = ft.strip()
        pt_clean = pt.strip()
        if _ascii_key(ft_clean) and _ascii_key(ft_clean) == _ascii_key(pt_clean):
            if ft_clean != pt_clean:
                # Adopt parent's title, keep file's year suffix
                year_match = YEAR_RE.search(file_stem)
                rest = file_stem[year_match.start():] if year_match else ""
                return f"{pt_clean} {rest}".strip()
        return None

    return None


def prepend_show_name(file_stem: str, grandparent: str) -> str | None:
    """Fix A: if filename starts with SxxExx (or whitespace before), prepend show name."""
    stripped = file_stem.lstrip(" .-")
    m = SXXEXX_RE.match(stripped)
    if not m:
        return None
    # Grandparent may include " (YYYY)" — strip for prefix
    show = YEAR_RE.sub("", grandparent).strip()
    if not show or show.lower() == "series":
        return None
    return f"{show} {stripped}"


def adopt_parent_year(file_stem: str, parent_name: str) -> str | None:
    """Fix H: if file's year differs from parent's year by 1, adopt parent's."""
    fm = YEAR_RE.search(file_stem)
    pm = YEAR_RE.search(parent_name)
    if not fm or not pm:
        return None
    fy, py = int(fm.group(1)), int(pm.group(1))
    if fy == py:
        return None
    if abs(fy - py) > 1:
        # Big gap = different release, don't auto-fix
        return None
    return file_stem.replace(fm.group(0), pm.group(0), 1)


def clean_extra_whitespace(stem: str) -> str:
    """Fix J: collapse double-spaces, strip leading/trailing noise."""
    s = _collapse_ws(stem)
    # Trim leading "-" or "." residue
    s = re.sub(r"^[\s\-\.]+", "", s)
    s = re.sub(r"[\s\-\.]+$", "", s)
    return s


# Smart-quote / en-dash / em-dash / fullwidth colon → ASCII equivalents,
# and U+FFFD replacement char (0xFFFD) → stripped entirely.
_SMART_PUNCT = {
    "\u2018": "'",  # ‘
    "\u2019": "'",  # ’
    "\u201c": '"',  # “
    "\u201d": '"',  # ”
    "\u2013": "-",  # –
    "\u2014": "-",  # —
    "\uFF1A": " - ",  # ：fullwidth colon
    "\uFFFD": "",  # � replacement
}


def normalise_smart_punct(stem: str) -> str:
    """Fix K: replace smart quotes / dashes / replacement chars with ASCII."""
    out = stem
    for smart, dumb in _SMART_PUNCT.items():
        out = out.replace(smart, dumb)
    return out


def compute_new_stem(
    file_stem: str, parent_name: str, grandparent_name: str
) -> tuple[str, list[str]]:
    """Run all fixes on a stem. Returns (new_stem, applied_fixes_list)."""
    fixes = []
    s = file_stem

    # Fix K: smart quotes / dashes / replacement char (run first — informs later fixes)
    s2 = normalise_smart_punct(s)
    if s2 != s:
        fixes.append("normalise_smart_punct")
        s = s2

    # Fix C: stray brackets
    s2 = strip_stray_brackets(s)
    if s2 != s:
        fixes.append("strip_stray_brackets")
        s = s2

    # Fix B: 1xNN → S01ENN
    s2 = normalise_1x_marker(s)
    if s2 != s:
        fixes.append("normalise_1xNN")
        s = s2

    # Fix A: prepend show from grandparent for bare-SxxExx
    s2 = prepend_show_name(s, grandparent_name)
    if s2 and s2 != s:
        fixes.append("prepend_show")
        s = s2

    # Fixes D/E/F/G/I: punctuation/diacritic restore from parent
    # Movies use parent folder, episodes use grandparent
    if SXXEXX_RE.search(s):
        source = grandparent_name
    else:
        source = parent_name
    s2 = restore_punctuation_from_parent(s, source)
    if s2 and s2 != s:
        fixes.append("restore_from_parent")
        s = s2

    # Fix H: year mismatch (movies only — parent has year)
    s2 = adopt_parent_year(s, parent_name)
    if s2 and s2 != s:
        fixes.append("adopt_parent_year")
        s = s2

    # Fix J: cleanup
    s2 = clean_extra_whitespace(s)
    if s2 != s:
        fixes.append("cleanup_ws")
        s = s2

    return s, fixes


def plan_renames(root: Path) -> list[dict]:
    import os

    plan = []
    target_map: dict[Path, Path] = {}

    for dirpath, _, filenames in os.walk(root):
        dp = Path(dirpath)
        parent_name = dp.name
        grandparent_name = dp.parent.name
        for fn in filenames:
            old_path = dp / fn
            ext = old_path.suffix.lower()
            if ext not in VIDEO_EXTS:
                continue
            stem = old_path.stem
            new_stem, fixes = compute_new_stem(stem, parent_name, grandparent_name)
            if not fixes or new_stem == stem:
                continue
            new_path = old_path.with_name(f"{new_stem}{ext}")
            if new_path == old_path:
                continue
            if new_path.exists() and new_path != old_path:
                plan.append({
                    "old_path": old_path,
                    "new_path": new_path,
                    "fixes": fixes,
                    "status": "collision_exists",
                })
                continue
            if new_path in target_map and target_map[new_path] != old_path:
                plan.append({
                    "old_path": old_path,
                    "new_path": new_path,
                    "fixes": fixes,
                    "status": "collision_batch",
                })
                continue
            target_map[new_path] = old_path
            plan.append({
                "old_path": old_path,
                "new_path": new_path,
                "fixes": fixes,
                "status": "rename",
            })
    return plan


def _find_sidecars(video_path: Path) -> list[Path]:
    """Find .srt/.nfo/etc that share the video's stem (with optional .lang[.hi] suffixes).

    E.g. for `Show S01E01.mkv` returns matches like:
      Show S01E01.srt, Show S01E01.en.srt, Show S01E01.en.hi.srt, Show S01E01.nfo
    """
    stem = video_path.stem
    parent = video_path.parent
    results = []
    for f in parent.iterdir():
        if not f.is_file():
            continue
        if f.suffix.lower() not in SIDECAR_EXTS:
            continue
        # Match the video stem exactly OR with .<something> appended before extension
        # E.g. "Show S01E01.mkv" + "Show S01E01.en.srt" → sidecar stem "Show S01E01.en"
        sidecar_stem = f.stem  # "Show S01E01.en"
        if sidecar_stem == stem or sidecar_stem.startswith(stem + "."):
            results.append(f)
    return results


def execute_renames(plan: list[dict], dry_run: bool) -> dict:
    renames = [e for e in plan if e["status"] == "rename"]
    collisions = [e for e in plan if e["status"].startswith("collision")]

    fix_counts: dict[str, int] = {}
    for e in renames:
        for f in e["fixes"]:
            fix_counts[f] = fix_counts.get(f, 0) + 1

    print(f"\nPlanned {len(renames)} renames, {len(collisions)} collisions.")
    print(f"Fix breakdown: {fix_counts}")
    print()

    done = 0
    sidecars_done = 0
    errors: list[str] = []
    for e in renames:
        old_rel = e["old_path"].name
        new_rel = e["new_path"].name
        fixes = ",".join(e["fixes"])
        old_stem = e["old_path"].stem
        new_stem = e["new_path"].stem

        # Find sidecars BEFORE renaming the video (while we can still find them)
        sidecars = _find_sidecars(e["old_path"])

        if dry_run:
            print(f"  [{fixes}]")
            print(f"    {old_rel}")
            print(f"    -> {new_rel}")
            for sc in sidecars:
                new_sc_name = new_stem + sc.name[len(old_stem):]
                print(f"      sidecar: {sc.name} -> {new_sc_name}")
        else:
            try:
                e["old_path"].rename(e["new_path"])
                done += 1
                # Rename matching sidecars
                for sc in sidecars:
                    new_sc = sc.with_name(new_stem + sc.name[len(old_stem):])
                    try:
                        if not new_sc.exists():
                            sc.rename(new_sc)
                            sidecars_done += 1
                    except OSError as se:
                        errors.append(f"sidecar {sc.name}: {se}")
                if done <= 50 or done % 25 == 0:
                    print(f"  [{fixes}] {old_rel} -> {new_rel}")
            except OSError as err:
                errors.append(f"{old_rel}: {err}")

    if collisions:
        print(f"\nSkipped {len(collisions)} collisions:")
        for e in collisions[:20]:
            print(f"  {e['old_path'].name} ({e['status']})")
        if len(collisions) > 20:
            print(f"  ... and {len(collisions) - 20} more")

    if errors:
        print(f"\n{len(errors)} errors:")
        for e in errors[:10]:
            print(f"  ! {e}")

    if not dry_run and sidecars_done:
        print(f"  + {sidecars_done} sidecars renamed alongside")
    return {"planned": len(renames), "done": done, "sidecars": sidecars_done,
            "collisions": len(collisions), "errors": len(errors), "fixes": fix_counts}


def _trigger_plex_scan() -> None:
    if not PLEX_URL or not PLEX_TOKEN:
        print("\n  Plex scan skipped (no PLEX_URL/PLEX_TOKEN)")
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
        print(f"\n  Triggered Plex scan ({len(sections)} sections)")
    except (URLError, OSError) as e:
        print(f"\n  Plex scan failed: {e}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--movies", action="store_true")
    parser.add_argument("--series", action="store_true")
    parser.add_argument("--root", type=str, default=None)
    args = parser.parse_args()
    if not (args.movies or args.series or args.root):
        args.movies = True
        args.series = True

    dry_run = not args.execute
    if dry_run:
        print("DRY RUN (pass --execute to rename)\n")

    totals = {"planned": 0, "done": 0, "sidecars": 0, "collisions": 0, "errors": 0, "fixes": {}}

    def _merge(d: dict) -> None:
        totals["planned"] += d["planned"]
        totals["done"] += d["done"]
        totals["sidecars"] += d.get("sidecars", 0)
        totals["collisions"] += d["collisions"]
        totals["errors"] += d["errors"]
        for k, v in d["fixes"].items():
            totals["fixes"][k] = totals["fixes"].get(k, 0) + v

    if args.root:
        root = Path(args.root)
        print(f"Scanning {root}")
        _merge(execute_renames(plan_renames(root), dry_run))
    else:
        if args.series:
            print(f"Scanning series: {NAS_SERIES}")
            _merge(execute_renames(plan_renames(NAS_SERIES), dry_run))
        if args.movies:
            print(f"\nScanning movies: {NAS_MOVIES}")
            _merge(execute_renames(plan_renames(NAS_MOVIES), dry_run))

    print(f"\n=== TOTAL: {totals['done']}/{totals['planned']} renamed "
          f"(+{totals['sidecars']} sidecars), "
          f"{totals['collisions']} collisions, {totals['errors']} errors ===")
    print(f"Fix breakdown: {totals['fixes']}")

    if not dry_run and totals["done"]:
        _trigger_plex_scan()


if __name__ == "__main__":
    main()
