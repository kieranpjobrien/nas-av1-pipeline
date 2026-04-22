"""Find same-episode duplicates and auto-delete obvious foreign-tag / junk siblings.

Safe deletes (only when a clean sibling exists):
  - Files with ITA / ENG / MULTI / DUAL / BOLUM / BÖLÜM / GERMAN / FRENCH / iTALiAN
    language markers in the filename
  - Files with retained scene-release residue (720N, 710N, 710NH1, codec concat)
  - Files with double-tag "[TAo E]" style copy indicators

Everything else is REPORTED only — manual review.
"""
import argparse
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

from paths import NAS_SERIES

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".flv", ".mov", ".ts", ".webm"}
SXXEXX = re.compile(r"S(\d{1,4})E(\d{1,2})", re.IGNORECASE)

# Patterns that mark a file as the dirty / foreign variant.
# IMPORTANT: avoid generic words like "Italian", "German" on their own — those
# appear in real episode titles (The White Lotus S02E02 "Italian Dream"). Only
# flag clear scene-tag patterns.
# Use stem (no .mkv) so $ works naturally at end
_DIRTY_MARKERS = re.compile(
    r"(?:^|[\s.])(?:ITA[\s.]+ENG|ENG[\s.]+ITA)(?:[\s.]|$)"   # ITA ENG / ENG ITA pair
    r"|\bMULTI\b|\bDUAL\b|\bDubbedGerman\b|\bAC3D\b"         # scene tags
    r"|\bBOLUM\b|\bB[ÖO]L[ÜU]M\b"                            # Turkish episode marker
    r"|\s\d{3}NH?\d*(?:\s|$)"                                # 710N, 710NH1 codec junk
    r"|\[TAo\s*E\]"                                          # [TAo E] copy marker
    r"|\(\s*Kappa\s*\)",                                     # ( Kappa) weird copy
    # NOTE: previously also matched `\bEpisode\s?\d+\b` ("Episode 6" placeholder)
    # but that eats real anime/doc titles like "Episode 50". Dropped to avoid
    # deleting genuinely-named episodes. Files with literal "Episode N" as the
    # full title need human review.
    re.IGNORECASE,
)


def scan(root: Path) -> dict:
    ep_map: dict[tuple[str, str, str], list[Path]] = defaultdict(list)
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            p = Path(dirpath) / fn
            if p.suffix.lower() not in VIDEO_EXTS:
                continue
            m = SXXEXX.search(p.stem)
            if not m:
                continue
            show = Path(dirpath).parent.name  # grandparent = show folder
            if not show or show.lower() == "series":
                show = Path(dirpath).name  # fallback
            key = (show.lower(), m.group(1).upper(), m.group(2).upper())
            ep_map[key].append(p)
    return {k: v for k, v in ep_map.items() if len(v) > 1}


def classify(paths: list[Path]) -> dict:
    """Return {'delete': [...], 'keep': Path, 'review': [...]}.

    Match against p.stem (no extension) so end-of-name markers ($) work.
    """
    dirty = [p for p in paths if _DIRTY_MARKERS.search(p.stem)]
    clean = [p for p in paths if not _DIRTY_MARKERS.search(p.stem)]

    # If exactly one clean + one-or-more dirty, safe auto-delete of dirty
    if clean and dirty and len(clean) == 1:
        return {"delete": dirty, "keep": clean[0], "review": []}
    # Multiple clean copies, OR all dirty — manual review
    return {"delete": [], "keep": None, "review": paths}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()
    dry_run = not args.execute

    dups = scan(Path(str(NAS_SERIES)))
    if not dups:
        print("No duplicate episodes found.")
        return

    auto_delete_count = 0
    review_count = 0
    freed_bytes = 0
    for key, paths in sorted(dups.items()):
        c = classify(paths)
        show, s, ep = key
        if c["delete"]:
            auto_delete_count += len(c["delete"])
            print(f"\n[{show} S{s}E{ep}]")
            print(f"  KEEP:   {c['keep'].name}")
            for p in c["delete"]:
                sz = p.stat().st_size if p.exists() else 0
                freed_bytes += sz
                if dry_run:
                    print(f"  DELETE: {p.name}  ({sz/1024/1024:.1f} MB)")
                else:
                    try:
                        p.unlink()
                        print(f"  DELETED: {p.name}  ({sz/1024/1024:.1f} MB)")
                    except OSError as e:
                        print(f"  ERR delete {p}: {e}")
        else:
            review_count += len(c["review"])

    print(f"\n=== auto-delete: {auto_delete_count} files "
          f"(~{freed_bytes/1024**3:.2f} GB), review: {review_count} files ===")

    if not dry_run:
        print("Executed deletes. Review pile unchanged.")
    else:
        print("DRY RUN — pass --execute to delete.")


if __name__ == "__main__":
    main()
