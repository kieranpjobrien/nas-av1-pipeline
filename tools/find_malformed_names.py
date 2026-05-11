"""Scan NAS for filenames where the title was mangled into the file
extension boundary. Two patterns:

  1. Whole last word IS a video extension
       Star Wars - The Bad Batch S02E04 MP4.mkv
       (the "MP4" was a leftover from a .mp4 source whose extension
       got promoted into the title when someone renamed to .mkv)

  2. Stem ends in a video-ext acronym preceded by a letter (lost dot)
       Star Wars - The Bad Batch S03E10 Identity Crisismkv.mkv
       (originally "Identity Crisis.mkv" — the dot before .mkv got
       eaten by some upstream cleaner, the .mkv extension was then
       re-appended, leaving "Crisismkv.mkv")

Run: ``uv run python -m tools.find_malformed_names``

Detection is purposefully strict — the first regex I wrote here
matched 64 legitimate titles ending in "ts" (Portraits, Visits,
Robots, Hurts, etc.) before I tightened to only check unambiguous
extension acronyms (mkv/mp4/avi/m4v/webm/wmv/flv) and exclude the
ambiguous ones (mov/ts/mpg/mpeg). The current logic finds 2 hits
on the production library — the genuine cases — and 0 false
positives.

This is detect-only by design. The 2 cases I found in production
needed a TMDb lookup (S02E04 MP4 → "Faster") or a hand decision
about where the dot should go (Crisismkv → Crisis); both judgement
calls. Manual rename via ``tools/fix_malformed_filenames.py``-style
one-offs is the right ergonomic.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from paths import NAS_MOVIES, NAS_SERIES  # noqa: E402

# Unambiguous video extensions. Excluded: mov / ts / mpg / mpeg / mpe — those
# also occur as common English letter sequences (e.g. "Movements", "Robots",
# "Tempo"). False-positive rate on those is too high to be useful.
UNAMBIGUOUS_EXTS = ("mkv", "mp4", "avi", "m4v", "webm", "wmv", "flv")


def detect(stem: str) -> str | None:
    """Return a reason string if ``stem`` looks malformed, else None.

    ``stem`` is the filename without the trailing ``.<ext>``.
    """
    # Pattern 1: last word IS a video extension
    last = stem.rsplit(" ", 1)[-1] if " " in stem else stem
    if last.lower() in UNAMBIGUOUS_EXTS:
        return f"last word {last!r} is a video extension"

    # Pattern 2: stem ends in a video-ext acronym preceded by a letter
    # (so "Crisismkv" matches, but "Crisis.mkv" doesn't and "S01E01.mkv"
    # — which has a dot before mkv — also doesn't because we're looking
    # at the stem, which already had its trailing extension stripped).
    for ext in UNAMBIGUOUS_EXTS:
        if stem.lower().endswith(ext) and len(stem) > len(ext):
            preceding = stem[-len(ext) - 1]
            if preceding.isalpha():
                tail = stem[-len(ext) - 3 :]
                return f"stem ends in '...{tail}' (lost dot before .mkv extension)"
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        action="append",
        default=None,
        help="Override scan roots (default: NAS_MOVIES + NAS_SERIES).",
    )
    args = parser.parse_args()

    roots = args.root or [str(NAS_MOVIES), str(NAS_SERIES)]
    print(f"Scanning {len(roots)} root(s) for malformed filenames…")
    for r in roots:
        print(f"  • {r}")
    print()

    hits: list[tuple[str, str, str]] = []  # (full_path, filename, reason)
    for root in roots:
        for dirpath, _dirnames, filenames in os.walk(root):
            for fn in filenames:
                base, ext = os.path.splitext(fn)
                if ext.lower() != ".mkv":
                    continue
                reason = detect(base)
                if reason:
                    hits.append((os.path.join(dirpath, fn), fn, reason))

    if not hits:
        print("No malformed filenames found. Library clean.")
        return 0

    print(f"Found {len(hits)} malformed filename(s):")
    print()
    for full, fn, reason in hits:
        print(f"  {fn}")
        print(f"    reason: {reason}")
        print(f"    path:   {full}")
        print()

    print("To fix: rename manually on the NAS, then update the state DB row")
    print("(or run a one-off rename script — see tools/fix_malformed_filenames.py")
    print("for a worked example).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
