"""Re-resolve stale paths in pipeline_state.db where the row references
``Show - SXXEXX - Title.mkv`` (dashed format) but the file on disk is
``Show SXXEXX Title.mkv`` (cleaned format produced by
``bulk_rename_clean``).

Same idea as ``tools.resolve_priority_paths`` but for the state DB.
Walks the error rows (default ``status='error' AND error LIKE
'%source file not found%'``), tries the cleaned-name version of the
basename, and rewrites the state row's filepath to the live name if
found.

The bulk_rename_clean tool already updates state DB paths when it
renames files on disk (commit 14c7a29 + extension). This resolver
covers the cases where the state row was added AFTER bulk_rename ran
— typically via:

  * the priority-API auto-seed path (user paste-imports a list of
    dashed paths into priority.json),
  * a leftover row from before the rename happened (categoriser
    hadn't seen the file for that path before the rename), or
  * an mtime drift where the auto-reset re-created the row pointing
    at the now-stale dashed path.

Dry-run by default; ``--apply`` writes. Strict: only rewrites when
exactly ONE match for the cleaned basename exists in the parent
directory, to avoid mis-routing on ambiguous matches.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from paths import PIPELINE_STATE_DB

# Match `Show (Year) - SXXEXX - Title.ext` or `Show - SXXEXX - Title.ext`
_DASHED_SERIES = re.compile(
    r"^(?P<show>.+?)(?: \(\d{4}\))? - (?P<sxxeyy>S\d+E\d+(?:-E\d+)?) - (?P<title>.+?)(?P<ext>\.\w+)$",
    re.I,
)


def _try_cleaned_basename(basename: str) -> str | None:
    """If basename matches the dashed-series pattern, return the cleaned
    form (`Show SXXEXX Title.ext`). Else None."""
    m = _DASHED_SERIES.match(basename)
    if not m:
        return None
    return f"{m['show']} {m['sxxeyy']} {m['title']}{m['ext']}"


def _resolve_one(filepath: str) -> str | None:
    """Try heuristics to find the live on-disk filepath that corresponds
    to a stale dashed filepath. Returns the resolved path or None."""
    if os.path.exists(filepath):
        return filepath  # already correct, no resolution needed
    parent = os.path.dirname(filepath)
    if not os.path.isdir(parent):
        return None
    bn = os.path.basename(filepath)
    cleaned = _try_cleaned_basename(bn)
    if not cleaned:
        return None
    candidate = os.path.join(parent, cleaned)
    if os.path.exists(candidate):
        return candidate
    # Also try a directory-listing match (case-insensitive) — handles
    # subtle case differences in titles like "We Got Us a Pippi Virgin".
    cleaned_lower = cleaned.lower()
    try:
        for entry in os.listdir(parent):
            if entry.lower() == cleaned_lower:
                return os.path.join(parent, entry)
    except OSError:
        pass
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Actually rewrite state DB rows (default: dry-run)")
    parser.add_argument("--status", default="error",
                        help="Status to operate on (default: error)")
    parser.add_argument("--error-like", default="source file not found",
                        help="Filter to rows whose error column matches this substring "
                             "(default: 'source file not found')")
    args = parser.parse_args()

    con = sqlite3.connect(str(PIPELINE_STATE_DB))
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT filepath, status, error, reason FROM pipeline_files "
        "WHERE status = ? AND COALESCE(error, '') LIKE ?",
        (args.status, f"%{args.error_like}%"),
    ).fetchall()

    print(f"Inspecting {len(rows)} rows with status={args.status!r} matching "
          f"error~={args.error_like!r}\n")

    resolved: list[tuple[str, str]] = []
    no_match: list[str] = []
    already_ok: list[str] = []

    for r in rows:
        fp = r["filepath"]
        live = _resolve_one(fp)
        if live is None:
            no_match.append(fp)
        elif live == fp:
            already_ok.append(fp)
        else:
            resolved.append((fp, live))

    print(f"  RESOLVED (rename ghosts → live file): {len(resolved)}")
    for old, new in resolved[:10]:
        print(f"    OLD: {os.path.basename(old)}")
        print(f"    NEW: {os.path.basename(new)}")
    if len(resolved) > 10:
        print(f"    ... ({len(resolved)-10} more)")
    print(f"  Already on disk at the stored path:  {len(already_ok)}")
    print(f"  No match found (genuinely missing):  {len(no_match)}")
    for fp in no_match[:5]:
        print(f"    {fp}")

    if not args.apply:
        print("\n(dry-run — pass --apply to rewrite state rows)")
        return 0

    if not resolved:
        print("\nNothing to apply.")
        return 0

    print("\nApplying...")
    fixed = 0
    for old, new in resolved:
        # Update filepath; also reset status to pending so the row gets
        # re-picked-up by the queue builder. Clear the stale error
        # message so it doesn't linger as misleading audit history.
        # Atomic UPDATE — INSERT OR REPLACE would lose extras if the new
        # path already had a row.
        try:
            con.execute(
                "UPDATE pipeline_files SET filepath = ?, status = 'pending', "
                "stage = NULL, error = NULL, "
                "reason = 'resolved by resolve_state_paths: cleaned-name match' "
                "WHERE filepath = ?",
                (new, old),
            )
            fixed += 1
        except sqlite3.IntegrityError:
            # The new path already has a row (older work for the cleaned
            # name) — drop the stale dashed row to keep state clean.
            con.execute("DELETE FROM pipeline_files WHERE filepath = ?", (old,))
            print(f"  cleaned-name row already existed, dropped stale dashed row: "
                  f"{os.path.basename(old)}")

    con.commit()
    con.close()
    print(f"\nFixed {fixed} rows. The next queue refresh will pick them up.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
