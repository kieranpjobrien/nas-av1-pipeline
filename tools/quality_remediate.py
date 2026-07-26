"""Bin every file the sweep judged garbage, and make the *arrs re-source it.

Deletes through the Sonarr/Radarr API rather than off the filesystem so the
media server's own database stays consistent, then triggers a search. With
the Quality Definition minimums saved 2026-07-25 in force, the replacement
grab cannot be junk again.

Also clears the pipeline state row so the file is re-categorised from
scratch when the replacement lands.

Run:  uv run python -m tools.quality_remediate --dry-run
      uv run python -m tools.quality_remediate --execute
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import sqlite3
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from paths import STAGING_DIR  # noqa: E402

SONARR = ("http://192.168.4.43:27483", "80d52d39113c45be9f3b94c3c2cfbdd1")
RADARR = ("http://192.168.4.43:27484", "2aeb7de56ea44035a438447a5911f77a")


def arr(base_key, path, method="GET", body=None, **params):
    base, key = base_key
    url = f"{base}{path}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-Api-Key", key)
    if data:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=120) as r:
        raw = r.read()
        return json.loads(raw) if raw else {}


import re  # noqa: E402

_SXXEXX = re.compile(r"[Ss](\d{1,2})[\s._-]*[Ee](\d{1,3})")


def norm(p: str) -> str:
    return os.path.basename(p.replace("\\", "/")).lower()


def _slug(s: str) -> str:
    """Loose title key — the pipeline renames files, so exact basenames
    do not survive. Strip everything but letters and digits."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _ep_key(path: str) -> tuple[str, int, int] | None:
    """(show-slug, season, episode) from a full path.

    Uses the directory ABOVE 'Season N' as the show name, which both the
    NAS layout and the *arr layout agree on even when the filename does not.
    """
    parts = path.replace("\\", "/").split("/")
    m = _SXXEXX.search(parts[-1])
    if not m:
        return None
    show = ""
    for i, seg in enumerate(parts):
        if re.match(r"(?i)^season[\s._-]", seg) and i > 0:
            show = parts[i - 1]
            break
    if not show and len(parts) > 2:
        show = parts[-3]
    return (_slug(show), int(m.group(1)), int(m.group(2)))


def build_file_index():
    """Two indexes: exact basename, and (show, season, episode)."""
    by_name, by_ep = {}, {}
    for s in arr(SONARR, "/api/v3/series"):
        s_slug = _slug(s.get("title", ""))
        try:
            for f in arr(SONARR, "/api/v3/episodefile", seriesId=s["id"]):
                p = f.get("path") or f.get("relativePath") or ""
                if not p:
                    continue
                rec = ("series", f["id"], s["id"])
                by_name[norm(p)] = rec
                m = _SXXEXX.search(os.path.basename(p))
                if m:
                    by_ep[(s_slug, int(m.group(1)), int(m.group(2)))] = rec
        except (urllib.error.URLError, KeyError):
            continue
    for m_ in arr(RADARR, "/api/v3/movie"):
        mf = m_.get("movieFile")
        if mf:
            p = mf.get("path") or mf.get("relativePath") or ""
            if p:
                by_name[norm(p)] = ("movie", mf["id"], m_["id"])
    return by_name, by_ep


def lookup(by_name, by_ep, filepath):
    hit = by_name.get(norm(filepath))
    if hit:
        return hit
    key = _ep_key(filepath)
    return by_ep.get(key) if key else None


def clear_state_rows(paths: list[str]) -> int:
    db = os.path.join(str(STAGING_DIR), "pipeline_state.db")
    con = sqlite3.connect(db)
    n = 0
    for p in paths:
        n += con.execute("DELETE FROM pipeline_files WHERE filepath=?", (p,)).rowcount
    con.commit()
    con.close()
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--garbage", required=True, help="garbage.json from the sweep")
    ap.add_argument("--execute", action="store_true", help="actually delete + search")
    ap.add_argument("--limit", type=int, default=0, help="cap for a cautious first pass")
    args = ap.parse_args()

    garbage = json.load(open(args.garbage, encoding="utf-8"))
    if args.limit:
        garbage = garbage[: args.limit]
    print(f"garbage files to remediate: {len(garbage)}")

    by_name, by_ep = build_file_index()
    print(f"*arr file index: {len(by_name)} by name, {len(by_ep)} by episode")

    matched, unmatched = [], []
    for g in garbage:
        hit = lookup(by_name, by_ep, g["filepath"])
        (matched if hit else unmatched).append((g, hit))
    print(f"  matched to an *arr file: {len(matched)}")
    print(f"  NOT matched (skipped)  : {len(unmatched)}")

    if not args.execute:
        print("\n(dry run — pass --execute to act)")
        return

    deleted = failed = 0
    series_ids, movie_ids, done_paths = set(), set(), []
    for g, hit in matched:
        kind, file_id, parent_id = hit
        ep = "episodefile" if kind == "series" else "moviefile"
        api = SONARR if kind == "series" else RADARR
        try:
            arr(api, f"/api/v3/{ep}/{file_id}", method="DELETE")
            deleted += 1
            done_paths.append(g["filepath"])
            (series_ids if kind == "series" else movie_ids).add(parent_id)
        except (urllib.error.URLError, urllib.error.HTTPError):
            failed += 1
    print(f"\ndeleted={deleted} failed={failed}")

    cleared = clear_state_rows(done_paths)
    print(f"pipeline state rows cleared: {cleared}")

    for sid in sorted(series_ids):
        try:
            arr(SONARR, "/api/v3/command", method="POST", body={"name": "SeriesSearch", "seriesId": sid})
        except (urllib.error.URLError, urllib.error.HTTPError):
            pass
    if movie_ids:
        try:
            arr(RADARR, "/api/v3/command", method="POST", body={"name": "MoviesSearch", "movieIds": sorted(movie_ids)})
        except (urllib.error.URLError, urllib.error.HTTPError):
            pass
    print(f"search triggered: {len(series_ids)} series, {len(movie_ids)} movies")

    if unmatched:
        by = collections.Counter(u[0]["title"][:40] for u in unmatched)
        print("\nunmatched (left on disk):")
        for t, n in by.most_common(10):
            print(f"  {n:>4}  {t}")


if __name__ == "__main__":
    main()
