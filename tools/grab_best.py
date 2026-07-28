"""Pick and grab the best available release for a show or movie, by hand.

Rules-based gating keeps leaking: Sonarr ranks by quality TIER, so a 1.2 GB
and a 3.0 GB WEBDL-1080p look identical to it and it will never swap one for
the other. No amount of floor-tuning fixes that. This just looks at what is
actually on offer and takes the best one.

Selection, in order:
  * drop foreign-dub releases (GERMAN/MULTI/NORDIC/...) - bigger, not better
  * drop obvious junk (CAM/TS/SCREENER)
  * prefer per-episode releases over season packs when grabbing an episode
  * among what is left, take the largest

Run:  uv run python -m tools.grab_best --series "Colin from Accounts"
      uv run python -m tools.grab_best --series "Colin from Accounts" --execute
      uv run python -m tools.grab_best --movie "Heat" --execute
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SONARR = ("http://192.168.4.43:27483", "80d52d39113c45be9f3b94c3c2cfbdd1")
RADARR = ("http://192.168.4.43:27484", "2aeb7de56ea44035a438447a5911f77a")

FOREIGN = re.compile(
    r"\b(GERMAN|FRENCH|ITALIAN|SPANISH|NORDIC|POLISH|MULTI|VOSTFR|TRUEFRENCH|DUAL|HINDI|KOREAN|LATINO)\b", re.I
)
JUNK = re.compile(r"\b(CAM|TS|TELESYNC|SCREENER|R5|HDCAM|WORKPRINT)\b", re.I)
_SXXEXX = re.compile(r"[Ss](\d{1,2})[\s._-]*[Ee](\d{1,3})")


def arr(base_key, path, method="GET", body=None):
    base, key = base_key
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method)
    req.add_header("X-Api-Key", key)
    if data:
        req.add_header("Content-Type", "application/json")
    raw = urllib.request.urlopen(req, timeout=240).read()
    return json.loads(raw) if raw else {}


def usable(rel) -> bool:
    t = rel.get("title") or ""
    if FOREIGN.search(t) or JUNK.search(t):
        return False
    return (rel.get("size") or 0) > 0


_UPSCALE = re.compile(r"(?:UPSCALE|UPSCALED|AI.?UPSCALE)", re.I)

# HARD CEILING on a single TV episode, 2026-07-28.
#
# "Take the largest available" with no upper bound queued a 124 GB Game of
# Thrones episode and House episodes to match - almost certainly season packs
# or absurd remuxes mis-parsed as single episodes. 3,907 jobs / 26 TB were
# queued before the operator caught it.
#
# Operator's rule: no TV episode over 20 GB, and that only for the rarest
# exceptions; 10 GB is the working maximum.
EPISODE_MAX_GB = 10.0
EPISODE_ABSOLUTE_MAX_GB = 20.0
# Movies legitimately run larger than episodes (a UHD remux feature is 60-90 GB),
# but not unboundedly.
MOVIE_MAX_GB = 80.0


def within_size_cap(rel, *, is_episode: bool) -> bool:
    """Reject releases too large to plausibly be what we asked for.

    A single episode above EPISODE_ABSOLUTE_MAX_GB is almost always a season
    pack or a mis-parse, not a better copy of one episode.
    """
    gb = (rel.get("size") or 0) / 1e9
    cap = EPISODE_ABSOLUTE_MAX_GB if is_episode else MOVIE_MAX_GB
    return gb <= cap


_IS_2160 = re.compile(r"(?:2160p|UHD|4K)", re.I)


def is_2160(rel) -> bool:
    """True for a genuine 2160p/UHD release.

    Module-level compiled pattern on purpose: an earlier in-place patch turned
    the inline word-boundary escapes into literal backspace bytes, so this
    matched NOTHING and the 4K preference silently never fired once.
    """
    return bool(_IS_2160.search(rel.get("title") or ""))


def pick(releases, want_single_episode=True, prefer_4k=True):
    """Best usable release.

    Operator's standard (2026-07-28): modern series deserve 4K. So prefer a
    genuine 2160p release when one exists, and only fall back to largest-
    overall otherwise. Without this a 1080p remux can outweigh a real 2160p
    release on raw size alone and win, which is the wrong call for anything
    made in the UHD era.

    AI upscales are excluded from the 4K preference - they are 2160p in name
    only and carry no more real detail than the 1080p they came from.
    """
    usable_rels = [r for r in releases if usable(r) and within_size_cap(r, is_episode=want_single_episode)]
    if not usable_rels:
        return None
    if want_single_episode:
        singles = [r for r in usable_rels if _SXXEXX.search(r.get("title") or "")]
        if singles:
            usable_rels = singles
        # Prefer to stay under the working maximum; only exceed it when nothing
        # sensible exists below it.
        modest = [r for r in usable_rels if (r.get("size") or 0) / 1e9 <= EPISODE_MAX_GB]
        if modest:
            usable_rels = modest
    if prefer_4k:
        real_4k = [r for r in usable_rels if is_2160(r) and not _UPSCALE.search(r.get("title") or "")]
        if real_4k:
            return max(real_4k, key=lambda r: r.get("size") or 0)
    return max(usable_rels, key=lambda r: r.get("size") or 0)


def do_series(name: str, execute: bool, limit: int) -> None:
    series = arr(SONARR, "/api/v3/series")
    hits = [s for s in series if name.lower() in s["title"].lower()]
    if not hits:
        print(f"no series matching {name!r}")
        return
    s = hits[0]
    print(f"{s['title']} (id={s['id']}, runtime={s.get('runtime')}min)")
    eps = arr(SONARR, f"/api/v3/episode?seriesId={s['id']}")
    eps = [e for e in eps if e.get("monitored") and e.get("seasonNumber", 0) > 0]
    eps.sort(key=lambda e: (e.get("seasonNumber", 0), e.get("episodeNumber", 0)))
    if limit:
        eps = eps[:limit]

    grabbed = skipped = 0
    for e in eps:
        cur_gb = 0.0
        if e.get("hasFile"):
            try:
                ef = arr(SONARR, f"/api/v3/episodefile/{e['episodeFileId']}")
                cur_gb = (ef.get("size") or 0) / 1e9
            except Exception:  # noqa: BLE001
                pass
        try:
            rel = arr(SONARR, f"/api/v3/release?episodeId={e['id']}")
        except Exception as exc:  # noqa: BLE001
            print(f"  S{e['seasonNumber']:02d}E{e['episodeNumber']:02d} search failed: {exc}")
            continue
        best = pick(rel)
        if not best:
            print(f"  S{e['seasonNumber']:02d}E{e['episodeNumber']:02d} no usable release")
            skipped += 1
            continue
        new_gb = (best.get("size") or 0) / 1e9
        tag = ""
        if cur_gb and new_gb <= cur_gb * 1.2:
            tag = "  (not meaningfully better - skipping)"
            skipped += 1
        print(
            f"  S{e['seasonNumber']:02d}E{e['episodeNumber']:02d} have {cur_gb:>5.2f}GB -> best {new_gb:>5.2f}GB  {best.get('title', '')[:44]}{tag}"
        )
        if execute and not tag:
            try:
                arr(SONARR, "/api/v3/release", "POST", {"guid": best["guid"], "indexerId": best["indexerId"]})
                grabbed += 1
            except Exception as exc:  # noqa: BLE001
                print(f"      grab failed: {exc}")
    print(f"\ngrabbed={grabbed} skipped={skipped}")


def do_movie(name: str, execute: bool) -> None:
    movies = arr(RADARR, "/api/v3/movie")
    hits = [m for m in movies if name.lower() in m["title"].lower()]
    if not hits:
        print(f"no movie matching {name!r}")
        return
    for m in hits[:1]:
        cur = ((m.get("movieFile") or {}).get("size") or 0) / 1e9
        rel = arr(RADARR, f"/api/v3/release?movieId={m['id']}")
        best = pick(rel, want_single_episode=False)
        if not best:
            print(f"  {m['title']}: no usable release")
            return
        new = (best.get("size") or 0) / 1e9
        print(f"  {m['title']} ({m.get('year')}) have {cur:.2f}GB -> best {new:.2f}GB  {best.get('title', '')[:50]}")
        if execute and new > cur * 1.2:
            arr(RADARR, "/api/v3/release", "POST", {"guid": best["guid"], "indexerId": best["indexerId"]})
            print("      grabbed")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--series")
    ap.add_argument("--movie")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    if args.series:
        do_series(args.series, args.execute, args.limit)
    elif args.movie:
        do_movie(args.movie, args.execute)
    else:
        ap.error("need --series or --movie")


if __name__ == "__main__":
    main()
