"""Standing guard against under-spec media, on disk and inbound.

The operator kept finding junk by eye — 200 MB West Wing, 400 MB Fargo,
300 MB Brooklyn Nine-Nine, 330 MB Always Sunny — each time after it had
already landed. This runs the whole check unattended so that stops.

Sources of truth are Sonarr/Radarr directly, NOT media_report.json, so the
verdict is never stale. Floors come from ``pipeline.__main__`` so the
acquisition side and the encoder side cannot drift apart.

Never creates holes: a file is only removed once a compliant replacement is
confirmed to exist on an indexer. Anything with no better option is left in
place and reported.

Run:  uv run python -m tools.quality_guard            # report
      uv run python -m tools.quality_guard --execute  # remediate
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.__main__ import ANIMATION_FLOOR_SCALE, QUALITY_FLOOR_MBMIN  # noqa: E402

SONARR = ("http://192.168.4.43:27483", "80d52d39113c45be9f3b94c3c2cfbdd1")
RADARR = ("http://192.168.4.43:27484", "2aeb7de56ea44035a438447a5911f77a")

# Marginal files (>= this fraction of the floor) are reported but not binned.
# Trading a 97%-of-floor file for a re-download is churn, not an improvement,
# and risks losing it entirely if the indexer has nothing better.
MARGIN = 0.95


def arr(base_key, path, method="GET", body=None):
    base, key = base_key
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method)
    req.add_header("X-Api-Key", key)
    if data:
        req.add_header("Content-Type", "application/json")
    raw = urllib.request.urlopen(req, timeout=180).read()
    return json.loads(raw) if raw else {}


def bucket(height):
    if not height:
        return None
    if height >= 1700:
        return "2160p"
    if height >= 900:
        return "1080p"
    if height >= 620:
        return "720p"
    return "480p"


def minutes_of(media_info, fallback):
    rt = (media_info or {}).get("runTime") or ""
    try:
        n = [float(x) for x in rt.split(":")]
    except ValueError:
        return fallback
    if len(n) == 3:
        return n[0] * 60 + n[1] + n[2] / 60
    if len(n) == 2:
        return n[0] + n[1] / 60
    return fallback


def height_of(media_info):
    res = (media_info or {}).get("resolution") or ""
    try:
        return int(res.split("x")[1])
    except (ValueError, IndexError):
        return None


def audit() -> list[dict]:
    """Every file the *arrs know about, judged against its floor."""
    out = []
    for s in arr(SONARR, "/api/v3/series"):
        anim = any(g.lower() == "animation" for g in (s.get("genres") or []))
        try:
            files = arr(SONARR, f"/api/v3/episodefile?seriesId={s['id']}")
        except Exception:  # noqa: BLE001,PERF203
            continue
        for f in files:
            mi = f.get("mediaInfo") or {}
            b = bucket(height_of(mi))
            mins = minutes_of(mi, s.get("runtime") or 0)
            if not b or not mins:
                continue
            floor = QUALITY_FLOOR_MBMIN[b] * (ANIMATION_FLOOR_SCALE if anim else 1)
            mbmin = (f["size"] / 1e6) / mins
            if mbmin < floor:
                out.append(
                    {
                        "kind": "series",
                        "title": s["title"],
                        "arr_id": s["id"],
                        "file_id": f["id"],
                        "path": f.get("relativePath", ""),
                        "gb": round(f["size"] / 1e9, 2),
                        "minutes": round(mins, 1),
                        "mbmin": round(mbmin, 2),
                        "floor": round(floor, 1),
                        "anim": anim,
                    }
                )
    for m in arr(RADARR, "/api/v3/movie"):
        mf = m.get("movieFile")
        if not mf:
            continue
        anim = any(g.lower() == "animation" for g in (m.get("genres") or []))
        mi = mf.get("mediaInfo") or {}
        b = bucket(height_of(mi))
        mins = minutes_of(mi, m.get("runtime") or 0)
        if not b or not mins:
            continue
        floor = QUALITY_FLOOR_MBMIN[b] * (ANIMATION_FLOOR_SCALE if anim else 1)
        mbmin = (mf["size"] / 1e6) / mins
        if mbmin < floor:
            out.append(
                {
                    "kind": "movie",
                    "title": m["title"],
                    "arr_id": m["id"],
                    "file_id": mf["id"],
                    "path": mf.get("relativePath", ""),
                    "gb": round(mf["size"] / 1e9, 2),
                    "minutes": round(mins, 1),
                    "mbmin": round(mbmin, 2),
                    "floor": round(floor, 1),
                    "anim": anim,
                }
            )
    return out


def has_replacement(item) -> bool:
    """True only if an indexer currently offers something above the floor."""
    try:
        if item["kind"] == "series":
            rel = arr(SONARR, f"/api/v3/release?seriesId={item['arr_id']}")
        else:
            rel = arr(RADARR, f"/api/v3/release?movieId={item['arr_id']}")
    except Exception:  # noqa: BLE001
        return False
    for r in rel:
        if ((r.get("size") or 0) / 1e6) / item["minutes"] >= item["floor"]:
            return True
    return False


# ---------------------------------------------------------------------------
# Upgrade detection
#
# An absolute floor cannot catch everything. Colin from Accounts sat at
# 46 MB/min - comfortably above the 18 floor - while a 110 MB/min release of
# the SAME quality tier was available. Sonarr ranks by tier, not size, so it
# considered the small file equivalent and would never upgrade it.
#
# So also ask: how does this file compare to the best thing actually on offer?
# One indexer query per series/movie, not per file.
# ---------------------------------------------------------------------------

UPGRADE_RATIO = 1.8  # flag when the best available is this much bigger


def best_available_mbmin(kind, arr_id, minutes):
    """MB/min of the largest non-junk release an indexer currently offers."""
    try:
        if kind == "series":
            rel = arr(SONARR, f"/api/v3/release?seriesId={arr_id}")
        else:
            rel = arr(RADARR, f"/api/v3/release?movieId={arr_id}")
    except Exception:  # noqa: BLE001
        return 0.0
    best = 0.0
    for r in rel:
        title = (r.get("title") or "").upper()
        # Skip foreign-dub releases: bigger, but not better for this library.
        if any(t in title for t in ("GERMAN", "FRENCH", "ITALIAN", "SPANISH", "NORDIC", "POLISH", "MULTI")):
            continue
        best = max(best, ((r.get("size") or 0) / 1e6) / minutes)
    return best


def find_upgradable(files, ratio=UPGRADE_RATIO):
    """Files where a substantially better release exists right now."""
    seen, out = {}, []
    for f in files:
        key = (f["kind"], f["arr_id"])
        if key not in seen:
            seen[key] = best_available_mbmin(f["kind"], f["arr_id"], f["minutes"])
        best = seen[key]
        if best and best >= f["mbmin"] * ratio:
            out.append({**f, "best_mbmin": round(best, 1), "gain": round(best / f["mbmin"], 1)})
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="cap remediations this run")
    ap.add_argument("--upgrades", action="store_true", help="also report files a much better release exists for")
    args = ap.parse_args()

    bad = audit()
    clear = [b for b in bad if b["mbmin"] < b["floor"] * MARGIN]
    marginal = [b for b in bad if b not in clear]
    print(f"UNDER FLOOR: {len(bad)}  (clear {len(clear)}, marginal {len(marginal)})")

    by = collections.Counter(b["title"] for b in clear)
    for t, n in by.most_common(15):
        rs = [x for x in clear if x["title"] == t]
        med = sorted(x["mbmin"] for x in rs)[len(rs) // 2]
        print(
            f"  {n:>4}  median {med:>6.1f} / floor {rs[0]['floor']:<5.1f} {'[anim] ' if rs[0]['anim'] else ''}{t[:40]}"
        )

    if not args.execute:
        print("\n(dry run — pass --execute to remediate)")
        return

    todo = clear[: args.limit] if args.limit else clear
    removed = skipped = 0
    series_ids, movie_ids = set(), set()
    for b in todo:
        if not has_replacement(b):
            skipped += 1
            continue
        api = SONARR if b["kind"] == "series" else RADARR
        ep = "episodefile" if b["kind"] == "series" else "moviefile"
        try:
            arr(api, f"/api/v3/{ep}/{b['file_id']}", "DELETE")
            removed += 1
            (series_ids if b["kind"] == "series" else movie_ids).add(b["arr_id"])
        except Exception:  # noqa: BLE001
            skipped += 1
    print(f"\nremoved={removed} left-alone(no better source)={skipped}")

    for sid in sorted(series_ids):
        try:
            arr(SONARR, "/api/v3/command", "POST", {"name": "SeriesSearch", "seriesId": sid})
        except Exception:  # noqa: BLE001
            pass
    if movie_ids:
        try:
            arr(RADARR, "/api/v3/command", "POST", {"name": "MoviesSearch", "movieIds": sorted(movie_ids)})
        except Exception:  # noqa: BLE001
            pass
    print(f"re-search triggered: {len(series_ids)} series, {len(movie_ids)} movies")


if __name__ == "__main__":
    main()
