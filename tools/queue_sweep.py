"""Find and remove under-spec grabs sitting in the SABnzbd queue.

The library sweep (``tools.quality_sweep``) judges what is already ON DISK.
This judges what is INBOUND, so junk is killed before it lands and before
the encoder wastes GPU on it.

Judging a queued job needs a runtime, which SAB does not know, so each job
is matched back to Sonarr/Radarr:
  * ``Show.S04E12`` -> that series' runtime
  * ``Show.S04``    -> runtime x episode count in that season (season pack)
  * a movie title   -> that movie's runtime

Special care for AV1 releases: a small AV1 grab is worse than an equally
small x264 one, because the pipeline treats AV1 as already-finished and
never re-encodes it. The bad quality becomes permanent.

Run:  uv run python -m tools.queue_sweep
      uv run python -m tools.queue_sweep --execute
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

from pipeline.__main__ import ANIMATION_FLOOR_SCALE, QUALITY_FLOOR_MBMIN  # noqa: E402

SAB = ("http://192.168.4.43:8185/api", "cd29eb7ca0d44e5f96bd51ac2258916e")
SONARR = ("http://192.168.4.43:27483", "80d52d39113c45be9f3b94c3c2cfbdd1")
RADARR = ("http://192.168.4.43:27484", "2aeb7de56ea44035a438447a5911f77a")

_SXXEXX = re.compile(r"[Ss](\d{1,2})[\s._-]*[Ee](\d{1,3})")
_SXX = re.compile(r"[Ss](\d{1,2})(?![\s._-]*[Ee]\d)")


def arr(base_key, path, method="GET", body=None):
    base, key = base_key
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method)
    req.add_header("X-Api-Key", key)
    if data:
        req.add_header("Content-Type", "application/json")
    raw = urllib.request.urlopen(req, timeout=120).read()
    return json.loads(raw) if raw else {}


def sab(**params):
    base, key = SAB
    url = base + "?" + urllib.parse.urlencode({**params, "output": "json", "apikey": key})
    with urllib.request.urlopen(url, timeout=90) as r:
        return json.loads(r.read() or b"{}")


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


def resolution_of(name: str) -> str | None:
    n = name.lower()
    for token, res in (("2160p", "2160p"), ("1080p", "1080p"), ("720p", "720p"), ("480p", "480p")):
        if token in n:
            return res
    return None


def build_lookup():
    """Series/movie metadata needed to turn a release name into MB/min."""
    series = {}
    for s in arr(SONARR, "/api/v3/series"):
        eps_by_season: dict[int, int] = {}
        for se in s.get("seasons") or []:
            stats = se.get("statistics") or {}
            eps_by_season[se.get("seasonNumber", -1)] = stats.get("totalEpisodeCount") or 0
        series[slug(s["title"])] = {
            "id": s["id"],
            "title": s["title"],
            "runtime": s.get("runtime") or 0,
            "genres": [g.lower() for g in (s.get("genres") or [])],
            "seasons": eps_by_season,
        }
    movies = {}
    for m in arr(RADARR, "/api/v3/movie"):
        movies[slug(m["title"])] = {
            "id": m["id"],
            "title": m["title"],
            "runtime": m.get("runtime") or 0,
            "genres": [g.lower() for g in (m.get("genres") or [])],
        }
    return series, movies


def match(name: str, series: dict, movies: dict):
    """Return (kind, meta, minutes) or None when unjudgeable."""
    cleaned = re.sub(r"[._]", " ", name)
    m = _SXXEXX.search(cleaned) or _SXX.search(cleaned)
    if m:
        head = cleaned[: m.start()].strip()
        key = slug(head)
        meta = series.get(key)
        if not meta:
            for k, v in series.items():
                if key and (key.startswith(k) or k.startswith(key)) and abs(len(k) - len(key)) < 6:
                    meta = v
                    break
        if not meta or not meta["runtime"]:
            return None
        season = int(m.group(1))
        if _SXXEXX.search(cleaned):
            return ("series", meta, float(meta["runtime"]))
        eps = meta["seasons"].get(season, 0)
        if not eps:
            return None
        return ("series", meta, float(meta["runtime"] * eps))
    ym = re.search(r"(.+?)\s(19|20)\d{2}\b", cleaned)
    if ym:
        meta = movies.get(slug(ym.group(1)))
        if meta and meta["runtime"]:
            return ("movie", meta, float(meta["runtime"]))
    return None


def floor_for(meta, res: str) -> float:
    base = QUALITY_FLOOR_MBMIN.get(res)
    if base is None:
        return 0.0
    if "animation" in meta["genres"]:
        base *= ANIMATION_FLOOR_SCALE
    return base


def sweep():
    q = sab(mode="queue", limit=2000)
    slots = (q.get("queue") or {}).get("slots") or []
    series, movies = build_lookup()
    print(f"queue: {len(slots)} jobs | sonarr {len(series)} series, radarr {len(movies)} movies", file=sys.stderr)

    bad, unjudged = [], 0
    for s in slots:
        name = s.get("filename", "")
        res = resolution_of(name)
        hit = match(name, series, movies)
        if not res or not hit:
            unjudged += 1
            continue
        kind, meta, minutes = hit
        floor = floor_for(meta, res)
        if not floor or minutes <= 0:
            unjudged += 1
            continue
        mb = float(s.get("mb") or 0)
        mbmin = mb / minutes
        if mbmin < floor:
            bad.append(
                {
                    "nzo_id": s["nzo_id"],
                    "name": name,
                    "kind": kind,
                    "title": meta["title"],
                    "arr_id": meta["id"],
                    "res": res,
                    "gb": round(mb / 1024, 2),
                    "minutes": round(minutes, 0),
                    "mbmin": round(mbmin, 2),
                    "floor": round(floor, 1),
                    "av1": "av1" in name.lower(),
                }
            )
    print(f"unjudgeable (no runtime/resolution match): {unjudged}", file=sys.stderr)
    return bad


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="delete + blocklist, not just report")
    args = ap.parse_args()

    bad = sweep()
    bad.sort(key=lambda x: x["mbmin"])
    print(f"\nUNDER-SPEC IN QUEUE: {len(bad)}")
    av1 = [b for b in bad if b["av1"]]
    print(f"  of which already-AV1 (pipeline would never re-encode): {len(av1)}")
    print(f"  total size: {sum(b['gb'] for b in bad):.0f} GB\n")
    for b in bad[:40]:
        tag = "AV1" if b["av1"] else "   "
        print(f"  {b['mbmin']:>6.1f}/{b['floor']:<5.1f} MB/min {tag} {b['gb']:>6.2f}GB {b['name'][:54]}")
    if len(bad) > 40:
        print(f"  ... and {len(bad) - 40} more")

    if not args.execute:
        print("\n(dry run — pass --execute to remove + blocklist)")
        return

    # Blocklist through the *arr so the same release is not re-grabbed, and
    # the *arr immediately searches for a compliant replacement.
    # Do not trade a small file for a missing episode: only bin a job when a
    # compliant replacement actually exists on an indexer. Anything with no
    # better option is left alone and reported, so nothing vanishes silently.
    keep = []
    for b in list(bad):
        try:
            if b["kind"] == "series":
                rel = arr(SONARR, f"/api/v3/release?seriesId={b['arr_id']}")
            else:
                rel = arr(RADARR, f"/api/v3/release?movieId={b['arr_id']}")
        except Exception:  # noqa: BLE001
            rel = []
        better = False
        for r in rel:
            rgb = (r.get("size") or 0) / 1e6
            # Ignore r["rejected"]: the commonest rejection is "release in
            # queue already meets cutoff", which is caused BY the junk we are
            # about to remove. Judging on size alone breaks that circularity.
            if rgb / b["minutes"] >= b["floor"]:
                better = True
                break
        if not better:
            keep.append(b)
            bad.remove(b)
    if keep:
        print("")
        print(f"LEFT ALONE ({len(keep)}) - no compliant replacement exists:")
        for b in keep:
            print(f"  {b['mbmin']:>6.1f}/{b['floor']:<5.1f} {b['name'][:58]}")

    removed = failed = 0
    for b in bad:
        api = SONARR if b["kind"] == "series" else RADARR
        try:
            qrecs = arr(api, "/api/v3/queue?pageSize=1000").get("records", [])
            rec = next(
                (r for r in qrecs if r.get("downloadId") == b["nzo_id"].upper() or b["name"] in (r.get("title") or "")),
                None,
            )
            if rec:
                arr(api, f"/api/v3/queue/{rec['id']}?removeFromClient=true&blocklist=true", "DELETE")
            else:
                sab(mode="queue", name="delete", value=b["nzo_id"], del_files=1)
            removed += 1
        except Exception:  # noqa: BLE001
            failed += 1
    print(f"\nremoved={removed} failed={failed}")

    ser = sorted({b["arr_id"] for b in bad if b["kind"] == "series"})
    mov = sorted({b["arr_id"] for b in bad if b["kind"] == "movie"})
    for sid in ser:
        try:
            arr(SONARR, "/api/v3/command", "POST", {"name": "SeriesSearch", "seriesId": sid})
        except Exception:  # noqa: BLE001
            pass
    if mov:
        try:
            arr(RADARR, "/api/v3/command", "POST", {"name": "MoviesSearch", "movieIds": mov})
        except Exception:  # noqa: BLE001
            pass
    print(f"re-search triggered: {len(ser)} series, {len(mov)} movies")


if __name__ == "__main__":
    main()
