"""Library-wide sweep for sources that violate the quality floor.

Judges EVERY file in the library, not a sample and not the ones someone
happened to notice. Two distinct violation classes:

1. ``under_floor``  — source bitrate below the MB/min floor for its
   resolution (animation-scaled). For files we already encoded, the
   judgement uses ``input_size_bytes`` from the state row, i.e. the size
   of the ORIGINAL source, because our own AV1 output is legitimately
   40-50% smaller and says nothing about source quality.

2. ``fake_remux``   — the release is tagged Remux/Bluray by Sonarr/Radarr
   but its source bitrate is nowhere near what that tier implies. A real
   1080p remux is untouched Blu-ray video at roughly 150-225 MB/min; a
   "Bluray-1080p Remux" at 14 MB/min is a re-encode wearing a Remux tag,
   and Sonarr will never upgrade it because it already believes it holds
   the top tier.

Run:  uv run python -m tools.quality_sweep            # report only
      uv run python -m tools.quality_sweep --json out.json
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from paths import MEDIA_REPORT, STAGING_DIR  # noqa: E402
from pipeline.__main__ import ANIMATION_FLOOR_SCALE as ANIM  # noqa: E402
from pipeline.__main__ import QUALITY_FLOOR_MBMIN, _is_animation  # noqa: E402

SONARR = ("http://192.168.4.43:27483", "80d52d39113c45be9f3b94c3c2cfbdd1")
RADARR = ("http://192.168.4.43:27484", "2aeb7de56ea44035a438447a5911f77a")

# Floor for what a quality TIER implies, MB/min. Well below the true figure
# for each tier so only egregious mislabels trip it.
TIER_FLOOR_MBMIN = {
    # A release claiming a tier must at least look like that tier. Raised
    # 2026-07-28 alongside the 50 MB/min standard - the old remux floor of 45
    # was below the plain 1080p floor of 50, which made no sense.
    "Bluray-2160p Remux": 150.0,
    "Bluray-1080p Remux": 100.0,
    "Bluray-2160p": 100.0,
    "Bluray-1080p": 60.0,
    "Bluray-720p": 45.0,
}


def arr_get(base_key, path, **params):
    base, key = base_key
    url = f"{base}{path}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    req = urllib.request.Request(url, headers={"X-Api-Key": key})
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.load(r)


def load_state_inputs() -> dict[str, int]:
    """filepath -> input_size_bytes (the pre-encode source size)."""
    db = os.path.join(str(STAGING_DIR), "pipeline_state.db")
    out: dict[str, int] = {}
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    for r in con.execute("SELECT filepath, extras FROM pipeline_files WHERE extras IS NOT NULL"):
        try:
            ex = json.loads(r["extras"] or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        n = ex.get("input_size_bytes")
        if isinstance(n, int) and n > 0:
            out[r["filepath"]] = n
    con.close()
    return out


def floor_for(entry: dict) -> float | None:
    res = str((entry.get("video") or {}).get("resolution_class") or "").lower()
    base = QUALITY_FLOOR_MBMIN.get(res)
    if base is None:
        return None
    return base * ANIM if _is_animation(entry) else base


def build_quality_index() -> dict[str, str]:
    """Map basename -> Sonarr/Radarr quality tier name."""
    idx: dict[str, str] = {}
    for s in arr_get(SONARR, "/api/v3/series"):
        try:
            for f in arr_get(SONARR, "/api/v3/episodefile", seriesId=s["id"]):
                q = ((f.get("quality") or {}).get("quality") or {}).get("name")
                p = f.get("path") or f.get("relativePath") or ""
                if q and p:
                    idx[os.path.basename(p).lower()] = q
        except Exception:  # noqa: BLE001,PERF203
            continue
    for m in arr_get(RADARR, "/api/v3/movie"):
        mf = m.get("movieFile")
        if not mf:
            continue
        q = ((mf.get("quality") or {}).get("quality") or {}).get("name")
        p = mf.get("path") or mf.get("relativePath") or ""
        if q and p:
            idx[os.path.basename(p).lower()] = q
    return idx


def sweep() -> list[dict]:
    report = json.load(open(MEDIA_REPORT, encoding="utf-8"))
    entries = report.get("files") or []
    inputs = load_state_inputs()
    quality = build_quality_index()

    violations = []
    for e in entries:
        fp = e.get("filepath", "")
        dur_min = (e.get("duration_seconds") or 0) / 60.0
        if not fp or dur_min <= 0:
            continue
        floor = floor_for(e)
        if floor is None:
            continue

        # Judge the SOURCE. For anything we encoded, that is input_size_bytes.
        src_bytes = inputs.get(fp) or e.get("file_size_bytes") or 0
        if src_bytes <= 0:
            continue
        src_mbmin = (src_bytes / 1_000_000) / dur_min
        was_encoded = fp in inputs

        tier = quality.get(os.path.basename(fp).lower())
        tier_floor = TIER_FLOOR_MBMIN.get(tier or "")

        reasons = []
        if src_mbmin < floor:
            reasons.append("under_floor")
        if tier_floor and src_mbmin < tier_floor:
            reasons.append("fake_remux")
        if not reasons:
            continue

        violations.append(
            {
                "filepath": fp,
                "title": (e.get("tmdb") or {}).get("title") or e.get("filename", ""),
                "library_type": e.get("library_type", ""),
                "resolution": (e.get("video") or {}).get("resolution_class", ""),
                "animation": _is_animation(e),
                "minutes": round(dur_min, 1),
                "source_gb": round(src_bytes / 1_000_000_000, 2),
                "source_mbmin": round(src_mbmin, 2),
                "floor": round(floor, 1),
                "tier": tier,
                "tier_floor": tier_floor,
                "already_encoded": was_encoded,
                "reasons": reasons,
            }
        )
    return violations


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", help="write full violation list here")
    args = ap.parse_args()

    v = sweep()
    v.sort(key=lambda x: x["source_mbmin"])
    print(f"VIOLATIONS: {len(v)}")
    under = [x for x in v if "under_floor" in x["reasons"]]
    fake = [x for x in v if "fake_remux" in x["reasons"]]
    enc = [x for x in v if x["already_encoded"]]
    print(f"  under_floor : {len(under)}")
    print(f"  fake_remux  : {len(fake)}")
    print(f"  already encoded from a bad source: {len(enc)}")

    import collections

    by_show = collections.Counter(x["title"] for x in v)
    print("\n--- worst affected titles ---")
    for t, n in by_show.most_common(25):
        rows = [x for x in v if x["title"] == t]
        med = sorted(r["source_mbmin"] for r in rows)[len(rows) // 2]
        print(f"  {n:>4} files  median {med:>6.1f} MB/min  {t[:50]}")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(v, fh, indent=1)
        print(f"\nwrote {args.json}")


if __name__ == "__main__":
    main()
