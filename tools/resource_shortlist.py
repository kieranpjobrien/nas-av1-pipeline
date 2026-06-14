"""Curated re-source shortlist (films only).

The bloat diagnosis (tools/diagnose_av1_bloat.py) found 4.45 TB of zero-benefit
bloat, but most of it is *sunk* — done AV1, sources replaced, AV1->AV1 down is
lossy. This narrows to the films actually worth re-sourcing, split by motive:

  PRIORITY 1 — quality deficit: the source is STARVED for its resolution, so the
    current AV1 is a faithful copy of mediocre. Re-sourcing a proper master buys
    real quality (and right-sizes). Only films you value (keeper or vote>=6.5).

  PRIORITY 2 — treasured but merely fat: source was fine, the AV1 is just bloated.
    Re-sourcing gains NO quality, only space. Optional; listed for completeness.

Projected sizes use the real resolve_encode_params logic (as if re-sourced to a
rich master), so they match what the fixed pipeline would actually produce.
Read-only; writes a report file only.
"""

import json
import os
import sqlite3

from pipeline.config import DEFAULT_CONFIG, resolve_encode_params

DB = "F:/AV1_Staging/pipeline_state.db"
REPORT = "F:/AV1_Staging/media_report.json"
OUT = "F:/AV1_Staging/resource_shortlist.txt"

# Source bitrate (Mbps) below which a source is "starved" for its resolution —
# i.e. the source itself, not the encoder, is the quality ceiling.
STARVED_MBPS = {"4K": 14.0, "1080p": 6.0, "720p": 3.0, "SD": 1.5}

lines: list[str] = []


def log(m: str = "") -> None:
    print(m, flush=True)
    lines.append(str(m))


def starved_floor(res_class: str) -> float:
    rc = (res_class or "").upper()
    if "4K" in rc or "2160" in rc:
        return STARVED_MBPS["4K"]
    if "1080" in rc:
        return STARVED_MBPS["1080p"]
    if "720" in rc:
        return STARVED_MBPS["720p"]
    return STARVED_MBPS["SD"]


def projected_gb(item: dict, dur: float, fallback_mbps: float) -> float:
    """Size the title would land at if re-sourced rich and re-encoded under the
    new tiered cap (huge bitrate so the source term doesn't bind)."""
    proj_item = {
        "library_type": item.get("library_type", "movie"),
        "resolution": (item.get("video") or {}).get("resolution_class", ""),
        "hdr": (item.get("video") or {}).get("hdr", False),
        "bitrate_kbps": 999_999,
        "duration_seconds": dur,
        "tmdb": item.get("tmdb") or {},
    }
    params = resolve_encode_params(DEFAULT_CONFIG, proj_item)
    mbps = float(params["maxrate"][:-1]) if params["maxrate"] else fallback_mbps
    return mbps * dur / 8 / 1000


def main() -> None:
    rep = json.load(open(REPORT, encoding="utf-8"))
    info = {f.get("filepath"): f for f in rep.get("files", []) if f.get("filepath")}

    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT filepath, extras FROM pipeline_files WHERE status='done'").fetchall()
    con.close()

    p1, p2 = [], []  # priority 1 (quality deficit), priority 2 (treasured but fat)
    for r in rows:
        f = info.get(r["filepath"]) or {}
        if (f.get("library_type") or "").lower() not in ("movie", "film"):
            continue  # films only
        if ((f.get("video") or {}).get("codec_raw") or "").lower() not in ("", "av1"):
            continue
        try:
            ex = json.loads(r["extras"] or "{}")
        except Exception:
            continue
        in_b, out_b = ex.get("input_size_bytes"), ex.get("output_size_bytes") or f.get("file_size_bytes")
        dur = ex.get("duration_seconds") or f.get("duration_seconds") or 0
        if not in_b or not out_b or not dur:
            continue
        src_mbps = in_b * 8 / dur / 1e6
        out_mbps = out_b * 8 / dur / 1e6
        tmdb = f.get("tmdb") or {}
        vote = tmdb.get("vote_average") or 0
        res_class = (f.get("video") or {}).get("resolution_class") or ""
        treasured = vote >= 8.0
        valued = vote >= 6.5
        starved = src_mbps < starved_floor(res_class)
        cur_gb = out_b / 1e9
        proj_gb = projected_gb(f, dur, out_mbps)
        rec = {
            "title": os.path.basename(r["filepath"]),
            "src_mbps": src_mbps,
            "out_mbps": out_mbps,
            "cur_gb": cur_gb,
            "proj_gb": proj_gb,
            "delta": proj_gb - cur_gb,
            "vote": vote,
            "res": res_class,
            "starved": starved,
        }
        if starved and valued:
            p1.append(rec)
        elif treasured and out_mbps >= src_mbps:  # fat copy of a treasured title
            p2.append(rec)

    p1.sort(key=lambda x: (-x["vote"], x["src_mbps"]))
    p2.sort(key=lambda x: x["cur_gb"] - x["proj_gb"], reverse=True)

    def render(group: list, header: str, note: str) -> None:
        log("")
        log(header)
        log(f"  {note}")
        log(f"  {'cur':>6} {'->proj':>7} {'delta':>7}  {'src':>5} {'vote':>4}  {'res':<6} title")
        cur = proj = 0.0
        for a in group:
            cur += a["cur_gb"]
            proj += a["proj_gb"]
            log(
                f"  {a['cur_gb']:5.1f}G {a['proj_gb']:6.1f}G {a['delta']:+6.1f}G  "
                f"{a['src_mbps']:4.1f} {a['vote']:4.1f}  {a['res']:<6} {a['title'][:50]}"
            )
        log(
            f"  --- {len(group)} films: {cur / 1000:.2f} TB now -> {proj / 1000:.2f} TB re-sourced  (delta {proj - cur:+.0f} GB)"
        )

    log("=== RE-SOURCE SHORTLIST (films only) ===")
    render(
        p1[:60],
        f"PRIORITY 1 -- QUALITY DEFICIT (starved source, value>=normal): {len(p1)} films",
        "re-source a proper master -> real quality gain + right-sized. THE list worth acting on.",
    )
    render(
        p2[:40],
        f"PRIORITY 2 -- treasured but merely fat (decent source): {len(p2)} films",
        "re-sourcing gains NO quality, only space. Optional space reclaim.",
    )
    log("")
    log(f"(full report written to {OUT})")
    open(OUT, "w", encoding="utf-8").write("\n".join(lines))


if __name__ == "__main__":
    main()
