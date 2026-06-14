"""Diagnose zero-benefit bloat in extant AV1 files.

Zero-benefit bloat = an AV1 file whose *output* bitrate is at or above its
*source* bitrate. AV1 is more bitrate-efficient than the H.264/HEVC sources it
replaced, so any AV1 that came out >= its source bitrate spent those bits
faithfully encoding the source's noise/grain/compression artifacts -- pure
waste, zero real-detail gain (the source never had the detail to begin with).

Read-only. Reads pipeline_state.db (extras: input/output size, duration,
encode params) + media_report.json (resolution, tmdb) + rewatchables. Writes a
report file under F:/AV1_Staging; makes no changes to state.
"""

import json
import os
import sqlite3

DB = "F:/AV1_Staging/pipeline_state.db"
REPORT = "F:/AV1_Staging/media_report.json"
OUT = "F:/AV1_Staging/av1_bloat_diagnosis.txt"

# AV1 reaches transparency to an H.264/HEVC source at roughly this fraction of
# the source bitrate. Anything the encoder spent *above* this was wasted. We use
# a conservative 0.7 (HEVC-like); H.264 sources are more like 0.55, so this
# under-counts waste rather than over-counts it.
TRANSPARENT_FRACTION = 0.7

lines: list[str] = []


def log(m: str = "") -> None:
    print(m, flush=True)
    lines.append(str(m))


def load_keepers() -> set[str]:
    """Best-effort set of lowercased keeper titles from rewatchables.json."""
    keepers: set[str] = set()
    try:
        rw = json.load(open("F:/AV1_Staging/control/rewatchables.json", encoding="utf-8"))
    except Exception as e:
        log(f"(rewatchables unavailable: {e})")
        return keepers

    def harvest(x) -> None:
        if isinstance(x, dict):
            for k, v in x.items():
                if k in ("title", "name") and isinstance(v, str):
                    keepers.add(v.lower())
                harvest(v)
        elif isinstance(x, list):
            for v in x:
                harvest(v)

    harvest(rw)
    return keepers


def tier_target_mbps(res_class: str, lib: str, vote: float, keeper: bool) -> float:
    """Target bitrate ceiling under the agreed value-tiered policy."""
    res_class = (res_class or "").upper()
    is_4k = "4K" in res_class or "2160" in res_class
    is_series = "series" in lib or "tv" in lib
    if is_series:
        return 14.0 if is_4k else 7.0
    # films
    treasured = keeper or vote >= 8.0
    if is_4k:
        return 30.0 if treasured else 24.0
    # 1080p / SD film
    if "1080" in res_class or res_class == "HD":
        return 12.0 if treasured else 9.0
    return 6.0


def value_tier(vote: float, keeper: bool) -> str:
    if keeper or vote >= 8.0:
        return "treasured"
    if vote >= 6.5:
        return "normal"
    return "casual"


def main() -> None:
    rep = json.load(open(REPORT, encoding="utf-8"))
    info = {f.get("filepath"): f for f in rep.get("files", []) if f.get("filepath")}
    keepers = load_keepers()
    log(f"(rewatchables keeper titles parsed: {len(keepers)})")

    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT filepath, status, extras FROM pipeline_files WHERE status='done'").fetchall()
    con.close()

    analysed = []
    skipped = 0
    for r in rows:
        f = info.get(r["filepath"]) or {}
        codec = ((f.get("video") or {}).get("codec_raw") or "").lower()
        if codec and codec != "av1":
            continue  # not an AV1 output (defensive)
        try:
            ex = json.loads(r["extras"] or "{}")
        except Exception:
            skipped += 1
            continue
        in_b = ex.get("input_size_bytes")
        out_b = ex.get("output_size_bytes") or f.get("file_size_bytes")
        dur = ex.get("duration_seconds") or f.get("duration_seconds") or 0
        if not in_b or not out_b or not dur:
            skipped += 1
            continue
        src_mbps = in_b * 8 / dur / 1e6
        out_mbps = out_b * 8 / dur / 1e6
        ratio = out_mbps / src_mbps if src_mbps else 0
        tmdb = f.get("tmdb") or {}
        vote = tmdb.get("vote_average") or 0
        title = tmdb.get("title") or tmdb.get("name") or os.path.basename(r["filepath"])
        keeper = (title or "").lower() in keepers
        lib = (f.get("library_type") or "").lower()
        res_class = (f.get("video") or {}).get("resolution_class") or ""
        tier = tier_target_mbps(res_class, lib, vote, keeper)
        analysed.append(
            {
                "fp": r["filepath"],
                "title": os.path.basename(r["filepath"]),
                "src_mbps": src_mbps,
                "out_mbps": out_mbps,
                "ratio": ratio,
                "in_b": in_b,
                "out_b": out_b,
                "grew_gb": (out_b - in_b) / 1e9,
                "ideal_mbps": src_mbps * TRANSPARENT_FRACTION,
                "wasted_gb": max(0.0, (out_mbps - src_mbps * TRANSPARENT_FRACTION)) * dur / 8 / 1000,
                "tier_target": tier,
                "over_tier_gb": max(0.0, (out_mbps - tier)) * dur / 8 / 1000,
                "value": value_tier(vote, keeper),
                "vote": vote,
                "res": res_class,
                "cq": (ex.get("encode_params_used") or {}).get("cq"),
            }
        )

    n = len(analysed)
    log("")
    log(f"=== AV1 BLOAT DIAGNOSIS ({n} done AV1 files, {skipped} skipped for missing size/duration) ===")

    # --- hard bloat: grew vs source (ratio >= 1.0) ---
    grew = [a for a in analysed if a["ratio"] >= 1.0]
    grew_gb = sum(a["grew_gb"] for a in grew)
    # --- efficient: ratio < 0.7 ---
    eff = [a for a in analysed if a["ratio"] < TRANSPARENT_FRACTION]
    eff_saved = sum((a["in_b"] - a["out_b"]) for a in eff) / 1e12

    log("")
    log("ZERO-BENEFIT BLOAT  (output >= source bitrate -- bytes that cannot be real detail):")
    log(f"  files : {len(grew)} of {n}  ({100 * len(grew) // max(n, 1)}%)")
    log(f"  bytes spent ABOVE the source size : {grew_gb / 1000:.2f} TB")
    tot_wasted = sum(a["wasted_gb"] for a in analysed) / 1000
    log(f"  bytes spent ABOVE transparent (0.7x source), all files : {tot_wasted:.2f} TB")
    by_val = {}
    for a in grew:
        b = by_val.setdefault(a["value"], [0, 0.0])
        b[0] += 1
        b[1] += a["grew_gb"]
    for v in ("treasured", "normal", "casual"):
        if v in by_val:
            c, g = by_val[v]
            tag = (
                "RE-SOURCE candidates"
                if v == "treasured"
                else ("re-source-or-leave" if v == "normal" else "sunk -- lesson only")
            )
            log(f"    {v:10}: {c:4} files, {g / 1000:.2f} TB   ({tag})")

    log("")
    log("OVER NEW TIER TARGET  (output Mbps above the agreed value-tiered ceiling):")
    over = [a for a in analysed if a["over_tier_gb"] > 0.5]
    log(f"  files : {len(over)}   total bytes above tier : {sum(a['over_tier_gb'] for a in over) / 1000:.2f} TB")

    log("")
    log("EFFICIENT  (ratio < 0.7 -- AV1 did its job):")
    log(f"  files : {len(eff)}   net saved : {eff_saved:.2f} TB")

    log("")
    log("=== WORST 35 OFFENDERS (by bytes grown above source) ===")
    log(f"  {'grew':>7} {'wasted':>7}  {'ratio':>5}  {'src->out Mbps':>16}  {'cq':>3}  {'value':<9} title")
    for a in sorted(analysed, key=lambda x: x["grew_gb"], reverse=True)[:35]:
        log(
            f"  {a['wasted_gb']:6.1f}G {a['grew_gb']:+6.1f}G  {a['ratio']:4.2f}x  "
            f"{a['src_mbps']:6.1f}->{a['out_mbps']:6.1f}  {str(a['cq'] or '?'):>3}  "
            f"{a['value']:<9} {a['title'][:46]}"
        )

    open(OUT, "w", encoding="utf-8").write("\n".join(lines))
    log("")
    log(f"(full report written to {OUT})")


if __name__ == "__main__":
    main()
