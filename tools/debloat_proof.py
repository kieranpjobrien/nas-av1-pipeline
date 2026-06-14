"""In-place de-bloat proof.

Re-encodes a short clip of each worst-offender AV1 down to the new
source-relative cap, then VMAFs the re-encode against the current file. Measures
the *generation loss* of reclaiming bloat in place (AV1->AV1) instead of
re-sourcing. High VMAF (>=~95) means the ~3.6 TB reclaim costs no download budget
and no meaningful quality; low VMAF means re-source instead.

Clip-based so it runs in minutes. Usage: python -m tools.debloat_proof [N]
"""
import json
import os
import re
import sqlite3
import subprocess
import sys

from pipeline.config import DEFAULT_CONFIG, resolve_encode_params

CLIP = 30  # seconds per probe
TMP = "F:/AV1_Staging/proof"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 8


def worst_offenders(n: int) -> list:
    rep = json.load(open("F:/AV1_Staging/media_report.json", encoding="utf-8"))
    info = {f.get("filepath"): f for f in rep.get("files", []) if f.get("filepath")}
    con = sqlite3.connect("file:F:/AV1_Staging/pipeline_state.db?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT filepath, extras FROM pipeline_files WHERE status='done'").fetchall()
    con.close()
    out = []
    for r in rows:
        f = info.get(r["filepath"]) or {}
        if (f.get("library_type") or "").lower() not in ("movie", "film"):
            continue
        try:
            ex = json.loads(r["extras"] or "{}")
        except Exception:
            continue
        in_b = ex.get("input_size_bytes")
        out_b = ex.get("output_size_bytes") or f.get("file_size_bytes")
        dur = ex.get("duration_seconds") or f.get("duration_seconds") or 0
        if not in_b or not out_b or not dur or out_b <= in_b:
            continue
        out.append(((out_b - in_b) / 1e9, r["filepath"], f, out_b, dur))
    out.sort(reverse=True)
    return out[:n]


def run(cmd: list) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


def vmaf(enc: str, ref: str) -> float | None:
    r = run(["ffmpeg", "-i", enc, "-i", ref, "-lavfi", "[0:v][1:v]libvmaf=n_threads=16", "-f", "null", "-"])
    m = re.search(r"VMAF score:\s*([\d.]+)", r.stderr)
    return float(m.group(1)) if m else None


def main() -> None:
    os.makedirs(TMP, exist_ok=True)
    results = []
    for grew, fp, f, out_b, dur in worst_offenders(N):
        name = os.path.basename(fp)
        cur_mbps = out_b * 8 / dur / 1e6
        hdr = (f.get("video") or {}).get("hdr", False)
        item = {
            "library_type": f.get("library_type", "movie"),
            "resolution": (f.get("video") or {}).get("resolution_class", ""),
            "hdr": hdr,
            "bitrate_kbps": cur_mbps * 1000,
            "duration_seconds": dur,
            "tmdb": f.get("tmdb") or {},
        }
        p = resolve_encode_params(DEFAULT_CONFIG, item)
        if not p["maxrate"]:
            print(f"SKIP {name}: no cap", flush=True)
            continue
        cap = float(p["maxrate"][:-1])
        mid = int(dur / 2)
        ref = os.path.join(TMP, "ref.mkv")
        enc = os.path.join(TMP, "enc.mkv")
        pix = DEFAULT_CONFIG.get("pixel_format_hdr" if hdr else "pixel_format_sdr", "p010le")
        # Production-matched colour flags so the re-encode decodes to the same
        # range/space as the reference; without -color_range tv NVENC lifts the
        # black floor and VMAF tanks for non-quality reasons (the 2026-05-28 bug).
        if hdr:
            color = ["-color_primaries", "bt2020", "-color_trc", "smpte2084", "-colorspace", "bt2020nc", "-color_range", "tv"]
        else:
            color = ["-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709", "-color_range", "tv"]
        print(f"[{name[:42]}] cur={cur_mbps:.1f} cap={cap:.1f} cq={p['cq']} -> ref(ffv1)", flush=True)
        # Lossless decode = frame-exact, deterministic reference (fixes alignment).
        e = run(["ffmpeg", "-y", "-ss", str(mid), "-i", fp, "-t", str(CLIP), "-map", "0:v:0", "-an", "-c:v", "ffv1", ref])
        if e.returncode != 0 or not os.path.exists(ref):
            print(f"  ref failed: {e.stderr[-160:]}", flush=True)
            continue
        print("  encode at cap", flush=True)
        e2 = run([
            "ffmpeg", "-y", "-i", ref, "-map", "0:v:0", "-c:v", "av1_nvenc", "-preset", str(p["preset"]),
            "-cq", str(p["cq"]), "-maxrate", p["maxrate"], "-bufsize", p["bufsize"], "-multipass", str(p["multipass"]),
            "-rc-lookahead", str(p["lookahead"]), "-pix_fmt", str(pix), *color, "-an", enc,
        ])
        if e2.returncode != 0 or not os.path.exists(enc):
            print(f"  encode failed: {e2.stderr[-300:]}", flush=True)
            continue
        score = vmaf(enc, ref)
        proj_full = cap * dur / 8 / 1000
        saved = out_b / 1e9 - proj_full
        results.append((name, cur_mbps, cap, p["cq"], score, saved))
        print(f"  VMAF={score}  full~saved {saved:.1f}GB", flush=True)
        for t in (ref, enc):
            try:
                os.remove(t)
            except OSError:
                pass

    print("\n=== DE-BLOAT PROOF (in-place AV1->AV1 re-encode to cap) ===")
    print(f"  {'VMAF':>6} {'cur->cap':>12} {'cq':>3} {'~full saved':>11}  title")
    for name, cur, cap, cq, score, saved in results:
        print(f"  {(score or 0):6.2f} {cur:5.1f}->{cap:4.1f}   {cq:>3} {saved:9.1f}G  {name[:46]}", flush=True)
    sc = [r[4] for r in results if r[4]]
    if sc:
        sc.sort()
        print(f"\n  median VMAF {sc[len(sc)//2]:.2f}  min {min(sc):.2f}  max {max(sc):.2f}"
              f"  | total ~saved (these {len(results)}): {sum(r[5] for r in results):.0f} GB", flush=True)


if __name__ == "__main__":
    main()
