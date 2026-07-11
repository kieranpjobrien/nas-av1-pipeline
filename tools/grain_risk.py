"""Predict which bloated films FAIL the VMAF gate before re-encoding them.

The de-bloat proof showed dark + grainy films (Eyes Wide Shut, 86.9 VMAF) lose
quality on the aggressive AV1->AV1 cut while clean/CGI films (Avatar 2, 99.5)
don't. This probes two cheap pre-encode signals -- film grain (denoise delta)
and darkness (mean luma) -- to flag high-risk titles and to locate each film's
darkest scene so the reclaim's VMAF gate can sample the worst case.

Read-only (probes only). Usage: python -m tools.grain_risk [N]
"""
import json
import os
import re
import sqlite3
import subprocess
import sys

# Known clip VMAFs from the 2026-06-14 proof, for calibrating the risk rule.
PROOF_VMAF = {
    "Schindler's List": 97.91,
    "Saving Private Ryan": 93.35,
    "Babylon": 94.23,
    "Avatar - The Way of Water": 99.47,
    "Pulp Fiction": 97.00,
    "Godfather Part III": 97.08,
    "Almost Famous": 96.36,
    "Eyes Wide Shut": 86.87,
    "Scent of a Woman": 94.58,
    "Matrix": 99.80,
}


def _run(cmd: list) -> subprocess.CompletedProcess:
    # encoding/errors are mandatory: ffmpeg echoes the input path, and a filename
    # with a non-cp1252 character (e.g. 'Sirāt', ā = UTF-8 0xC4 0x81) otherwise
    # crashes the subprocess reader thread on the Windows default codepage,
    # leaving stdout/stderr None (reclaim died on this 2026-07-11).
    return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")


def _yavg(path: str, t: float, secs: int = 2) -> float | None:
    r = _run([
        "ffmpeg", "-hide_banner", "-nostats", "-ss", str(int(t)), "-i", path, "-t", str(secs),
        "-an", "-vf", "signalstats,metadata=print:file=-", "-f", "null", "-",
    ])
    vals = [float(m) for m in re.findall(r"signalstats\.YAVG=([\d.]+)", (r.stdout or "") + (r.stderr or ""))]
    return sum(vals) / len(vals) if vals else None


def _grain(path: str, t: float, secs: int = 2) -> float | None:
    # SSIM of frames vs their denoised version; low SSIM => lots of grain/noise.
    r = _run([
        "ffmpeg", "-hide_banner", "-nostats", "-ss", str(int(t)), "-i", path, "-t", str(secs),
        "-an", "-filter_complex", "split[a][b];[b]hqdn3d=4:3:6:4[d];[a][d]ssim", "-f", "null", "-",
    ])
    m = re.search(r"SSIM.*?All:([\d.]+)", r.stderr or "")
    return float(m.group(1)) if m else None


def probe(path: str, dur: float, n: int = 6) -> dict | None:
    pts = [dur * (i + 1) / (n + 1) for i in range(n)]
    ys, gs = [], []
    dark_t, dark_y = pts[len(pts) // 2], 1e9
    for t in pts:
        y = _yavg(path, t)
        if y is not None:
            ys.append(y)
            if y < dark_y:
                dark_y, dark_t = y, t
        g = _grain(path, t)
        if g is not None:
            gs.append(g)
    if not ys or not gs:
        return None
    scale = 4.0 if max(ys) > 300 else 1.0  # 10-bit signalstats can report 0-1023
    return {
        "mean_yavg": (sum(ys) / len(ys)) / scale,
        "dark_yavg": dark_y / scale,
        "dark_t": dark_t,
        "grain_idx": 1 - (sum(gs) / len(gs)),  # 0 clean .. higher grainier
    }


def risk_level(p: dict) -> str:
    """Calibrated 2026-06-15 against PROOF_VMAF (10 films).

    LOW films all scored >=96 on the gate; HIGH/MED caught every sub-95.
    grain_idx (denoise-SSIM delta) is the primary driver, darkness secondary.
    The VMAF gate is still the final arbiter — this just orders + routes.
    """
    g, d = p["grain_idx"], p["dark_yavg"]
    if g > 0.016 or (g > 0.013 and d < 35):  # Eyes Wide Shut 0.019, Scent 0.017, SPR 0.015+dark
        return "HIGH"
    if g > 0.011 or d < 30:  # Godfather 0.014, Babylon dark 26.9
        return "MED"
    return "LOW"


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
        out.append(((out_b - in_b) / 1e9, r["filepath"], dur))
    out.sort(reverse=True)
    return out[:n]


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    print(f"{'grain':>6} {'meanY':>6} {'darkY':>6} {'darkT':>6} {'risk':>5} {'VMAF':>6}  title", flush=True)
    for grew, fp, dur in worst_offenders(n):
        name = os.path.basename(fp)
        p = probe(fp, dur)
        if not p:
            print(f"  (probe failed) {name}", flush=True)
            continue
        vm = next((v for k, v in PROOF_VMAF.items() if k in name), None)
        lvl = risk_level(p)
        print(f"  {p['grain_idx']:.3f} {p['mean_yavg']:6.1f} {p['dark_yavg']:6.1f} "
              f"{int(p['dark_t']):6d} {lvl:>5} {(vm or 0):6.1f}  {name[:44]}", flush=True)


if __name__ == "__main__":
    main()
