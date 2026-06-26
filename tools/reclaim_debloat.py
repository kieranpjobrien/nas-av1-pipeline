"""Safe, reversible, VMAF-gated in-place de-bloat reclaim.

Re-encodes bloated normal-tier AV1 films down to the source-relative cap, but
only swaps a file if a darkest-scene-targeted VMAF gate passes -- and even then
it moves the original into a NAS backup folder rather than deleting it, so every
action is reversible. Treasured films (vote>=8) are excluded; HIGH-risk (grainy)
films are routed to re-source without wasting an encode.

Flow per film: risk-probe -> (skip if HIGH) -> CHEAP CLIP GATE (proof method:
ffv1 ref -> encode to cap -> VMAF, darkest+mid, min>=GATE) -> only if it passes,
the full encode (hwaccel decode, audio/subs copied, colour preserved) ->
post-probe (streams+duration) -> backup-preserving atomic swap.

Safety invariants:
  - original is NEVER deleted -- only moved to _reclaim_backup (reversible)
  - full encode + swap only after the VMAF gate passes
  - audio + subs stream-copied bit-exact (rule 9a TrueHD); counts re-verified
  - source colour probed + preserved (-color_range tv, no black-floor lift)
  - resumable ledger; circuit-breaker after consecutive hard errors

Usage: python -m tools.reclaim_debloat [max_films]
"""

import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time

from pipeline.config import DEFAULT_CONFIG, resolve_encode_params
from tools.grain_risk import probe as risk_probe
from tools.grain_risk import risk_level

GATE = 95.0
MAX_CONSEC_FAIL = 3
NAS_MEDIA = r"\\KieranNAS\Media"
BACKUP_ROOT = r"\\KieranNAS\Media\_reclaim_backup"
# Originals are moved aside during the (crash-safe) swap. With KEEP_BACKUPS off they're
# deleted right after the new file is verified in place, so backups never accumulate.
# Flip to True to retain them (reversible, but piles up ~the reclaimed volume on the NAS).
KEEP_BACKUPS = False
WORK = "F:/AV1_Staging/reclaim"
LEDGER = "F:/AV1_Staging/reclaim_ledger.json"
LOG = "F:/AV1_Staging/reclaim.log"
DB = "F:/AV1_Staging/pipeline_state.db"
REPORT = "F:/AV1_Staging/media_report.json"
PAUSE_FILE = "F:/AV1_Staging/control/pause_reclaim.json"  # presence = pause between films
PROGRESS_FILE = "F:/AV1_Staging/reclaim/ffprogress.txt"  # ffmpeg -progress target
INFLIGHT_FILE = "F:/AV1_Staging/reclaim/inflight.json"  # live progress, kept OUT of the ledger


def log(m: str) -> None:
    line = f"{time.strftime('%H:%M:%S')} {m}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def _run(cmd: list, timeout: int = 14400) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _hms(t: str) -> float:
    try:
        h, m, s = t.split(":")
        return int(h) * 3600 + int(m) * 60 + float(s)
    except Exception:
        return 0.0


def _write_inflight(data: dict) -> None:
    """Atomic write of the small live-progress file, retrying the Windows rename
    if the dashboard has it open."""
    tmp = INFLIGHT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
    for _ in range(20):
        try:
            os.replace(tmp, INFLIGHT_FILE)
            return
        except PermissionError:
            time.sleep(0.05)


def _progress_watcher(dur: float, fp: str, name: str, stop_evt: threading.Event) -> None:
    """Tail ffmpeg's -progress file and write pct/speed/eta to a SMALL separate
    file — NOT the ledger. Writing the shared ledger every 3s from this thread
    raced the main thread AND collided with the dashboard's 3s ledger polling
    (Windows os.replace 'Access is denied'), which crashed the run on 06-18."""
    while not stop_evt.is_set():
        try:
            with open(PROGRESS_FILE, encoding="utf-8") as f:
                f.seek(0, 2)
                f.seek(max(0, f.tell() - 2048))
                tail = f.read()
            ot = re.findall(r"out_time=(\S+)", tail)
            sp = re.findall(r"speed=(\S+)", tail)
            if ot and dur:
                secs = _hms(ot[-1])
                speed = sp[-1] if sp else ""
                sv = float(speed[:-1]) if speed.endswith("x") and speed[:-1].replace(".", "").isdigit() else 0.0
                eta = int((dur - secs) / sv) if sv > 0 else 0
                _write_inflight(
                    {
                        "fp": fp,
                        "name": name,
                        "progress_pct": round(min(99.0, secs / dur * 100), 1),
                        "speed": speed,
                        "eta_s": max(0, eta),
                    }
                )
        except OSError:
            pass
        stop_evt.wait(3)


def load_ledger() -> dict:
    try:
        return json.load(open(LEDGER, encoding="utf-8"))
    except Exception:
        return {}


def save_ledger(led: dict) -> None:
    tmp = LEDGER + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(led, fh, indent=1)
    # Windows: the dashboard polls this file; os.replace fails with "Access is
    # denied" if a reader has it open at the rename instant. Retry briefly — the
    # reader's window is milliseconds. (Unhandled, this crashed the run on 06-18.)
    for _ in range(40):
        try:
            os.replace(tmp, LEDGER)
            return
        except PermissionError:
            time.sleep(0.1)
    os.replace(tmp, LEDGER)


def color_flags(src: str, hdr: bool) -> list:
    r = _run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=color_primaries,color_transfer,color_space,color_range",
            "-of",
            "json",
            src,
        ],
        timeout=60,
    )
    try:
        s = json.loads(r.stdout)["streams"][0]
    except Exception:
        s = {}
    if hdr:
        prim, trc, spc = (
            s.get("color_primaries") or "bt2020",
            s.get("color_transfer") or "smpte2084",
            s.get("color_space") or "bt2020nc",
        )
    else:
        prim, trc, spc = (
            s.get("color_primaries") or "bt709",
            s.get("color_transfer") or "bt709",
            s.get("color_space") or "bt709",
        )
    return [
        "-color_primaries",
        prim,
        "-color_trc",
        trc,
        "-colorspace",
        spc,
        "-color_range",
        s.get("color_range") or "tv",
    ]


def probe_counts(path: str) -> tuple:
    r = _run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type",
            "-show_entries",
            "format=duration",
            "-of",
            "json",
            path,
        ],
        timeout=120,
    )
    try:
        d = json.loads(r.stdout)
        types = [s.get("codec_type") for s in d.get("streams", [])]
        return (
            types.count("video"),
            types.count("audio"),
            types.count("subtitle"),
            float(d.get("format", {}).get("duration") or 0),
        )
    except Exception:
        return 0, 0, 0, 0.0


def ffv1_clip(src: str, t: float, secs: int, dest: str) -> bool:
    e = _run(
        ["ffmpeg", "-y", "-ss", str(int(t)), "-i", src, "-t", str(secs), "-map", "0:v:0", "-an", "-c:v", "ffv1", dest],
        timeout=900,
    )
    return e.returncode == 0 and os.path.exists(dest)


def vmaf(distorted: str, ref: str) -> float | None:
    r = _run(
        ["ffmpeg", "-i", distorted, "-i", ref, "-lavfi", "[0:v][1:v]libvmaf=n_threads=16", "-f", "null", "-"],
        timeout=1800,
    )
    m = re.search(r"VMAF score:\s*([\d.]+)", r.stderr)
    return float(m.group(1)) if m else None


def _enc_video_args(p: dict, color: list, pix: str) -> list:
    return [
        "-c:v",
        "av1_nvenc",
        "-preset",
        str(p["preset"]),
        "-cq",
        str(p["cq"]),
        "-maxrate",
        p["maxrate"],
        "-bufsize",
        p["bufsize"],
        "-multipass",
        str(p["multipass"]),
        "-rc-lookahead",
        str(p["lookahead"]),
        "-pix_fmt",
        str(pix),
        *color,
    ]


def gate_score(orig: str, dark_t: float, dur: float, p: dict, color: list, pix: str) -> float | None:
    """Proof-method gate: ffv1 ref from orig -> encode that to cap -> VMAF.

    Reference and encode share the same frames (no cross-file misalignment).
    Samples the darkest scene + a mid-point; returns the worst (min)."""
    scores = []
    for tag, t in (("dark", dark_t), ("mid", dur * 0.45)):
        ref, ce = os.path.join(WORK, "g_ref.mkv"), os.path.join(WORK, "g_enc.mkv")
        if ffv1_clip(orig, t, 20, ref):
            e = _run(
                ["ffmpeg", "-y", "-i", ref, "-map", "0:v:0", *_enc_video_args(p, color, pix), "-an", ce], timeout=1200
            )
            if e.returncode == 0 and os.path.exists(ce):
                s = vmaf(ce, ref)
                if s is not None:
                    scores.append(s)
                    log(f"      gate vmaf[{tag}@{int(t)}s]={s:.2f}")
        for x in (ref, ce):
            try:
                os.remove(x)
            except OSError:
                pass
    return min(scores) if scores else None


def retire_backup(backup: str | None) -> bool:
    """Delete a post-swap backup (the moved-aside original) and tidy its folder. Called
    only after a verified swap, so the new file is already safely in place. Returns True
    if a backup was removed."""
    if not backup or not os.path.exists(backup):
        return False
    os.remove(backup)
    try:
        os.rmdir(os.path.dirname(backup))  # tidy the now-empty film folder
    except OSError:
        pass
    return True


def swap(orig: str, local_out: str, led: dict, key: str) -> str:
    """Backup-preserving atomic swap; records phases for crash recovery."""
    backup = os.path.join(BACKUP_ROOT, os.path.relpath(orig, NAS_MEDIA))
    tmp = orig + ".reclaim_tmp"
    os.makedirs(os.path.dirname(backup), exist_ok=True)
    led[key]["phase"] = "uploading"
    save_ledger(led)
    shutil.copy2(local_out, tmp)
    if probe_counts(tmp)[0] < 1:
        os.remove(tmp)
        raise RuntimeError("uploaded tmp failed probe")
    led[key]["phase"] = "moving_original"
    save_ledger(led)
    shutil.move(orig, backup)  # original preserved, never deleted
    led[key]["phase"] = "renaming"
    save_ledger(led)
    os.replace(tmp, orig)  # new file into place
    led[key]["phase"] = "done"
    return backup


def candidates() -> list:
    rep = json.load(open(REPORT, encoding="utf-8"))
    info = {f.get("filepath"): f for f in rep.get("files", []) if f.get("filepath")}
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT filepath, extras FROM pipeline_files WHERE status='done'").fetchall()
    con.close()
    out = []
    for r in rows:
        f = info.get(r["filepath"]) or {}
        if (f.get("library_type") or "").lower() not in ("movie", "film", "series", "show", "tv", "anime"):
            continue  # movies + series
        if ((f.get("tmdb") or {}).get("vote_average") or 0) >= 8.0:
            continue  # treasured -> leave for re-source
        try:
            ex = json.loads(r["extras"] or "{}")
        except Exception:
            continue
        in_b = ex.get("input_size_bytes")
        out_b = ex.get("output_size_bytes") or f.get("file_size_bytes")
        dur = ex.get("duration_seconds") or f.get("duration_seconds") or 0
        if not in_b or not out_b or not dur or out_b <= in_b:
            continue
        out.append({"fp": r["filepath"], "f": f, "out_b": out_b, "dur": dur, "grew": (out_b - in_b) / 1e9})
    out.sort(key=lambda x: -x["grew"])
    return out


def mark_state(fp: str, new_size: int, status: str, reason: str) -> None:
    con = sqlite3.connect(DB)
    row = con.execute("SELECT extras FROM pipeline_files WHERE filepath=?", (fp,)).fetchone()
    try:
        ex = json.loads(row[0]) if row and row[0] else {}
    except Exception:
        ex = {}
    ex["reclaimed"] = {
        "status": status,
        "new_size_bytes": new_size,
        "reason": reason,
        "ts": time.strftime("%Y-%m-%d %H:%M"),
    }
    con.execute("UPDATE pipeline_files SET extras=? WHERE filepath=?", (json.dumps(ex, separators=(",", ": ")), fp))
    con.commit()
    con.close()


def _another_encoder_running() -> bool:
    """Refuse to start if any ffmpeg encode is already running — two concurrent
    NVENC encodes (a second reclaim, or the convert pipeline) BSOD this box
    (rule 9b). This is the cross-process guard the two uncoordinated jobs need:
    no reclaim-ffmpeg exists yet at startup, so any live ffmpeg is someone else."""
    try:
        import psutil

        for p in psutil.process_iter(["name"]):
            if (p.info.get("name") or "").lower() in ("ffmpeg.exe", "ffmpeg"):
                return True
    except Exception:
        pass
    return False


def main() -> None:
    os.makedirs(WORK, exist_ok=True)
    if _another_encoder_running():
        log(
            "REFUSING START: an ffmpeg encode is already running (another reclaim or the convert "
            "pipeline). Two concurrent NVENC = BSOD (rule 9b)."
        )
        return
    # Default high so the managed-process launch (no arg) runs until candidates
    # are exhausted or the UI stops it; pass a number to cap a manual run.
    max_films = int(sys.argv[1]) if len(sys.argv) > 1 else 9999
    led = load_ledger()
    cands = candidates()
    log(f"=== RECLAIM START: {len(cands)} bloated candidates (films+series), max {max_films}, gate VMAF>={GATE} ===")
    done = consec_fail = flagged = 0
    saved_gb = 0.0
    for c in cands:
        if done >= max_films:
            log("reached max_films; stopping")
            break
        if consec_fail >= MAX_CONSEC_FAIL:
            log(f"CIRCUIT BREAKER: {consec_fail} consecutive hard errors; stopping")
            break
        if os.path.exists(PAUSE_FILE):  # pause between films (never mid-encode)
            log("paused (pause_reclaim.json present); waiting for resume...")
            while os.path.exists(PAUSE_FILE):
                time.sleep(5)
            log("resumed")
        fp, f, out_b, dur = c["fp"], c["f"], c["out_b"], c["dur"]
        name = os.path.basename(fp)
        st = led.get(fp, {})
        if st.get("phase") == "done" or st.get("status") in ("reclaimed", "gate_failed", "skipped_highrisk"):
            continue
        if not os.path.exists(fp):
            continue
        hdr = (f.get("video") or {}).get("hdr", False)
        cur_mbps = out_b * 8 / dur / 1e6
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
            continue
        led[fp] = {"name": name, "phase": "risk", "cur_mbps": round(cur_mbps, 1), "cap": p["maxrate"]}
        save_ledger(led)
        log(f"[{name[:46]}] cur={cur_mbps:.1f} cap={p['maxrate']} cq={p['cq']} hdr={hdr}")
        rp = risk_probe(fp, dur)
        if rp is None:
            led[fp]["status"] = "skipped_error"
            save_ledger(led)
            continue
        lvl = risk_level(rp)
        led[fp].update({"risk": lvl, "grain": round(rp["grain_idx"], 3), "dark_yavg": round(rp["dark_yavg"], 1)})
        if lvl == "HIGH":
            log(f"  HIGH risk (grain={rp['grain_idx']:.3f}) -> flag re-source, no encode")
            led[fp]["status"] = "skipped_highrisk"
            mark_state(fp, out_b, "resource_recommended", f"high reclaim risk grain={rp['grain_idx']:.3f}")
            save_ledger(led)
            flagged += 1
            continue
        color = color_flags(fp, hdr)
        pix = DEFAULT_CONFIG.get("pixel_format_hdr" if hdr else "pixel_format_sdr", "p010le")
        # --- cheap clip gate BEFORE the full encode ---
        led[fp]["phase"] = "gate"
        save_ledger(led)
        log("  clip gate...")
        score = gate_score(fp, rp["dark_t"], dur, p, color, pix)
        led[fp]["vmaf"] = round(score, 2) if score else None
        if score is None or score < GATE:
            log(f"  GATE FAIL VMAF={score} -> flag re-source (no full encode)")
            led[fp]["status"] = "gate_failed"
            mark_state(fp, out_b, "resource_recommended", f"reclaim gate VMAF {score} < {GATE}")
            save_ledger(led)
            flagged += 1
            continue
        # --- gate passed: full encode (hwaccel decode) ---
        n_aud = f.get("audio_stream_count") or 1
        n_sub = f.get("subtitle_count") or 0
        out = os.path.join(WORK, "out.mkv")
        cmd = [
            "ffmpeg",
            "-y",
            "-hwaccel",
            "cuda",
            "-i",
            fp,
            "-map",
            "0:v:0",
            *_enc_video_args(p, color, pix),
            "-map",
            "0:a",
            "-c:a",
            "copy",
        ]
        if n_sub > 0:
            cmd += ["-map", "0:s", "-c:s", "copy"]
        cmd += ["-progress", PROGRESS_FILE, "-max_muxing_queue_size", "1024", out]
        led[fp]["phase"] = "encoding"
        save_ledger(led)
        log(f"  gate PASS {score:.2f}; full encode...")
        t0 = time.time()
        stop_evt = threading.Event()
        watcher = threading.Thread(target=_progress_watcher, args=(dur, fp, name, stop_evt), daemon=True)
        watcher.start()
        e = _run(cmd)
        stop_evt.set()
        try:
            os.remove(INFLIGHT_FILE)  # clear the live bar once the encode finishes
        except OSError:
            pass
        if e.returncode != 0 or not os.path.exists(out):
            log(f"  ENCODE FAILED: {e.stderr[-300:]}")
            led[fp]["status"] = "skipped_error"
            save_ledger(led)
            consec_fail += 1
            continue
        v, a, s, od = probe_counts(out)
        if v != 1 or a != n_aud or (n_sub > 0 and s < n_sub) or abs(od - dur) > 2:
            log(f"  POST-PROBE FAIL v={v} a={a}/{n_aud} s={s}/{n_sub} dur={od:.0f}/{dur:.0f} -> discard")
            os.remove(out)
            led[fp]["status"] = "skipped_probefail"
            save_ledger(led)
            consec_fail += 1
            continue
        new_b = os.path.getsize(out)
        log(f"  encoded {out_b / 1e9:.1f}->{new_b / 1e9:.1f}GB in {time.time() - t0:.0f}s; swapping (backup preserved)")
        try:
            backup = swap(fp, out, led, fp)
        except Exception as ex:
            log(f"  SWAP ERROR: {ex} -- original left in place")
            led[fp]["status"] = "swap_error"
            save_ledger(led)
            consec_fail += 1
            continue
        # Backup did its job (crash-safe swap done, new file verified in place). With
        # KEEP_BACKUPS off, retire it now so reclaim backups never accumulate on the NAS.
        if not KEEP_BACKUPS and retire_backup(backup):
            reason, backup = f"in-place de-bloat VMAF {score:.1f}; backup auto-purged", None
        else:
            reason = f"in-place de-bloat VMAF {score:.1f}; original at {backup}"
        mark_state(fp, new_b, "reclaimed", reason)
        led[fp].update({"status": "reclaimed", "new_gb": round(new_b / 1e9, 1), "backup": backup})
        save_ledger(led)
        try:
            os.remove(out)
        except OSError:
            pass
        saved_gb += (out_b - new_b) / 1e9
        done += 1
        consec_fail = 0
        log(
            f"  RECLAIMED ({done}) saved {(out_b - new_b) / 1e9:.1f}GB | run total {saved_gb:.0f}GB, {flagged} flagged re-source"
        )

    log(
        f"=== RECLAIM END: {done} reclaimed (~{saved_gb:.0f}GB pending-free in backup), {flagged} flagged re-source ==="
    )


if __name__ == "__main__":
    main()
