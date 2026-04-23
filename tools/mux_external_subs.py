"""Mux external English .srt sidecars into their MKV files. NO STRIPS.

Pure additive operation: adds one subtitle stream (from the .srt) and keeps
every existing audio / video / subtitle track untouched. After a successful
mux + verify, the external .srt is deleted (it's now internal).

Triggered by: files with an external English .srt but no internal English
sub. Usually 1,000-2,000 files at a time after Bazarr downloads catch up.

Safety gates:
  1. remote_strip_and_mux is called with audio_keep_ids=None AND
     sub_keep_ids=None — meaning "keep all tracks, no stripping". The
     existing ValueError guard in remote_strip_and_mux refuses empty lists;
     None bypasses the --audio-tracks and --subtitle-tracks flags entirely.
  2. After mkvmerge returns, we ffprobe the output and assert:
       - video stream count >= 1
       - audio stream count >= input audio count (not dropped)
       - subtitle stream count == input sub count + 1 (the one we added)
     If any fail, delete tmp, keep source untouched.
  3. Parallelism is capped at 3 to not hammer the NAS.

Usage:
    uv run python -m tools.mux_external_subs               # dry-run
    uv run python -m tools.mux_external_subs --execute     # do it
    uv run python -m tools.mux_external_subs --limit 50    # cap count
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

from paths import MEDIA_REPORT
from pipeline.nas_worker import NAS, SERVER, remote_identify, remote_strip_and_mux, unc_to_container_path
from pipeline.streams import is_hi_external, is_hi_internal

ENG_LANGS = {"en", "eng", "english"}


# Thin wrappers preserve existing call-site names; logic lives in pipeline.streams.
def _is_hi_ext(s: dict) -> bool:
    return is_hi_external(s.get("filename") or "")


def _is_hi_int(s: dict) -> bool:
    return is_hi_internal(s)


def find_candidates() -> list[dict]:
    """Scan media_report for AV1 files with external English .srt, no internal English."""
    with open(MEDIA_REPORT, "rb") as f:
        d = json.load(f)

    targets = []
    for e in d.get("files", []):
        codec = ((e.get("video") or {}).get("codec_raw") or "").lower()
        if codec != "av1":
            continue
        internal = e.get("subtitle_streams") or []
        external = e.get("external_subtitles") or []
        has_int_en = any(
            (s.get("language") or "").lower().strip() in ENG_LANGS and not _is_hi_int(s)
            for s in internal
        )
        if has_int_en:
            continue
        eng_ext = [
            s for s in external
            if (s.get("language") or "").lower().strip() in ENG_LANGS and not _is_hi_ext(s)
        ]
        if not eng_ext:
            continue
        # Prefer the shortest-named English sub as the canonical one to mux
        eng_ext.sort(key=lambda s: len(s.get("filename") or ""))
        chosen = eng_ext[0]
        parent_dir = os.path.dirname(e.get("filepath", ""))
        sidecar_path = os.path.join(parent_dir, chosen.get("filename", ""))
        targets.append({
            "filepath": e["filepath"],
            "sidecar": sidecar_path,
            "internal_audio_count": len(e.get("audio_streams") or []),
            "internal_sub_count": len(internal),
            "library_type": e.get("library_type", ""),
        })
    return targets


def mux_one(target: dict, machine: dict) -> tuple[str, str]:
    """Mux one file. Returns (status, message).

    status is one of: ok, skip, fail
    """
    fp = target["filepath"]
    sidecar = target["sidecar"]
    expected_audio = target["internal_audio_count"]
    expected_subs = target["internal_sub_count"] + 1  # +1 for the new one

    filename = os.path.basename(fp)

    if not os.path.exists(fp):
        return "skip", f"source gone: {filename}"
    if not os.path.exists(sidecar):
        return "skip", f"sidecar gone: {os.path.basename(sidecar)}"

    tmp_path = fp + ".submux_tmp.mkv"
    container_fp = unc_to_container_path(fp)
    container_tmp = unc_to_container_path(tmp_path)
    container_sub = unc_to_container_path(sidecar)

    # Call remote mkvmerge with KEEP-ALL (no strip) + external sub appended
    # audio_keep_ids=None AND sub_keep_ids=None means mkvmerge gets no
    # --audio-tracks or --subtitle-tracks flag — every existing track is kept.
    try:
        result = remote_strip_and_mux(
            machine,
            container_fp,
            container_tmp,
            audio_keep_ids=None,  # KEEP ALL — no strip
            sub_keep_ids=None,    # KEEP ALL — no strip
            no_subs=False,
            external_sub_paths=[(container_sub, "eng")],
            timeout=600,
        )
    except Exception as e:
        return "fail", f"mkvmerge invocation error: {e}"

    if result.returncode >= 2:
        combined = (result.stderr or "") + "\n" + (result.stdout or "")
        err_lines = [ln for ln in combined.splitlines() if not ln.lstrip().startswith("**")]
        err = "\n".join(err_lines).strip()[:200]
        return "fail", f"mkvmerge rc={result.returncode}: {err}"

    # Wait for tmp file to appear (SMB cache)
    for _ in range(5):
        if os.path.exists(tmp_path):
            break
        time.sleep(1)
    if not os.path.exists(tmp_path):
        return "fail", "tmp file not visible over SMB after 5s"

    # Post-mkvmerge verify — ffprobe tmp, count streams.
    try:
        pr = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", tmp_path],
            capture_output=True, text=True, timeout=60,
        )
        probed = json.loads(pr.stdout or "{}")
    except Exception as e:
        try: os.remove(tmp_path)
        except OSError: pass
        return "fail", f"post-mkvmerge ffprobe error: {e}"

    streams = probed.get("streams", []) or []
    vcount = sum(1 for s in streams if s.get("codec_type") == "video")
    acount = sum(1 for s in streams if s.get("codec_type") == "audio")
    scount = sum(1 for s in streams if s.get("codec_type") == "subtitle")

    if vcount < 1:
        try: os.remove(tmp_path)
        except OSError: pass
        return "fail", f"verify: 0 video streams in output"
    # Absolute floor: zero audio is NEVER a valid mux-sub output. The equality check
    # below against `expected_audio` is tautologically satisfied when the SOURCE
    # was already damaged (expected_audio=0, acount=0) — which would rubber-stamp
    # pre-existing damage as "muxed OK". Enforce a hard floor first.
    if acount < 1:
        try: os.remove(tmp_path)
        except OSError: pass
        return "fail", (
            f"verify: output has 0 audio streams (source likely already damaged; "
            f"expected_audio from scanner was {expected_audio} — refusing to "
            f"launder a zero-audio file as successful mux)"
        )
    if acount != expected_audio:
        try: os.remove(tmp_path)
        except OSError: pass
        return "fail", f"verify: audio count {acount} != expected {expected_audio}"
    if scount != expected_subs:
        try: os.remove(tmp_path)
        except OSError: pass
        return "fail", f"verify: sub count {scount} != expected {expected_subs}"

    # Replace source with muxed output
    try:
        os.replace(tmp_path, fp)
    except OSError as e:
        try: os.remove(tmp_path)
        except OSError: pass
        return "fail", f"replace failed: {e}"

    # Delete the now-internal sidecar
    try:
        os.remove(sidecar)
    except OSError as e:
        return "ok", f"muxed OK (sidecar delete failed: {e})"

    return "ok", f"{filename} [{vcount}v {acount}a {scount}s]"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="Cap number of files (0 = no cap)")
    ap.add_argument("--workers", type=int, default=2, help="Parallel NAS SSH workers")
    ap.add_argument("--machine", choices=("nas", "server"), default="nas")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    machine = NAS if args.machine == "nas" else SERVER
    if not machine.get("host"):
        print(f"ERROR: {args.machine}_SSH_HOST env var not set")
        sys.exit(1)

    targets = find_candidates()
    print(f"Found {len(targets)} AV1 files with external English .srt, no internal English sub.")
    if args.limit and len(targets) > args.limit:
        targets = targets[: args.limit]
        print(f"Limited to {len(targets)} for this run.")

    if not args.execute:
        print("\n[DRY RUN — pass --execute to actually mux]\n")
        for t in targets[:10]:
            print(f"  {os.path.basename(t['filepath'])}  <- {os.path.basename(t['sidecar'])}")
        if len(targets) > 10:
            print(f"  ... +{len(targets) - 10} more")
        return

    if not targets:
        print("Nothing to do.")
        return

    print(f"Running with {args.workers} parallel worker(s) on {machine['label']}...")
    ok = skip = fail = 0
    fail_msgs: list[str] = []
    started = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(mux_one, t, machine): t for t in targets}
        for i, fut in enumerate(as_completed(futures), 1):
            t = futures[fut]
            status, msg = fut.result()
            if status == "ok":
                ok += 1
                print(f"  [{i}/{len(targets)}] OK: {msg}")
            elif status == "skip":
                skip += 1
                print(f"  [{i}/{len(targets)}] SKIP: {msg}")
            else:
                fail += 1
                fail_msgs.append(msg)
                print(f"  [{i}/{len(targets)}] FAIL: {os.path.basename(t['filepath'])} — {msg}")

    elapsed = time.time() - started
    print(f"\n=== Done in {elapsed/60:.1f} min: {ok} muxed, {skip} skipped, {fail} failed ===")
    if fail_msgs:
        print("\nFailure reasons (first 20):")
        for m in fail_msgs[:20]:
            print(f"  {m}")


if __name__ == "__main__":
    main()
