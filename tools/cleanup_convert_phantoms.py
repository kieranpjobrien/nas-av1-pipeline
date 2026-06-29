"""One-off cleanup for the convert→replace stale-entry bug.

When the encoder converted a ``.mp4`` source to a ``.mkv`` AV1 output it
replaced the file on disk under a different name but left the dead ``.mp4``
path behind in media_report.json and the state DB. On the next restart that
dead path got re-queued, failed to fetch (``SOURCE_MISSING``), and was
mis-flagged ``flagged_corrupt`` — a phantom duplicate on the dashboard. The
root cause is fixed in ``full_gamut.finalize_upload`` (it now purges the dead
path on the DONE transition), but the rows created before that fix are still
sitting in the two stores. This tool removes them.

Candidates are sourced from BOTH the media_report .mp4 entries AND the .mp4
state rows (any status) — keying off only already-flagged rows misses the
un-flagged stale report entries a restart would queue and freshly flag. A
candidate is acted on ONLY when it unambiguously matches the convert-bug
signature:

  * path ends in ``.mp4``
  * the ``.mp4`` no longer exists on disk          (never drop a live file)
  * a sibling ``.mkv`` DOES exist on disk          (the converted output)
  * that ``.mkv`` is AV1 per media_report          (it really is our output)

A genuinely-missing file (deleted, or lost in the 2026-06-19 NAS drive-4
incident) fails the "sibling .mkv present on disk" test and is left alone, as
are the ``.mkv``-path flagged_corrupt rows (a different case). For each match it
drops the stale media_report entry (single-writer report_lock path, rules
12/13) and deletes the stale state row (done / flagged_corrupt / pending /
error — active rows are skipped so a resuming worker is never raced; the
scanner reconciles done/pending/error but deliberately never clears
flagged_*).

Dry-run by default — prints the plan and changes nothing. Pass ``--apply`` to
execute. ``--apply`` REFUSES to run while the pipeline looks live (active
ffmpeg, or a pipeline.log heartbeat in the last 2 minutes) because mutating
media_report alongside the live encoder is the cascade-of-loss hazard (rule
13). Stop/pause the pipeline first.

Usage:
    uv run python -m tools.cleanup_convert_phantoms            # dry run
    uv run python -m tools.cleanup_convert_phantoms --apply    # execute (paused only)
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time

from paths import MEDIA_REPORT, PIPELINE_STATE_DB

_STAGING = os.path.dirname(str(MEDIA_REPORT))
_PIPELINE_LOG = os.path.join(_STAGING, "pipeline.log")
_HEARTBEAT_WINDOW_S = 120  # gap filler heartbeats every 62s; >2 min gap = not live


def _pipeline_looks_live() -> tuple[bool, str]:
    """Best-effort liveness check (rules 5, 14). Returns (is_live, reason)."""
    try:
        import psutil
    except Exception:  # noqa: BLE001 — psutil optional
        psutil = None

    # (a) Active ffmpeg child = an encode in flight, regardless of registry.
    if psutil is not None:
        try:
            for p in psutil.process_iter(["name"]):
                if (p.info.get("name") or "").lower() in ("ffmpeg.exe", "ffmpeg"):
                    return True, f"active ffmpeg (pid {p.pid})"
        except Exception:  # noqa: BLE001 — transient probe failure
            pass

    # (b) The process registry is authoritative (rule 5): if a 'pipeline' role
    # is registered, its PID liveness IS the answer — a dead registered PID
    # means a clean stop, so don't second-guess it with the log-age proxy.
    try:
        reg = os.path.join(_STAGING, "control", "agents.registry.json")
        with open(reg, encoding="utf-8") as f:
            pipe = [e for e in json.load(f) if e.get("role") == "pipeline"]
        if pipe:
            for e in pipe:
                pid = int(e.get("pid", 0))
                alive = psutil.pid_exists(pid) if psutil is not None else True
                if pid > 0 and alive:
                    return True, f"registered pipeline supervisor alive (pid {pid})"
            return False, "registered pipeline supervisor not running (registry stale entry)"
    except Exception:  # noqa: BLE001 — no/unreadable registry → fall through
        pass

    # (c) Fallback when there's no registry signal: fresh pipeline.log heartbeat.
    try:
        age = time.time() - os.path.getmtime(_PIPELINE_LOG)
        if age < _HEARTBEAT_WINDOW_S:
            return True, f"pipeline.log heartbeat {age:.0f}s ago (< {_HEARTBEAT_WINDOW_S}s)"
    except OSError:
        pass

    return False, "no ffmpeg, no live registered supervisor, no recent heartbeat"


def _load_report_files() -> dict[str, dict]:
    """Read media_report.json untorn (atomic-replace target) without taking the
    lock — a plain read never blocks the live writer."""
    with open(MEDIA_REPORT, encoding="utf-8") as f:
        rep = json.load(f)
    return {e.get("filepath"): e for e in rep.get("files", [])}


def _codec(entry: dict | None) -> str | None:
    return ((entry or {}).get("video") or {}).get("codec_raw")


_ACTIVE_STATUSES = ("processing", "uploading", "fetching", "qualifying")


def _classify() -> tuple[list[dict], dict[str, int]]:
    """Find stale convert-bug .mp4 artifacts in both stores.

    Candidates are sourced from BOTH the media_report .mp4 entries AND the
    .mp4 state rows (any status). Keying off only already-flagged rows misses
    the un-flagged stale report entries that a restart would queue and freshly
    flag — observed live 2026-06-29 when the pipeline re-flagged Prometheus &
    others seconds after a restart. Each match dict:
    ``{mp4, mkv, mp4_in_report, state_status}``.
    """
    by_path = _load_report_files()

    con = sqlite3.connect(f"file:{PIPELINE_STATE_DB}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    state_status = {
        r["filepath"]: r["status"]
        for r in con.execute("SELECT filepath, status FROM pipeline_files WHERE filepath LIKE '%.mp4'").fetchall()
    }
    con.close()

    candidates = {fp for fp in by_path if fp and fp.lower().endswith(".mp4")} | set(state_status)

    matches: list[dict] = []
    skips = {"mp4_still_on_disk": 0, "no_mkv_on_disk": 0, "mkv_not_av1": 0, "disk_error": 0}

    for fp in sorted(candidates):
        stem, ext = os.path.splitext(fp)
        if ext.lower() != ".mp4":
            continue
        mkv = stem + ".mkv"
        try:
            if os.path.exists(fp):
                skips["mp4_still_on_disk"] += 1  # live file — never drop (rule 8)
                continue
            if not os.path.exists(mkv):
                skips["no_mkv_on_disk"] += 1  # genuinely missing, not a convert — leave it
                continue
        except OSError:
            skips["disk_error"] += 1  # NAS unreachable — don't guess, skip
            continue
        if _codec(by_path.get(mkv)) != "av1":
            skips["mkv_not_av1"] += 1  # sibling isn't our AV1 output — leave it
            continue
        matches.append(
            {"mp4": fp, "mkv": mkv, "mp4_in_report": fp in by_path, "state_status": state_status.get(fp)}
        )

    return matches, skips


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="execute changes (default: dry run)")
    args = ap.parse_args()

    matches, skips = _classify()

    in_report = [m for m in matches if m["mp4_in_report"]]
    deletable = [m for m in matches if m["state_status"] and m["state_status"] not in _ACTIVE_STATUSES]
    active = [m for m in matches if m["state_status"] in _ACTIVE_STATUSES]

    print(f"Stale convert-bug .mp4 artifacts matched: {len(matches)}")
    print(f"  stale media_report entries to drop     : {len(in_report)}")
    print(f"  state rows to delete (non-active)      : {len(deletable)}")
    if active:
        print(f"  state rows SKIPPED (active, not raced)  : {len(active)}")
    print(f"Skipped (not the convert signature)      : {skips}")
    print()
    for m in matches:
        bits = []
        if m["mp4_in_report"]:
            bits.append("report")
        if m["state_status"]:
            bits.append(f"state:{m['state_status']}")
        print(f"  [{'+'.join(bits) or 'none'}] {os.path.basename(m['mp4'])}")

    if not matches:
        print("\nNothing to do.")
        return 0

    if not args.apply:
        print("\nDRY RUN -- nothing changed. Re-run with --apply (pipeline paused) to execute.")
        return 0

    live, reason = _pipeline_looks_live()
    if live:
        print(f"\nREFUSING to apply: pipeline looks live ({reason}).")
        print("Stop/pause the pipeline first -- mutating media_report alongside the live")
        print("encoder is the cascade-of-loss hazard (rule 13).")
        return 2

    # 1. Drop stale media_report entries (single-writer report_lock path).
    from pipeline.report import remove_entry

    removed_report = 0
    for m in in_report:
        if remove_entry(m["mp4"]):
            removed_report += 1
        else:
            print(f"  WARNING: media_report remove failed for {os.path.basename(m['mp4'])}")

    # 2. Delete the stale state rows (done/flagged_corrupt/pending/error). The
    # scanner reconciles done/pending/error but deliberately never clears
    # flagged_*; active rows are skipped so a (paused-but-resuming) worker is
    # never raced.
    con = sqlite3.connect(PIPELINE_STATE_DB)
    try:
        deleted_state = 0
        for m in deletable:
            cur = con.execute(
                "DELETE FROM pipeline_files WHERE filepath = ? AND status = ?",
                (m["mp4"], m["state_status"]),
            )
            deleted_state += cur.rowcount
        con.commit()
    finally:
        con.close()

    print(f"\nApplied: removed {removed_report} stale media_report entries, "
          f"deleted {deleted_state} stale state rows.")

    # Re-verify: no convert-bug phantoms should remain.
    remaining, _ = _classify()
    print(f"Re-check: {len(remaining)} convert-bug phantom artifacts remain (expected 0).")
    return 0 if not remaining else 1


if __name__ == "__main__":
    sys.exit(main())
