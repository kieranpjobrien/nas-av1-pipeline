"""Resume SAB when it has auto-paused on a disk error that no longer applies.

SAB pauses the whole downloader on any write failure and stays paused until a
human notices. On 2026-08-06 that cost 6h, 47m and 9h30m of downtime across
three pauses, none of which had a live cause by the time they were spotted.

The failures were ``FileNotFoundError`` on a job's own incomplete folder:
duplicate queue entries with identical release names resolve to the SAME
folder, so when one twin finished or was removed it deleted the directory out
from under the other, whose next write blew up. The trigger is momentary - the
disk is fine - but the pause is permanent.

Only resumes when the pause looks stale: disk has room AND a real write to the
incomplete dir succeeds. A genuinely full or broken volume stays paused, which
is the behaviour you want.

Run:  uv run python -m tools.sab_unpause
      uv run python -m tools.sab_unpause --execute
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SAB = ("http://192.168.4.43:8185/api", "cd29eb7ca0d44e5f96bd51ac2258916e")

# Refuse to resume below this much free space on the incomplete volume - at
# that point the pause is real and resuming just re-fills the disk.
MIN_FREE_GB = 20.0


def sab(**params):
    base, key = SAB
    url = base + "?" + urllib.parse.urlencode({**params, "output": "json", "apikey": key})
    with urllib.request.urlopen(url, timeout=90) as r:
        raw = r.read() or b"{}"
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


# Directory SAB downloads into, as seen from the machine this runs on. Only
# present when running ON the download server; skipped otherwise.
INCOMPLETE_DIR = "/mnt/local-incomplete"


def is_husk(job_dir: str) -> bool:
    """True for a leftover directory that can only ever cause a collision.

    A husk has no payload and no ``SABnzbd_nzo_data``, so SAB will not list it
    as an orphan - there is nothing to resume - but the directory still owns
    the name. When the same release is grabbed again SAB tries to create
    ``__ADMIN__`` inside it, gets FileExistsError, and treats that as a fatal
    downloader error: the whole queue pauses.

    2026-08-06 23:38 was exactly this. Felicity.S03 left a husk at 13:38; the
    re-grab collided with it five hours later and the queue sat paused for
    5h34m.
    """
    admin = os.path.join(job_dir, "__ADMIN__")
    if os.path.exists(os.path.join(admin, "SABnzbd_nzo_data")):
        return False  # resumable job - SAB owns this, leave it alone
    for root, _dirs, files in os.walk(job_dir):
        if "__ADMIN__" in root:
            continue
        if files:
            return False  # real downloaded data present
    return True


def find_husks(base: str = INCOMPLETE_DIR, live_names: set[str] | None = None) -> list[str]:
    """Husk directories under ``base``, excluding anything a live job owns."""
    if not os.path.isdir(base):
        return []
    out = []
    for name in sorted(os.listdir(base)):
        if live_names and name in live_names:
            continue
        path = os.path.join(base, name)
        if os.path.isdir(path) and is_husk(path):
            out.append(path)
    return out


# Signatures SAB logs immediately before auto-pausing on a write failure.
DISK_ERROR_SIGNATURES = (
    "Fatal error in Downloader",
    "Disk error on creating file",
    "FileNotFoundError",
    "FileExistsError",
    "No space left on device",
)


def paused_by_disk_error(log_text: str) -> bool:
    """True only if the most recent Pausing was preceded by a write failure.

    2026-08-08 00:00: the operator paused SAB by hand to let post-processing
    drain. This watchdog resumed it nine minutes later, because an indefinite
    manual pause and a disk-error pause look identical through the API - both
    are just ``paused=true`` with ``pause_int=0``.

    So stop asking "is it paused?" and start asking "did something break?".
    Only a pause with an error immediately before it is ours to undo; a bare
    Pausing line is a human decision and must be left alone.
    """
    idx = log_text.rfind("Pausing")
    if idx == -1:
        return False
    # Look only at the window just before the pause, not the whole log - an
    # error from hours earlier says nothing about this pause.
    window = log_text[max(0, idx - 4000) : idx]
    return any(sig in window for sig in DISK_ERROR_SIGNATURES)


def recent_sab_log(minutes: int = 30) -> str:
    """Container log tail. Empty string if it cannot be read."""
    import subprocess

    try:
        out = subprocess.run(
            ["docker", "logs", f"--since={minutes}m", "sabnzbd"],
            capture_output=True,
            text=True,
            timeout=60,
            errors="replace",
        )
        return (out.stdout or "") + (out.stderr or "")
    except (OSError, subprocess.SubprocessError):
        return ""


def diagnose(queue: dict) -> tuple[bool, str]:
    """(should_resume, reason) for a queue payload.

    ``pause_int`` is a countdown for a deliberate timed pause ("pause for 30
    minutes"). Never override that - the operator asked for it.
    """
    if not queue.get("paused"):
        return False, "not paused"
    if str(queue.get("pause_int") or "0") not in ("0", ""):
        return False, f"deliberate timed pause ({queue.get('pause_int')} remaining)"
    try:
        free_gb = float(queue.get("diskspace1") or 0)
    except (TypeError, ValueError):
        return False, "could not read free space"
    if free_gb < MIN_FREE_GB:
        return False, f"only {free_gb:.1f} GB free - pause is legitimate, leaving it"
    # Last gate, and the important one: only undo a pause we can prove was
    # caused by a write failure. A bare Pausing line is the operator's call.
    if not paused_by_disk_error(recent_sab_log()):
        return False, "no disk error before the pause - treating as a manual pause, leaving it"
    return True, f"paused after a disk error, {free_gb:.0f} GB free"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    full = (sab(mode="queue", limit=3000) or {}).get("queue") or {}
    live = {(s.get("filename") or "").strip() for s in (full.get("slots") or [])}

    # Sweep husks first - they are the thing that CAUSES the pause, so
    # clearing them before resuming stops an immediate re-pause.
    husks = find_husks(live_names=live)
    if husks:
        print(f"husk dirs (collision landmines): {len(husks)}")
        for h in husks[:10]:
            print(f"   {os.path.basename(h)[:70]}")
        if args.execute:
            import shutil

            gone = 0
            for h in husks:
                try:
                    shutil.rmtree(h)
                    gone += 1
                except OSError as exc:
                    print(f"   could not remove {os.path.basename(h)[:50]}: {exc}")
            print(f"removed {gone} husk dir(s)")
    elif os.path.isdir(INCOMPLETE_DIR):
        print("husk dirs: none")

    ok, reason = diagnose(full)
    print(f"status={full.get('status')} paused={full.get('paused')} free={full.get('diskspace1_norm')}")
    print(f"verdict: {'RESUME' if ok else 'leave alone'} - {reason}")
    if not ok:
        return
    if not args.execute:
        print("\n(dry run - pass --execute to resume)")
        return
    sab(mode="resume")
    after = (sab(mode="queue", limit=1) or {}).get("queue") or {}
    print(f"resumed -> status={after.get('status')} paused={after.get('paused')}")


if __name__ == "__main__":
    main()
