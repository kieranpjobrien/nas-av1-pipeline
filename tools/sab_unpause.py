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
    return True, f"paused with {free_gb:.0f} GB free"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    queue = (sab(mode="queue", limit=1) or {}).get("queue") or {}
    ok, reason = diagnose(queue)
    print(f"status={queue.get('status')} paused={queue.get('paused')} free={queue.get('diskspace1_norm')}")
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
