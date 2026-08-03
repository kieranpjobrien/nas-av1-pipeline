"""Fail unrecoverable SAB jobs early instead of waiting for SAB to notice.

SAB's fail_hopeless_jobs only fires once it can prove par2 cannot cover the
missing articles - and par2 blocks sit at the END of an NZB. So a job that is
already doomed at 10% downloaded still has to pull the remaining 90% before
SAB reaches the repair files and gives up. On a 69-day-old backfill queue that
wastes hours of connections per job.

``mbmissing`` is SAB's own count of articles it has already confirmed absent
on every configured server. Any non-trivial value means the job cannot reach
the 100% that req_completion_rate demands. Kill it now, blocklist through the
*arr so the same release is not re-grabbed, and let a search find a live one.

Run:  uv run python -m tools.kill_doomed
      uv run python -m tools.kill_doomed --execute
      uv run python -m tools.kill_doomed --execute --threshold 5
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
SONARR = ("http://192.168.4.43:27483", "80d52d39113c45be9f3b94c3c2cfbdd1")
RADARR = ("http://192.168.4.43:27484", "2aeb7de56ea44035a438447a5911f77a")

# MB of confirmed-absent articles above which a job is treated as unrecoverable.
# Deliberately low: par2 recovery on a typical release covers ~10% of payload,
# but anything SAB has already proven missing on every server is not coming
# back, and the download continues to burn connections until SAB works it out.
DEFAULT_THRESHOLD_MB = 10.0

# The absolute test alone is far too lenient early on. ``mbmissing`` only counts
# articles SAB has ALREADY tried, so a job 16% in has only tested 16% of the
# payload. Rick and Morty S03E06 sat at 47 MB missing of a 2 GB release - under
# any sane absolute threshold - while actually having lost 13% of everything
# fetched so far, i.e. heading for ~260 MB missing by the end. par2 recovery on
# a typical release covers 5-10%, so anything above that is already dead and
# every further byte it pulls is wasted.
#
# Judge on the proportion of FETCHED payload proven dead, not the raw MB.
DEFAULT_LOSS_PCT = 5.0
# Below this many MB fetched the ratio is noise (one bad article on a 20 MB
# sample reads as 5%), so let the absolute test handle those.
MIN_FETCHED_MB = 50.0


def arr(base_key, path, method="GET", body=None):
    base, key = base_key
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method)
    req.add_header("X-Api-Key", key)
    if data:
        req.add_header("Content-Type", "application/json")
    raw = urllib.request.urlopen(req, timeout=120).read()
    return json.loads(raw) if raw else {}


def sab(**params):
    base, key = SAB
    url = base + "?" + urllib.parse.urlencode({**params, "output": "json", "apikey": key})
    with urllib.request.urlopen(url, timeout=90) as r:
        return json.loads(r.read() or b"{}")


def _pct(slot) -> float:
    try:
        return float(slot.get("percentage") or 0)
    except (TypeError, ValueError):
        return 0.0


def judge(slot, threshold_mb: float, loss_pct: float) -> tuple[bool, str, float]:
    """Is this job unrecoverable, and on which test.

    Returns (doomed, reason, projected_loss_pct).
    """
    missing = float(slot.get("mbmissing") or 0)
    total_mb = float(slot.get("mb") or 0)
    fetched = total_mb * _pct(slot) / 100.0
    loss = (missing / fetched * 100.0) if fetched > 0 else 0.0

    if fetched >= MIN_FETCHED_MB and loss >= loss_pct:
        return True, f"{loss:.1f}% of fetched payload dead", loss
    if missing >= threshold_mb:
        return True, f"{missing:.0f}MB confirmed missing", loss
    return False, "", loss


def doomed(threshold_mb: float, loss_pct: float = DEFAULT_LOSS_PCT) -> list[dict]:
    slots = (sab(mode="queue", limit=2000).get("queue") or {}).get("slots") or []
    out = []
    for s in slots:
        bad, reason, loss = judge(s, threshold_mb, loss_pct)
        if not bad:
            continue
        out.append(
            {
                "nzo_id": s["nzo_id"],
                "name": s.get("filename", ""),
                "missing_mb": float(s.get("mbmissing") or 0),
                "loss_pct": round(loss, 1),
                "reason": reason,
                "pct": s.get("percentage"),
                "gb": round(float(s.get("mb") or 0) / 1024, 2),
                "age": s.get("avg_age"),
                "priority": s.get("priority"),
            }
        )
    out.sort(key=lambda x: -x["loss_pct"])
    return out


def blocklist_and_remove(job) -> str:
    """Remove via the *arr so the release is blocklisted, else fall back to SAB.

    The fallback matters and is NOT equivalent: a SAB-only delete leaves the
    release un-blocklisted, so the *arr re-grabs the identical dead post within
    minutes. That happened with an upscale release on 2026-08-03.
    """
    nzo = job["nzo_id"]
    for api in (SONARR, RADARR):
        try:
            for rec in arr(api, "/api/v3/queue?pageSize=1000").get("records", []):
                if (rec.get("downloadId") or "").upper() == nzo.upper():
                    arr(api, f"/api/v3/queue/{rec['id']}?removeFromClient=true&blocklist=true", "DELETE")
                    return "blocklisted"
        except Exception:  # noqa: BLE001,PERF203
            continue
    sab(mode="queue", name="delete", value=nzo, del_files=1)
    return "sab-only (NOT blocklisted - may be re-grabbed)"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD_MB)
    ap.add_argument("--loss-pct", type=float, default=DEFAULT_LOSS_PCT)
    args = ap.parse_args()

    jobs = doomed(args.threshold, args.loss_pct)
    wasted = sum(j["gb"] for j in jobs)
    print(f"UNRECOVERABLE JOBS (>={args.loss_pct:g}% of fetched dead, or >={args.threshold:g} MB missing): {len(jobs)}")
    print(f"  queue space they occupy: {wasted:.0f} GB\n")
    for j in jobs[:30]:
        print(
            f"  {j['loss_pct']:>5.1f}% dead  {str(j['pct']):>3}%  {str(j['age']):>6}  [{j['priority']}]  {j['name'][:40]}  ({j['reason']})"
        )
    if len(jobs) > 30:
        print(f"  ... and {len(jobs) - 30} more")

    if not args.execute:
        print("\n(dry run - pass --execute to fail + blocklist)")
        return

    counts: dict[str, int] = {}
    for j in jobs:
        how = blocklist_and_remove(j)
        counts[how] = counts.get(how, 0) + 1
    print(f"\nremoved {len(jobs)}: {counts}")


if __name__ == "__main__":
    main()
