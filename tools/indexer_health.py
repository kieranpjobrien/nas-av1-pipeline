"""Score indexers on what actually lands, not on what they advertise.

2026-08-03: 38% of all TV grabs were failing with "Aborted, cannot be
completed". The cause was one indexer - Nzb.su was serving cross-posted
reposts of dead articles (714 of them) while sitting at the JOINT TOP
priority, so it won most grabs. Over a 3-hour window 85% of its grabs failed;
over the settled history it runs at 25% against altHUB's 10% on the same
library and the same servers. Sonarr dutifully re-searched each failure and
grabbed another dead xpost from the same source, so the library never filled
and nothing surfaced as an error.

Prowlarr has no notion of whether a grab survives contact with the servers, so
nothing was watching this. This does: join Sonarr/Radarr grab events to their
outcome, per indexer, and demote anything that mostly fails.

Run:  uv run python -m tools.indexer_health
      uv run python -m tools.indexer_health --execute
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SONARR = ("http://192.168.4.43:27483", "80d52d39113c45be9f3b94c3c2cfbdd1")
RADARR = ("http://192.168.4.43:27484", "2aeb7de56ea44035a438447a5911f77a")
PROWLARR = ("http://192.168.4.43:27487", "80fc2d10a4254c338a9cacb6e9f424e9")

# Failure rate above which an indexer is demoted outright. Deliberately
# generous - some failure is normal on old posts, and the point is to catch a
# source that is structurally broken, not to punish noise.
FAIL_RATE_DEMOTE = 0.40
# Below this many resolved grabs the rate is not meaningful.
MIN_SAMPLE = 15

# An absolute threshold alone is too blunt. What actually mattered on
# 2026-08-03 was the SPREAD: Nzb.su failed 2.5x as often as altHUB on the same
# library, the same servers and the same period. That is a property of the
# indexer, not of usenet. Flag an indexer that is much worse than the best
# available alternative, even when its absolute rate looks survivable.
RELATIVE_FACTOR = 2.0
RELATIVE_FLOOR = 0.18  # ...but never on a rate this low; that is just noise


def failing(data: dict[str, dict], threshold: float = FAIL_RATE_DEMOTE) -> list[str]:
    """Indexers worth demoting, on either the absolute or the relative test."""
    eligible = {n: d for n, d in data.items() if d["resolved"] >= MIN_SAMPLE}
    if not eligible:
        return []
    best = min(d["rate"] for d in eligible.values())
    out = []
    for name, d in eligible.items():
        if d["rate"] >= threshold:
            out.append(name)
        elif d["rate"] >= RELATIVE_FLOOR and best > 0 and d["rate"] >= best * RELATIVE_FACTOR:
            out.append(name)
    return out


# Prowlarr priority applied to a failing indexer. Not disabled: a bad indexer
# is still better than no result when it is the only source for something.
DEMOTED_PRIORITY = 45


def arr(base_key, path, method="GET", body=None):
    base, key = base_key
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method)
    req.add_header("X-Api-Key", key)
    if data:
        req.add_header("Content-Type", "application/json")
    raw = urllib.request.urlopen(req, timeout=240).read()
    return json.loads(raw) if raw else {}


SUCCESS_EVENTS = ("downloadFolderImported",)
FAILURE_EVENTS = ("downloadFailed", "importFailed")


def outcomes(api, pages: int = 3) -> tuple[collections.Counter, collections.Counter, collections.Counter]:
    """(resolved, failures, xposts) per indexer name.

    Only grabs that have actually RESOLVED - imported or failed - are counted.
    Counting every grab as the denominator understates the failure rate badly,
    because a grab issued five minutes ago has not had time to fail yet: over a
    3,000-event window Nzb.su scored 5% by that measure while its resolved
    grabs were failing 85% of the time.
    """
    owner: dict[str, str] = {}
    xpost: collections.Counter = collections.Counter()
    verdict: dict[str, str] = {}
    for page in range(1, pages + 1):
        recs = arr(api, f"/api/v3/history?page={page}&pageSize=1000&sortKey=date&sortDirection=descending").get(
            "records", []
        )
        if not recs:
            break
        for r in recs:
            data = r.get("data") or {}
            did = r.get("downloadId")
            if not did:
                continue
            if r["eventType"] == "grabbed":
                owner[did] = data.get("indexer", "?")
                if "xpost" in (r.get("sourceTitle") or "").lower():
                    xpost[data.get("indexer", "?")] += 1
            elif r["eventType"] in FAILURE_EVENTS:
                verdict[did] = "fail"
            elif r["eventType"] in SUCCESS_EVENTS:
                # A failure anywhere in the chain wins: a job that failed and
                # was later re-grabbed under the same id still cost us a cycle.
                verdict.setdefault(did, "ok")

    resolved: collections.Counter = collections.Counter()
    fails: collections.Counter = collections.Counter()
    for did, v in verdict.items():
        name = owner.get(did)
        if not name:
            continue  # grab is outside the window - cannot attribute it
        resolved[name] += 1
        if v == "fail":
            fails[name] += 1
    return resolved, fails, xpost


def report() -> dict[str, dict]:
    total_g: collections.Counter = collections.Counter()
    total_f: collections.Counter = collections.Counter()
    total_x: collections.Counter = collections.Counter()
    for api in (SONARR, RADARR):
        try:
            g, f, x = outcomes(api)
        except Exception:  # noqa: BLE001,PERF203
            continue
        total_g.update(g)
        total_f.update(f)
        total_x.update(x)
    out = {}
    for name, n in total_g.items():
        f = total_f.get(name, 0)
        out[name] = {
            "resolved": n,
            "fails": f,
            "rate": f / n if n else 0.0,
            "xpost": total_x.get(name, 0),
        }
    return out


def prowlarr_key(display_name: str) -> str:
    """Sonarr reports "altHUB (Prowlarr)"; Prowlarr knows it as "altHUB"."""
    return display_name.replace(" (Prowlarr)", "").strip()


def demote(name: str) -> str:
    want = prowlarr_key(name)
    for ix in arr(PROWLARR, "/api/v1/indexer"):
        if ix["name"].strip().lower() == want.lower():
            if ix.get("priority") == DEMOTED_PRIORITY:
                return "already demoted"
            ix["priority"] = DEMOTED_PRIORITY
            arr(PROWLARR, f"/api/v1/indexer/{ix['id']}", "PUT", ix)
            return f"priority -> {DEMOTED_PRIORITY}"
    return "not found in Prowlarr"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--threshold", type=float, default=FAIL_RATE_DEMOTE)
    args = ap.parse_args()

    data = report()
    print(f"{'indexer':<28}{'resolvd':>8}{'fails':>7}{'rate':>7}{'xpost':>7}")
    for name, d in sorted(data.items(), key=lambda kv: -kv[1]["rate"]):
        print(f"  {name[:26]:<26}{d['resolved']:>8}{d['fails']:>7}{d['rate'] * 100:>6.0f}%{d['xpost']:>7}")

    bad = failing(data, args.threshold)
    print(f"\nfailing indexers (>={args.threshold * 100:.0f}%, or {RELATIVE_FACTOR:g}x the best): {bad or 'none'}")

    if not args.execute:
        print("\n(dry run - pass --execute to demote in Prowlarr)")
        return
    for name in bad:
        print(f"  {name}: {demote(name)}")


if __name__ == "__main__":
    main()
