"""Walk the ENTIRE library and grab the best available release for everything.

The lesson, learned the hard way over 2026-07-26/28: configuring rules and
waiting for Sonarr to act does not produce quality. Sonarr ranks by quality
TIER, so it sees a 1.2 GB and a 5.7 GB release of the same tier as
equivalent and will never swap them. Floors, ceilings, cutoffs and upgrade
flags were all fixed and STILL the operator kept finding 1 GB episodes of
shows that should be 4-10 GB.

So: go and take the best thing on offer, for every episode of every series
and every movie. Do not wait to be told which show is wrong.

Ordering puts the operator's named shows first, then everything else by how
much is to be gained, so the biggest wins land soonest.

Run:  uv run python -m tools.grab_all --dry-run
      uv run python -m tools.grab_all --execute
      uv run python -m tools.grab_all --execute --only "Band of Brothers"
"""

from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.grab_best import SONARR, arr, pick  # noqa: E402

# Shows the operator called out explicitly. Prestige HBO/AppleTV war drama and
# similar belong at ~10 GB/episode; these go first.
PRIORITY_TITLES = [
    "The Pacific",
    "Band of Brothers",
    "Masters of the Air",
    "The Bear",
    "Colin from Accounts",
]

MIN_GAIN = 1.2  # only grab when the candidate is this much bigger
LOG = "D:/AV1_Staging/grab_all.log"


def say(msg: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def series_order(all_series, only=None):
    if only:
        return [s for s in all_series if only.lower() in s["title"].lower()]
    named, rest = [], []
    for s in all_series:
        if any(p.lower() in s["title"].lower() for p in PRIORITY_TITLES):
            named.append(s)
        else:
            rest.append(s)
    named.sort(key=lambda s: next(i for i, p in enumerate(PRIORITY_TITLES) if p.lower() in s["title"].lower()))
    rest.sort(key=lambda s: s["title"])
    return named + rest


def do_series(s, execute: bool) -> tuple[int, float]:
    grabbed = 0
    gained = 0.0
    try:
        eps = arr(SONARR, f"/api/v3/episode?seriesId={s['id']}")
    except Exception as exc:  # noqa: BLE001
        say(f"  {s['title']}: episode list failed ({exc})")
        return 0, 0.0
    eps = [e for e in eps if e.get("monitored") and e.get("seasonNumber", 0) > 0]
    eps.sort(key=lambda e: (e.get("seasonNumber", 0), e.get("episodeNumber", 0)))
    for e in eps:
        cur = 0.0
        if e.get("hasFile") and e.get("episodeFileId"):
            try:
                cur = (arr(SONARR, f"/api/v3/episodefile/{e['episodeFileId']}").get("size") or 0) / 1e9
            except Exception:  # noqa: BLE001
                pass
        try:
            rel = arr(SONARR, f"/api/v3/release?episodeId={e['id']}")
        except Exception:  # noqa: BLE001
            continue
        best = pick(rel)
        if not best:
            continue
        new = (best.get("size") or 0) / 1e9
        if cur and new <= cur * MIN_GAIN:
            continue
        tag = f"S{e['seasonNumber']:02d}E{e['episodeNumber']:02d}"
        say(f"  {s['title'][:32]} {tag}: {cur:.2f} -> {new:.2f} GB  {best.get('title', '')[:40]}")
        if execute:
            try:
                arr(SONARR, "/api/v3/release", "POST", {"guid": best["guid"], "indexerId": best["indexerId"]})
                grabbed += 1
                gained += new - cur
            except Exception as exc:  # noqa: BLE001
                say(f"      grab failed: {exc}")
    return grabbed, gained


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", help="restrict to one series title")
    ap.add_argument("--max-series", type=int, default=0)
    args = ap.parse_args()
    execute = args.execute and not args.dry_run

    all_series = arr(SONARR, "/api/v3/series")
    todo = series_order(all_series, args.only)
    if args.max_series:
        todo = todo[: args.max_series]
    say(f"=== grab_all over {len(todo)} series (execute={execute}) ===")

    total = 0
    gained = 0.0
    for i, s in enumerate(todo, 1):
        say(f"[{i}/{len(todo)}] {s['title']}")
        g, gb = do_series(s, execute)
        total += g
        gained += gb
    say(f"=== done: grabbed {total} episodes, +{gained:.0f} GB of quality ===")


if __name__ == "__main__":
    main()
