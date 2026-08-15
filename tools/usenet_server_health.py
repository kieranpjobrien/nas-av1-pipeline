"""Report which usenet servers are actually earning their subscription.

Two questions this answers, neither of which SAB surfaces on its own:

1. Is a server BROKEN? A dead subscription does not go quiet - it retries
   forever. 2026-08-15: news.newsgroupdirect.com returned "502 Access Denied.
   Please check your login/pw." and eu-tst returned "502 Connection failure",
   1,586 times in twenty minutes, while SAB carried on reporting healthy.

2. Is a server WORTH IT? Several "servers" share one subscription - group by
   username before judging. On 2026-08-15 viper/news/eu-tst.newsgroupdirect.com
   were all username pqy542681482: one account, three endpoints, of which two
   had dead credentials and the third had pulled 122 GB that month against
   Frugal's 14,066 GB.

Auth failures are the important signal: an article missing from one server is
normal usenet, but "Access Denied" means nobody is being served at all.

Run:  uv run python -m tools.usenet_server_health
      uv run python -m tools.usenet_server_health --window 60
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SAB = ("http://192.168.4.43:8185/api", "cd29eb7ca0d44e5f96bd51ac2258916e")

# Fatal, server-level failures. Deliberately NOT "article unavailable" - that
# is ordinary retention loss and says nothing about the subscription.
FATAL_PATTERNS = (
    re.compile(r"Failed login for server (\S+)", re.I),
    re.compile(r"Cannot connect to server (\S+)", re.I),
    re.compile(r"Server (\S+) uses an untrusted certificate", re.I),
)


def sab(**params):
    base, key = SAB
    url = base + "?" + urllib.parse.urlencode({**params, "output": "json", "apikey": key})
    with urllib.request.urlopen(url, timeout=90) as r:
        return json.loads(r.read() or b"{}")


def recent_log(minutes: int) -> str:
    try:
        out = subprocess.run(
            ["docker", "logs", f"--since={minutes}m", "sabnzbd"],
            capture_output=True,
            text=True,
            timeout=120,
            errors="replace",
        )
        return (out.stdout or "") + (out.stderr or "")
    except (OSError, subprocess.SubprocessError):
        return ""


def fatal_errors(log_text: str) -> collections.Counter:
    """host -> count of fatal, server-level failures."""
    hits: collections.Counter = collections.Counter()
    for line in log_text.splitlines():
        for pat in FATAL_PATTERNS:
            m = pat.search(line)
            if m:
                hits[m.group(1)] += 1
                break
    return hits


def volume_for(host: str, stats: dict) -> float | None:
    """Month GB for ``host``, or None when the host cannot be matched at all.

    SAB records stats against the host it actually CONNECTED to, which is not
    always the host in the config: Newshosting is configured as
    news.newshosting.com but reports as news.eweka.nl, because it resells
    Eweka. Returning 0.0 for that case accused a server doing 9,579 GB/month
    of "paying for nothing".

    None means "cannot judge", which is not the same as zero, and callers must
    not treat it as evidence of waste.
    """
    if host in stats:
        return (stats[host].get("month") or 0) / 1e9
    # Fall back to the registrable-domain, which survives the reseller alias
    # only when it happens to share a domain - otherwise we admit we can't tell.
    tail = ".".join(host.split(".")[-2:])
    for k, v in stats.items():
        if ".".join(k.split(".")[-2:]) == tail:
            return (v.get("month") or 0) / 1e9
    return None


def group_by_account(servers: list[dict]) -> dict[str, list[dict]]:
    """Several hosts often share one subscription - judge the ACCOUNT, not the
    host, or a working endpoint masks two dead ones on the same bill."""
    out: dict[str, list[dict]] = collections.defaultdict(list)
    for s in servers:
        out[str(s.get("username") or "(none)")].append(s)
    return dict(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=int, default=30, help="minutes of log to scan")
    args = ap.parse_args()

    cfg = (sab(mode="get_config").get("config") or {}).get("servers") or []
    stats = (sab(mode="server_stats").get("servers")) or {}
    errs = fatal_errors(recent_log(args.window))

    print(f"{'server':<32}{'on':<4}{'prio':<6}{'month GB':>10}{'fatal/' + str(args.window) + 'm':>12}")
    for s in cfg:
        host = str(s.get("host") or "")
        month = volume_for(host, stats)
        bad = errs.get(host, 0)
        flag = "  <- BROKEN" if bad else ""
        shown = f"{month:>10.1f}" if month is not None else f"{'unknown':>10}"
        print(f"  {host[:30]:<30}{str(s.get('enable')):<4}{str(s.get('priority')):<6}{shown}{bad:>12}{flag}")

    print("\nby subscription (username):")
    for user, group in group_by_account(cfg).items():
        vols = [volume_for(str(g.get("host") or ""), stats) for g in group]
        known = [v for v in vols if v is not None]
        month = sum(known)
        bad = sum(errs.get(str(g.get("host")) or "", 0) for g in group)
        hosts = ", ".join(str(g.get("displayname") or g.get("host")) for g in group)
        mask = (user[:3] + "***" + user[-2:]) if len(user) > 5 else user
        # Only accuse when every endpoint's volume is actually KNOWN. An
        # unmatched host is ignorance, not evidence.
        note = ""
        if not known:
            note = "  (volume unknown - cannot judge)"
        elif len(known) == len(vols) and month < 1 and any(g.get("enable") for g in group):
            note = "  <- paying for nothing"
        print(f"  {mask:<14}{month:>10.1f} GB/month  fatal={bad:<6}{hosts[:44]}{note}")

    broken = [h for h, n in errs.items() if n >= 5]
    if broken:
        print(f"\nBROKEN (>=5 fatal errors in {args.window}m): {broken}")
        print("These retry forever and waste connections. Disable them or fix the credentials.")


if __name__ == "__main__":
    main()
