"""Import completed downloads the *arr never knew about.

Sonarr and Radarr only import what they are actively TRACKING in their queue.
If that association is lost - the client restarts, the queue is rebuilt, a job
is re-added from an orphaned folder, the box crashes - the finished download
becomes invisible to them. Forever. No import attempt, no error, no history
entry. The *arr still believes the episode is missing, so it grabs it again,
the new download lands beside the old one with a ``.1`` suffix, and the loop
repeats every cycle.

2026-08-10: /downloads/Series held 1,360 directories and 2.7 TB, the oldest
from July 2025. The McBee Dynasty had three full seasons on disk and 0 of 30
episodes in Sonarr, with history showing only ``grabbed`` - not one import had
ever been attempted.

Why this and not DownloadedEpisodesScan: that command is queued and processed
serially at roughly 90 s each, so 1,360 folders is ~33 hours. The manualimport
endpoint parses a folder without importing, and a single ManualImport command
carries many files, so the same work batches down to minutes.

Asking Sonarr to parse the whole tree in one call returns HTTP 500 - it has to
be per folder.

Run:  uv run python -m tools.import_orphaned_downloads
      uv run python -m tools.import_orphaned_downloads --execute
      uv run python -m tools.import_orphaned_downloads --execute --mode Copy
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SONARR = ("http://192.168.4.43:27483", "80d52d39113c45be9f3b94c3c2cfbdd1")
RADARR = ("http://192.168.4.43:27484", "2aeb7de56ea44035a438447a5911f77a")

# Host path -> the path the *arr container sees.
LIBRARIES = (
    ("series", SONARR, "/mnt/nas/downloads/Series", "/downloads/Series"),
    ("movie", RADARR, "/mnt/nas/downloads/Movies", "/downloads/Movies"),
)

# Files per ManualImport command. Large enough to be fast, small enough that
# one bad folder doesn't sink a huge batch.
BATCH = 40


def arr(base_key, path, method="GET", body=None, timeout=300):
    base, key = base_key
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method)
    req.add_header("X-Api-Key", key)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        raw = urllib.request.urlopen(req, timeout=timeout).read()
    except Exception:  # noqa: BLE001 - one bad folder must not stop the sweep
        return None
    return json.loads(raw) if raw else {}


def list_folders(host_path: str) -> list[str]:
    """Subdirectory names under ``host_path``, read over ssh."""
    out = subprocess.run(
        ["ssh", "plex", f"ls -1 {host_path!r} 2>/dev/null"],
        capture_output=True,
        text=True,
        timeout=180,
    )
    return [x.strip() for x in out.stdout.splitlines() if x.strip()]


def importable(candidate: dict, kind: str) -> bool:
    """True when the *arr has matched this file well enough to import it.

    Rejections are respected, not overridden: "already have this quality" is
    the *arr protecting a better existing file, and forcing past it would
    downgrade the library.
    """
    if candidate.get("rejections"):
        return False
    if kind == "series":
        return bool(candidate.get("series")) and bool(candidate.get("episodes"))
    return bool(candidate.get("movie"))


def to_import_file(candidate: dict, kind: str) -> dict:
    """Shape a manualimport candidate into a ManualImport command entry."""
    entry = {
        "path": candidate["path"],
        "quality": candidate.get("quality"),
        "languages": candidate.get("languages"),
    }
    if kind == "series":
        entry["seriesId"] = candidate["series"]["id"]
        entry["episodeIds"] = [e["id"] for e in candidate.get("episodes") or []]
    else:
        entry["movieId"] = candidate["movie"]["id"]
    return entry


def sweep(kind, api, host_path, container_path, *, execute: bool, mode: str) -> dict:
    folders = list_folders(host_path)
    print(f"[{kind}] {len(folders)} folders under {host_path}")
    files, skipped, unmatched = [], 0, 0
    for i, name in enumerate(folders, 1):
        q = urllib.parse.quote(f"{container_path}/{name}")
        cands = arr(api, f"/api/v3/manualimport?folder={q}&filterExistingFiles=true")
        if not cands:
            unmatched += 1
        else:
            for c in cands:
                if importable(c, kind):
                    files.append(to_import_file(c, kind))
                else:
                    skipped += 1
        if i % 100 == 0:
            print(f"  [{kind}] parsed {i}/{len(folders)} — importable so far: {len(files)}")

    print(f"[{kind}] importable={len(files)}  rejected/already-have={skipped}  unparsed folders={unmatched}")
    if not execute or not files:
        return {"importable": len(files), "skipped": skipped, "unmatched": unmatched, "sent": 0}

    sent = 0
    for i in range(0, len(files), BATCH):
        chunk = files[i : i + BATCH]
        res = arr(api, "/api/v3/command", "POST", {"name": "ManualImport", "files": chunk, "importMode": mode})
        if res is not None:
            sent += len(chunk)
            print(f"  [{kind}] queued {sent}/{len(files)}")
    return {"importable": len(files), "skipped": skipped, "unmatched": unmatched, "sent": sent}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--mode", default="Move", choices=("Move", "Copy"))
    ap.add_argument("--only", choices=("series", "movie"))
    args = ap.parse_args()

    for kind, api, host, cont in LIBRARIES:
        if args.only and args.only != kind:
            continue
        sweep(kind, api, host, cont, execute=args.execute, mode=args.mode)
    if not args.execute:
        print("\n(dry run - pass --execute to import)")


if __name__ == "__main__":
    main()
