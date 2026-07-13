"""Autonomous flagged_corrupt handler (runs detached overnight).

For each flagged_corrupt file, decode-probe it CORRECTLY (ProbeResult.healthy,
150s/file timeout) and act:
  HEALTHY -> false-flag: clear the flag, re-queue for AV1 convert.
  BAD     -> genuinely corrupt (decode errors): delete + Radarr/Sonarr re-grab.
  GONE    -> already missing: Radarr/Sonarr re-grab.
  HUNG    -> ambiguous (could be NAS contention): LEFT flagged for review.

Guards: arr connectivity pre-check (no deletions if unreachable); locate the
title in the arr BEFORE deleting (never delete a file we can't re-grab);
episode-precise EpisodeSearch for TV. User pre-authorised delete->re-source.
"""
import os
import re
import subprocess
import sqlite3
import sys
import time

sys.path.insert(0, "D:/MediaProject")
from pipeline.state import FileStatus, PipelineState
from tools import radarr, sonarr

LOG = "F:/AV1_Staging/corrupt_handler.log"
STATE_DB = "F:/AV1_Staging/pipeline_state.db"
PROBE_CODE = (
    "import sys; sys.path.insert(0,'D:/MediaProject'); "
    "from tools.probe_source_integrity import probe_file; "
    "r=probe_file(sys.argv[1]); print('HEALTHY' if r.healthy else 'BAD')"
)


def log(m):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {m}"
    with open(LOG, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def probe(fp):
    if not os.path.exists(fp):
        return "GONE"
    try:
        p = subprocess.run([sys.executable, "-c", PROBE_CODE, fp], timeout=150,
                           capture_output=True, text=True, encoding="utf-8", errors="replace")
        out = (p.stdout or "").strip().splitlines()
        return out[0] if out and out[0] in ("HEALTHY", "BAD") else "BAD"
    except subprocess.TimeoutExpired:
        return "HUNG"


def title_year(name):
    m = re.search(r"\((\d{4})\)", name)
    year = int(m.group(1)) if m else None
    title = re.sub(r"\s*\(\d{4}\).*", "", name).strip()
    return title, year


def resource(fp, state, con):
    """Delete + re-grab. Returns a status string. Locate-before-delete guard."""
    posix = fp.replace("\\", "/")
    fn = os.path.basename(fp)
    if "/Series/" in posix:
        series = sonarr.find_series_by_path(fp) or sonarr.find_series_by_title_year(*title_year(fn.split(" - ")[0]))
        if not series:
            return "SKIP (series not in Sonarr — not deleted)"
        m = re.search(r"S(\d+)E(\d+)", fn, re.I)
        if not m:
            return "SKIP (can't parse SxxExx)"
        season, epnum = int(m.group(1)), int(m.group(2))
        eps = sonarr._request("GET", "/api/v3/episode", params={"seriesId": int(series["id"])}) or []
        ep = next((e for e in eps if e.get("seasonNumber") == season and e.get("episodeNumber") == epnum), None)
        if not ep:
            return f"SKIP (S{season:02}E{epnum:02} not in Sonarr — not deleted)"
        if ep.get("episodeFileId"):
            sonarr._request("DELETE", f"/api/v3/episodefile/{ep['episodeFileId']}")
        if os.path.exists(fp):
            os.remove(fp)
        sonarr._request("POST", "/api/v3/command", body={"name": "EpisodeSearch", "episodeIds": [int(ep["id"])]})
        con.execute("DELETE FROM pipeline_files WHERE filepath=?", (fp,))
        con.commit()
        return "RE-SOURCED (Sonarr episode)"
    else:
        title, year = title_year(fn)
        movie = radarr.find_movie_by_path(fp) or radarr.find_movie_by_title_year(title, year)
        if not movie:
            return "SKIP (movie not in Radarr — not deleted)"
        mfid = (movie.get("movieFile") or {}).get("id")
        if mfid:
            radarr._request("DELETE", f"/api/v3/moviefile/{mfid}")
        if os.path.exists(fp):
            os.remove(fp)
        radarr.trigger_search(int(movie["id"]))
        con.execute("DELETE FROM pipeline_files WHERE filepath=?", (fp,))
        con.commit()
        return "RE-SOURCED (Radarr movie)"


def main():
    log("=== corrupt handler start ===")
    try:
        radarr.list_quality_profiles()
        sonarr.list_quality_profiles()
    except Exception as e:  # noqa: BLE001
        log(f"ABORT — arr unreachable, no deletions: {e}")
        return

    state = PipelineState(STATE_DB)
    con = sqlite3.connect(STATE_DB)
    con.row_factory = sqlite3.Row
    corrupt = [r["filepath"] for r in con.execute(
        "SELECT filepath FROM pipeline_files WHERE status='flagged_corrupt'").fetchall()]
    log(f"{len(corrupt)} flagged_corrupt to classify")

    tally = {"HEALTHY": 0, "BAD": 0, "GONE": 0, "HUNG": 0}
    for fp in corrupt:
        fn = os.path.basename(fp)[:44]
        v = probe(fp)
        tally[v] = tally.get(v, 0) + 1
        try:
            if v == "HEALTHY":
                state.set_file(fp, FileStatus.PENDING, force_reencode=False,
                               reason="re-probe HEALTHY: false corrupt-flag cleared, re-queued")
                log(f"HEALTHY  {fn} -> un-flagged, re-queued for convert")
            elif v in ("BAD", "GONE"):
                res = resource(fp, state, con)
                log(f"{v:7} {fn} -> {res}")
            else:  # HUNG
                log(f"HUNG    {fn} -> left flagged (ambiguous; review)")
        except Exception as e:  # noqa: BLE001
            log(f"ERROR   {fn}: {e}")

    con.close()
    log(f"=== corrupt handler done: {tally} ===")


if __name__ == "__main__":
    main()
