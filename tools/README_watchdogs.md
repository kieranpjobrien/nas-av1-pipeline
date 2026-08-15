# Acquisition watchdogs

Three stdlib-only tools that keep the download side healthy without anyone
watching it:

| tool | what it does | cadence |
|---|---|---|
| `sab_unpause.py` | clears husk dirs, resumes a downloader stuck on a stale disk error | every 10 min |
| `kill_doomed.py` | fails + blocklists jobs whose articles are proven dead | 6-hourly |
| `indexer_health.py` | demotes an indexer failing far more than its peers | 6-hourly |

## They run on the download server, not Windows

Deployed to `~/nascleanup-tools/` on `plex` (192.168.4.43) and driven by that
host's crontab:

```
*/10 * * * * sudo -n /usr/bin/python3 $HOME/nascleanup-tools/sab_unpause.py --execute >> $HOME/nascleanup-tools/watch.log 2>&1
17 */6 * * * /usr/bin/python3 $HOME/nascleanup-tools/kill_doomed.py --execute >> $HOME/nascleanup-tools/watch.log 2>&1
37 */6 * * * /usr/bin/python3 $HOME/nascleanup-tools/indexer_health.py --execute >> $HOME/nascleanup-tools/watch.log 2>&1
```

`sab_unpause` needs `sudo` because `/mnt/local-incomplete` is owned by uid 1026
(the container's PUID) while the ssh/cron user is uid 1000.

### Why not the Windows scheduled task

`NASCleanup-AcquisitionWatch` ran `uv run python -m tools.<x>` and failed on
**every** invocation with:

```
did not find executable at 'C:\Users\kiera\AppData\Roaming\uv\python\cpython-3.14.3-windows-x86_64-none\python.exe'
LastTaskResult : 103
```

**Corrected 2026-08-15.** This was originally recorded as an Application
Control policy needing admin to fix. That was wrong, and the wrong note kept
the pipeline dying with every session. A probe task showed:

```
APPDATA=C:\Users\kiera\AppData\Roaming     <- environment is fine
BASE_PY=MISSING                            <- uv's python invisible to the task
VENV_PY_OK 3.13.14                         <- venv python runs, falls back to Store 3.13
pipeline import OK / server deps OK
```

So a scheduled task works perfectly well **provided it invokes
`.venv\Scripts\python.exe` directly and never `uv run`** - only uv's managed
interpreter is unreachable from that context. `NASCleanup-Pipeline` and
`NASCleanup-Dashboard` now run that way, with parent `svchost.exe`, which is
what finally stopped them dying with the session.

These acquisition tools still run from cron on the download server, for two
independent reasons: it reacts in 10 minutes instead of 6 hours, and
`sab_unpause` / `usenet_server_health` need `docker logs` on that host.

The cost of not noticing: the task never ran between 2026-08-03 and 08-07,
straight through a 5h34m SAB outage it existed to catch.

**When editing any of these tools, re-deploy them** - the server copies are
snapshots, not symlinks:

```bash
for f in sab_unpause kill_doomed indexer_health; do cat "tools/$f.py" | ssh plex "cat > ~/nascleanup-tools/$f.py"; done
```

## The failure they exist to catch

SAB pauses the *entire* downloader on any write error and stays paused until a
human notices. Three separate causes hit this in 24h on 2026-08-06:

- `FileNotFoundError` - duplicate queue entries share one incomplete folder;
  one twin's cleanup deletes the directory out from under the other.
- `FileExistsError` on `__ADMIN__` - a **husk** directory (no payload, no
  `SABnzbd_nzo_data`) survives, SAB won't list it as an orphan because there is
  nothing to resume, but it owns the name and the re-grab collides with it.
- `OSError` from `get_new_id` - the genuinely-full-disk case, from when
  `/mnt/local-incomplete` was unmounted and SAB was writing to the 98 GB root.

Total downtime that day: 6h + 47m + 9h30m + 5h34m.
