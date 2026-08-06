@echo off
REM Standing watch over the acquisition side, so a broken indexer or a queue
REM full of dead posts surfaces on its own instead of being noticed by eye
REM hours later. 2026-08-03: 38% of TV grabs were failing silently.
REM
REM SUPERSEDED 2026-08-07 - kept only for manual/on-demand use.
REM Every scheduled run of this failed with "did not find executable at
REM ...uv\python\cpython-3.14.3..." (result 103): the Task Scheduler context
REM cannot execute the uv-managed interpreter. It therefore never ran once
REM between 08-03 and 08-07, including through a 5h34m SAB outage it was
REM supposed to catch. The three tools are stdlib-only, so they now run from
REM cron ON the download server instead - sab_unpause every 10 min,
REM kill_doomed and indexer_health 6-hourly. See tools/README_watchdogs.md.
cd /d D:\MediaProject
if not exist "F:\AV1_Staging\logs" mkdir "F:\AV1_Staging\logs"
set LOG=F:\AV1_Staging\logs\acquisition_watch.log
echo. >> "%LOG%"
echo ==== %DATE% %TIME% ==== >> "%LOG%"
uv run python -m tools.indexer_health --execute >> "%LOG%" 2>&1
uv run python -m tools.kill_doomed --execute >> "%LOG%" 2>&1
uv run python -m tools.sab_unpause --execute >> "%LOG%" 2>&1
