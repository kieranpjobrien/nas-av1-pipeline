@echo off
REM Standing watch over the acquisition side, so a broken indexer or a queue
REM full of dead posts surfaces on its own instead of being noticed by eye
REM hours later. 2026-08-03: 38% of TV grabs were failing silently.
cd /d D:\MediaProject
if not exist "F:\AV1_Staging\logs" mkdir "F:\AV1_Staging\logs"
set LOG=F:\AV1_Staging\logs\acquisition_watch.log
echo. >> "%LOG%"
echo ==== %DATE% %TIME% ==== >> "%LOG%"
uv run python -m tools.indexer_health --execute >> "%LOG%" 2>&1
uv run python -m tools.kill_doomed --execute >> "%LOG%" 2>&1
uv run python -m tools.sab_unpause --execute >> "%LOG%" 2>&1
