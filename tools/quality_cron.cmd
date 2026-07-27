@echo off
REM Scheduled quality enforcement. Runs unattended; see quality_guard.log.
REM
REM queue_sweep runs unrestricted: killing an inbound junk grab costs nothing
REM but a re-search. quality_guard is capped per run because it deletes files
REM that already exist on disk - a cap bounds the blast radius if a future
REM change to the floors is ever wrong.
cd /d D:\MediaProject
echo ==== %DATE% %TIME% ==== >> D:\AV1_Staging\quality_guard.log
echo --- inbound queue --- >> D:\AV1_Staging\quality_guard.log
uv run python -m tools.queue_sweep --execute >> D:\AV1_Staging\quality_guard.log 2>&1
echo --- on disk --- >> D:\AV1_Staging\quality_guard.log
uv run python -m tools.quality_guard --execute --limit 60 >> D:\AV1_Staging\quality_guard.log 2>&1
echo. >> D:\AV1_Staging\quality_guard.log
