@echo off
REM Launcher for the encode pipeline, run via Task Scheduler.
REM
REM Start-Process from an agent/CLI session is NOT detached: the child
REM inherits the session's job object and dies with it. 2026-08-04: both the
REM pipeline and the dashboard were killed mid-encode at 05:38 that way, with
REM no traceback and no sleep event - just a clean stop after 8.5h of work.
REM A scheduled task is owned by the Task Scheduler service instead, so it
REM survives the session that started it.
REM
REM Calls the venv interpreter DIRECTLY rather than going through `uv run`:
REM the Task Scheduler environment resolves uv's managed python to a path
REM that does not exist ("did not find executable at ...cpython-3.14.3...").
cd /d D:\MediaProject
"D:\MediaProject\.venv\Scripts\python.exe" -m pipeline --resume >> "F:\AV1_Staging\pipeline_stdout.log" 2>&1
