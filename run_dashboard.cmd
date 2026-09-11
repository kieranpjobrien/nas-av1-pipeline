@echo off
REM Launch the dashboard (which is also the job supervisor for the pipeline).
REM
REM Launched via explorer.exe so the parent process is Explorer, not whatever
REM shell started it. Processes started directly from the Claude Code app land
REM in its job object and are killed when that app is closed or hangs - which
REM took down a 9h overnight run on 2026-06-19 and again at 08:05 on
REM 2026-09-12, both times taking the pipeline and dashboard with it.
REM
REM Deliberately NOT a scheduled task: the operator starts these by hand.
REM
REM   Start-Process explorer.exe -ArgumentList "D:\MediaProject\run_dashboard.cmd"

cd /d D:\MediaProject
start "" /b "D:\MediaProject\.venv\Scripts\python.exe" -m server >> "F:\AV1_Staging\dashboard.log" 2>&1
