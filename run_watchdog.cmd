@echo off
REM Keep the dashboard up and timestamp every poll. Own console, same reason as
REM run_dashboard.cmd: `start /b` shares the caller's console, so one Ctrl+C
REM took down every job launched that way. See tools/dashboard_watchdog.py.
REM
REM   Start-Process explorer.exe -ArgumentList "D:\MediaProject\run_watchdog.cmd"

cd /d D:\MediaProject
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

start "AV1 Watchdog" /min cmd /c ""D:\MediaProject\.venv\Scripts\python.exe" -u -m tools.dashboard_watchdog >> "F:\AV1_Staging\watchdog.log" 2>&1"
