@echo off
REM Keep the dashboard up and record the circumstances of each death.
REM See tools/dashboard_watchdog.py for why this exists.
REM
REM   Start-Process explorer.exe -ArgumentList "D:\MediaProject\run_watchdog.cmd"

cd /d D:\MediaProject
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
start "" /b "D:\MediaProject\.venv\Scripts\python.exe" -u -m tools.dashboard_watchdog >> "F:\AV1_Staging\watchdog.log" 2>&1
