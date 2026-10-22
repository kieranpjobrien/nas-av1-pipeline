@echo off
REM Dashboard watchdog, with NO CONSOLE - same reasoning as run_dashboard.cmd.
REM
REM On 2026-10-22 the watchdog was killed in the same instant as the thing it
REM was watching: its log ends "health check failed (1/2)" then ^C. A watchdog
REM that shares a kill switch with its subject is not a watchdog. pythonw has
REM no console, so no console control event can reach it.
REM
REM   Start-Process explorer.exe -ArgumentList "D:\MediaProject\run_watchdog.cmd"

cd /d D:\MediaProject
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

echo [%DATE% %TIME%] watchdog starting (pythonw, no console) >> "F:\AV1_Staging\watchdog.log"
start "" /b "D:\MediaProject\.venv\Scripts\pythonw.exe" -u -m tools.dashboard_watchdog >> "F:\AV1_Staging\watchdog.log" 2>&1
