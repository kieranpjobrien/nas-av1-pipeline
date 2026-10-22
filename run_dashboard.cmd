@echo off
REM Launch the dashboard (also the pipeline's process supervisor) with NO CONSOLE.
REM
REM   Start-Process explorer.exe -ArgumentList "D:\MediaProject\run_dashboard.cmd"
REM
REM History of this one line, because it has been wrong twice:
REM
REM  1. `start /b python.exe` - ran the child in the CALLER'S console, so every
REM     job launched this way shared one control-event group and a Ctrl+C
REM     delivered to any of them killed all of them. Six silent deaths.
REM  2. `start "title" /min cmd /c python.exe` - meant to give it its own
REM     console. It died again on 2026-10-22, same signature: dashboard.log
REM     ending in ^C, and
REM
REM       [07/10 21:52:22] dashboard EXITED code 0
REM
REM     Exit code 0 is a GRACEFUL shutdown - uvicorn handling SIGINT - not a
REM     crash. The watchdog died in the same instant, mid-recovery, with
REM     "health check failed (1/2)" then ^C in its own log.
REM
REM  3. pythonw.exe. It is a GUI-subsystem binary: it has no console at all, so
REM     there is no console group to join and no CTRL_C_EVENT can reach it.
REM     This is not a better guess about grouping, it removes the mechanism.
REM
REM Redirection still works - cmd sets up the file handles before the process
REM starts, so stdout/stderr land in the log exactly as before.
REM
REM Nothing survives a LOGOFF regardless (0xC000026B). Lock the screen instead.

cd /d D:\MediaProject
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

echo [%DATE% %TIME%] dashboard starting (pythonw, no console) >> "F:\AV1_Staging\dashboard.log"
start "" /b "D:\MediaProject\.venv\Scripts\pythonw.exe" -u -m server >> "F:\AV1_Staging\dashboard.log" 2>&1
