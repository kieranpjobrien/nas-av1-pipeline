@echo off
REM Launch the dashboard (which is also the job supervisor for the pipeline).
REM
REM Launched via explorer.exe so the parent is Explorer, not whatever shell
REM started it. Processes started directly from the Claude Code app land in its
REM job object and die when that app is closed or hangs - that took down a 9h
REM overnight run on 2026-06-19 and again at 08:05 on 2026-09-12. Nothing
REM survives a LOGOFF either (0xC000026B, STATUS_DLL_INIT_FAILED_LOGOFF).
REM
REM Deliberately NOT a scheduled task: the operator starts these by hand.
REM
REM   Start-Process explorer.exe -ArgumentList "D:\MediaProject\run_dashboard.cmd"
REM
REM 2026-10-07: the server died silently on five consecutive days. Each time the
REM log simply stopped after a normal request - no traceback, no Windows crash
REM event, 70 GB RAM free. The old launcher used `start /b`, which detaches and
REM THROWS AWAY the exit code, so five deaths taught us nothing. Running python
REM in the foreground of this detached cmd keeps the exit code, and the echo
REM below records it. Next death names itself.

cd /d D:\MediaProject
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

echo [%DATE% %TIME%] dashboard starting >> "F:\AV1_Staging\dashboard.log"
"D:\MediaProject\.venv\Scripts\python.exe" -u -m server >> "F:\AV1_Staging\dashboard.log" 2>&1
echo [%DATE% %TIME%] dashboard EXITED with code %ERRORLEVEL% >> "F:\AV1_Staging\dashboard.log"
