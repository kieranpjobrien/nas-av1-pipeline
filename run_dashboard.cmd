@echo off
REM Launch the dashboard (also the pipeline's process supervisor) in its OWN
REM console, isolated from every other job's console control events.
REM
REM 2026-10-07, after six silent deaths in six days: the cause is a console
REM Ctrl+C, caught red-handed in dashboard.log -
REM
REM   INFO:  127.0.0.1:63415 - "GET /api/health HTTP/1.1" 200 OK
REM   ^C[Wed 07/10/2026 21:51:12.66] dashboard starting
REM
REM uvicorn handles SIGINT by shutting down cleanly, which is why there was
REM never a traceback, never a Windows Application crash event, and never a
REM captured exit code - it was not crashing, it was being interrupted.
REM
REM `start /b` runs a child in the CALLER'S console, so every job launched this
REM way joined one control-event group: a break delivered to any of them hit
REM all of them. Each new .cmd launched while working was killing the server.
REM
REM `start "title" /min` gives this one its own console window instead, so a
REM break elsewhere cannot reach it. Minimised, not hidden, so it is visible in
REM the taskbar if you want to stop it by hand.
REM
REM Deliberately NOT a scheduled task; the operator starts these by hand.
REM Nothing survives a LOGOFF regardless (0xC000026B).
REM
REM   Start-Process explorer.exe -ArgumentList "D:\MediaProject\run_dashboard.cmd"

cd /d D:\MediaProject
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

start "AV1 Dashboard" /min cmd /c ""D:\MediaProject\.venv\Scripts\python.exe" -u -m server >> "F:\AV1_Staging\dashboard.log" 2>&1 & echo [%%DATE%% %%TIME%%] dashboard EXITED code %%ERRORLEVEL%% >> "F:\AV1_Staging\dashboard.log""
