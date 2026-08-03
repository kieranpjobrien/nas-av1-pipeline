@echo off
REM Launcher for the dashboard/API server (:8000), run via Task Scheduler.
REM Same reasons as run_pipeline.cmd: a session-launched child gets reaped
REM when the launching session goes away, and `uv run` cannot resolve its
REM managed python under the Task Scheduler environment.
cd /d D:\MediaProject
"D:\MediaProject\.venv\Scripts\python.exe" -m server >> "F:\AV1_Staging\dashboard_stdout.log" 2>&1
