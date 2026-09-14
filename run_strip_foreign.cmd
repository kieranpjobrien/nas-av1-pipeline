@echo off
REM Strip foreign audio/sub tracks from the ~1,060 files the pipeline will
REM never revisit (already AV1, or parked flagged_undersized).
REM
REM Long-running: a pure remux still has to rewrite each file over SMB, so
REM budget hours, not minutes. Single-threaded on purpose - concurrent UNC
REM writes saturate SMB and fight the encoder for the same NAS.
REM
REM Launched via explorer.exe so it is not inside the Claude Code app's job
REM object (see run_dashboard.cmd for why).
REM
REM   Start-Process explorer.exe -ArgumentList "D:\MediaProject\run_strip_foreign.cmd"

cd /d D:\MediaProject
start "" /b "D:\MediaProject\.venv\Scripts\python.exe" -u -m tools.strip_foreign_tracks --execute >> "F:\AV1_Staging\strip_foreign.log" 2>&1
