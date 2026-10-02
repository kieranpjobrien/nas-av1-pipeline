@echo off
REM Strip foreign audio/sub tracks from the files the pipeline will never
REM revisit (already AV1, or parked flagged_undersized).
REM
REM Long-running: a pure remux still has to rewrite each file over SMB, so
REM budget hours, not minutes. Single-threaded on purpose - concurrent UNC
REM writes saturate SMB and fight the encoder for the same NAS.
REM
REM PYTHONUTF8 because redirected stdout on Windows is cp1252 and a filename
REM with a macron crashed the 2026-09-15 run at file 62 of 1062 - in the
REM progress print, not the mux. The tool also reconfigures stdout itself;
REM this is belt and braces.
REM
REM Launched via explorer.exe so it is not inside the Claude Code app's job
REM object (see run_dashboard.cmd for why). Note that NOTHING survives a
REM logoff - the 2026-09-15 pipeline death was exit 0xC000026B,
REM STATUS_DLL_INIT_FAILED_LOGOFF.
REM
REM   Start-Process explorer.exe -ArgumentList "D:\MediaProject\run_strip_foreign.cmd"

cd /d D:\MediaProject
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
start "" /b "D:\MediaProject\.venv\Scripts\python.exe" -u -m tools.strip_foreign_tracks --execute >> "F:\AV1_Staging\strip_foreign.log" 2>&1
