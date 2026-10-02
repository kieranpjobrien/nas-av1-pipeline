@echo off
REM Two long jobs, run back to back so they never fight each other for SMB:
REM
REM  1. fix_mislabelled_audio over Bluey - strips the Mandarin dubs whose tags
REM     claim 'eng' and promotes the real English track to default.
REM  2. strip_foreign_tracks - resumes the 2026-09-15 batch, which got 61 of
REM     1062 files in before crashing on a filename with a macron. Already
REM     stripped files are simply skipped (nothing foreign left to find), so
REM     re-running is safe and needs no bookkeeping.
REM
REM PYTHONUTF8 because redirected stdout on Windows is cp1252 - that encoding
REM is what killed the last run, in the progress print rather than the mux.
REM
REM Launched via explorer.exe to stay out of the Claude Code app's job object.
REM Nothing survives a LOGOFF though: the 2026-09-15 pipeline death was exit
REM 0xC000026B, STATUS_DLL_INIT_FAILED_LOGOFF.
REM
REM   Start-Process explorer.exe -ArgumentList "D:\MediaProject\run_audio_cleanup.cmd"

cd /d D:\MediaProject
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
set WHISPER_FORCE_CPU=1

start "" /b cmd /c ""D:\MediaProject\.venv\Scripts\python.exe" -u -m tools.fix_mislabelled_audio --match Bluey --execute >> "F:\AV1_Staging\bluey_fix.log" 2>&1 && "D:\MediaProject\.venv\Scripts\python.exe" -u -m tools.strip_foreign_tracks --execute >> "F:\AV1_Staging\strip_foreign.log" 2>&1"
