@echo off
REM Drive the three hero metrics to 100% overnight, in dependency order.
REM
REM Order is NOT arbitrary:
REM   1. TMDb enrichment first  - language detection's fallback layers read
REM      original_language, and the foreign-subs decision needs it too (a
REM      French sub on a French film is legitimate, not a violation).
REM   2. Language detection second - runs whisper with WHISPER_FORCE_CPU=1
REM      (set inside lang_detect_pipeline) so it never takes a CUDA context
REM      alongside the NVENC encoder. Rule 9c: one CUDA inference at a time.
REM   3. Foreign-sub strip LAST - the inviolate rule is never to strip a track
REM      without first knowing its language. Running this before step 2 would
REM      strip on incomplete information.
REM
REM Step 3 only re-queues the 'targeted' bucket: AV1-but-non-compliant files,
REM fixed in place by the gap-filler via mkvmerge over SMB. No re-encode, no
REM fetch+upload, and no second NVENC process (rule 9b).
cd /d D:\MediaProject
if not exist "F:\AV1_Staging\logs" mkdir "F:\AV1_Staging\logs"
set LOG=F:\AV1_Staging\logs\overnight_metrics.log

echo. >> "%LOG%"
echo ================ %DATE% %TIME% ================ >> "%LOG%"

echo [1/3] TMDb enrichment + UND fill >> "%LOG%"
uv run python -m tools.one_off_enrich_and_fill >> "%LOG%" 2>&1

echo [2/3] language detection (3-phase, CPU whisper) >> "%LOG%"
uv run python -m tools.lang_detect_pipeline >> "%LOG%" 2>&1

REM No rescan between steps 2 and 3 on purpose. Both the enrichment and the
REM language passes patch media_report.json themselves via report_lock, so the
REM audit already sees fresh data - and a full scanner run alongside the live
REM pipeline is the exact shape of the 2026-04-29 report wipe (rule 13).
echo [3/3] compliance audit + re-queue foreign-sub strips >> "%LOG%"
uv run python -m tools.audit_compliance --write-control >> "%LOG%" 2>&1
uv run python -m tools.requeue_noncompliant --bucket targeted --apply >> "%LOG%" 2>&1

echo ================ DONE %DATE% %TIME% ================ >> "%LOG%"
