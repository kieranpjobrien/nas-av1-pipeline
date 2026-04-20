#!/usr/bin/env bash
# Hourly quick-win backfill loop.
#
# Runs incremental, idempotent tools — each one is cheap if there's nothing to do.
# Intended to catch up on files that *arr downloads between full pipeline runs.
#
# Start via: uv run bash tools/hourly_backfill.sh &
# Stop via: TaskStop on the background task id.
set -u

LOG_PREFIX="[hourly-backfill]"
LIVE_PIPELINE="http://localhost:8002/api/pipeline"

while true; do
  started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo ""
  echo "$LOG_PREFIX === TICK START $started ==="

  echo "$LOG_PREFIX [1/3] Incremental media_report scan..."
  uv run python -m tools.scanner 2>&1 | tail -8

  echo "$LOG_PREFIX [2/3] TMDb enrichment for files missing tmdb..."
  uv run python -m tools.tmdb --enrich-and-apply --workers 8 2>&1 | tail -8

  echo "$LOG_PREFIX [3/3] Language detection (text-based, no whisper)..."
  uv run python -m tools.detect_languages --apply --workers 4 2>&1 | tail -8

  ended=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "$LOG_PREFIX === TICK DONE $ended ==="

  # Sleep 60 min. Can be interrupted by TaskStop.
  sleep 3600
done
