# NASCleanup

Personal AV1 re-encoding pipeline for a home NAS. Converts H.264/HEVC media to AV1 using NVENC hardware encoding, normalises bulky audio to EAC-3, strips non-English subtitles, and enriches metadata via TMDb -- all managed through a React dashboard.

## What It Does

- **Video re-encoding** -- H.264 and HEVC to AV1 via `av1_nvenc` (RTX 4080), with resolution-aware CQ targets and quality profiles
- **Audio normalisation to EAC-3** -- everything that isn't already EAC-3 gets transcoded (lossless, DTS, AC-3, AAC, MP3, Opus). 640 kbps surround, 256 kbps stereo. Codec uniformity across the library
- **Subtitle management** -- strips non-English subtitle streams during encode, preserving English, undetermined, and forced tracks
- **Language detection** -- identifies undetermined audio/subtitle tracks using Whisper (audio) and Tesseract+langdetect (subtitle text), then writes tags back via mkvpropedit
- **TMDb metadata enrichment** -- genres, cast, crew, ratings, and content ratings added to the media report
- **Plex integration** -- library scans, collection management, metadata auditing, and a Rewatchables podcast collection sync
- **Integrity checking** -- spot-checks encoded files by decoding segments at start/middle/end
- **VMAF spot-checks** -- compares original vs encoded quality on sample segments

## Architecture

```
frontend/          Vite + React dashboard (served by FastAPI in production)
server/            FastAPI + uvicorn -- dashboard API and process management
pipeline/          Encoding pipeline (stdlib only, no pip deps)
tools/             Standalone CLI utilities for scanning, metadata, subs, etc.
paths.py           Single source of truth for all env-var-backed paths
```

The **server** serves the built frontend and exposes REST + WebSocket APIs for pipeline state, control files, media report data, and process management (start/stop scanner, pipeline, language detection from the dashboard).

The **pipeline** runs independently as `python -m pipeline`. It reads `media_report.json`, builds a priority queue, and processes files through the stage machine. The server can also start/stop it as a subprocess.

**Tools** are standalone CLI scripts. Each can be run via `uv run python -m tools.<name>`.

## Requirements

### Hardware
- NVIDIA RTX 4000+ GPU (NVENC AV1 support required)
- Sufficient local staging space (default budget: 2.5 TB on `F:\`)

### Software
- Python 3.11+ (developed on 3.13)
- Node.js LTS (for frontend build)
- [uv](https://docs.astral.sh/uv/) package manager
- ffmpeg and ffprobe (with NVENC support)
- MKVToolNix (`mkvmerge`, `mkvpropedit`)
- Tesseract OCR (for subtitle language detection)

## Setup

```bash
# Install Python dependencies
uv sync

# Build the frontend
cd frontend && npm install && npm run build && cd ..

# Configure paths -- edit .env in project root, or set environment variables:
#   AV1_STAGING=F:\AV1_Staging
#   NAS_MOVIES=\\KieranNAS\Media\Movies
#   NAS_SERIES=\\KieranNAS\Media\Series
#   PLEX_URL=http://192.168.4.43:32400
#   PLEX_TOKEN=<your-token>
#   TMDB_API_KEY=<your-key>
```

All paths are defined in `paths.py`, which reads from `.env` or environment variables with sensible defaults.

## Usage

### Start the dashboard

```bash
uv run python -m server
# or
uv run uvicorn server:app --host 0.0.0.0 --port 8000
```

The dashboard has four pages:

| Page | Purpose |
|------|---------|
| **Pipeline** | Live encoding progress, ETA, per-tier stats, error log |
| **Library** | Browse the media report -- codec breakdown, bitrates, duplicates |
| **Controls** | Pause/resume, skip files, set priorities, adjust quality profiles, force re-encodes |
| **History** | Completed encode history with space savings |

### Run the pipeline directly

```bash
uv run python -m pipeline            # Start encoding
uv run python -m pipeline --resume   # Resume after reboot
uv run python -m pipeline --dry-run  # Preview what would be encoded
uv run python -m pipeline --tier "H.264 1080p"  # Run a specific tier only
```

### Run individual tools

```bash
uv run python -m tools.scanner           # Scan NAS and build media_report.json
uv run python -m tools.detect_languages  # Detect undetermined track languages
uv run python -m tools.tmdb              # Enrich report with TMDb metadata
uv run python -m tools.strip_subs        # Strip non-English subs (remux, no re-encode)
uv run python -m tools.strip_tags        # Clean filenames (dry-run by default, --execute to apply)
uv run python -m tools.integrity         # Spot-check files for corruption
uv run python -m tools.vmaf              # VMAF quality comparison
uv run python -m tools.duplicates        # Find duplicate files
uv run python -m tools.rewatchables      # Sync Rewatchables collection to Plex
uv run python -m tools.plex_metadata     # Audit/manage Plex metadata
uv run python -m tools.plex_collections  # Manage Plex collections and genres
uv run python -m tools.plex_languages    # Find movies without English audio (from Plex DB)
uv run python -m tools.fix_extensions    # Fix missing .mkv extensions on series files
```

## Pipeline Stages

Each file progresses through this state machine (persisted to `pipeline_state.json`, crash-safe):

```
PENDING -> FETCHING -> FETCHED -> ENCODING -> ENCODED -> UPLOADING -> UPLOADED -> VERIFIED -> REPLACING -> REPLACED
                                                                                      |
                                                                                  SKIPPED / ERROR
```

1. **FETCHING** -- copy from NAS to local staging (`F:\AV1_Staging`)
2. **ENCODING** -- ffmpeg AV1 encode with NVENC, smart audio handling, subtitle stripping
3. **UPLOADING** -- copy encoded file back to NAS alongside the original
4. **VERIFIED** -- duration check (within 2s tolerance) confirms the encode is valid
5. **REPLACING** -- swap the original for the encoded version on NAS

Prefetching runs in a background thread to keep the GPU fed. Staging space is budgeted (200 GB fetch buffer, 50 GB minimum free space).

Containers that cause NVENC failures (`.m2ts`, `.avi`, `.wmv`, `.ts`, `.mp4`, etc.) are automatically remuxed to `.mkv` before encoding.

## Quality Profiles

Assigned per-title via `control/profiles.json`. The pipeline looks up each file's profile and adjusts encoding parameters accordingly.

| Profile | CQ Offset | Preset | Multipass | Use Case |
|---------|-----------|--------|-----------|----------|
| **protected** | -3 | p7 | fullres | Reference films, visually important content |
| **baseline** | 0 | default | default | Standard -- good balance of quality and space |
| **lossy** | +6 | p4 | disabled | Expendable content (sitcoms, reality TV) |
| **tonemap** | 0 | default | default | HDR to SDR conversion (BT.2020 to BT.709) |

CQ targets vary by content type (movie vs series) and resolution (4K HDR/SDR, 1080p, 720p, 480p, SD). See `pipeline/config.py` for the full matrix.

Example `profiles.json`:
```json
{
    "paths": {
        "\\\\KieranNAS\\Media\\Movies\\Interstellar (2014)\\": "protected",
        "\\\\KieranNAS\\Media\\Series\\Seinfeld\\": "lossy"
    },
    "patterns": {
        "*IMAX*": "protected"
    },
    "default": "baseline"
}
```

Profiles stack with `gentle.json` (per-file CQ tweaks) and `reencode.json` (re-do already-processed files).

## Priority Queue

Files are sorted into tiers for encoding order (biggest savings first):

1. H.264 1080p
2. Bloated HEVC 1080p (>15 Mbps)
3. Bloated HEVC 4K (>25 Mbps)
4. H.264 720p/other
5. HEVC 1080p (<15 Mbps)
6. HEVC 4K by bitrate bands (>20 Mbps, then <=20 Mbps)
7. HEVC 720p/SD + other codecs

Within each tier, files are sorted by size (largest first). The priority queue also filters out files that are already AV1, or have only audio-only issues (handled separately).

## Audio Strategy

The `smart` audio mode (default) decides per-track:

| Codec | Action |
|-------|--------|
| TrueHD, DTS-HD MA, FLAC, PCM, ALAC | Transcode to EAC-3 |
| DTS core >700 kbps | Transcode to EAC-3 |
| AC-3 >400 kbps | Transcode to EAC-3 |
| AAC, Opus, EAC-3, MP3, low-bitrate AC-3/DTS | Copy (passthrough) |

EAC-3 bitrates: 640k for surround (>2 channels), 256k for stereo/mono.

Non-English audio streams are stripped during encode, keeping the first stream (original language) plus any English/undetermined tracks.

## Tools

| Tool | Description |
|------|-------------|
| `scanner` | Scans NAS directories with ffprobe, builds `media_report.json` with codec, resolution, bitrate, audio, and subtitle info |
| `detect_languages` | Identifies undetermined track languages using Whisper (audio) and Tesseract/langdetect (subtitles), writes tags back via mkvpropedit |
| `tmdb` | Enriches media report entries with TMDb metadata (genres, cast, crew, ratings, content ratings) |
| `strip_subs` | Remuxes files to remove non-English subtitles (mkvmerge preferred, ffmpeg fallback). No re-encoding. |
| `strip_tags` | Cleans filenames -- strips codec/resolution/group tags, keeps title + SxxExx/year. Dry-run by default. |
| `integrity` | Spot-checks files for corruption by decoding segments at start, middle, and end |
| `vmaf` | VMAF quality comparison between original and encoded files on sample segments |
| `duplicates` | Finds potential duplicates via fuzzy title matching and duration/resolution clustering |
| `rewatchables` | Parses The Rewatchables podcast RSS feed, matches to library via TMDb, maintains a Plex collection |
| `plex_metadata` | Audits and manages Plex metadata -- genres, collections, content ratings, labels. Rules-based application. |
| `plex_collections` | Creates smart collections based on rules (studio, genre, keyword). Audits and fixes miscategorised content. |
| `plex_languages` | Finds movies without English audio from a Plex database backup |
| `fix_extensions` | Fixes missing `.mkv` extensions on series files |
| `report_lock` | File-based lock for safe concurrent access to `media_report.json` across tools |

## File Structure

```
D:\MediaProject\
+-- paths.py                    # All path configuration (env-var-backed)
+-- pyproject.toml              # uv project definition and script entries
+-- pipeline/
|   +-- __main__.py             # CLI entry point
|   +-- runner.py               # Pipeline orchestration, prefetch thread, signal handling
|   +-- config.py               # CQ targets, NVENC presets, quality profiles, priority tiers
|   +-- encoding.py             # FFmpeg command building, audio strategy, subtitle mapping
|   +-- queue.py                # Priority queue builder from media report
|   +-- stages.py               # Fetch, upload, verify, replace stages
|   +-- state.py                # Crash-safe JSON state persistence (FileStatus enum)
|   +-- control.py              # Runtime control file reading (pause, skip, profiles, gentle, etc.)
+-- server/
|   +-- __init__.py             # FastAPI app, all API endpoints, WebSocket, process management
|   +-- __main__.py             # Server entry point
+-- frontend/
|   +-- src/pages/
|       +-- PipelinePage.jsx    # Live encoding progress dashboard
|       +-- LibraryPage.jsx     # Media report browser
|       +-- ControlPage.jsx     # Pipeline controls (pause, skip, profiles, priorities)
|       +-- HistoryPage.jsx     # Encode history and space savings
+-- tools/                      # Standalone CLI utilities (see Tools section)
```

## Configuration

### Environment variables / `.env`

| Variable | Default | Description |
|----------|---------|-------------|
| `AV1_STAGING` | `F:\AV1_Staging` | Local staging directory for state, control files, temp encodes |
| `NAS_MOVIES` | `\\KieranNAS\Media\Movies` | NAS movie library path |
| `NAS_SERIES` | `\\KieranNAS\Media\Series` | NAS series library path |
| `PLEX_URL` | `http://192.168.4.43:32400` | Plex server URL |
| `PLEX_TOKEN` | *(empty)* | Plex authentication token |
| `TMDB_API_KEY` | *(set)* | TMDb API key for metadata enrichment |

### Control files (`F:\AV1_Staging\control\`)

The pipeline watches these JSON files at runtime. Edit them live (the pipeline re-reads each loop iteration) or use the Controls page in the dashboard:

- **`profiles.json`** -- map titles/patterns to quality profiles (protected/baseline/lossy/tonemap)
- **`priority.json`** -- force specific files to the front of the queue, or pattern-match for priority
- **`gentle.json`** -- per-file or per-pattern CQ offsets for fine-tuning quality
- **`skip.json`** -- list of file paths to skip entirely
- **`reencode.json`** -- force re-encode of files already processed (e.g. with a better profile)
- **`pause_all.json`** / **`pause_fetch.json`** / **`pause_encode.json`** -- drop these to pause

The pipeline auto-creates persistent control files with sensible defaults on startup (see `PipelineControl._PERSISTENT_FILES`).

### Key staging limits

| Setting | Default | Description |
|---------|---------|-------------|
| `max_staging_bytes` | 2.5 TB | Total local staging budget |
| `max_fetch_buffer_bytes` | 200 GB | Maximum pre-fetched but not yet encoded |
| `min_free_space_bytes` | 50 GB | Minimum free space on staging drive |

### Entry points (pyproject.toml)

| Command | Module |
|---------|--------|
| `pipeline` | `pipeline.__main__:main` |
| `scan` | `tools.scanner:main` |
| `subtitles` | `tools.subtitles:main` |
| `plex-collections` | `tools.plex_collections:main` |
| `plex-metadata` | `tools.plex_metadata:main` |
| `dashboard` | `server:run` |
