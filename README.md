# NAS Cleanup — AV1 Pipeline

Personal AV1 re-encoding pipeline for a home NAS. Converts H.264/HEVC media to AV1 using NVENC hardware encoding (RTX 4000+ GPU), with a web dashboard for monitoring and control.

**This is a personal project / learning exercise** — shared for portfolio purposes and for friends running similar setups.

## Quick Start (after reboot)

```bash
cd C:\Projects\NASCleanup

# 1. Start the dashboard (accessible from any device on LAN)
uv run python -m server

# 2. Start the pipeline (in a separate terminal)
uv run python -m pipeline --resume
```

Dashboard: **http://localhost:8000** (or `http://<your-PC-IP>:8000` from phone/other devices)

## First Run (no previous state)

```bash
# Scan your library first
uv run python -m tools.scanner

# Start pipeline with the report
uv run python -m pipeline
```

## Common Tasks

| What | Command |
|-|-|
| Resume pipeline after shutdown | `uv run python -m pipeline --resume` |
| Start dashboard | `uv run python -m server` |
| Dry run (see what would happen) | `uv run python -m pipeline --dry-run` |
| Run specific tier only | `uv run python -m pipeline --tier "H.264 1080p"` |
| Scan + find non-English films | `uv run python -m tools.scanner --non-english-csv non_eng.csv` |
| Rebuild frontend after edits | `cd frontend && npm run build` |

## Pause / Resume

Use the dashboard Controls tab, or drop/delete JSON files in `E:\AV1_Staging\control\`.

## Project Structure

```
paths.py                 Shared env-var-backed path defaults
pipeline/                AV1 encoding pipeline package
  __main__.py            CLI entry point (argparse + main)
  config.py              Encoding presets, constants, DEFAULT_CONFIG
  state.py               FileStatus enum, PipelineState (persistent JSON)
  queue.py               Priority queue builder from media report
  encoding.py            FFmpeg command building, AV1 encoding logic
  stages.py              Fetch, upload, verify, replace stages
  control.py             Pause/resume/skip/priority/gentle overrides
  runner.py              Pipeline orchestration, prefetch thread

server/                  FastAPI dashboard server (port 8000)
  __init__.py            App, routes, process manager
  __main__.py            `python -m server` entry point

tools/                   Utility scripts
  scanner.py             Scan NAS with ffprobe -> media_report.json (+ --non-english-csv)
  plex_languages.py      Find non-English movies via Plex database backup
  strip_tags.py          Strip release group tags from series filenames
  fix_extensions.py      Fix missing file extensions on series files

frontend/                Vite + React dashboard (built files in dist/)
control_templates/       Template JSON files for pipeline control
```

## Utility Tools

All tools support `--help` and accept CLI arguments.

```bash
# Scan library and produce report
python -m tools.scanner --movies "Z:\Movies" --series "Z:\Series"

# Find non-English movies (Plex DB)
python -m tools.plex_languages --backup path/to/databaseBackup

# Strip release group tags from series filenames
python -m tools.strip_tags --root "Z:\Series"

# Fix missing file extensions
python -m tools.fix_extensions --root "Z:\Series"
```

## Environment Variables

All tools read defaults from `paths.py`, which respects these env vars:

| Variable | Default | Used by |
|-|-|-|
| `AV1_STAGING` | `E:\AV1_Staging` | Pipeline, server, scanner |
| `NAS_MOVIES` | `Z:\Movies` | Scanner |
| `NAS_SERIES` | `Z:\Series` | Scanner, strip_tags, fix_extensions |

See `.env.example` for a template.

## Entry Points (via pyproject.toml)

After `uv sync`, these commands are available:

| Command | Runs |
|-|-|
| `pipeline` | `pipeline.__main__:main` |
| `scan` | `tools.scanner:main` |
| `dashboard` | `server:run` |

## Requirements

- Python 3.11+
- FFmpeg with NVENC AV1 support (RTX 4000+ GPU)
- [uv](https://github.com/astral-sh/uv) for dependency management
- Node.js (for frontend development only)
