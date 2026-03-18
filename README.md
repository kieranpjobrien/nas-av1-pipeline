# NAS Cleanup — AV1 Pipeline

Personal AV1 re-encoding pipeline for a home NAS. Converts H.264/HEVC media to AV1 using NVENC hardware encoding (RTX 4000+ GPU), with a web dashboard for monitoring and control.

## Setup (fresh machine / after rebuild)

### Prerequisites

1. **Python 3.11+** — install from https://www.python.org/downloads/ (tick "Add to PATH")
2. **uv** — `pip install uv`
3. **FFmpeg** with NVENC AV1 — download from https://www.gyan.dev/ffmpeg/builds/ ("release full"), extract to `C:\ffmpeg`, add `C:\ffmpeg\bin` to PATH
4. **Node.js LTS** — only needed if editing the dashboard frontend
5. **NVIDIA drivers** — keep up to date for latest NVENC fixes

### Install dependencies

```bash
cd D:\Projects\nas-av1-pipeline

# Python deps
uv sync

# Frontend (already built in dist/, only needed after frontend edits)
cd frontend && npm install && npm run build && cd ..
```

### Map NAS drives

Map your NAS shares as network drives, or set env vars in a `.env` file in the project root:

```
NAS_MOVIES=Z:\Movies
NAS_SERIES=Z:\Series
AV1_STAGING=E:\AV1_Staging
PLEX_URL=http://192.168.4.43:32400
PLEX_TOKEN=your-plex-token-here
```

### Create staging folder

```bash
mkdir E:\AV1_Staging
```

## Quick Start (after reboot)

```bash
# 1. Start the dashboard (http://localhost:8000)
uv run python -m server

# 2. In another terminal — resume the pipeline
uv run python -m pipeline --resume
```

## First Run (no previous state)

```bash
# Scan your library (builds media_report.json)
uv run python -m tools.scanner

# Preview what would be encoded
uv run python -m pipeline --dry-run

# Start encoding
uv run python -m pipeline
```

## Common Tasks

| What | Command |
|-|-|
| Resume pipeline | `uv run python -m pipeline --resume` |
| Start dashboard | `uv run python -m server` |
| Dry run | `uv run python -m pipeline --dry-run` |
| Run specific tier only | `uv run python -m pipeline --tier "H.264 1080p"` |
| Clean filenames (preview) | `uv run python -m tools.strip_tags` |
| Clean filenames (execute) | `uv run python -m tools.strip_tags --execute` |
| Clean movie filenames too | `uv run python -m tools.strip_tags --movies --execute` |
| Check subtitle coverage | `uv run python -m tools.subtitles` |
| Export missing subs CSV | `uv run python -m tools.subtitles --csv missing_subs.csv` |
| Plex collection audit | `uv run python -m tools.plex_collections audit` |
| Find miscategorised films | `uv run python -m tools.plex_collections missing-genres` |
| Apply collection rules | `uv run python -m tools.plex_collections apply-rules --execute` |
| Find non-English films | `uv run python -m tools.scanner --non-english-csv non_eng.csv` |
| Rebuild frontend | `cd frontend && npm run build` |

## Quality Profiles

Three encoding profiles, assigned via `E:\AV1_Staging\control\profiles.json`:

| Profile | CQ offset | Preset | Multipass | Use for |
|-|-|-|-|-|
| **protected** | -3 (better) | p7 (slowest) | fullres | Reference films, visually important content |
| **baseline** | 0 | per-config | per-config | Default — good balance |
| **lossy** | +6 (worse) | p4 (fast) | disabled | Sitcoms, reality TV, expendable content |

Example `profiles.json`:
```json
{
    "paths": {
        "Z:\\Movies\\Interstellar (2014)\\": "protected",
        "Z:\\Series\\Seinfeld\\": "lossy"
    },
    "patterns": {
        "*IMAX*": "protected"
    },
    "default": "baseline"
}
```

Profiles stack with `gentle.json` (per-file CQ tweaks) and `reencode.json` (re-do already-AV1 files).

## Pipeline Control

Drop/edit JSON files in `E:\AV1_Staging\control\` — the pipeline picks them up in real time.

| File | Purpose |
|-|-|
| `pause_all.json` | Pause everything |
| `pause_fetch.json` | Pause fetching (encoding continues) |
| `pause_encode.json` | Pause encoding (fetching continues) |
| `skip.json` | `{"paths": ["Z:\\path\\file.mkv"]}` |
| `priority.json` | `{"paths": ["Z:\\path\\file.mkv"]}` — bump to front |
| `gentle.json` | Per-file/pattern CQ and preset overrides |
| `profiles.json` | Quality profile assignments (protected/baseline/lossy) |
| `reencode.json` | Re-encode already-AV1 files with different CQ |
| `custom_tags.json` | Extra keywords for strip_tags |

Templates are in `control_templates/`.

## Project Structure

```
paths.py                 Shared env-var-backed path defaults
pipeline/                AV1 encoding pipeline
  __main__.py            CLI entry point
  config.py              Encoding presets, quality profiles, constants
  state.py               FileStatus enum, PipelineState (JSON persistence)
  queue.py               Priority queue builder (9 tiers)
  encoding.py            FFmpeg command building, AV1 encoding
  stages.py              Fetch, upload, verify, replace stages
  control.py             File-based control system (pause/gentle/profiles/etc.)
  runner.py              Pipeline orchestration, prefetch thread

server/                  FastAPI dashboard (port 8000)
frontend/                Vite + React dashboard UI

tools/                   Utility scripts
  scanner.py             Scan NAS with ffprobe -> media_report.json
  strip_tags.py          Strip release tags from filenames (preserves edition tags)
  subtitles.py           Check subtitle availability (embedded + external)
  plex_collections.py    Plex collection/genre manager
  plex_languages.py      Find non-English movies via Plex DB backup
  fix_extensions.py      Fix missing .mkv extensions
  duplicates.py          Fuzzy duplicate finder

control_templates/       Template JSON files for pipeline control
```

## Environment Variables

| Variable | Default | Used by |
|-|-|-|
| `AV1_STAGING` | `E:\AV1_Staging` | Pipeline, server, scanner, tools |
| `NAS_MOVIES` | `Z:\Movies` | Scanner, strip_tags, subtitles |
| `NAS_SERIES` | `Z:\Series` | Scanner, strip_tags, subtitles |
| `PLEX_URL` | `http://192.168.4.43:32400` | strip_tags, plex_collections |
| `PLEX_TOKEN` | (none) | strip_tags, plex_collections |

## Entry Points (pyproject.toml)

| Command | Module |
|-|-|
| `pipeline` | `pipeline.__main__:main` |
| `scan` | `tools.scanner:main` |
| `subtitles` | `tools.subtitles:main` |
| `plex-collections` | `tools.plex_collections:main` |
| `dashboard` | `server:run` |

## Requirements

- Python 3.11+
- FFmpeg with NVENC AV1 support (RTX 4000+ GPU)
- [uv](https://github.com/astral-sh/uv) for dependency management
- Node.js LTS (frontend development only)
