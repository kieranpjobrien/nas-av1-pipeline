# NASCleanup

Personal AV1 re-encoding pipeline for a home NAS. Converts H.264/HEVC media to AV1 using NVENC hardware encoding (RTX 4000+ GPU).

## Package Manager

This project uses **uv** (not Poetry). Run scripts with `uv run python -m <module>`.

## Architecture

### Shared config
- `paths.py` — Single source of truth for env-var-backed paths (`STAGING_DIR`, `NAS_MOVIES`, `NAS_SERIES`, `MEDIA_REPORT`)

### Core pipeline (`pipeline/` package)
The encoding pipeline is split into focused modules:

- `config.py` — `DEFAULT_CONFIG`, encoding presets, `QUALITY_PROFILES`, constants
- `state.py` — `FileStatus` enum, `PipelineState` (persistent JSON state tracker)
- `queue.py` — `build_priority_queue()`, tier matching, filtering
- `encoding.py` — `build_ffmpeg_cmd()`, `encode_file()`, audio codec logic, remuxing
- `stages.py` — `fetch()`, `upload()`, `verify()`, `replace_original()`
- `control.py` — `PipelineControl` class: pause/resume/skip/priority/gentle/profiles overrides
- `runner.py` — `Pipeline` class: main orchestration loop, prefetch thread, signal handling
- `__main__.py` — `argparse` CLI + `main()` entry point

Run via: `python -m pipeline --resume`

### Dashboard (`server/` package)
- `server/__init__.py` — FastAPI app, routes, process manager (port 8000)
- `server/__main__.py` — `python -m server` entry point (uvicorn.run)
- `frontend/` — Vite + React dashboard (built files in `frontend/dist/`)

### Utility tools (`tools/` package)
- `tools/scanner.py` — Scans NAS directories with ffprobe, outputs `media_report.json`. Also supports `--non-english-csv` to find files missing English audio.
- `tools/plex_languages.py` — Finds movies without English audio tracks via Plex database backup
- `tools/plex_collections.py` — Plex collection/genre manager: audit, find missing genres, apply rules (studio→collection mapping)
- `tools/subtitles.py` — Subtitle availability checker: finds files missing English subtitles (embedded + external)
- `tools/strip_tags.py` — Strips release group tags from series/movie filenames (preserves edition tags)
- `tools/fix_extensions.py` — Fixes missing `.mkv` extensions on series files

## Key paths
- `E:\AV1_Staging\` — Local staging drive (pipeline state, control files, temp encodes)
- `Z:\Movies\`, `Z:\Series\` — NAS media libraries (defaults, configurable via env/args)
- `control_templates/` — Template JSON files for pipeline control

## Dependencies
- Pipeline: stdlib only (no pip packages)
- Server: `fastapi`, `uvicorn` (managed via `pyproject.toml` + `uv`)
- Frontend: `npm` (in `frontend/`)

## Quality Profiles

Three encoding quality profiles, assigned via `control/profiles.json`:
- **protected** — Lower CQ (-3), p7 preset, full multipass, 32-frame lookahead (reference films, visually important content)
- **baseline** — Default settings (standard balance)
- **lossy** — Higher CQ (+6), p4 preset, no multipass (sitcoms, reality TV, expendable content)

Assign by path prefix or glob pattern. Stacks with `gentle.json` and `reencode.json`.

## Running
```bash
uv run python -m tools.scanner                    # Scan library
uv run python -m pipeline                         # First run
uv run python -m pipeline --resume                # Resume
uv run python -m server                           # Dashboard
uv run python -m tools.subtitles                  # Check subtitle availability
uv run python -m tools.plex_collections audit     # Plex genre/collection stats
uv run python -m tools.strip_tags                 # Preview filename cleanup
```

## Entry Points (pyproject.toml)
```
pipeline          -> pipeline.__main__:main
scan              -> tools.scanner:main
subtitles         -> tools.subtitles:main
plex-collections  -> tools.plex_collections:main
dashboard         -> server:run
```
