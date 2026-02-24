# NASCleanup

Personal AV1 re-encoding pipeline for a home NAS. Converts H.264/HEVC media to AV1 using NVENC hardware encoding (RTX 4000+ GPU).

## Package Manager

This project uses **uv** (not Poetry). Run scripts with `uv run python -m <module>`.

## Architecture

### Shared config
- `paths.py` — Single source of truth for env-var-backed paths (`STAGING_DIR`, `NAS_MOVIES`, `NAS_SERIES`, `MEDIA_REPORT`)

### Core pipeline (`pipeline/` package)
The encoding pipeline is split into focused modules:

- `config.py` — `DEFAULT_CONFIG`, encoding presets, constants
- `state.py` — `FileStatus` enum, `PipelineState` (persistent JSON state tracker)
- `queue.py` — `build_priority_queue()`, tier matching, filtering
- `encoding.py` — `build_ffmpeg_cmd()`, `encode_file()`, audio codec logic, remuxing
- `stages.py` — `fetch()`, `upload()`, `verify()`, `replace_original()`
- `control.py` — `PipelineControl` class: pause/resume/skip/priority/gentle overrides
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
- `tools/strip_tags.py` — Strips release group tags from series filenames
- `tools/fix_extensions.py` — Fixes missing `.mkv` extensions on series files

## Key paths
- `E:\AV1_Staging\` — Local staging drive (pipeline state, control files, temp encodes)
- `Z:\Movies\`, `Z:\Series\` — NAS media libraries (defaults, configurable via env/args)
- `control_templates/` — Template JSON files for pipeline control

## Dependencies
- Pipeline: stdlib only (no pip packages)
- Server: `fastapi`, `uvicorn` (managed via `pyproject.toml` + `uv`)
- Frontend: `npm` (in `frontend/`)

## Running
```bash
uv run python -m tools.scanner                    # Scan library
uv run python -m pipeline                         # First run
uv run python -m pipeline --resume                # Resume
uv run python -m server                           # Dashboard
```

## Entry Points (pyproject.toml)
```
pipeline  -> pipeline.__main__:main
scan      -> tools.scanner:main
dashboard -> server:run
```
