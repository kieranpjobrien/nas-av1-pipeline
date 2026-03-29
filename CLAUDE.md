# NASCleanup

Personal AV1 re-encoding pipeline for a home NAS. Converts H.264/HEVC media to AV1 using NVENC hardware encoding (RTX 4000+ GPU).

## Essentials
- Package manager: **uv** (not Poetry). Run with `uv run python -m <module>`
- `paths.py` is the single source of truth for all env-var-backed paths
- Pipeline (`pipeline/`) uses stdlib only — no pip dependencies
- Server (`server/`) uses FastAPI + uvicorn; frontend is Vite + React in `frontend/`
- Three quality profiles (protected/baseline/lossy) assigned via `control/profiles.json`

## Key Paths
- `F:\AV1_Staging\` — Local staging (state, control files, temp encodes)
- `\\KieranNAS\Media\Movies\`, `\\KieranNAS\Media\Series\` — NAS libraries
- `control_templates/` — Template JSON files for pipeline control
