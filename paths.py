"""Single source of truth for env-var-backed paths used across the project."""

import os
from pathlib import Path

# Load .env file if present (stdlib only — no dotenv dependency)
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

STAGING_DIR = Path(os.environ.get("AV1_STAGING", r"F:\AV1_Staging"))
NAS_MOVIES = Path(os.environ.get("NAS_MOVIES", r"\\KieranNAS\Media\Movies"))
NAS_SERIES = Path(os.environ.get("NAS_SERIES", r"\\KieranNAS\Media\Series"))

MEDIA_REPORT = STAGING_DIR / "media_report.json"
MEDIA_REPORT_LOCK = STAGING_DIR / "media_report.lock"
PIPELINE_STATE_DB = STAGING_DIR / "pipeline_state.db"

# Plex server config (for triggering library scans after renames/encodes)
PLEX_URL = os.environ.get("PLEX_URL", "http://192.168.4.43:32400")
PLEX_TOKEN = os.environ.get("PLEX_TOKEN", "")

# TMDb API key (for metadata enrichment)
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "363884c381745615bb12a803becf09b6")
