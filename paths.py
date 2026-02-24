"""Single source of truth for env-var-backed paths used across the project."""

import os
from pathlib import Path

STAGING_DIR = Path(os.environ.get("AV1_STAGING", r"E:\AV1_Staging"))
NAS_MOVIES = Path(os.environ.get("NAS_MOVIES", r"Z:\Movies"))
NAS_SERIES = Path(os.environ.get("NAS_SERIES", r"Z:\Series"))

MEDIA_REPORT = STAGING_DIR / "media_report.json"
