"""Shared fixtures for the NAS AV1 pipeline test suite."""

import os
import sys
import tempfile

import pytest

# Ensure the project root is on sys.path so `pipeline`, `server`, `paths` are importable.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Override paths BEFORE any project code imports `paths` —
# point staging at a temp dir so tests never touch the real NAS / staging drive.
_test_staging = tempfile.mkdtemp(prefix="av1_test_staging_")
os.environ["AV1_STAGING"] = _test_staging
os.environ["NAS_MOVIES"] = os.path.join(_test_staging, "movies")
os.environ["NAS_SERIES"] = os.path.join(_test_staging, "series")
os.makedirs(os.environ["NAS_MOVIES"], exist_ok=True)
os.makedirs(os.environ["NAS_SERIES"], exist_ok=True)


@pytest.fixture()
def tmp_state_db(tmp_path):
    """Yield a path to a fresh temporary SQLite state database."""
    return str(tmp_path / "pipeline_state.db")


@pytest.fixture()
def sample_report():
    """Return a dict mimicking a media_report.json with a few representative files."""
    return {
        "generated": "2026-04-15T12:00:00",
        "files": [
            {
                "filepath": r"\\KieranNAS\Media\Movies\Inception (2010)\Inception (2010).mkv",
                "filename": "Inception (2010).mkv",
                "library_type": "movie",
                "video": {"codec": "AV1", "codec_raw": "av1", "width": 1920, "height": 1080},
                "audio_streams": [
                    {"codec": "EAC3", "codec_raw": "eac3", "channels": 6, "language": "eng", "bitrate_kbps": 640}
                ],
                "subtitle_streams": [{"language": "eng", "title": "", "codec": "subrip"}],
                "resolution": "1080p",
                "hdr": False,
                "size_bytes": 5_000_000_000,
                "tmdb": {"id": 27205, "title": "Inception"},
            },
            {
                "filepath": r"\\KieranNAS\Media\Movies\Blade Runner 2049 (2017)\Blade Runner 2049 (2017).mkv",
                "filename": "Blade Runner 2049 (2017).mkv",
                "library_type": "movie",
                "video": {"codec": "HEVC (H.265)", "codec_raw": "hevc", "width": 3840, "height": 2160},
                "audio_streams": [
                    {"codec": "TrueHD", "codec_raw": "truehd", "channels": 8, "language": "eng", "bitrate_kbps": 4500},
                    {"codec": "AC-3", "codec_raw": "ac3", "channels": 6, "language": "eng", "bitrate_kbps": 640},
                ],
                "subtitle_streams": [
                    {"language": "eng", "title": "", "codec": "subrip"},
                    {"language": "fra", "title": "French", "codec": "subrip"},
                ],
                "resolution": "4K",
                "hdr": True,
                "size_bytes": 50_000_000_000,
            },
            {
                "filepath": r"\\KieranNAS\Media\Series\The Wire\Season 01\The Wire S01E01.mkv",
                "filename": "The Wire S01E01.mkv",
                "library_type": "series",
                "video": {"codec": "H.264", "codec_raw": "h264", "width": 1920, "height": 1080},
                "audio_streams": [
                    {"codec": "AAC", "codec_raw": "aac", "channels": 2, "language": "eng", "bitrate_kbps": 192}
                ],
                "subtitle_streams": [],
                "resolution": "1080p",
                "hdr": False,
                "size_bytes": 1_500_000_000,
            },
        ],
    }


@pytest.fixture()
def test_app():
    """Return a FastAPI TestClient for the server application."""
    from fastapi.testclient import TestClient

    from server import app

    return TestClient(app)
