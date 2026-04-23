"""Regression tests for compliance math — zero audio is NOT compliant.

The 2026-04-23 postmortem revealed that the dashboard treated 1,787 audio-less
AV1 files as "Done" because the old idiom was roughly:

    audio_codec_ok = all(... for a in audio_streams) if audio_streams else True

An empty list of audio streams is not "vacuously compliant" — it's damage
(almost always the pipeline silently stripping audio during a previous encode).
These tests exercise the full ``/api/library-completion`` and
``/api/completion-missing?category=audio`` endpoints against fixture media
reports to lock in the correct behaviour.
"""
from __future__ import annotations

import json

import pytest

from paths import MEDIA_REPORT


@pytest.fixture()
def write_report():
    """Helper that writes a media_report.json fixture and cleans up."""
    created: list = []

    def _write(payload: dict) -> None:
        MEDIA_REPORT.parent.mkdir(parents=True, exist_ok=True)
        MEDIA_REPORT.write_text(json.dumps(payload), encoding="utf-8")
        created.append(MEDIA_REPORT)
        # Bust the 5-second completion cache between tests so each write is
        # actually reflected in the response.
        from server.routers import library as _lib

        _lib._completion_cache = None
        _lib._completion_cache_time = 0.0

    yield _write

    for p in created:
        try:
            p.unlink()
        except FileNotFoundError:
            pass


def _movie(filepath: str, audio: list, codec_raw: str = "av1", subs: list | None = None) -> dict:
    """Build a minimal movie entry."""
    return {
        "filepath": filepath,
        "filename": filepath.rsplit("\\", 1)[-1],
        "library_type": "movie",
        "video": {"codec_raw": codec_raw, "codec": codec_raw.upper()},
        "audio_streams": audio,
        "subtitle_streams": subs if subs is not None else [{"language": "eng", "codec": "subrip"}],
        "hdr": False,
    }


class TestLibraryCompletionZeroAudio:
    """``/api/library-completion`` must not treat zero-audio files as Done."""

    def test_zero_audio_av1_not_fully_done(self, test_app, write_report) -> None:
        """The canonical 2026-04-23 incident case: AV1 file with no audio."""
        write_report(
            {
                "files": [
                    _movie(
                        r"\\KieranNAS\Media\Movies\Broken (2020).mkv",
                        audio=[],  # zero-audio — the damage we must detect
                    ),
                ]
            }
        )
        resp = test_app.get("/api/library-completion")
        assert resp.status_code == 200
        data = resp.json()

        assert data["total"] == 1
        # The file is AV1, so video compliance is fine.
        assert data["av1"] == 1
        # But audio is NOT ok — zero streams is damage, not compliance.
        assert data["eac3_done"] == 0
        # And the file must NOT be counted as fully done.
        assert data["fully_done"] == 0
        # Must show up in "needs audio" (AV1 + non-compliant audio).
        assert data["needs_audio"] == 1

    def test_zero_audio_missing_key_not_fully_done(self, test_app, write_report) -> None:
        """A file with no ``audio_streams`` key at all is also damage."""
        entry = _movie(
            r"\\KieranNAS\Media\Movies\NoKey (2020).mkv",
            audio=[],
        )
        entry.pop("audio_streams")
        write_report({"files": [entry]})
        resp = test_app.get("/api/library-completion")
        data = resp.json()
        assert data["fully_done"] == 0
        assert data["eac3_done"] == 0

    def test_healthy_eac3_av1_is_done(self, test_app, write_report) -> None:
        """Sanity check — a fully-compliant file still counts as Done."""
        write_report(
            {
                "files": [
                    _movie(
                        r"\\KieranNAS\Media\Movies\Good (2020).mkv",
                        audio=[{"codec_raw": "eac3", "channels": 6, "language": "eng"}],
                        subs=[{"language": "eng", "codec": "subrip"}],
                    ),
                ]
            }
        )
        resp = test_app.get("/api/library-completion")
        data = resp.json()
        assert data["fully_done"] == 1
        assert data["eac3_done"] == 1


class TestCompletionMissingAudio:
    """``/api/completion-missing?category=audio`` surfaces zero-audio files."""

    def test_zero_audio_file_appears_in_drilldown(self, test_app, write_report) -> None:
        """The drill-down list for category=audio must include the zero-audio AV1 file."""
        fp = r"\\KieranNAS\Media\Movies\Broken (2020).mkv"
        write_report(
            {
                "files": [
                    _movie(fp, audio=[]),
                ]
            }
        )
        resp = test_app.get("/api/completion-missing", params={"category": "audio"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["files"][0]["filepath"] == fp


class TestQuickWinsZeroAudio:
    """``POST /api/quick-wins`` must treat zero-audio AV1 files as non-compliant."""

    def test_zero_audio_av1_queued_as_quick_win(self, test_app, write_report, tmp_path) -> None:
        """Zero-audio AV1 file should end up on the priority force list."""
        from server.helpers import CONTROL_DIR

        # Make sure we have a clean control dir for this test.
        CONTROL_DIR.mkdir(parents=True, exist_ok=True)
        priority = CONTROL_DIR / "priority.json"
        if priority.exists():
            priority.unlink()

        fp = r"\\KieranNAS\Media\Movies\Broken (2020).mkv"
        write_report(
            {
                "files": [
                    _movie(fp, audio=[]),
                ]
            }
        )
        resp = test_app.post("/api/quick-wins")
        assert resp.status_code == 200
        data = resp.json()
        # Zero-audio AV1 file must be treated as non-compliant → queued.
        assert data["ok"] is True
        assert data["added"] >= 1
