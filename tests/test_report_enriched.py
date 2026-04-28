"""Regression tests for the whisper-detection persistence path.

Background: pre-2026-04-29, ``pipeline.report.update_entry`` re-probed each
file from disk and only carried forward ``detected_language`` from the OLD
report entry. New whisper detections done during encode were held only in
the worker's in-memory ``item`` dict — they never reached media_report.json.

Result: ``Langs Known`` stagnated at 65.3% (6,020 files with ``und`` tracks),
and the strip code (Phase 1 inviolate guard) had no way to ever clear them.

Fix: ``update_entry`` now accepts ``enriched_streams`` — the worker passes
the whisper-enriched stream lists explicitly, and they are merged onto the
re-probed entry BEFORE the old-report fall-through.

These tests pin the contract.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline import report as report_mod


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_report(tmp_path: Path, monkeypatch) -> Path:
    """Create a tmp media_report.json and point patch_report at it."""
    rp = tmp_path / "media_report.json"
    rp.write_text(json.dumps({"files": [], "summary": {}}), encoding="utf-8")

    # patch_report uses MEDIA_REPORT from paths — monkeypatch there.
    import paths
    monkeypatch.setattr(paths, "MEDIA_REPORT", rp)
    # tools.report_lock imports MEDIA_REPORT once at module load — patch its bound name too.
    import tools.report_lock as report_lock
    monkeypatch.setattr(report_lock, "MEDIA_REPORT", rp)
    return rp


@pytest.fixture
def fake_probe(monkeypatch):
    """Stub probe_file/build_file_entry so we don't actually run ffprobe."""
    def _fake_probe(filepath: str):
        return {"format": {"duration": "60.0"}, "streams": []}

    def _fake_build(filepath: str, probe_data, library_type: str = ""):
        return {
            "filepath": filepath,
            "filename": Path(filepath).name,
            "library_type": library_type,
            "video": {"codec_raw": "av1", "codec": "AV1"},
            "audio_streams": [
                {"codec": "eac3", "codec_raw": "eac3", "language": "und"},
                {"codec": "eac3", "codec_raw": "eac3", "language": "und"},
            ],
            "subtitle_streams": [],
        }

    monkeypatch.setattr(report_mod, "probe_file", _fake_probe)
    monkeypatch.setattr(report_mod, "build_file_entry", _fake_build)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_update_entry_writes_enriched_detected_language(tmp_report, fake_probe):
    """Newly-detected languages from whisper must persist to media_report.json."""
    fp = r"\\NAS\Series\Test\E01.mkv"
    enriched = {
        "audio_streams": [
            {"detected_language": "eng", "detection_confidence": 0.92, "detection_method": "whisper_tiny"},
            {"detected_language": "fre", "detection_confidence": 0.88, "detection_method": "whisper_small"},
        ],
        "subtitle_streams": [],
    }
    ok = report_mod.update_entry(fp, "series", enriched_streams=enriched)
    assert ok is True

    with tmp_report.open(encoding="utf-8") as f:
        rep = json.load(f)
    files = rep.get("files", [])
    assert len(files) == 1
    saved = files[0]
    assert saved["audio_streams"][0]["detected_language"] == "eng"
    assert saved["audio_streams"][0]["detection_confidence"] == 0.92
    assert saved["audio_streams"][1]["detected_language"] == "fre"


def test_update_entry_no_enriched_keeps_legacy_behaviour(tmp_report, fake_probe):
    """Without enriched_streams arg, behaviour matches pre-fix (only old-report carryover)."""
    fp = r"\\NAS\Series\Test\E02.mkv"
    ok = report_mod.update_entry(fp, "series")  # no enriched_streams
    assert ok is True

    with tmp_report.open(encoding="utf-8") as f:
        rep = json.load(f)
    saved = rep["files"][0]
    # No detection data anywhere — neither enriched nor old report has it.
    assert "detected_language" not in saved["audio_streams"][0] or saved["audio_streams"][0].get("detected_language") is None


def test_update_entry_enriched_does_not_clobber_existing_old_detection(tmp_report, fake_probe):
    """If the OLD report already has a detection for stream N and enriched
    doesn't supply one for that stream, the old value is preserved."""
    fp = r"\\NAS\Series\Test\E03.mkv"

    # Seed old report with a detection on stream 0 only
    old_rep = {
        "files": [{
            "filepath": fp,
            "filename": "E03.mkv",
            "library_type": "series",
            "video": {"codec_raw": "av1"},
            "audio_streams": [
                {"language": "und", "detected_language": "eng"},
                {"language": "und"},
            ],
            "subtitle_streams": [],
        }]
    }
    tmp_report.write_text(json.dumps(old_rep), encoding="utf-8")

    # Enriched only has new detection for stream 1
    enriched = {
        "audio_streams": [
            {},  # nothing new for stream 0
            {"detected_language": "spa"},
        ],
    }
    report_mod.update_entry(fp, "series", enriched_streams=enriched)

    with tmp_report.open(encoding="utf-8") as f:
        rep = json.load(f)
    saved = rep["files"][0]
    # Stream 0: kept "eng" from old report (since enriched didn't cover it)
    assert saved["audio_streams"][0]["detected_language"] == "eng"
    # Stream 1: got "spa" from enriched
    assert saved["audio_streams"][1]["detected_language"] == "spa"


def test_update_entry_enriched_takes_precedence_over_old(tmp_report, fake_probe):
    """When both old report and enriched have a detection for stream N,
    enriched wins (it's the fresher signal)."""
    fp = r"\\NAS\Series\Test\E04.mkv"

    old_rep = {
        "files": [{
            "filepath": fp,
            "filename": "E04.mkv",
            "library_type": "series",
            "video": {"codec_raw": "av1"},
            "audio_streams": [
                {"language": "und", "detected_language": "eng"},  # old says eng
                {"language": "und"},
            ],
            "subtitle_streams": [],
        }]
    }
    tmp_report.write_text(json.dumps(old_rep), encoding="utf-8")

    # Enriched re-detects stream 0 as fre (whisper run with more data)
    enriched = {
        "audio_streams": [
            {"detected_language": "fre", "detection_confidence": 0.95},
            {},
        ],
    }
    report_mod.update_entry(fp, "series", enriched_streams=enriched)

    with tmp_report.open(encoding="utf-8") as f:
        rep = json.load(f)
    saved = rep["files"][0]
    # Enriched wins
    assert saved["audio_streams"][0]["detected_language"] == "fre"
    assert saved["audio_streams"][0]["detection_confidence"] == 0.95
