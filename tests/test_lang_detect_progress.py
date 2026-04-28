"""Tests for the whisper batch progress reporting (Track A).

The lang-detection batch tool writes ``F:\\AV1_Staging\\lang_detect_state.json``
after each file so the dashboard can render a progress card. These tests cover:

  * write_progress_state: atomic write, valid JSON, expected fields
  * clear_progress_state: marks ``running: false`` + sets ``finished_at``
  * /api/lang-detect/status endpoint: reads the file, graceful fallback
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline import language as lang_mod


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch) -> Path:
    """Point write_progress_state at a tmp dir."""
    import paths
    monkeypatch.setattr(paths, "STAGING_DIR", tmp_path)
    return tmp_path


def test_write_progress_state_creates_file(state_dir):
    state = {"running": True, "total": 100, "processed": 5}
    lang_mod.write_progress_state(state)
    p = state_dir / "lang_detect_state.json"
    assert p.exists()
    with p.open(encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded == state


def test_write_progress_state_atomic(state_dir):
    """Writes via a tmp + rename so a half-written file is never visible."""
    p = state_dir / "lang_detect_state.json"
    # Prime the file with a known-good state
    initial = {"running": True, "total": 100, "processed": 5}
    lang_mod.write_progress_state(initial)
    # Confirm tmp file does not survive after the write
    assert not (state_dir / "lang_detect_state.json.tmp").exists()
    # Re-write — the rename should atomically replace
    lang_mod.write_progress_state({"running": True, "total": 100, "processed": 50})
    with p.open(encoding="utf-8") as f:
        assert json.load(f)["processed"] == 50


def test_clear_progress_state_marks_finished(state_dir):
    lang_mod.write_progress_state({"running": True, "total": 100, "processed": 100})
    lang_mod.clear_progress_state()
    p = state_dir / "lang_detect_state.json"
    with p.open(encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded["running"] is False
    assert "finished_at" in loaded


def test_clear_progress_state_noop_if_file_missing(state_dir):
    """Should not crash when called before any progress write."""
    lang_mod.clear_progress_state()  # No exception
    assert not (state_dir / "lang_detect_state.json").exists()


def test_lang_detect_status_endpoint_returns_idle_when_missing(tmp_path, monkeypatch):
    """When the state file doesn't exist, endpoint returns {running: False}."""
    import paths
    monkeypatch.setattr(paths, "STAGING_DIR", tmp_path)

    from server.routers.admin import get_lang_detect_status

    result = get_lang_detect_status()
    assert result == {"running": False}


def test_lang_detect_status_endpoint_returns_state(tmp_path, monkeypatch):
    """When the state file exists, endpoint returns its parsed contents."""
    import paths
    monkeypatch.setattr(paths, "STAGING_DIR", tmp_path)

    state = {
        "running": True,
        "total": 2965,
        "processed": 142,
        "detected": 138,
        "failed": 4,
        "current_file": "Bob's Burgers S03E05.mkv",
        "rate_files_per_min": 8.3,
        "eta_secs": 20460,
        "recent": [],
    }
    (tmp_path / "lang_detect_state.json").write_text(json.dumps(state), encoding="utf-8")

    from server.routers.admin import get_lang_detect_status

    result = get_lang_detect_status()
    assert result == state


def test_lang_detect_status_endpoint_handles_corrupt_file(tmp_path, monkeypatch):
    """Malformed JSON shouldn't crash the dashboard — fall back to idle."""
    import paths
    monkeypatch.setattr(paths, "STAGING_DIR", tmp_path)

    (tmp_path / "lang_detect_state.json").write_text("not valid json {", encoding="utf-8")

    from server.routers.admin import get_lang_detect_status

    result = get_lang_detect_status()
    assert result == {"running": False}
