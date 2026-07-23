"""Tests for the audio_foreign_ok override (2026-07-14).

The user can opt English-canonical foreign-origin films (Leone's Dollars
westerns, HK films watched in the English dub) OUT of the foreign-audio
policy so they encode instead of sitting in flagged_foreign_audio forever.
Mirrors the subs_optional exclusion mechanism.
"""
from __future__ import annotations

import json

import pytest

from pipeline import audio_exclusion
from pipeline.config import build_config
from pipeline.qualify import QualifyOutcome, qualify_file


@pytest.fixture()
def cfg():
    return build_config()


@pytest.fixture(autouse=True)
def _clear_cache():
    audio_exclusion.reset_cache_for_tests()
    yield
    audio_exclusion.reset_cache_for_tests()


def _write(path, patterns):
    path.write_text(json.dumps({"patterns": patterns}), encoding="utf-8")


def test_is_foreign_audio_ok_substring_match(tmp_path):
    p = tmp_path / "audio_foreign_ok.json"
    _write(p, ["A Fistful of Dollars (1964)"])
    fp = r"\\KieranNAS\Media\Movies\A Fistful of Dollars (1964)\A Fistful of Dollars (1964).mp4"
    assert audio_exclusion.is_foreign_audio_ok(fp, path=p) is True
    audio_exclusion.reset_cache_for_tests()
    assert audio_exclusion.is_foreign_audio_ok(
        r"\\KieranNAS\Media\Movies\Amelie (2001)\Amelie (2001).mkv", path=p) is False


def test_missing_file_is_permissive_empty(tmp_path):
    """A missing control file must never crash — returns empty (nothing excluded)."""
    assert audio_exclusion.is_foreign_audio_ok("anything", path=tmp_path / "nope.json") is False


def _fistful_english_only_entry() -> dict:
    """English-audio-only copy of an Italian-original film — normally flags."""
    return {
        "filepath": r"\\KieranNAS\Media\Movies\A Fistful of Dollars (1964)\A Fistful of Dollars (1964).mp4",
        "filename": "A Fistful of Dollars (1964).mp4",
        "library_type": "movie",
        "duration_seconds": 5940,
        "audio_streams": [{"language": "eng", "codec": "ac3", "channels": 6}],
        "subtitle_streams": [],
        "tmdb": {"original_language": "it", "title": "A Fistful of Dollars"},
        "video": {"codec_raw": "h264"},
    }


def test_without_override_english_only_italian_flags(cfg, monkeypatch):
    monkeypatch.setattr("pipeline.qualify.is_foreign_audio_ok", lambda fp: False)
    result = qualify_file(_fistful_english_only_entry(), cfg, use_whisper=False)
    assert result.outcome == QualifyOutcome.FLAGGED_FOREIGN


def test_override_lets_english_only_italian_encode(cfg, monkeypatch):
    """With the title whitelisted, it must NOT flag — it qualifies for encode."""
    monkeypatch.setattr("pipeline.qualify.is_foreign_audio_ok", lambda fp: True)
    result = qualify_file(_fistful_english_only_entry(), cfg, use_whisper=False)
    assert result.outcome == QualifyOutcome.QUALIFIED, result.rationale
