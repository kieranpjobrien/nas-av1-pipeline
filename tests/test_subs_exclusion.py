"""Tests for the per-title subs-optional exclusion list.

Covers the substring matcher in pipeline.subs_exclusion AND the integration
with server.routers.library._compliance_for_entry — a file matched by a
pattern must:
  * stop adding ``subs_english_count_wrong`` to violations
  * still flag ``subs_foreign_present`` / ``subs_hi_present`` (those represent
    active garbage to clean up regardless of the no-subs-needed policy)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline import subs_exclusion
from server.routers.library import _compliance_for_entry


@pytest.fixture
def patterns_file(tmp_path: Path, monkeypatch) -> Path:
    """Create a subs_optional.json in a tmp dir and point the loader at it."""
    f = tmp_path / "subs_optional.json"
    f.write_text(
        json.dumps({"patterns": ["Puffin Rock", "Birth of a Nation"]}),
        encoding="utf-8",
    )
    monkeypatch.setattr(subs_exclusion, "DEFAULT_PATH", f)
    subs_exclusion.reset_cache_for_tests()
    return f


def test_matches_substring(patterns_file):
    assert subs_exclusion.is_subs_optional(
        r"\\NAS\Series\Puffin Rock\Season 1\Puffin.Rock.S01E01.mkv"
    )


def test_matches_case_insensitive(patterns_file):
    assert subs_exclusion.is_subs_optional(r"\\nas\media\PUFFIN ROCK\episode.mkv")


def test_does_not_match_unrelated(patterns_file):
    assert not subs_exclusion.is_subs_optional(r"\\NAS\Series\The Office\E01.mkv")


def test_empty_filepath_returns_false(patterns_file):
    assert not subs_exclusion.is_subs_optional("")


def test_missing_file_returns_false(tmp_path, monkeypatch):
    monkeypatch.setattr(subs_exclusion, "DEFAULT_PATH", tmp_path / "does_not_exist.json")
    subs_exclusion.reset_cache_for_tests()
    assert not subs_exclusion.is_subs_optional("anything")


def test_malformed_json_returns_false(tmp_path, monkeypatch):
    f = tmp_path / "subs_optional.json"
    f.write_text("not valid json {", encoding="utf-8")
    monkeypatch.setattr(subs_exclusion, "DEFAULT_PATH", f)
    subs_exclusion.reset_cache_for_tests()
    assert not subs_exclusion.is_subs_optional("Puffin Rock")


def test_mtime_change_repicks_patterns(tmp_path, monkeypatch):
    f = tmp_path / "subs_optional.json"
    f.write_text(json.dumps({"patterns": ["Original"]}), encoding="utf-8")
    monkeypatch.setattr(subs_exclusion, "DEFAULT_PATH", f)
    subs_exclusion.reset_cache_for_tests()
    assert subs_exclusion.is_subs_optional("Original Show")
    assert not subs_exclusion.is_subs_optional("Updated Show")

    # Mutate file with a future mtime so the cache picks it up
    import os
    f.write_text(json.dumps({"patterns": ["Updated"]}), encoding="utf-8")
    future = f.stat().st_mtime + 10
    os.utime(f, (future, future))
    subs_exclusion.reset_cache_for_tests()
    assert subs_exclusion.is_subs_optional("Updated Show")


# ---------------------------------------------------------------------------
# Integration: _compliance_for_entry honours the exclusion list
# ---------------------------------------------------------------------------


def _av1_entry(filepath: str, subtitle_streams=None) -> dict:
    """Build a minimal media-report entry: AV1 video, valid EAC-3 audio."""
    return {
        "filepath": filepath,
        "video": {"codec_raw": "av1"},
        "audio_streams": [{"codec_raw": "eac3", "language": "eng"}],
        "subtitle_streams": subtitle_streams or [],
        "tmdb": {"original_language": "en"},
    }


def test_excluded_title_with_no_subs_passes_compliance(patterns_file):
    """Puffin Rock with zero subs is compliant (was 'needs_subs' before)."""
    entry = _av1_entry(r"\\NAS\Series\Puffin Rock\E01.mkv")
    c = _compliance_for_entry(entry)
    assert c["subs_ok"] is True
    assert "subs_english_count_wrong" not in c["violations"]


def test_non_excluded_title_with_no_subs_still_flagged(patterns_file):
    """Plain show with zero subs still fails compliance."""
    entry = _av1_entry(r"\\NAS\Series\The Office\E01.mkv")
    c = _compliance_for_entry(entry)
    assert c["subs_ok"] is False
    assert "subs_english_count_wrong" in c["violations"]


def test_excluded_title_with_foreign_sub_still_flagged(patterns_file):
    """Even on excluded titles, an actual foreign sub is still garbage we surface."""
    entry = _av1_entry(
        r"\\NAS\Series\Puffin Rock\E01.mkv",
        subtitle_streams=[{"language": "fre", "title": "French"}],
    )
    c = _compliance_for_entry(entry)
    # The exclusion only covers "no English sub" — it doesn't authorise leaving
    # foreign tracks behind.
    assert c["no_foreign_subs"] is False
    assert c["subs_ok"] is False
    assert "subs_foreign_present" in c["violations"]


def test_excluded_title_with_english_sub_compliant(patterns_file):
    """Excluded title that happens to have an English sub is also fine."""
    entry = _av1_entry(
        r"\\NAS\Movies\Birth of a Nation (1915)\Birth.mkv",
        subtitle_streams=[{"language": "eng", "title": ""}],
    )
    c = _compliance_for_entry(entry)
    assert c["subs_ok"] is True
