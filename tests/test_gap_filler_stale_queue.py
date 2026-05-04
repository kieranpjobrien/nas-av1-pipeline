"""Regression tests for the 2026-05-05 stale-queue-snapshot bug.

Symptom: gap_filler kept iterating Seinfeld episodes that were already
fully compliant on disk (eng audio + eng subs). User saw the daemon
hop from S03E18 to S03E17 to S03E16... doing wasteful work each time.

Root cause: ``Orchestrator._refresh_worker`` only ADDS unseen files to
the in-memory queue (``known_full / known_gap`` set check). It never
replaces existing queue items with fresh entries from the
media_report. So queue items frozen at daemon startup keep getting
analysed against stale entries forever, even after backfill scripts
patched the on-disk language tags AND updated the report.

Fix: ``_gap_filler_pass`` now reloads media_report (mtime-cached) at
the top of each pass and replaces stale queue items with fresh ones
before calling ``analyse_gaps``. Items that no longer need anything
are skipped this pass without mutating state DB (cheap re-eval next
pass; doesn't lock out future legitimate work).

These tests mock the orchestrator at a low level — we don't spin up
threads or workers, just exercise the lookup helper + the
fresh-vs-stale entry logic.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.gap_filler import analyse_gaps


def _entry(filepath: str, *, language: str = "und") -> dict:
    """Minimal AV1 entry with one audio + one sub track at the given language."""
    return {
        "filepath": filepath,
        "filename": filepath.rsplit("/", 1)[-1].rsplit("\\", 1)[-1],
        "library_type": "series",
        "video": {"codec_raw": "av1"},
        "audio_streams": [{"codec_raw": "eac3", "language": language}],
        "subtitle_streams": [{"codec_raw": "subrip", "language": language}],
        "external_subtitles": [],
        "tmdb": {"id": 1, "original_language": "en"},
    }


def test_und_entry_triggers_language_detect():
    """Pin baseline: an entry with und audio/sub langs IS flagged for
    language detect. This is the state the queue was built with at
    startup before backfills landed."""
    config = {
        "strip_non_english_audio": True,
        "strip_non_english_subs": True,
        "lossless_audio_codecs": [],
    }
    entry = _entry("\\\\NAS\\Series\\Show\\Show S01E01.mkv", language="und")
    gaps = analyse_gaps(entry, config)
    assert gaps.needs_language_detect is True
    assert gaps.needs_anything is True


def test_eng_entry_needs_nothing():
    """Pin: same path, eng langs — analyse_gaps says needs_anything=False.
    This is what the report should look like AFTER the backfill ran. The
    bug was that the daemon was still processing these files because its
    queue snapshot held the stale 'und' entry."""
    config = {
        "strip_non_english_audio": True,
        "strip_non_english_subs": True,
        "lossless_audio_codecs": [],
    }
    entry = _entry("\\\\NAS\\Series\\Show\\Show S01E01.mkv", language="eng")
    gaps = analyse_gaps(entry, config)
    assert gaps.needs_language_detect is False
    assert gaps.needs_track_removal is False
    assert gaps.needs_filename_clean is False
    assert gaps.needs_metadata is False
    assert gaps.needs_anything is False


def test_load_fresh_report_lookup_caches_by_mtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """The lookup helper should mtime-cache the parsed report so a 60s
    rescan loop doesn't re-parse 8000 entries every pass.

    Reaches into the orchestrator class to call the helper without
    spinning up the full thread machinery.
    """
    from pipeline.orchestrator import Orchestrator

    report_path = tmp_path / "media_report.json"
    initial = {"files": [{"filepath": "/a", "video": {"codec_raw": "av1"}}]}
    report_path.write_text(json.dumps(initial), encoding="utf-8")

    # Bare-minimum Orchestrator instance for the helper. The helper only
    # touches `os.path.getmtime`, opens the report, and reads
    # `self._fresh_report_cache`. We bypass __init__ entirely so we don't
    # need state DB / config / etc.
    orch = Orchestrator.__new__(Orchestrator)

    # The helper imports MEDIA_REPORT inside the function — patch it there.
    with patch("paths.MEDIA_REPORT", str(report_path)):
        first = orch._load_fresh_report_lookup()
        assert "/a" in first
        # Second call with same mtime → cached dict, identity-equal.
        second = orch._load_fresh_report_lookup()
        assert first is second  # cache hit returns the same object


def test_load_fresh_report_lookup_returns_empty_on_corrupt_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """If the report can't be parsed (transient I/O hiccup, partial
    write), the helper returns empty dict so the gap_filler falls back
    to stale queue items rather than stalling the whole pass."""
    from pipeline.orchestrator import Orchestrator

    report_path = tmp_path / "media_report.json"
    report_path.write_text("{not valid json", encoding="utf-8")
    orch = Orchestrator.__new__(Orchestrator)

    with patch("paths.MEDIA_REPORT", str(report_path)):
        result = orch._load_fresh_report_lookup()

    assert result == {}


def test_load_fresh_report_lookup_returns_empty_when_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Same defensive behaviour when the file simply doesn't exist."""
    from pipeline.orchestrator import Orchestrator

    orch = Orchestrator.__new__(Orchestrator)
    missing = tmp_path / "does_not_exist.json"

    with patch("paths.MEDIA_REPORT", str(missing)):
        result = orch._load_fresh_report_lookup()

    assert result == {}
