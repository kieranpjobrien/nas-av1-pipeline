"""Pin the 2026-05-17 auto-reset of flagged_* rows when the file is
refreshed on disk.

Canonical case: user deletes a corrupt source file from NAS (Ford v
Ferrari, GoodFellas, Caddyshack, etc.), Sonarr / Radarr re-downloads
a clean release at the same path. Pre-fix, the state DB row stayed
``flagged_corrupt`` forever — ``categorise_entry`` returned
``("skip", None)`` for any terminal status, so the new file sat
invisible until the user manually cleared the row.

Post-fix: when the entry's ``file_mtime`` is newer than the row's
``last_updated`` (with a 60s clock-skew tolerance), the row is auto-reset
to pending and the file flows through normal categorisation. Only
``flagged_corrupt`` / ``flagged_foreign_audio`` / ``flagged_undetermined``
get this treatment — ``done`` and ``flagged_manual`` are NEVER auto-reset
(DONE requires explicit force_reencode; flagged_manual is the user's
park button).
"""

from __future__ import annotations

import time
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from pipeline.__main__ import categorise_entry
from pipeline.control import PipelineControl
from pipeline.state import FileStatus, PipelineState


def _state(tmp_path) -> PipelineState:
    return PipelineState(str(tmp_path / "state.db"))


def _control(tmp_path) -> PipelineControl:
    return PipelineControl(str(tmp_path))


def _entry(filepath: str, *, mtime: float, codec: str = "h264") -> dict:
    """Minimal media-report entry shape with the fields the categoriser reads."""
    return {
        "filepath": filepath,
        "filename": filepath.split("\\")[-1],
        "library_type": "movie",
        "file_size_bytes": 10_000_000_000,
        "file_mtime": mtime,
        "video": {"codec_raw": codec},
        "audio_streams": [{"codec_raw": "eac3", "language": "eng", "channels": 6}],
        "subtitle_streams": [],
        "tmdb": {"original_language": "en", "title": "Test"},
    }


def test_flagged_corrupt_resets_when_file_refreshed(tmp_path):
    """The Ford v Ferrari case: corrupt file flagged at time T, user
    deletes + re-downloads, new file mtime is now T+1d. Categoriser
    must auto-reset and re-route the row."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Movies\Ford v Ferrari (2019)\Ford v Ferrari (2019).mkv"

    # Flag the row at time T (now).
    state.set_file(fp, FileStatus.FLAGGED_CORRUPT, stage="prep_source_integrity",
                   error="source corruption (prep-time probe)")
    flag_time = datetime.fromisoformat(state.get_file(fp)["last_updated"]).timestamp()

    # Fresh-download entry: mtime = flag_time + 1 day.
    entry = _entry(fp, mtime=flag_time + 86400)

    category, item = categorise_entry(entry, {}, state, control)
    assert category == "full_gamut", (
        f"refreshed file must be routed for re-encode, got category={category!r}"
    )
    # And the state row should now be pending (the next pipeline pickup
    # finds a normal row, not the flagged one).
    assert state.get_file(fp)["status"] == FileStatus.PENDING.value


def test_flagged_foreign_audio_resets_when_file_refreshed(tmp_path):
    """Same shape for foreign_audio. Bluey episodes tagged as Swedish:
    user re-downloads English release, file mtime advances, row auto-resets."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Series\Bluey\Season 1\Bluey S01E10 Hotel.mkv"

    state.set_file(fp, FileStatus.FLAGGED_FOREIGN_AUDIO, stage="qualify",
                   reason="no eng track")
    flag_time = datetime.fromisoformat(state.get_file(fp)["last_updated"]).timestamp()

    entry = _entry(fp, mtime=flag_time + 86400)
    category, _ = categorise_entry(entry, {}, state, control)
    assert category == "full_gamut"
    assert state.get_file(fp)["status"] == FileStatus.PENDING.value


def test_flagged_corrupt_skipped_when_file_unchanged(tmp_path):
    """Sanity: a still-corrupt file (same mtime as the flag time) must
    NOT be reset. Pipeline-restart loops would re-flag-then-reset
    forever if the comparison was too lenient."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Movies\Still Corrupt\Still Corrupt.mkv"

    state.set_file(fp, FileStatus.FLAGGED_CORRUPT, stage="prep_source_integrity")
    flag_time = datetime.fromisoformat(state.get_file(fp)["last_updated"]).timestamp()

    # Same mtime as flag time — file hasn't been touched.
    entry = _entry(fp, mtime=flag_time)
    category, _ = categorise_entry(entry, {}, state, control)
    assert category == "skip"
    assert state.get_file(fp)["status"] == FileStatus.FLAGGED_CORRUPT.value


def test_flagged_corrupt_clock_skew_tolerance(tmp_path):
    """A 30-second mtime difference (clock skew on the NAS / a tiny
    metadata write that bumped mtime without the file content changing)
    should NOT trigger the reset. The 60s tolerance protects against
    that class of false-positive."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Movies\Slightly Bumped\Slightly Bumped.mkv"

    state.set_file(fp, FileStatus.FLAGGED_CORRUPT, stage="prep_source_integrity")
    flag_time = datetime.fromisoformat(state.get_file(fp)["last_updated"]).timestamp()

    entry = _entry(fp, mtime=flag_time + 30)  # within tolerance
    category, _ = categorise_entry(entry, {}, state, control)
    assert category == "skip"
    assert state.get_file(fp)["status"] == FileStatus.FLAGGED_CORRUPT.value


def test_done_never_auto_resets(tmp_path):
    """DONE files are NEVER auto-reset by this path. Re-encoding only
    runs through explicit force_reencode. A file that's been encoded
    successfully and then had its mtime bumped (mkvpropedit tag write,
    NAS metadata refresh) must stay DONE."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Movies\Done File\Done File.mkv"

    state.set_file(fp, FileStatus.DONE, mode="full_gamut", reason="encoded")
    flag_time = datetime.fromisoformat(state.get_file(fp)["last_updated"]).timestamp()

    # File mtime in the far future — DONE should still be respected.
    entry = _entry(fp, mtime=flag_time + 86400, codec="av1")
    category, _ = categorise_entry(entry, {}, state, control)
    assert category == "skip"
    assert state.get_file(fp)["status"] == FileStatus.DONE.value


def test_flagged_manual_never_auto_resets(tmp_path):
    """flagged_manual is the user's park button — only the user clears
    it. A refreshed file at the same path must stay parked."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Movies\Manually Parked\Manually Parked.mkv"

    state.set_file(fp, FileStatus.FLAGGED_MANUAL, reason="user park")
    flag_time = datetime.fromisoformat(state.get_file(fp)["last_updated"]).timestamp()

    entry = _entry(fp, mtime=flag_time + 86400)
    category, _ = categorise_entry(entry, {}, state, control)
    assert category == "skip"
    assert state.get_file(fp)["status"] == FileStatus.FLAGGED_MANUAL.value
