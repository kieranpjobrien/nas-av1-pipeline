"""Pin the 2026-05-12 fix for the gap_filler audio-transcode routing bug.

Pre-fix: ``categorise_entry`` routed any AV1 file with ``needs_anything``
to gap_filler — including ones that needed AC-3/DTS/Opus → EAC-3 audio
transcoding. The gap_filler's own comment said it does NOT do audio
transcodes (fetch+ffmpeg+upload is heavy, excluded by design), but it
then ran its other operations and marked the file DONE anyway. That
left files on the NAS with non-policy audio AND a green-light DONE
status. Lord of the Rings — Return of the King shipped this way on
2026-05-11: TrueHD passthrough kept, AC-3 5.1-EX track survived
un-transcoded, commentary sub track survived, ENCODER/CQ tags missing.

Two-layer fix:

  1. ``categorise_entry``: if ``gaps.needs_audio_transcode`` is True,
     route to ``full_gamut`` (which actually does the transcode), not
     gap_filler. This prevents the mis-route at the source.

  2. ``gap_fill`` entry guard: defensive — if we somehow received an
     audio-transcode-needing file anyway (orchestrator bug, stale
     queue, etc.), refuse to proceed. Mark ERROR with diagnostic so
     the next queue-build pass re-routes correctly.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pipeline.gap_filler import GapAnalysis, gap_fill
from pipeline.state import FileStatus


# --------------------------------------------------------------------------
# Layer 1: categorise_entry routes audio-transcode AV1 to full_gamut
# --------------------------------------------------------------------------


def _make_state_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "pipeline_state.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """CREATE TABLE pipeline_files (
            filepath TEXT PRIMARY KEY, status TEXT, mode TEXT, added TEXT,
            last_updated TEXT, tier TEXT, local_path TEXT, output_path TEXT,
            dest_path TEXT, error TEXT, stage TEXT, reason TEXT, res_key TEXT,
            extras TEXT DEFAULT '{}'
        )"""
    )
    conn.commit()
    conn.close()
    return db_path


def test_categorise_av1_with_audio_transcode_needs_routes_to_full_gamut(
    tmp_path, monkeypatch
):
    """AV1 file with AC-3 audio must be routed to full_gamut, NOT gap_filler."""
    from pipeline.__main__ import categorise_entry
    from pipeline.state import PipelineState
    from pipeline.control import PipelineControl

    # Entry: AV1 video + AC-3 audio (the LotR ROTK shape, simplified)
    entry = {
        "filepath": r"\\KieranNAS\Media\Movies\LOTR.mkv",
        "filename": "LOTR.mkv",
        "video": {"codec_raw": "av1", "codec": "AV1"},
        "audio_streams": [
            {"codec_raw": "truehd", "channels": 8, "language": "eng"},
            {"codec_raw": "ac3",    "channels": 6, "language": "eng"},
        ],
        "subtitle_streams": [],
    }

    db_path = _make_state_db(tmp_path)
    state = PipelineState(str(db_path))
    control = PipelineControl(str(tmp_path))

    # Minimal config that enables transcode
    config = {
        "strip_non_english_audio": True,
        "audio_keep_policy": "english_und",
        "audio_eac3_surround_bitrate": "640k",
    }

    category, _item = categorise_entry(entry, config, state, control)
    assert category == "full_gamut", (
        f"AV1 + non-EAC-3 audio must go to full_gamut for the transcode, "
        f"got: {category}"
    )


def test_categorise_av1_compliant_audio_still_routes_to_gap_filler_when_other_work(
    tmp_path
):
    """AV1 + EAC-3 audio + commentary track to strip → gap_filler is still
    appropriate (no transcode needed). Pin so the new guard doesn't over-route."""
    from pipeline.__main__ import categorise_entry
    from pipeline.state import PipelineState
    from pipeline.control import PipelineControl

    entry = {
        "filepath": r"\\KieranNAS\Media\Movies\X.mkv",
        "filename": "X.mkv",
        "video": {"codec_raw": "av1"},
        "audio_streams": [
            {"codec_raw": "eac3", "channels": 6, "language": "eng"},
            {"codec_raw": "eac3", "channels": 2, "language": "fra"},  # foreign — needs strip
        ],
        "subtitle_streams": [],
    }
    db_path = _make_state_db(tmp_path)
    state = PipelineState(str(db_path))
    control = PipelineControl(str(tmp_path))
    config = {
        "strip_non_english_audio": True,
        "audio_keep_policy": "english_und",
        "audio_eac3_surround_bitrate": "640k",
    }
    category, _ = categorise_entry(entry, config, state, control)
    # No transcode needed (both are EAC-3), so gap_filler is OK
    assert category in ("gap_filler", "skip"), (
        f"AV1 + all-EAC-3 should not go to full_gamut (no transcode owed), "
        f"got: {category}"
    )


# --------------------------------------------------------------------------
# Layer 2: gap_fill defensive guard refuses to lie DONE on audio-transcode work
# --------------------------------------------------------------------------


def test_gap_fill_refuses_audio_transcode_marks_error(tmp_path, monkeypatch):
    """If gap_fill receives a needs_audio_transcode entry (bug upstream),
    it must NOT mark DONE. It must mark ERROR + force_reencode=True so
    the next queue build re-routes to full_gamut."""
    from pipeline.state import PipelineState

    db_path = _make_state_db(tmp_path)
    state = PipelineState(str(db_path))
    # Seed a pending row so set_file in the guard updates it
    state.set_file(r"\\NAS\x.mkv", FileStatus.PENDING)

    # Make a fake file on disk so the file-exists check passes
    real_mkv = tmp_path / "x.mkv"
    real_mkv.write_bytes(b"fake")

    entry = {
        "filepath": str(real_mkv),
        "filename": real_mkv.name,
        "video": {"codec_raw": "av1"},
        "audio_streams": [{"codec_raw": "ac3", "channels": 6, "language": "eng"}],
        "subtitle_streams": [],
    }
    gaps = GapAnalysis()
    gaps.needs_audio_transcode = True
    gaps.audio_transcode_indices = [0]

    # Seed pending row for the actual filepath we'll pass
    state.set_file(str(real_mkv), FileStatus.PENDING)

    config = {
        "strip_non_english_audio": True,
        "audio_keep_policy": "english_und",
    }

    result = gap_fill(str(real_mkv), entry, gaps, config, state)
    assert result is False, "gap_fill must NOT report success on audio-transcode mis-route"
    row = state.get_file(str(real_mkv))
    assert row is not None
    assert (row.get("status") or "").lower() == "error", (
        f"audio-transcode mis-route must land in ERROR, got: {row.get('status')}"
    )
    assert row.get("force_reencode") is True, (
        "force_reencode must be set so the next queue build sends to full_gamut"
    )


def test_gap_fill_proceeds_when_no_audio_transcode_needed(tmp_path, monkeypatch):
    """Positive case: gap_fill still works for files it CAN handle (no audio
    transcode owed). The guard must not over-fire."""
    from pipeline.state import PipelineState

    db_path = _make_state_db(tmp_path)
    state = PipelineState(str(db_path))

    real_mkv = tmp_path / "y.mkv"
    real_mkv.write_bytes(b"fake")

    entry = {
        "filepath": str(real_mkv),
        "filename": real_mkv.name,
        "video": {"codec_raw": "av1"},
        "audio_streams": [{"codec_raw": "eac3", "channels": 6, "language": "eng"}],
        "subtitle_streams": [],
    }
    gaps = GapAnalysis()
    # nothing needed
    state.set_file(str(real_mkv), FileStatus.PENDING)

    result = gap_fill(str(real_mkv), entry, gaps, {}, state)
    assert result is True
    row = state.get_file(str(real_mkv))
    assert (row.get("status") or "").lower() == "done", (
        f"clean file should still get DONE, got: {row.get('status')}"
    )
