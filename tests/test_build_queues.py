"""Tests for ``pipeline.__main__.build_queues`` — queue construction from media_report."""

import json

from pipeline.__main__ import build_queues
from pipeline.config import build_config
from pipeline.control import PipelineControl
from pipeline.state import FileStatus, PipelineState


def _write_report(tmp_path, files):
    path = tmp_path / "media_report.json"
    path.write_text(json.dumps({"generated": "test", "files": files}), encoding="utf-8")
    return str(path)


def test_unprobeable_file_is_flagged_corrupt(tmp_path):
    """Files with codec_raw missing must be flagged FLAGGED_CORRUPT, not silently dropped.

    Regression: prior to this fix, build_queues did `if not codec_raw: continue`,
    which meant ffprobe-failed files sat in PENDING forever without ever being
    queued or surfaced to the user. Five real files in production were stuck
    that way for two weeks before the bug was caught.
    """
    files = [
        {
            "filepath": r"\\KieranNAS\Media\Movies\BrokenFile.mkv",
            "filename": "BrokenFile.mkv",
            "library_type": "movie",
            "video": {"codec_raw": None, "codec": None},  # ffprobe couldn't determine codec
            "audio_streams": [],
            "subtitle_streams": [],
            "file_size_bytes": 4_000_000_000,
            "duration_seconds": 0,
        },
    ]
    report_path = _write_report(tmp_path, files)
    state = PipelineState(str(tmp_path / "pipeline_state.db"))
    control = PipelineControl(str(tmp_path))
    config = build_config({})

    full, gap = build_queues(report_path, config, state, control)

    assert full == [], "broken file must NOT enter full_gamut queue"
    assert gap == [], "broken file must NOT enter gap_filler queue"

    entry = state.get_file(r"\\KieranNAS\Media\Movies\BrokenFile.mkv")
    assert entry is not None, "build_queues must persist a row for broken files"
    assert entry["status"] == FileStatus.FLAGGED_CORRUPT.value
    assert entry["stage"] == "scan"
    assert "ffprobe" in (entry.get("reason") or "")

    state.close()


def test_processable_files_still_queued(tmp_path):
    """Sanity: a normal H.264 file still lands in full_gamut after the fix."""
    files = [
        {
            "filepath": r"\\KieranNAS\Media\Movies\Normal.mkv",
            "filename": "Normal.mkv",
            "library_type": "movie",
            "video": {"codec_raw": "h264", "codec": "H.264", "resolution_class": "1080p"},
            "audio_streams": [{"codec_raw": "eac3", "channels": 6, "language": "eng"}],
            "subtitle_streams": [],
            "file_size_bytes": 2_000_000_000,
            "duration_seconds": 6000,
            "overall_bitrate_kbps": 2700,
        },
    ]
    report_path = _write_report(tmp_path, files)
    state = PipelineState(str(tmp_path / "pipeline_state.db"))
    control = PipelineControl(str(tmp_path))
    config = build_config({})

    full, gap = build_queues(report_path, config, state, control)

    assert len(full) == 1
    assert full[0]["filepath"] == r"\\KieranNAS\Media\Movies\Normal.mkv"
    assert gap == []

    state.close()


def test_corrupt_flag_persists_on_rerun(tmp_path):
    """A second build_queues pass over the same broken entry must not flip it back to PENDING."""
    files = [
        {
            "filepath": r"\\KieranNAS\Media\Movies\BrokenFile.mkv",
            "filename": "BrokenFile.mkv",
            "library_type": "movie",
            "video": {"codec_raw": None, "codec": None},
            "audio_streams": [],
            "subtitle_streams": [],
            "file_size_bytes": 4_000_000_000,
        },
    ]
    report_path = _write_report(tmp_path, files)
    state = PipelineState(str(tmp_path / "pipeline_state.db"))
    control = PipelineControl(str(tmp_path))
    config = build_config({})

    build_queues(report_path, config, state, control)
    build_queues(report_path, config, state, control)  # second pass

    entry = state.get_file(r"\\KieranNAS\Media\Movies\BrokenFile.mkv")
    assert entry["status"] == FileStatus.FLAGGED_CORRUPT.value, (
        "FLAGGED_CORRUPT is terminal; build_queues must not revert to PENDING"
    )
    state.close()
