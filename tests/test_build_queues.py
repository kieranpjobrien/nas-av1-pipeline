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


def test_hevc_with_nonpolicy_audio_routes_to_full_gamut(tmp_path):
    """accept-HEVC-as-finished regression (CRITICAL).

    HEVC is a finished VIDEO target, but a HEVC file carrying DTS/AAC/FLAC/Opus
    still needs an EAC-3 transcode. It must be routed to full_gamut with
    ``force_reencode`` stamped — otherwise full_gamut's finished-codec guard
    (full_gamut.py:645) marks it DONE with the non-policy audio untouched. That
    is the 2026-04-23 audio-loss class at HEVC-library scale.
    """
    files = [
        {
            "filepath": r"\\KieranNAS\Media\Movies\HevcDts.mkv",
            "filename": "HevcDts.mkv",
            "library_type": "movie",
            "video": {"codec_raw": "hevc", "codec": "HEVC (H.265)", "resolution_class": "1080p"},
            "audio_streams": [{"codec_raw": "dts", "channels": 6, "language": "eng"}],
            "subtitle_streams": [],
            "file_size_bytes": 8_000_000_000,
            "duration_seconds": 6000,
            "overall_bitrate_kbps": 10000,
        },
    ]
    report_path = _write_report(tmp_path, files)
    state = PipelineState(str(tmp_path / "pipeline_state.db"))
    control = PipelineControl(str(tmp_path))
    config = build_config({})

    full, gap = build_queues(report_path, config, state, control)

    assert len(full) == 1, "HEVC + DTS must be queued for full_gamut (EAC-3 transcode)"
    assert full[0]["filepath"] == r"\\KieranNAS\Media\Movies\HevcDts.mkv"
    assert gap == []

    entry = state.get_file(r"\\KieranNAS\Media\Movies\HevcDts.mkv")
    assert entry is not None
    assert entry.get("force_reencode"), (
        "HEVC needing an audio transcode must be force-stamped, else the "
        "finished-codec guard marks it DONE with the DTS audio untouched"
    )
    assert entry["status"] != FileStatus.DONE.value, "must not be DONE before the encode runs"

    state.close()


def test_compliant_hevc_is_not_reencoded(tmp_path):
    """A HEVC file already carrying EAC-3 audio is a finished target — it must
    NOT be force-re-encoded (the whole point of accept-HEVC-as-finished). It
    must not land in full_gamut, and must not be force-stamped.
    """
    files = [
        {
            "filepath": r"\\KieranNAS\Media\Movies\HevcEac3.mkv",
            "filename": "HevcEac3.mkv",
            "library_type": "movie",
            "video": {"codec_raw": "hevc", "codec": "HEVC (H.265)", "resolution_class": "1080p"},
            "audio_streams": [{"codec_raw": "eac3", "channels": 6, "language": "eng"}],
            "subtitle_streams": [],
            "file_size_bytes": 8_000_000_000,
            "duration_seconds": 6000,
            "overall_bitrate_kbps": 10000,
        },
    ]
    report_path = _write_report(tmp_path, files)
    state = PipelineState(str(tmp_path / "pipeline_state.db"))
    control = PipelineControl(str(tmp_path))
    config = build_config({})

    full, gap = build_queues(report_path, config, state, control)

    assert full == [], "compliant HEVC must NOT be re-encoded"
    entry = state.get_file(r"\\KieranNAS\Media\Movies\HevcEac3.mkv")
    assert not (entry and entry.get("force_reencode")), "compliant HEVC must not be force-stamped"

    state.close()


def test_av1_with_opus_audio_routes_to_full_gamut(tmp_path):
    """Same guard hole on the AV1 branch: an externally-sourced AV1 with Opus
    audio (Sonos Arc can't decode Opus → Plex transcodes on every play) needs
    the EAC-3 transcode. It must be force-stamped and routed to full_gamut, not
    DONE'd untouched.
    """
    files = [
        {
            "filepath": r"\\KieranNAS\Media\Series\Wire\Av1Opus.mkv",
            "filename": "Av1Opus.mkv",
            "library_type": "series",
            "video": {"codec_raw": "av1", "codec": "AV1", "resolution_class": "1080p"},
            "audio_streams": [{"codec_raw": "opus", "channels": 6, "language": "eng"}],
            "subtitle_streams": [],
            "file_size_bytes": 1_500_000_000,
            "duration_seconds": 3000,
            "overall_bitrate_kbps": 4000,
        },
    ]
    report_path = _write_report(tmp_path, files)
    state = PipelineState(str(tmp_path / "pipeline_state.db"))
    control = PipelineControl(str(tmp_path))
    config = build_config({})

    full, gap = build_queues(report_path, config, state, control)

    assert len(full) == 1, "AV1 + Opus must be queued for full_gamut (EAC-3 transcode)"
    entry = state.get_file(r"\\KieranNAS\Media\Series\Wire\Av1Opus.mkv")
    assert entry is not None
    assert entry.get("force_reencode"), "AV1 needing an audio transcode must be force-stamped"

    state.close()
