"""Regression tests for tools.detect_languages safety contract.

These tests prove that _apply_file_ffmpeg:
1. Preserves every source stream (-map 0 in the command).
2. Refuses to replace a file whose post-probe shows zero audio.

Tests use real ffmpeg/ffprobe — no mocks — because the thing under test is
exactly the command line we pass to ffmpeg. Skipped if ffmpeg isn't on PATH.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

# Ensure repo root on path so ``tools.detect_languages`` imports cleanly.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tools.detect_languages import _apply_file_ffmpeg, _probe_stream_counts  # noqa: E402


def _have_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None


pytestmark = pytest.mark.skipif(not _have_ffmpeg(), reason="ffmpeg not on PATH")


def _build_mkv(path: Path, *, audio_tracks: int, sub_tracks: int) -> None:
    """Synthesise a tiny MKV with 1 video + N audio + M subtitle streams.

    Uses ffmpeg lavfi sources + srt subtitles, encoded with the fastest codecs
    available. Every track gets language=und so the fixture exercises the
    "tag undetermined track" codepath the detector is meant for.
    """
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=160x120:rate=10:duration=1",
    ]
    for _ in range(audio_tracks):
        cmd += ["-f", "lavfi", "-i", "sine=frequency=440:duration=1"]
    # Sub tracks are built from a tiny SRT written inline on each call.
    sub_srt: Path | None = None
    if sub_tracks > 0:
        sub_srt = path.with_suffix(".tmp.srt")
        sub_srt.write_text(
            "1\n00:00:00,000 --> 00:00:01,000\nhello\n",
            encoding="utf-8",
        )
        for _ in range(sub_tracks):
            cmd += ["-i", str(sub_srt)]

    # Map the video first.
    cmd += ["-map", "0:v:0", "-c:v", "libx264", "-preset", "ultrafast", "-t", "1"]
    for i in range(audio_tracks):
        cmd += ["-map", f"{i + 1}:a:0", f"-c:a:{i}", "aac"]
    for j in range(sub_tracks):
        input_idx = 1 + audio_tracks + j
        cmd += ["-map", f"{input_idx}:s:0", f"-c:s:{j}", "srt"]
    cmd.append(str(path))
    subprocess.run(cmd, check=True, capture_output=True)
    if sub_srt is not None and sub_srt.exists():
        sub_srt.unlink()


def test_apply_file_ffmpeg_preserves_streams(tmp_path: Path) -> None:
    """-map 0 guarantees every source stream survives the rewrite.

    Before the fix, ffmpeg would pick exactly one audio and one subtitle
    stream (its default behaviour), dropping the rest. The post-verify also
    catches the regression even if -map 0 were somehow missing.
    """
    src = tmp_path / "three_audio_two_subs.mkv"
    _build_mkv(src, audio_tracks=3, sub_tracks=2)

    src_counts = _probe_stream_counts(str(src))
    assert src_counts is not None
    src_v, src_a, src_s = src_counts
    assert src_a == 3, f"fixture build failed: expected 3 audio, got {src_a}"
    assert src_s == 2, f"fixture build failed: expected 2 subs, got {src_s}"

    detections = [
        {"track_type": "audio", "stream_index": 0, "detected_language": "eng"},
        {"track_type": "subtitle", "stream_index": 0, "detected_language": "eng"},
    ]
    applied, failed = _apply_file_ffmpeg(str(src), detections)
    assert applied == 2
    assert failed == 0

    after_counts = _probe_stream_counts(str(src))
    assert after_counts is not None
    dst_v, dst_a, dst_s = after_counts
    assert dst_v == src_v
    assert dst_a == src_a, f"audio regression: {src_a} -> {dst_a}"
    assert dst_s == src_s, f"sub regression: {src_s} -> {dst_s}"


def test_apply_file_ffmpeg_refuses_zero_audio_source(tmp_path: Path) -> None:
    """Zero-audio source cannot produce a valid tagged MKV — refuse upfront.

    The post-verify's absolute floor (``dst_a < 1``) guarantees the swap never
    happens even if the source itself had no audio to begin with.
    """
    src = tmp_path / "no_audio.mkv"
    _build_mkv(src, audio_tracks=0, sub_tracks=1)
    src_counts = _probe_stream_counts(str(src))
    assert src_counts is not None
    _, src_a, _ = src_counts
    assert src_a == 0, "fixture should have zero audio"

    # Detection list targets the subtitle track — the only stream available.
    detections = [
        {"track_type": "subtitle", "stream_index": 0, "detected_language": "eng"},
    ]
    applied, failed = _apply_file_ffmpeg(str(src), detections)
    # Either the zero-audio floor refused, or ffmpeg never ran — both are
    # acceptable outcomes; the critical invariant is "applied == 0".
    assert applied == 0
    assert failed == len(detections)
