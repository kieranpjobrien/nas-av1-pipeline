"""Regression tests for the consolidated ``pipeline.language`` module.

Covers:
1. Safety contract on ``_apply_file_ffmpeg`` (``-map 0`` + post-verify +
   zero-audio floor — originally landed in commit 64098ab against
   ``tools.detect_languages``). Relocated here after the merge.
2. High-level ``detect_all_languages`` pipeline entry — exercises the
   no-whisper heuristic path with a fixture where subtitle text is
   title-detectable.
3. The ``tools.detect_languages`` shim still forwards to
   ``pipeline.language.main`` — ``python -m tools.detect_languages --help``
   must succeed.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline.language import (  # noqa: E402
    _apply_file_ffmpeg,
    _probe_stream_counts,
    detect_all_languages,
    detect_language,
    to_iso2,
)


def _have_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None


ffmpeg_required = pytest.mark.skipif(not _have_ffmpeg(), reason="ffmpeg not on PATH")


# ---------------------------------------------------------------------------
# _apply_file_ffmpeg safety contract (from commit 64098ab)
# ---------------------------------------------------------------------------


def _build_mkv(path: Path, *, audio_tracks: int, sub_tracks: int) -> None:
    """Synthesise a tiny MKV with 1 video + N audio + M subtitle streams.

    Uses ffmpeg lavfi sources + inline SRT subtitles; every track gets
    language=und so the fixture exercises the "tag undetermined track"
    codepath that the detector is meant for.
    """
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-f", "lavfi", "-i", "testsrc2=size=160x120:rate=10:duration=1",
    ]
    for _ in range(audio_tracks):
        cmd += ["-f", "lavfi", "-i", "sine=frequency=440:duration=1"]

    sub_srt: Path | None = None
    if sub_tracks > 0:
        sub_srt = path.with_suffix(".tmp.srt")
        sub_srt.write_text("1\n00:00:00,000 --> 00:00:01,000\nhello\n", encoding="utf-8")
        for _ in range(sub_tracks):
            cmd += ["-i", str(sub_srt)]

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


@ffmpeg_required
def test_apply_file_ffmpeg_preserves_streams(tmp_path: Path) -> None:
    """``-map 0`` guarantees every source stream survives the rewrite.

    Pre-merge bug class: without ``-map 0`` ffmpeg picks exactly one audio and
    one subtitle stream and silently drops the rest. The post-verify also
    catches the regression even if ``-map 0`` were somehow missing.
    """
    src = tmp_path / "three_audio_two_subs.mkv"
    _build_mkv(src, audio_tracks=3, sub_tracks=2)

    src_counts = _probe_stream_counts(str(src))
    assert src_counts is not None
    _, src_a, src_s = src_counts
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
    _, dst_a, dst_s = after_counts
    assert dst_a == src_a, f"audio regression: {src_a} -> {dst_a}"
    assert dst_s == src_s, f"sub regression: {src_s} -> {dst_s}"


@ffmpeg_required
def test_apply_file_ffmpeg_refuses_zero_audio_source(tmp_path: Path) -> None:
    """Zero-audio source can never yield a valid tagged MKV — refuse upfront.

    The post-verify's absolute floor (``dst_a < 1``) guarantees the swap never
    happens even if the source itself had no audio to begin with.
    """
    src = tmp_path / "no_audio.mkv"
    _build_mkv(src, audio_tracks=0, sub_tracks=1)
    src_counts = _probe_stream_counts(str(src))
    assert src_counts is not None
    _, src_a, _ = src_counts
    assert src_a == 0, "fixture should have zero audio"

    detections = [
        {"track_type": "subtitle", "stream_index": 0, "detected_language": "eng"},
    ]
    applied, failed = _apply_file_ffmpeg(str(src), detections)
    assert applied == 0
    assert failed == len(detections)


# ---------------------------------------------------------------------------
# Public pipeline entry point
# ---------------------------------------------------------------------------


def test_detect_all_languages_no_whisper_title_hint() -> None:
    """Title hint populates detected_language without extraction or whisper.

    Exercises the heuristic branch: the audio track's title contains
    ``"English"`` so ``infer_audio_language`` returns early, before we ever
    need to extract subtitle text or spin whisper up. No media files
    required.
    """
    entry = {
        "filepath": "/tmp/does-not-exist.mkv",
        "duration_seconds": 120,
        "audio_streams": [
            {"language": "und", "title": "English 5.1 (DTS)", "codec": "dts"},
        ],
        "subtitle_streams": [],
    }
    result = detect_all_languages(entry, use_whisper=False)
    audio = result["audio_streams"][0]
    assert audio["detected_language"] == "en"
    assert audio["detection_method"] == "heuristic"
    assert audio["detection_confidence"] >= 0.8


def test_detect_all_languages_leaves_tagged_tracks_alone() -> None:
    """Tracks that already have a determined language tag are left untouched."""
    entry = {
        "filepath": "/tmp/does-not-exist.mkv",
        "duration_seconds": 120,
        "audio_streams": [
            {"language": "eng", "title": "English", "codec": "dts"},
        ],
        "subtitle_streams": [],
    }
    result = detect_all_languages(entry, use_whisper=False)
    assert result["audio_streams"][0].get("detected_language") is None


# ---------------------------------------------------------------------------
# CLI shim
# ---------------------------------------------------------------------------


def test_cli_shim_forwards_to_main() -> None:
    """``python -m tools.detect_languages --help`` imports without error.

    The shim re-exports ``pipeline.language.main`` — if the re-export breaks
    (e.g. missing symbol), argparse's ``--help`` exit-code-0 goes away.
    """
    proc = subprocess.run(
        [sys.executable, "-m", "tools.detect_languages", "--help"],
        capture_output=True, text=True, timeout=30, cwd=str(_REPO_ROOT),
    )
    assert proc.returncode == 0, f"stderr: {proc.stderr}"
    assert "Detect languages for undetermined subtitle/audio tracks" in proc.stdout


def test_cli_module_mode_matches_shim() -> None:
    """Running via ``-m pipeline.language`` yields the same CLI as the shim."""
    direct = subprocess.run(
        [sys.executable, "-m", "pipeline.language", "--help"],
        capture_output=True, text=True, timeout=30, cwd=str(_REPO_ROOT),
    )
    assert direct.returncode == 0, direct.stderr
    assert "--whisper" in direct.stdout
    assert "--spot-check" in direct.stdout


# ---------------------------------------------------------------------------
# Small utilities (no external deps)
# ---------------------------------------------------------------------------


def test_to_iso2_round_trip() -> None:
    assert to_iso2("en") == "eng"
    assert to_iso2("fr") == "fra"
    assert to_iso2("fre") == "fra"  # historical alias collapsed to canonical.
    assert to_iso2("eng") == "eng"  # already-3-letter passes through.
    assert to_iso2("unknown") == "unknown"  # unknown falls through lowercased.


def test_detect_language_empty_is_und() -> None:
    lang, conf = detect_language("")
    assert lang == "und"
    assert conf == 0.0


def test_detect_language_cjk_hangul_is_korean() -> None:
    # Hangul block text should be detected as Korean via the Unicode-range fast path.
    korean = "안녕하세요 반갑습니다 오늘도 좋은 하루 되세요 저는 한국어 공부하고 있습니다"
    lang, conf = detect_language(korean)
    assert lang == "ko"
    assert conf > 0.5
