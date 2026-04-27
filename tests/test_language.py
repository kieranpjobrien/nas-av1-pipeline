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

    The track's ``title`` contains ``"English"``, which is high-signal
    human-authored metadata. We accept it and label as ``title_hint`` —
    distinct from the deleted ``heuristic`` inference branch (single-audio +
    single-sub-language → infer match), which gave false positives on
    foreign dubs (Bluey/Swedish + Bazarr English srt = falsely labelled
    English audio).
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
    assert audio["detection_method"] == "title_hint"
    assert audio["detection_confidence"] >= 0.8


def test_detect_all_languages_drops_inference_for_dub_with_english_sub() -> None:
    """REGRESSION: Bluey-style dub-with-English-sub must NOT auto-label audio English.

    The deleted heuristic said: 1 audio + 1 sub language → infer audio matches
    sub. For Bluey episodes with Swedish/Dutch dubbed audio (tagged ``und``)
    and a Bazarr-added English srt, that produced wrongly-labelled English
    audio that the strip stage then preserved through encode.

    The fix: with ``use_whisper=False`` and no title hint, we MUST leave the
    audio's ``detected_language`` unset so downstream qualification flags
    the file rather than blindly trusting the audio.
    """
    entry = {
        "filepath": "/tmp/bluey-foreign-dub.mkv",
        "duration_seconds": 1380,  # ~23 min episode
        "audio_streams": [
            # Single audio track tagged und, no helpful title — exactly the
            # scenario where the old heuristic mis-fired
            {"language": "und", "title": "Audio", "codec": "eac3"},
        ],
        "subtitle_streams": [
            {"language": "eng", "codec": "subrip"},
        ],
    }
    result = detect_all_languages(entry, use_whisper=False)
    audio = result["audio_streams"][0]
    # The fix: no inference, no fake "eng" label
    assert audio.get("detected_language") is None, (
        "audio language was inferred from sub language — that's the deleted "
        "heuristic that mis-IDs Bluey/Spirited Away dubs. Should stay und."
    )
    assert audio.get("detection_method") is None


def test_clear_legacy_heuristic_detections_strips_inference_only() -> None:
    """Sweeper clears the deleted heuristic's results but keeps title_hint + whisper."""
    from pipeline.language import clear_legacy_heuristic_detections

    entry = {
        "filepath": "/tmp/x.mkv",
        "audio_streams": [
            {"language": "und", "detected_language": "en", "detection_method": "heuristic"},
            {"language": "und", "detected_language": "en", "detection_method": "title_hint"},
            {"language": "und", "detected_language": "sv", "detection_method": "whisper_tiny_3x30"},
        ],
        "subtitle_streams": [
            {"language": "eng", "detection_method": "text_extraction"},
        ],
    }
    out, n = clear_legacy_heuristic_detections(entry)
    assert n == 1, f"expected 1 cleared, got {n}"
    # Heuristic-detected audio: cleared
    assert "detected_language" not in out["audio_streams"][0]
    # title_hint: kept (legitimate signal)
    assert out["audio_streams"][1]["detected_language"] == "en"
    # whisper: kept
    assert out["audio_streams"][2]["detected_language"] == "sv"
    # text_extraction sub: kept
    assert out["subtitle_streams"][0]["detection_method"] == "text_extraction"


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


# ---------------------------------------------------------------------------
# Filename language hint (D-tier heuristic — runs before whisper)
# ---------------------------------------------------------------------------


def test_filename_hint_picks_up_german_token() -> None:
    """`Movie.German.1080p.BluRay.mkv` → 'de' via the filename hint."""
    from pipeline.language import _detect_lang_from_filename

    assert _detect_lang_from_filename(r"\\NAS\Movies\Foo\Foo.German.1080p.BluRay.mkv") == "de"


def test_filename_hint_picks_up_iso_token() -> None:
    """3-letter ISO codes embedded in release names also work — `SPA`, `JPN`."""
    from pipeline.language import _detect_lang_from_filename

    assert _detect_lang_from_filename("Movie.SPA.1080p.x264.mkv") == "es"
    assert _detect_lang_from_filename("Anime.JPN.WEB-DL.mkv") == "ja"


def test_filename_hint_ignores_multi_language_releases() -> None:
    """`MULTi` / `DUAL` / `MULTILANG` mean we can't pick one — return None."""
    from pipeline.language import _detect_lang_from_filename

    assert _detect_lang_from_filename("Movie.MULTi.1080p.mkv") is None
    assert _detect_lang_from_filename("Movie.DUAL.German.English.mkv") is None


def test_filename_hint_returns_none_when_ambiguous() -> None:
    """If two distinct languages are present in the filename, return None
    rather than guessing wrong."""
    from pipeline.language import _detect_lang_from_filename

    # German + Italian both appear → ambiguous
    assert _detect_lang_from_filename("Movie.German.iTALiAN.1080p.mkv") is None


def test_filename_hint_returns_none_for_plain_titles() -> None:
    """Filenames without language tokens — return None, fall through to other signals."""
    from pipeline.language import _detect_lang_from_filename

    assert _detect_lang_from_filename("Inception (2010).mkv") is None
    assert _detect_lang_from_filename("Avatar.2009.1080p.BluRay.x264.mkv") is None


def test_filename_hint_token_boundary_not_substring() -> None:
    """`English` as a substring of an unrelated word shouldn't false-positive.
    Tokens are split on `.`/`_`/`-`/space — substring matches don't apply."""
    from pipeline.language import _detect_lang_from_filename

    # "engineering" contains "eng" as substring but not as a separate token
    assert _detect_lang_from_filename("Engineering.Disasters.S01E01.mkv") is None


def test_detect_all_languages_applies_filename_hint_on_solo_und() -> None:
    """detect_all_languages should set detected_language='de' on a single
    und audio track when the filename has a strong German token."""
    entry = {
        "filepath": r"\\NAS\Movies\Foo (2020)\Foo.German.1080p.mkv",
        "filename": "Foo.German.1080p.mkv",
        "library_type": "movie",
        "duration_seconds": 6000,
        "audio_streams": [
            {"language": "und", "codec": "eac3", "channels": 6, "title": ""},
        ],
        "subtitle_streams": [],
    }
    enriched = detect_all_languages(entry, use_whisper=False)
    a = enriched["audio_streams"][0]
    assert a.get("detected_language") == "de"
    assert a.get("detection_method") == "filename_hint"


def test_detect_all_languages_skips_filename_hint_when_multiple_und_tracks() -> None:
    """With multiple und tracks, we can't tell which one the filename refers to.
    The hint must NOT be applied — better to leave them und than mark a dub
    as the original."""
    entry = {
        "filepath": r"\\NAS\Movies\Foo (2020)\Foo.German.mkv",
        "filename": "Foo.German.mkv",
        "library_type": "movie",
        "duration_seconds": 6000,
        "audio_streams": [
            {"language": "und", "codec": "eac3", "channels": 6, "title": ""},
            {"language": "und", "codec": "eac3", "channels": 2, "title": ""},
        ],
        "subtitle_streams": [],
    }
    enriched = detect_all_languages(entry, use_whisper=False)
    for a in enriched["audio_streams"]:
        assert a.get("detection_method") != "filename_hint", (
            "filename hint must not fire when there are multiple und tracks"
        )


# ---------------------------------------------------------------------------
# Channel-layout + bitrate heuristic (runs after filename hint, before whisper)
# ---------------------------------------------------------------------------


def test_channel_bitrate_heuristic_corroborated_signals_pick_original() -> None:
    """5.1+640k vs 2.0+192k, TMDb says es → 5.1 track tagged es, conf 0.75.

    Both channel and bitrate signals agree on track 0. We tag it with TMDb's
    original_language ('es') and label as ``heuristic_channel_bitrate`` with
    the corroborated 0.75 confidence.
    """
    entry = {
        "filepath": r"\\NAS\Movies\Foo (2020)\Foo.mkv",
        "duration_seconds": 6000,
        "audio_streams": [
            {"language": "und", "codec": "eac3", "channels": 6, "bitrate_kbps": 640, "title": ""},
            {"language": "und", "codec": "eac3", "channels": 2, "bitrate_kbps": 192, "title": ""},
        ],
        "subtitle_streams": [],
        "tmdb": {"original_language": "es"},
    }
    enriched = detect_all_languages(entry, use_whisper=False)
    a0 = enriched["audio_streams"][0]
    a1 = enriched["audio_streams"][1]
    assert a0.get("detected_language") == "es"
    assert a0.get("detection_method") == "heuristic_channel_bitrate"
    assert a0.get("detection_confidence") == 0.75
    # The losing track must remain untouched.
    assert a1.get("detected_language") is None
    assert a1.get("detection_method") is None


def test_channel_bitrate_heuristic_no_signal_on_identical_tracks() -> None:
    """Two identical 2.0 / 192k und tracks → neither signal qualifies, no winner."""
    entry = {
        "filepath": r"\\NAS\Movies\Foo\Foo.mkv",
        "duration_seconds": 6000,
        "audio_streams": [
            {"language": "und", "codec": "eac3", "channels": 2, "bitrate_kbps": 192, "title": ""},
            {"language": "und", "codec": "eac3", "channels": 2, "bitrate_kbps": 192, "title": ""},
        ],
        "subtitle_streams": [],
        "tmdb": {"original_language": "es"},
    }
    enriched = detect_all_languages(entry, use_whisper=False)
    for a in enriched["audio_streams"]:
        assert a.get("detected_language") is None
        assert a.get("detection_method") is None


def test_channel_bitrate_heuristic_channel_only_uses_065() -> None:
    """Only channel signal applies (similar bitrates, ratio < 1.5x) → conf 0.65.

    5.1 vs 2.0 channel split is decisive, but bitrates of 384 / 320 kbps fall
    well under the 1.5x ratio threshold, so the bitrate signal does not vote.
    """
    entry = {
        "filepath": r"\\NAS\Movies\Foo\Foo.mkv",
        "duration_seconds": 6000,
        "audio_streams": [
            {"language": "und", "codec": "eac3", "channels": 6, "bitrate_kbps": 384, "title": ""},
            {"language": "und", "codec": "eac3", "channels": 2, "bitrate_kbps": 320, "title": ""},
        ],
        "subtitle_streams": [],
        "tmdb": {"original_language": "ja"},
    }
    enriched = detect_all_languages(entry, use_whisper=False)
    a0 = enriched["audio_streams"][0]
    assert a0.get("detected_language") == "ja"
    assert a0.get("detection_method") == "heuristic_channel_bitrate"
    assert a0.get("detection_confidence") == 0.65


def test_channel_bitrate_heuristic_skipped_for_single_und_track() -> None:
    """One und track → heuristic doesn't fire (filename hint already handles solo-und).

    The heuristic deliberately requires at least 2 und tracks because, with
    only one, there's nothing to compare against and the filename hint covers
    the use case more precisely.
    """
    entry = {
        "filepath": r"\\NAS\Movies\Foo\Foo.mkv",
        "duration_seconds": 6000,
        "audio_streams": [
            {"language": "und", "codec": "eac3", "channels": 6, "bitrate_kbps": 640, "title": ""},
        ],
        "subtitle_streams": [],
        "tmdb": {"original_language": "es"},
    }
    enriched = detect_all_languages(entry, use_whisper=False)
    a = enriched["audio_streams"][0]
    assert a.get("detection_method") != "heuristic_channel_bitrate"


def test_channel_bitrate_heuristic_skipped_when_no_tmdb_data() -> None:
    """Without TMDb original_language we can't claim a specific language, so
    the heuristic must not fire even when its signals would otherwise win."""
    entry = {
        "filepath": r"\\NAS\Movies\Foo\Foo.mkv",
        "duration_seconds": 6000,
        "audio_streams": [
            {"language": "und", "codec": "eac3", "channels": 6, "bitrate_kbps": 640, "title": ""},
            {"language": "und", "codec": "eac3", "channels": 2, "bitrate_kbps": 192, "title": ""},
        ],
        "subtitle_streams": [],
        # No 'tmdb' key at all.
    }
    enriched = detect_all_languages(entry, use_whisper=False)
    for a in enriched["audio_streams"]:
        assert a.get("detected_language") is None
        assert a.get("detection_method") is None

    # Empty original_language string is also a no-op.
    entry["tmdb"] = {"original_language": ""}
    enriched2 = detect_all_languages(entry, use_whisper=False)
    for a in enriched2["audio_streams"]:
        assert a.get("detected_language") is None


def test_channel_bitrate_heuristic_skipped_when_all_tracks_tagged() -> None:
    """All audio tracks already have an explicit language → no und tracks for
    the heuristic to act on, even with strong channel/bitrate signals."""
    entry = {
        "filepath": r"\\NAS\Movies\Foo\Foo.mkv",
        "duration_seconds": 6000,
        "audio_streams": [
            {"language": "eng", "codec": "eac3", "channels": 6, "bitrate_kbps": 640, "title": ""},
            {"language": "spa", "codec": "eac3", "channels": 2, "bitrate_kbps": 192, "title": ""},
        ],
        "subtitle_streams": [],
        "tmdb": {"original_language": "es"},
    }
    enriched = detect_all_languages(entry, use_whisper=False)
    for a in enriched["audio_streams"]:
        # detected_language stays None because the explicit lang tag was
        # already authoritative — heuristic must not overwrite tagged tracks.
        assert a.get("detected_language") is None
        assert a.get("detection_method") is None


def test_channel_bitrate_heuristic_bitrate_only_uses_06() -> None:
    """Both tracks 2.0 (no channel signal) but one is 4x the other → bitrate-only,
    confidence 0.6. Confirms the single-signal branches are wired correctly."""
    entry = {
        "filepath": r"\\NAS\Movies\Foo\Foo.mkv",
        "duration_seconds": 6000,
        "audio_streams": [
            {"language": "und", "codec": "eac3", "channels": 2, "bitrate_kbps": 640, "title": ""},
            {"language": "und", "codec": "eac3", "channels": 2, "bitrate_kbps": 160, "title": ""},
        ],
        "subtitle_streams": [],
        "tmdb": {"original_language": "fr"},
    }
    enriched = detect_all_languages(entry, use_whisper=False)
    a0 = enriched["audio_streams"][0]
    assert a0.get("detected_language") == "fr"
    assert a0.get("detection_method") == "heuristic_channel_bitrate"
    assert a0.get("detection_confidence") == 0.6


def test_channel_bitrate_heuristic_conflicting_signals_no_winner() -> None:
    """Channel signal points to one track, bitrate signal to another → abort.

    Conflicting signals mean we can't confidently identify the original. The
    heuristic must not pick a side; both tracks stay und.
    """
    entry = {
        "filepath": r"\\NAS\Movies\Foo\Foo.mkv",
        "duration_seconds": 6000,
        "audio_streams": [
            # 5.1 surround but lower bitrate
            {"language": "und", "codec": "eac3", "channels": 6, "bitrate_kbps": 192, "title": ""},
            # 2.0 stereo but a much higher bitrate
            {"language": "und", "codec": "eac3", "channels": 2, "bitrate_kbps": 640, "title": ""},
        ],
        "subtitle_streams": [],
        "tmdb": {"original_language": "es"},
    }
    enriched = detect_all_languages(entry, use_whisper=False)
    for a in enriched["audio_streams"]:
        assert a.get("detected_language") is None
        assert a.get("detection_method") is None
