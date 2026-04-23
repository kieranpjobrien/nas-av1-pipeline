"""Regression tests for the 2026-04-23 incident fixes.

Each test pins down one of the seven "mark DONE on failure" anti-patterns that
lost files overnight. The failure modes: track-strip deferred DONE, audio
transcode no post-verify, fetch missing source DONE, probe failure silent
success, and the state-layer guard itself.
"""

import os
from unittest.mock import patch

import pytest

from pipeline.gap_filler import GapAnalysis, _audio_transcode, gap_fill
from pipeline.state import FileStatus, PipelineState
from pipeline.transfer import fetch_file


@pytest.fixture()
def min_config():
    """Minimal config dict for gap_fill / transfer tests."""
    return {
        "strip_non_english_audio": True,
        "strip_non_english_subs": True,
        "max_staging_bytes": 10_000_000_000,
        "min_free_space_bytes": 1_000_000,
        "max_fetch_buffer_bytes": 10_000_000_000,
        "lossless_audio_codecs": [],
    }


def _entry(filepath: str, filename: str) -> dict:
    return {
        "filepath": filepath,
        "filename": filename,
        "library_type": "movie",
        "audio_streams": [{"codec_raw": "eac3", "language": "eng"}],
        "subtitle_streams": [],
        "tmdb": {"id": 1},
    }


class TestTrackStripFailure:
    """FIX 1: a track-strip failure must not mark the file DONE."""

    def test_track_strip_deferred_does_not_mark_done(self, tmp_state_db, tmp_path, min_config):
        """When _strip_tracks_on_nas returns False, status must be ERROR, not DONE."""
        filepath = str(tmp_path / "movie.mkv")
        # A real file so os.path.exists() passes
        with open(filepath, "wb") as f:
            f.write(b"fake mkv content for the test")

        state = PipelineState(tmp_state_db)
        entry = _entry(filepath, "movie.mkv")
        # Force a gap that requires track-stripping but NOT audio transcode
        gaps = GapAnalysis(
            needs_track_removal=True,
            audio_keep_indices=[0],
            sub_keep_indices=[],
        )
        gaps._external_scan_done = True  # skip the NAS scan

        # Mock the NAS strip to simulate the SSH-unavailable / rc=137 case
        with patch(
            "pipeline.gap_filler._strip_tracks_on_nas",
            return_value=False,
        ):
            # Also stub metadata / report so we don't hit the network
            with patch("pipeline.metadata.enrich_and_tag", return_value=None, create=True), \
                 patch("pipeline.report.update_entry", return_value=True):
                gap_fill(filepath, entry, gaps, min_config, state)

        saved = state.get_file(filepath)
        state.close()
        assert saved is not None, "expected a state row after gap_fill"
        assert saved["status"] == FileStatus.ERROR.value, (
            f"expected ERROR on strip failure, got {saved.get('status')!r} "
            f"with reason={saved.get('reason')!r}"
        )
        # Stage is the machine-readable hook that the next queue build uses to retry.
        assert saved.get("stage") == "track_strip"


class TestAudioTranscodeVerify:
    """FIX 2: _audio_transcode must ffprobe the staging output before replacing."""

    def test_audio_transcode_verifies_before_replace(self, tmp_state_db, tmp_path, min_config):
        """When ffprobe finds zero audio streams, os.replace must NOT be called."""
        filepath = str(tmp_path / "dest.mkv")
        with open(filepath, "wb") as f:
            f.write(b"original")
        original_bytes = open(filepath, "rb").read()

        state = PipelineState(tmp_state_db)
        entry = {
            "filepath": filepath,
            "filename": "dest.mkv",
            "library_type": "movie",
            "audio_streams": [{"codec_raw": "truehd", "language": "eng"}],
            "subtitle_streams": [],
            "tmdb": {"id": 1},
        }
        gaps = GapAnalysis(
            needs_audio_transcode=True,
            audio_keep_indices=[0],
            audio_transcode_indices=[0],
        )
        gaps._external_scan_done = True

        class _FakeProc:
            returncode = 0

        # Build the ffmpeg command builder to a no-op and simulate an output file exists
        # but has zero audio streams in its ffprobe JSON.
        def _fake_ffmpeg_cmd(*a, **kw):
            return ["ffmpeg", "-fake"]

        def _fake_run_with_retry(cmd, in_path, out_path):
            # Pretend ffmpeg succeeded and wrote a staging file.
            with open(out_path, "wb") as fh:
                fh.write(b"staged output")
            return _FakeProc(), ""

        def _fake_probe(probe_cmd, capture_output, text, timeout):
            """Return stdout JSON showing VIDEO but ZERO AUDIO — the damage-path we guard against."""

            class _Res:
                returncode = 0
                stdout = '{"streams": [{"codec_type": "video"}]}'

            return _Res()

        replace_calls = []

        def _fake_replace(src, dst):
            replace_calls.append((src, dst))

        with patch("pipeline.ffmpeg.build_audio_remux_cmd", side_effect=_fake_ffmpeg_cmd), \
             patch("pipeline.gap_filler._run_audio_transcode_with_retry", side_effect=_fake_run_with_retry), \
             patch("pipeline.gap_filler.shutil.copy2") as m_copy, \
             patch("pipeline.gap_filler.subprocess.run", side_effect=_fake_probe), \
             patch("pipeline.gap_filler.os.replace", side_effect=_fake_replace):
            ok = _audio_transcode(filepath, entry, gaps, min_config, state)

        # Original file should NOT have been clobbered via os.replace
        assert ok is False, "_audio_transcode should return False on zero-audio output"
        assert replace_calls == [], f"os.replace must not be invoked on verify failure, got {replace_calls!r}"
        # shutil.copy2 to the NAS staging tmp must also not run (replace+copy both suppressed)
        assert not any(
            ("audiotrans_tmp" in str(c.args[1])) for c in m_copy.call_args_list if len(c.args) >= 2
        ), "copy to NAS tmp path must not run when verify fails"
        # Original file contents preserved
        assert open(filepath, "rb").read() == original_bytes

        saved = state.get_file(filepath)
        state.close()
        assert saved is not None
        assert saved["status"] == FileStatus.ERROR.value
        assert saved.get("stage") == "audio_transcode"


class TestFetchMissingSource:
    """FIX 3: fetch_file with a missing source must mark ERROR (after 2s re-probe), not DONE."""

    def test_fetch_missing_source_marks_error(self, tmp_state_db, tmp_path, min_config):
        """os.path.exists returns False on both probes → ERROR with stage='fetch'."""
        source = str(tmp_path / "missing.mkv")
        # Don't create the file — it's genuinely missing
        item = {
            "filepath": source,
            "filename": "missing.mkv",
            "file_size_bytes": 100_000,
        }
        state = PipelineState(tmp_state_db)

        # Patch time.sleep so the 2s re-probe delay doesn't slow the test
        with patch("pipeline.transfer.time.sleep"):
            result = fetch_file(item, str(tmp_path), min_config, state)

        saved = state.get_file(source)
        state.close()
        assert result is None
        assert saved is not None
        assert saved["status"] == FileStatus.ERROR.value
        assert saved.get("stage") == "fetch"
        assert "not found" in (saved.get("error") or "").lower()


class TestStateGuard:
    """FIX 7: PipelineState.set_file must reject DONE paired with a deferred/skipped reason."""

    def test_state_rejects_done_with_deferred_reason(self, tmp_state_db):
        """Explicit ValueError when a caller tries to encode DONE+deferred."""
        state = PipelineState(tmp_state_db)
        fp = r"\\KieranNAS\Media\Movies\Guarded.mkv"

        with pytest.raises(ValueError, match="deferred/skipped"):
            state.set_file(fp, FileStatus.DONE, reason="track_strip deferred")

        with pytest.raises(ValueError, match="deferred/skipped"):
            state.set_file(fp, FileStatus.DONE, reason="local ops done; strip deferred (ssh unavailable)")

        with pytest.raises(ValueError, match="deferred/skipped"):
            state.set_file(fp, FileStatus.DONE, reason="source file skipped")

        # DONE without a suspicious reason is still fine
        state.set_file(fp, FileStatus.DONE, reason="nothing to do")
        assert state.get_file(fp)["status"] == FileStatus.DONE.value
        state.close()


class TestProbeFailure:
    """FIX 5: finalize_upload must hard-ERROR when _probe_full returns {"error": ...}."""

    def test_probe_failure_marks_error(self, tmp_state_db, tmp_path, min_config):
        """A staging file that ffprobe can't parse is rejected, not quietly shipped."""
        from pipeline.full_gamut import finalize_upload

        filepath = str(tmp_path / "source.mkv")
        final_name = "source.mkv"
        # Create a staging output that finalize_upload will try to verify
        source_dir = str(tmp_path)
        dest_path = os.path.join(source_dir, final_name + ".av1.tmp")

        state = PipelineState(tmp_state_db)
        # Seed a minimal UPLOADING entry that finalize_upload will pick up
        state.set_file(
            filepath,
            FileStatus.UPLOADING,
            output_path=str(tmp_path / "encoded.mkv"),
            final_name=final_name,
            library_type="movie",
            input_size_bytes=1_000_000,
            output_size_bytes=500_000,
            bytes_saved=500_000,
            encode_time_secs=10,
            duration_seconds=1800,
            compression_ratio=0.5,
        )
        # Create the encoded output so it passes the "missing" check
        with open(str(tmp_path / "encoded.mkv"), "wb") as fh:
            fh.write(b"x" * 500_000)

        def _fake_copy2(src, dst):
            with open(dst, "wb") as fh:
                fh.write(b"y" * 500_000)

        # Force _probe_full to report a probe failure on the staging output
        with patch("pipeline.full_gamut.shutil.copy2", side_effect=_fake_copy2), \
             patch("pipeline.full_gamut.get_duration", return_value=1800), \
             patch("pipeline.full_gamut._probe_full", return_value={"error": "bad container header"}):
            ok = finalize_upload(filepath, state, min_config)

        saved = state.get_file(filepath)
        state.close()
        assert ok is False, "finalize_upload must return False on probe failure"
        assert saved is not None
        assert saved["status"] == FileStatus.ERROR.value
        assert saved.get("stage") == "verify"
        assert "probe failed" in (saved.get("error") or "").lower()
        # Staging tmp file should have been cleaned up
        assert not os.path.exists(dest_path)
