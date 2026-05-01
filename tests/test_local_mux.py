"""Tests for the local mkvmerge backend (Track C, 2026-04-29).

Cover:
  * pipeline.local_mux.is_available / _find_mkvmerge — discovery
  * pipeline.local_mux.local_strip_and_mux — empty-keep-list safety gates
    (audio AND sub) match the remote contract verbatim
  * pipeline.gap_filler._strip_tracks dispatcher — picks the right backend
    by config and raises clearly when neither backend is wireable

We don't actually invoke mkvmerge here — the safety contracts are the
critical surface. End-to-end is exercised by the integration suite +
real runs.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pipeline import local_mux
from pipeline.gap_filler import GapAnalysis, _strip_tracks


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def test_find_mkvmerge_returns_path_or_none():
    """Sanity: discovery returns a string or None — never raises."""
    found = local_mux._find_mkvmerge()
    assert found is None or isinstance(found, str)


def test_is_available_matches_find_mkvmerge():
    assert local_mux.is_available() == (local_mux._find_mkvmerge() is not None)


# ---------------------------------------------------------------------------
# Safety gates: empty keep-list rejection
# ---------------------------------------------------------------------------


def test_local_strip_and_mux_rejects_empty_audio_keep_ids():
    """An empty audio_keep_ids list is the 2026-04-22 256-file destruction
    pattern — mkvmerge would receive ``--audio-tracks `` and strip ALL audio.
    The local path must reject this exactly like the remote path."""
    with pytest.raises(ValueError, match="audio_keep_ids is an empty list"):
        local_mux.local_strip_and_mux(
            "in.mkv", "out.mkv",
            audio_keep_ids=[],
            sub_keep_ids=None,
        )


def test_local_strip_and_mux_rejects_empty_sub_keep_without_no_subs():
    """An empty sub_keep_ids without no_subs=True is ambiguous — refuse."""
    with pytest.raises(ValueError, match="sub_keep_ids is an empty list"):
        local_mux.local_strip_and_mux(
            "in.mkv", "out.mkv",
            audio_keep_ids=None,
            sub_keep_ids=[],
            no_subs=False,
        )


def test_progress_stall_watchdog_kills_frozen_mkvmerge(tmp_path):
    """The 2026-05-01 House S01E17 case: mkvmerge wrote 140 MB then froze
    for 16+ min, blocking the gap_filler queue. The progress watchdog
    must detect the stall (output file size not advancing) and kill the
    process. Without this watchdog the queue stays jammed forever.
    """
    if not local_mux.is_available():
        pytest.skip("mkvmerge not available locally")

    out = tmp_path / "frozen.mkv"
    out.write_bytes(b"x" * 100)  # simulate "wrote 100 bytes then froze"

    poll_calls = {"n": 0}

    class FrozenProc:
        returncode: int | None = None
        killed = False

        def communicate(self, timeout=None):
            poll_calls["n"] += 1
            # Once kill() has been called the post-kill communicate() should
            # return cleanly with whatever output was buffered.
            if self.killed:
                return ("", "stalled")
            raise __import__("subprocess").TimeoutExpired(cmd=["mkvmerge"], timeout=timeout or 5)

        def kill(self):
            self.returncode = -9
            self.killed = True

    proc = FrozenProc()

    # Speed up the test: tiny stall threshold, fake monotonic that jumps fast
    times = iter([0, 1, 2, 3, 100, 101, 102])  # last values exceed any reasonable threshold

    with patch("pipeline.local_mux.subprocess.Popen", return_value=proc), \
         patch("pipeline.local_mux.time.monotonic", side_effect=lambda: next(times)):
        result = local_mux.local_strip_and_mux(
            str(tmp_path / "in.mkv"), str(out),
            audio_keep_ids=[0],
            sub_keep_ids=None,
            timeout=999,
            progress_stall_secs=5,
        )

    # The kill was triggered (returncode reflects forced kill)
    assert proc.returncode == -9
    # And we got at least 2 poll cycles before kill
    assert poll_calls["n"] >= 2


def test_local_strip_and_mux_allows_empty_sub_when_no_subs_set():
    """no_subs=True is the explicit "drop all subs" signal — empty sub_keep_ids
    is fine when paired with it. Don't actually run mkvmerge — just confirm
    the safety gate doesn't fire and the right CLI flags are passed."""
    if not local_mux.is_available():
        pytest.skip("mkvmerge not available locally")

    # 2026-05-01: switched the underlying call from subprocess.run to
    # subprocess.Popen + a progress-stall watchdog (test_progress_watchdog
    # below). Mock Popen and short-circuit communicate() to return cleanly.
    fake_proc = type("P", (), {})()
    fake_proc.returncode = 0
    fake_proc.communicate = lambda timeout=None: ("", "")
    fake_proc.kill = lambda: None

    with patch("pipeline.local_mux.subprocess.Popen") as m:
        m.return_value = fake_proc
        local_mux.local_strip_and_mux(
            "in.mkv", "out.mkv",
            audio_keep_ids=[0, 1],
            sub_keep_ids=[],
            no_subs=True,
        )
        # Should have called Popen exactly once with --no-subtitles
        assert m.call_count == 1
        cmd = m.call_args[0][0]
        assert "--no-subtitles" in cmd
        assert "--subtitle-tracks" not in cmd


# ---------------------------------------------------------------------------
# Dispatcher — _strip_tracks(config) routes by backend
# ---------------------------------------------------------------------------


def test_strip_tracks_routes_to_local_when_backend_local():
    """gap_filler_mux_backend='local' → calls _strip_tracks_locally, not remote."""
    gaps = GapAnalysis(needs_track_removal=True, audio_keep_indices=[0])
    config = {"gap_filler_mux_backend": "local"}
    with patch("pipeline.gap_filler._strip_tracks_locally", return_value=True) as m_local, \
         patch("pipeline.gap_filler._strip_tracks_on_nas", return_value=True) as m_remote:
        ok = _strip_tracks("foo.mkv", gaps, config)
        assert ok is True
        m_local.assert_called_once()
        m_remote.assert_not_called()


def test_strip_tracks_routes_to_remote_when_backend_remote():
    gaps = GapAnalysis(needs_track_removal=True, audio_keep_indices=[0])
    config = {"gap_filler_mux_backend": "remote"}
    with patch("pipeline.gap_filler._strip_tracks_locally", return_value=True) as m_local, \
         patch("pipeline.gap_filler._strip_tracks_on_nas", return_value=True) as m_remote:
        ok = _strip_tracks("foo.mkv", gaps, config, machine={"host": "nas", "label": "SRV"})
        assert ok is True
        m_remote.assert_called_once()
        m_local.assert_not_called()


def test_strip_tracks_default_backend_is_local():
    """No explicit config key → default "local"."""
    gaps = GapAnalysis(needs_track_removal=True, audio_keep_indices=[0])
    config = {}  # no backend key
    with patch("pipeline.gap_filler._strip_tracks_locally", return_value=True) as m_local:
        _strip_tracks("foo.mkv", gaps, config)
        m_local.assert_called_once()


def test_strip_tracks_unknown_backend_raises():
    gaps = GapAnalysis(needs_track_removal=True, audio_keep_indices=[0])
    config = {"gap_filler_mux_backend": "magic"}
    with pytest.raises(RuntimeError, match="unknown gap_filler_mux_backend"):
        _strip_tracks("foo.mkv", gaps, config)
