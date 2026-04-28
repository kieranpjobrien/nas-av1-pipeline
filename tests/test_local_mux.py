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


def test_local_strip_and_mux_allows_empty_sub_when_no_subs_set():
    """no_subs=True is the explicit "drop all subs" signal — empty sub_keep_ids
    is fine when paired with it. Don't actually run mkvmerge — just confirm
    the safety gate doesn't fire."""
    if not local_mux.is_available():
        pytest.skip("mkvmerge not available locally")
    with patch("pipeline.local_mux.subprocess.run") as m:
        m.return_value = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()
        local_mux.local_strip_and_mux(
            "in.mkv", "out.mkv",
            audio_keep_ids=[0, 1],
            sub_keep_ids=[],
            no_subs=True,
        )
        # Should have called subprocess.run exactly once with --no-subtitles
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
