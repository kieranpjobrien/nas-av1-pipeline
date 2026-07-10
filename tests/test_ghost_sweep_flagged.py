"""Regression (2026-07-11): the startup ghost sweep must cover deleted
flagged_corrupt (and the other auto-flagged) rows, not just DONE. Before the fix,
a deleted file that was flagged_corrupt lingered in the state DB forever and
inflated the dashboard's "corrupt" count long after the scanner had already
pruned it from media_report. flagged_manual stays put — a user park is not ours
to auto-drop.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.orchestrator import _ghost_candidate_paths
from pipeline.state import FileStatus, PipelineState


def test_ghost_candidates_cover_autoflags_but_not_manual(tmp_path):
    state = PipelineState(str(tmp_path / "state.db"))
    state.set_file("done.mkv", FileStatus.DONE)
    state.set_file("corrupt.mkv", FileStatus.FLAGGED_CORRUPT)
    state.set_file("foreign.mkv", FileStatus.FLAGGED_FOREIGN_AUDIO)
    state.set_file("undetermined.mkv", FileStatus.FLAGGED_UNDETERMINED)
    state.set_file("manual.mkv", FileStatus.FLAGGED_MANUAL)
    state.set_file("pending.mkv", FileStatus.PENDING)
    state.set_file("processing.mkv", FileStatus.PROCESSING)

    cands = set(_ghost_candidate_paths(state))

    assert {"done.mkv", "corrupt.mkv", "foreign.mkv", "undetermined.mkv"} <= cands
    assert "manual.mkv" not in cands, "user park must not be an auto-ghost candidate"
    assert "pending.mkv" not in cands
    assert "processing.mkv" not in cands


def test_remove_ghosts_deletes_a_flagged_corrupt_row(tmp_path):
    """The sweep's effect end-to-end: a flagged_corrupt row is deletable from
    state (the 74 phantom rows cleaned on 2026-07-11)."""
    state = PipelineState(str(tmp_path / "state.db"))
    state.set_file("gone_corrupt.mkv", FileStatus.FLAGGED_CORRUPT)
    assert state.get_file("gone_corrupt.mkv") is not None
    removed = state.remove_ghosts(["gone_corrupt.mkv"])
    assert removed == 1
    assert state.get_file("gone_corrupt.mkv") is None
