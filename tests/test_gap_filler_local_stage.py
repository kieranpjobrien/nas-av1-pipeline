"""Regression tests for the 2026-05-01 'big-file gap_filler stalls on SMB' bug.

Background: ``_strip_tracks_locally`` ran mkvmerge with both INPUT and OUTPUT
on UNC paths. mkvmerge's read pattern + SMB contention with the encoder's
fetch/upload workers degraded throughput to ~520 KB/s — a 9.17 GB file
projected to take 5+ hours. We hung gap_filler for 2h before killing the
process.

Fix: above ``gap_filler_local_stage_threshold_bytes`` (default 2 GB), copy
the input file to local SSD first, run mkvmerge against the local input,
and write the output back over UNC. The local copy is unlinked whether
mkvmerge succeeded or not, so no SSD bloat.

These tests pin the contract.
"""

from __future__ import annotations

from pipeline.config import build_config


def test_threshold_default_is_2gb():
    """A regression on the default would silently re-create the slow path."""
    cfg = build_config()
    assert cfg["gap_filler_local_stage_threshold_bytes"] == 2 * 1024**3


def test_threshold_overridable():
    """Operators can tune the threshold without editing source."""
    cfg = build_config({"gap_filler_local_stage_threshold_bytes": 500 * 1024**2})
    assert cfg["gap_filler_local_stage_threshold_bytes"] == 500 * 1024**2


def test_analyse_gaps_attaches_config():
    """``_strip_tracks_locally`` reads the threshold via ``gaps._config``.
    If analyse_gaps stops attaching it, the worker silently falls back to
    the 2 GB default — which is fine, but a config override would be
    invisible. Pin the attach to make the override path explicit."""
    from pipeline.gap_filler import analyse_gaps

    cfg = build_config({"gap_filler_local_stage_threshold_bytes": 7777})
    entry = {
        "filepath": "/x.mkv",
        "audio_streams": [{"codec_raw": "eac3", "language": "eng"}],
        "subtitle_streams": [],
        "tmdb": {},
    }
    gaps = analyse_gaps(entry, cfg)
    assert getattr(gaps, "_config", None) is cfg
    assert gaps._config["gap_filler_local_stage_threshold_bytes"] == 7777


def test_strip_tracks_locally_signature_unchanged():
    """The worker's public signature stays (filepath, gaps) so existing
    callers don't break. Threshold consultation is internal."""
    import inspect

    from pipeline.gap_filler import _strip_tracks_locally

    sig = inspect.signature(_strip_tracks_locally)
    assert list(sig.parameters) == ["filepath", "gaps"]


def test_local_stage_path_is_under_staging_dir():
    """Sanity: the staged copy must live under STAGING_DIR/gap_stage so
    the orchestrator's startup cleanup can reclaim it after a crash. If
    we put it elsewhere, leftovers would accumulate forever."""
    from pathlib import Path

    from paths import STAGING_DIR

    expected_parent = Path(STAGING_DIR) / "gap_stage"
    # Just verify the literal — the actual mkdir happens at runtime
    src = Path("pipeline/gap_filler.py").read_text(encoding="utf-8")
    assert "gap_stage" in src
    # And that we use STAGING_DIR not a hardcoded path
    assert "from paths import STAGING_DIR" in src
