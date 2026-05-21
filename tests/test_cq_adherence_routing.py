"""Regression test for the CQ-adherence routing rule in
``pipeline.__main__.categorise_entry``.

Background: pre-2026-05-21 the categoriser gated AV1 compliance on
codec + audio config + sub config only. An AV1 file encoded at CQ 30
under the older policy stayed DONE forever even when the tv_animation
grade rule moved the target to CQ 37. Operator policy (re-stated
2026-05-21): "if they're too low then they're not done — that needs
to be stopped."

These tests pin the new behaviour: any AV1 entry whose audit blob
shows ``current_cq != target_cq`` MUST be routed to ``full_gamut``,
regardless of bucket label (so inferred_uncertain rows are actioned
too — the user explicitly wants those Bluey-class files re-encoded).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.__main__ import categorise_entry
from pipeline.control import PipelineControl
from pipeline.state import PipelineState


def _state(tmp_path) -> PipelineState:
    return PipelineState(str(tmp_path / "state.db"))


def _control(tmp_path) -> PipelineControl:
    return PipelineControl(str(tmp_path))


def _av1_entry(filepath: str, *, current_cq, target_cq, bucket="optimal") -> dict:
    """A clean AV1 entry with no other gaps — audio is EAC-3 6ch, no subs.

    Without the CQ-adherence guard, this entry would have routed to ``skip``
    (analyse_gaps returns needs_anything=False) and the file would have
    sat DONE forever. With the guard, ``cur != tgt`` flips it to full_gamut.
    """
    return {
        "filepath": filepath,
        "filename": filepath.split("\\")[-1],
        "library_type": "series",
        "file_size_bytes": 250_000_000,
        "video": {"codec_raw": "av1"},
        "audio_streams": [{"codec_raw": "eac3", "language": "eng", "channels": 6}],
        "subtitle_streams": [],
        "tmdb": {"original_language": "en", "title": "Test"},
        "audit": {
            "current_cq": current_cq,
            "target_cq": target_cq,
            "bucket": bucket,
            "source": "tag" if bucket != "inferred_uncertain" else "bitrate_inferred",
        },
    }


def test_av1_too_low_routes_to_full_gamut(tmp_path):
    """The Bluey class: AV1 encoded at CQ 30 under the older policy,
    current target CQ 37 (tv_animation grade rule). Must re-encode."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Series\Bluey\Season 1\Bluey S01E11 Bike.mkv"

    entry = _av1_entry(fp, current_cq=30, target_cq=37, bucket="inferred_uncertain")
    category, item = categorise_entry(entry, {}, state, control)
    assert category == "full_gamut", (
        f"AV1 with cur=30 < tgt=37 must re-encode; got category={category!r}"
    )
    assert item is not None


def test_av1_too_high_routes_to_full_gamut(tmp_path):
    """An AV1 file encoded at a HIGHER CQ than current target (lower
    quality than wanted). Must re-encode to improve."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Movies\Example\Example.mkv"

    entry = _av1_entry(fp, current_cq=35, target_cq=30, bucket="too_high")
    category, _item = categorise_entry(entry, {}, state, control)
    assert category == "full_gamut", (
        f"AV1 with cur=35 > tgt=30 must re-encode; got category={category!r}"
    )


def test_av1_on_target_does_not_route_to_full_gamut(tmp_path):
    """An AV1 file already at target CQ + clean audio/subs must NOT
    re-encode — that's the whole point of compliance. Without this
    sanity guard, my new check could over-trigger and re-encode every
    optimal file too."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Movies\Optimal\Optimal.mkv"

    entry = _av1_entry(fp, current_cq=30, target_cq=30, bucket="optimal")
    category, _item = categorise_entry(entry, {}, state, control)
    assert category == "skip", (
        f"AV1 with cur==tgt must skip; got category={category!r}"
    )


def test_av1_missing_audit_blob_does_not_force_reencode(tmp_path):
    """If the audit blob is absent (e.g. scanner hasn't audited a brand-new
    file yet), categorise_entry must NOT panic and route to full_gamut.
    Fall through to the existing analyse_gaps logic."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Movies\Unaudited\Unaudited.mkv"

    entry = _av1_entry(fp, current_cq=30, target_cq=30, bucket="optimal")
    entry.pop("audit", None)  # No audit data yet.
    category, _item = categorise_entry(entry, {}, state, control)
    assert category == "skip", (
        f"AV1 with no audit blob and no other gaps must skip; got {category!r}"
    )


def test_av1_partial_audit_blob_does_not_force_reencode(tmp_path):
    """current_cq or target_cq is None — partial audit shouldn't trigger
    the re-encode either. Required guard against ``None != int`` evaluating
    truthy."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Movies\PartialAudit\PartialAudit.mkv"

    for cur, tgt in [(None, 30), (30, None), (None, None)]:
        entry = _av1_entry(fp, current_cq=cur, target_cq=tgt, bucket="optimal")
        category, _item = categorise_entry(entry, {}, state, control)
        assert category == "skip", (
            f"partial audit ({cur}, {tgt}) must skip; got {category!r}"
        )
