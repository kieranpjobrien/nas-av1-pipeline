"""Pin the 2026-06-30 prep-miss mislabel fix.

A drop violation (foreign_audio / commentary_audio / foreign_subs /
extra_eng_subs) that survives the pre-encode strip is a COMPLIANCE /
POLICY hold, not source corruption — the source bytes are intact, prep
just couldn't remove a track (commonly because TMDb ``original_language``
was empty at encode time, so ``_select_audio_streams`` kept all audio via
the ``<=2``-track legacy guard).

Pre-fix, the post-encode ``finalize_upload`` breaker parked ALL of these
as ``FLAGGED_CORRUPT`` once ``compliance_refuse_count`` hit 3. That (a)
confused triage — the file looked unreadable when it was perfectly intact
— and (b) hid the Flagged pane's ``encode_anyway`` / ``delete_redownload``
actions behind the "corrupt / re-acquire" bucket. Real-world hit:
*Eternity (2025)* and *Saturday Night (2024)*, both intact HEVC with a
French dub, sat as flagged_corrupt until manually requeued.

Post-fix: ``_prep_miss_flag_status`` routes by violation class —
foreign_audio → FLAGGED_FOREIGN_AUDIO, the rest → FLAGGED_MANUAL —
and FLAGGED_CORRUPT stays reserved for the prep_source_integrity
decode-error path.
"""

from __future__ import annotations

import inspect

import pipeline.full_gamut as fg
from pipeline.compliance import Category, Violation
from pipeline.full_gamut import _prep_miss_flag_status
from pipeline.state import FileStatus


def _v(tag: str) -> Violation:
    """A drop-class violation with the given tag (category is always
    FIXABLE for the drop class — see pipeline.compliance)."""
    return Violation(tag=tag, message=f"{tag} survived strip", category=Category.FIXABLE, data={})


def test_foreign_audio_prep_miss_is_flagged_foreign_audio_not_corrupt():
    """The Eternity / Saturday Night class: a surviving foreign dub track
    lands in FLAGGED_FOREIGN_AUDIO so the pane's re-grab / encode-anyway
    actions apply — NOT FLAGGED_CORRUPT."""
    status = _prep_miss_flag_status([_v("foreign_audio")])
    assert status == FileStatus.FLAGGED_FOREIGN_AUDIO
    assert status != FileStatus.FLAGGED_CORRUPT


def test_commentary_audio_prep_miss_is_manual_not_corrupt():
    status = _prep_miss_flag_status([_v("commentary_audio")])
    assert status == FileStatus.FLAGGED_MANUAL
    assert status != FileStatus.FLAGGED_CORRUPT


def test_foreign_subs_prep_miss_is_manual_not_corrupt():
    status = _prep_miss_flag_status([_v("foreign_subs")])
    assert status == FileStatus.FLAGGED_MANUAL
    assert status != FileStatus.FLAGGED_CORRUPT


def test_extra_eng_subs_prep_miss_is_manual_not_corrupt():
    status = _prep_miss_flag_status([_v("extra_eng_subs")])
    assert status == FileStatus.FLAGGED_MANUAL
    assert status != FileStatus.FLAGGED_CORRUPT


def test_mixed_audio_and_sub_prep_miss_prefers_foreign_audio():
    """When both a foreign-audio and a sub-layout violation survive, the
    foreign-audio class wins — it's the most actionable and carries the
    heaviest 'keep the foreign audio?' decision for the user."""
    status = _prep_miss_flag_status([_v("foreign_subs"), _v("foreign_audio")])
    assert status == FileStatus.FLAGGED_FOREIGN_AUDIO


def test_no_drop_class_ever_maps_to_corrupt():
    """Belt-and-suspenders: none of the four drop tags — alone or in any
    combination — may resolve to FLAGGED_CORRUPT."""
    tags = ["foreign_audio", "commentary_audio", "foreign_subs", "extra_eng_subs"]
    for tag in tags:
        assert _prep_miss_flag_status([_v(tag)]) != FileStatus.FLAGGED_CORRUPT
    # All four at once, too.
    assert _prep_miss_flag_status([_v(t) for t in tags]) != FileStatus.FLAGGED_CORRUPT


def test_breaker_block_routes_via_helper_not_hardcoded_corrupt():
    """Guard the production callsite: the drop-violation breaker region in
    finalize_upload must route through _prep_miss_flag_status and must NOT
    hardcode FLAGGED_CORRUPT (the pre-fix bug). Source-level so a future
    revert to `FileStatus.FLAGGED_CORRUPT` in that block fails loudly.
    """
    src = inspect.getsource(fg)

    # Slice the drop-violation breaker block: from the `if drop_violations:`
    # guard to the comment that starts the non-drop fixer loop right after.
    start = src.find("if drop_violations:")
    assert start >= 0, "drop_violations breaker block not found"
    end = src.find("# Run the remaining (non-drop) fixers", start)
    assert end > start, "end anchor (non-drop fixer loop) not found"
    block = src[start:end]

    assert "_prep_miss_flag_status(drop_violations)" in block, (
        "the drop-violation breaker must choose its terminal status via "
        "_prep_miss_flag_status, not a hardcoded flag"
    )
    # Check the actual enum usage (FileStatus.FLAGGED_CORRUPT), not the
    # bare word — the explanatory comment legitimately names the status
    # it is deliberately NOT using.
    assert "FileStatus.FLAGGED_CORRUPT" not in block, (
        "a surviving drop violation is a compliance hold, not source "
        "corruption - FileStatus.FLAGGED_CORRUPT must not be set here"
    )
