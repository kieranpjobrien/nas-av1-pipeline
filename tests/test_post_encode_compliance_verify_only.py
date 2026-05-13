"""Pin the 2026-05-13 phase-2 refactor: post-encode compliance is
verify-only for the drop class.

Pre-2026-05-13 the post-encode compliance gate had a drop-fixer
dispatch — if it found foreign_audio / commentary_audio / foreign_subs
/ extra_eng_subs violations AFTER the encode, it ran mkvmerge against
the uploaded .av1.tmp on NAS over slow SMB. That dispatch hit four
distinct bug classes in 48 hours (per-type-vs-global IDs, sequential
stale indices, len(dict) video count, cached output_probe).

Post-fix: pre-encode prep strips those streams on the LOCAL file
before the GPU runs. The post-encode gate has NO drop fixer — if a
drop violation survives prep, that's a PREP BUG, and the file is
REFUSE-d loudly so the bug surfaces rather than getting silently
re-patched. Tag fixers (mkvpropedit for ENCODER/CQ/GRADE), TMDb,
and filename renames remain — they address encoder-side
post-conditions, not source-layout issues.
"""

from __future__ import annotations

import inspect

import pipeline.full_gamut as fg


def test_post_encode_no_drop_fixer_dispatch():
    """The compliance loop in finalize_upload must NOT call
    _mkvmerge_drop_streams on dest_path. Pre-fix the merged-drop block
    did exactly that; phase 2 replaces it with a PREP MISS refuse."""
    src = inspect.getsource(fg)

    # Locate the finalize_upload function body
    needle = "def finalize_upload"
    start = src.find(needle)
    assert start >= 0, "finalize_upload not found"
    # End is the next top-level def OR end of file
    end = src.find("\ndef ", start + len(needle))
    if end < 0:
        end = len(src)
    body = src[start:end]

    # The drop-fixer dispatch is gone — body must not import
    # _mkvmerge_drop_streams (the only callsite was the dispatch).
    assert "_mkvmerge_drop_streams" not in body, (
        "post-encode drop-fixer dispatch should be removed — drops "
        "are handled by prep_streams pre-encode"
    )


def test_post_encode_prep_miss_logged_and_refused():
    """When a drop violation survives prep, the code path logs PREP MISS
    and increments the compliance_refuse_count breaker instead of
    silently re-patching. Verify the log/error wording is present
    in source so anyone debugging future runs sees the trail."""
    src = inspect.getsource(fg)
    assert "PREP MISS" in src, (
        "the PREP MISS marker must exist so log greps surface prep "
        "regressions instantly"
    )
    assert "drop violation survived pre-encode strip" in src, (
        "the human-readable diagnostic for prep misses must be in the "
        "error path"
    )


def test_non_drop_fixers_kept():
    """Tag / TMDb / filename fixers stay — they're encoder-output
    fixes, not source-layout fixes, and they're cheap. Keep them."""
    src = inspect.getsource(fg)
    # The dispatch table check still mentions the kept fixers.
    for kept_tag in ("missing_encode_tags", "cq_mismatch",
                     "grade_mismatch", "missing_tmdb_tags",
                     "filename_mismatch"):
        assert kept_tag in src, (
            f"non-drop fixer '{kept_tag}' was inadvertently removed — "
            f"these are cheap mkvpropedit/rename ops and stay"
        )
