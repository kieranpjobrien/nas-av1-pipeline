"""Pin the 2026-05-12 circuit breaker — Ford v Ferrari class.

A file that fails integrity check 3+ times in a row produces 3+ corrupt
encode outputs across 3+ rounds of GPU + bandwidth. Pre-fix: Ford v
Ferrari ran this loop 10 times across 9 days. The user explicitly
called out that the discipline contract required this and the code
never enforced it (CLAUDE.md rule 2: "Never downplay a recurring error.
If the same error class fires 3+ times in a row, circuit breaker opens.").

These tests pin the breaker on three failure pathways:
  1. Integrity check on the encoded output (Ford v Ferrari).
  2. Compliance gate REFUSE up-front (file's video codec wrong, etc.).
  3. Compliance gate post-fix RESIDUAL (fixers couldn't repair).

In each case after ``BREAKER`` consecutive failures the status must
be FLAGGED_CORRUPT not ERROR — FLAGGED_CORRUPT is terminal; the queue
builder skips it and stops the loop. ERROR rows get re-queued via the
dashboard which is how Ford v Ferrari kept coming back.
"""

from __future__ import annotations


def _src(path: str = "pipeline/full_gamut.py") -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


def test_integrity_breaker_constant():
    """Source contains the breaker threshold constant. Pinning the value
    keeps test + production in sync — if someone changes the threshold
    they have to update this test on purpose, not by accident."""
    src = _src()
    assert "INTEGRITY_FAIL_BREAKER = 3" in src, (
        "integrity breaker threshold missing or moved — was 3 at 2026-05-12 ship"
    )


def test_integrity_breaker_uses_flagged_corrupt():
    """At the breaker the file must transition to FLAGGED_CORRUPT (terminal),
    not ERROR (re-queueable). The discipline rule is "stop the loop";
    ERROR doesn't stop it."""
    src = _src()
    # Anchor on the integrity-failure code block + breaker logic in the same window
    block_start = src.find("INTEGRITY_FAIL_BREAKER")
    assert block_start != -1
    block = src[block_start : block_start + 1500]
    # Must transition to FLAGGED_CORRUPT, set integrity_failure_count, clear
    # force_reencode so the dashboard can't accidentally re-flag it.
    assert "FileStatus.FLAGGED_CORRUPT" in block, "breaker must land in flagged_corrupt"
    assert "integrity_failure_count=total_failures" in block, "counter must be persisted"
    assert "force_reencode=False" in block, "must clear force_reencode at the breaker"


def test_integrity_counter_persists_across_attempts():
    """The counter that drives the breaker must come from the file's previous
    state (state.get_file), not from a fresh-attempt local variable. Otherwise
    each re-queue resets to 0 and the breaker never fires (which is exactly
    what happened to Ford v Ferrari for 10 attempts).
    """
    src = _src()
    block_start = src.find("INTEGRITY_FAIL_BREAKER")
    block = src[block_start : block_start + 1500]
    # Counter must be loaded from previous state extras + incremented
    assert "prev_extras = state.get_file(filepath) or {}" in block
    assert 'prev_extras.get("integrity_failure_count"' in block
    assert "+ 1" in block, "counter must be incremented (not just read)"


def test_compliance_refuse_breaker_constant():
    src = _src()
    # Compliance refuse uses the same threshold (3). Two call sites: up-front
    # REFUSE and post-fix residual. Both should match.
    count = src.count("COMPLIANCE_REFUSE_BREAKER = 3")
    assert count >= 2, (
        f"compliance refuse breaker constant should be defined at both "
        f"call sites (up-front + post-fix residual); found {count}"
    )


def _breaker_setfile_window(src: str, error_needle: str) -> str:
    """Return the ``state.set_file(...)`` call that a breaker's error-string
    anchor belongs to, so assertions target one specific breaker site
    rather than a fixed-width window that can bleed into a neighbour."""
    anchor = src.find(error_needle)
    assert anchor != -1, f"breaker anchor moved / not found: {error_needle!r}"
    call = src.rfind("state.set_file(", 0, anchor)
    assert call != -1, f"no set_file precedes anchor {error_needle!r}"
    return src[call : anchor + 300]


def test_compliance_breaker_uses_flagged_corrupt():
    """The two GENUINE-refuse compliance breakers must land in
    FLAGGED_CORRUPT (terminal) so the queue builder stops re-trying an
    output that is actually wrong:

      * up-front REFUSE — wrong video/audio codec, zero-audio, probe error.
      * post-fix RESIDUAL — the tag/TMDb/rename fixers ran and violations
        still remain.

    NOTE: the THIRD compliance breaker — the drop-violation "prep miss"
    block — is deliberately NOT covered here. A foreign-audio / sub track
    that survived the pre-encode strip is a compliance/policy hold on an
    INTACT source, not corruption, so it routes via _prep_miss_flag_status
    to FLAGGED_FOREIGN_AUDIO / FLAGGED_MANUAL (2026-06-30 fix). That
    routing is pinned in tests/test_prep_miss_flag_status.py.
    """
    src = _src()

    # Site 1: up-front REFUSE (grouped REFUSE + UNRECOVERABLE).
    upfront = _breaker_setfile_window(src, 'f"{refuse_count} consecutive compliance refuses:')
    assert "FileStatus.FLAGGED_CORRUPT" in upfront, "up-front REFUSE breaker must land in FLAGGED_CORRUPT"
    assert "compliance_refuse_count=refuse_count" in upfront, "up-front breaker must persist the counter"

    # Site 2: post-fix RESIDUAL (fixers ran, violations remain).
    residual = _breaker_setfile_window(src, 'f"{refuse_count} unfixed:')
    assert "FileStatus.FLAGGED_CORRUPT" in residual, "post-fix RESIDUAL breaker must land in FLAGGED_CORRUPT"
    assert "compliance_refuse_count=refuse_count" in residual, "residual breaker must persist the counter"


def test_prep_miss_breaker_does_not_use_flagged_corrupt():
    """Pin the 2026-06-30 fix from the other side: the drop-violation
    "prep miss" breaker must NOT hardcode FLAGGED_CORRUPT — that
    mislabelled intact sources (Eternity / Saturday Night) as corrupt.
    It routes by violation class via _prep_miss_flag_status instead."""
    src = _src()
    start = src.find("if drop_violations:")
    assert start != -1, "drop_violations breaker block not found"
    end = src.find("# Run the remaining (non-drop) fixers", start)
    assert end > start, "end anchor (non-drop fixer loop) not found"
    block = src[start:end]
    assert "_prep_miss_flag_status(drop_violations)" in block, (
        "drop-violation breaker must route via _prep_miss_flag_status"
    )
    assert "FileStatus.FLAGGED_CORRUPT" not in block, (
        "a surviving drop violation is a compliance hold, not corruption"
    )


def test_categorise_entry_skips_flagged_corrupt():
    """Once a file is FLAGGED_CORRUPT, the queue builder must not re-add it.
    The is_terminal helper covers all FLAGGED_* statuses — pinning here so
    a regression where someone exempts FLAGGED_CORRUPT from terminal would
    fail loud."""
    from pipeline.state import FileStatus, is_terminal

    assert is_terminal(FileStatus.FLAGGED_CORRUPT), (
        "FLAGGED_CORRUPT must be terminal so the queue builder skips it — "
        "this is what stops the circuit-breaker loop"
    )
    assert is_terminal(FileStatus.FLAGGED_FOREIGN_AUDIO)
    assert is_terminal(FileStatus.FLAGGED_UNDETERMINED)
    assert is_terminal(FileStatus.FLAGGED_MANUAL)
    # And ERROR must NOT be terminal (otherwise we'd never auto-retry the
    # transient stuff like fetch retries)
    assert not is_terminal(FileStatus.ERROR), (
        "ERROR must stay non-terminal so the queue builder retries; the "
        "circuit breaker transitions to FLAGGED_CORRUPT specifically to "
        "halt the retry loop without converting all errors to terminal"
    )
