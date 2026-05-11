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

import re


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


def test_compliance_breaker_uses_flagged_corrupt():
    src = _src()
    # Anchor on the compliance refuse block. There are two — confirm BOTH have
    # the breaker transition.
    sections = src.split("COMPLIANCE_REFUSE_BREAKER = 3")
    assert len(sections) >= 3, "expected COMPLIANCE_REFUSE_BREAKER defined 2+ times"
    for i, section in enumerate(sections[1:], 1):
        window = section[:1500]
        assert "FileStatus.FLAGGED_CORRUPT" in window, (
            f"compliance breaker section #{i} must transition to FLAGGED_CORRUPT"
        )
        assert "compliance_refuse_count=refuse_count" in window, (
            f"compliance breaker section #{i} must persist the counter"
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
