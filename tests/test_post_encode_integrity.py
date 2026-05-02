"""Regression tests for the 2026-05-02 'pre-replace integrity check' fix.

The 2026-04-13/15 distributed-gap-filler sprint produced 962 AV1 files
that had valid Matroska headers + clean metadata (codec=AV1, audio=EAC-3,
subs OK) but corrupt AV1 streams. Header-only probes missed every one.
The encoder's pre-replace standards check ran and passed; the corrupt
output then replaced the user's clean source.

Fix: after standards compliance passes, decode the first 10 s of the
encoded output via ``ffmpeg -v error -t 10 -f null -``. Any error output
means the file is structurally damaged. Stop the replace, preserve the
corrupt output for inspection, and park in ERROR.

These tests pin the contract — the integrity-check signatures, the
preserve-and-rename behaviour, and the no-replace-on-failure invariant.
"""

from __future__ import annotations

import re


def test_integrity_signatures_present():
    """All 11 known corruption signatures must be in the encoder's
    integrity check. Adding new signatures requires keeping this set in
    sync between scan_corrupt_av1.py and full_gamut.py."""
    src = open("pipeline/full_gamut.py", encoding="utf-8").read()
    block = src.split("integrity_signatures = (", 1)[1].split(")", 1)[0]
    sigs = [s.strip().strip('"').strip("'").strip(",") for s in re.split(r"[,\n]", block) if s.strip()]
    sigs = [s for s in sigs if s and not s.startswith("#")]
    expected = {
        "exceeds containing master element",
        "exceeds max length",
        "unknown-sized element",
        "inside parent with finite size",
        "obu_forbidden_bit out of range",
        "failed to parse temporal unit",
        "unknown obu type",
        "overrun in obu bit buffer",
        "error parsing obu data",
        "invalid data found when processing input",
        "error submitting packet to decoder",
    }
    missing = expected - set(sigs)
    assert not missing, f"missing signatures: {missing}"


def test_corrupt_path_uses_corrupt_suffix():
    """Damaged outputs are preserved as ``<dest>.corrupt`` rather than
    deleted — so a post-mortem can inspect the broken file."""
    src = open("pipeline/full_gamut.py", encoding="utf-8").read()
    assert "dest_path + \".corrupt\"" in src, "preserve-as-.corrupt suffix removed"


def test_integrity_runs_before_replace():
    """The integrity check must sit between the standards compliance check
    and the atomic replace. Order matters: compliance proves codecs+langs;
    integrity proves the bytes actually decode. If the order is swapped
    or the check moves below the rename(), the user's source is already
    gone by the time we know the encode is broken.

    We anchor on the closest unique markers near the encode/replace path
    (the compliance ``violations.append`` cluster and the ``Replace
    original (crash-safe)`` section header) so we don't get fooled by
    earlier docstring mentions of those concepts."""
    src = open("pipeline/full_gamut.py", encoding="utf-8").read()
    integrity_pos = src.find("Stream-level integrity check")
    # rfind: the literal Replace-original section comment may appear in an
    # earlier docstring; we want the actual code location which is later.
    replace_pos = src.rfind("Replace original (crash-safe)")
    # Find the LAST "violations.append" before integrity_pos — that's the
    # tail of the compliance block.
    compliance_block_end = src.rfind("violations.append", 0, integrity_pos)
    assert compliance_block_end != -1, "compliance block not found before integrity"
    assert integrity_pos != -1, "integrity check section missing"
    assert replace_pos != -1, "replace section missing"
    assert compliance_block_end < integrity_pos < replace_pos, (
        f"section order broken: expected compliance({compliance_block_end}) "
        f"-> integrity({integrity_pos}) -> replace({replace_pos})"
    )


def test_integrity_failure_sets_error_status():
    """On detected corruption the state row must go to ERROR with
    ``corruption_signatures`` extras. The re-queue tooling depends on
    this for triage."""
    src = open("pipeline/full_gamut.py", encoding="utf-8").read()
    # The relevant block: state.set_file(..., FileStatus.ERROR, ...,
    #   stage="integrity", corruption_signatures=hits)
    assert "stage=\"integrity\"" in src
    assert "corruption_signatures=hits" in src
