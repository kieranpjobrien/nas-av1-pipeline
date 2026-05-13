"""Pin the 2026-05-13 filename-mismatch fix in the compliance loop.

Nine "Arrested Development" files in the 50/50/50 mixed-codec batch
ERRORed with::

    compliance unfixed: filename is 'Arrested Development - S01E18 -
    Missing Kitty.mkv', expected 'Arrested Development S01E18 Missing
    Kitty.mkv'

Root cause: ``_run_compliance`` in ``pipeline.full_gamut.finalize_upload``
passed ``filepath=filepath`` to :func:`pipeline.compliance.check_compliance`.
``filepath`` is the SOURCE NAS path (UNC, dashed); the compliance check
does ``os.path.basename(filepath)`` for the filename_mismatch test. The
fixer renames the LOCAL staging file but the residual compliance run
keeps reading the source basename — so the violation never goes away.

The canonical destination is ``final_path`` (computed at line 1081 from
``source_dir + final_name``); after the atomic replace that's where the
new file lives. ``check_compliance``'s docstring explicitly says the
``filepath`` argument is "final NAS destination". The fix is one line:
pass ``final_path``, not ``filepath``, into the check.
"""

from __future__ import annotations

import inspect


def test_run_compliance_uses_final_path_not_source_filepath():
    """Static check: the _run_compliance closure must call
    check_compliance with the canonical final_path. If someone reverts
    to passing the outer ``filepath`` (source NAS path), files whose
    source basename differs from their canonical form will ERROR with
    "compliance unfixed: filename is …" and no amount of fixer work
    will repair it."""
    import pipeline.full_gamut as fg

    src = inspect.getsource(fg)
    needle = "def _run_compliance() -> list:"
    idx = src.find(needle)
    assert idx >= 0, "_run_compliance closure not found in full_gamut.py"
    body = src[idx : idx + 2500]
    assert "filepath=final_path" in body, (
        "_run_compliance must pass filepath=final_path (the canonical NAS "
        "destination) into check_compliance. Passing the source NAS path "
        "makes the filename_mismatch check compare a dashed source basename "
        "against the canonical expected name — and the fixer can't repair "
        "the source basename, so the violation re-fires forever."
    )
    # And the explicit negative: must NOT pass the outer filepath into the
    # compliance call.
    assert "filepath=filepath," not in body, (
        "regression: _run_compliance is passing the outer source filepath "
        "into check_compliance. Files with non-canonical source basenames "
        "(e.g. 'Show - SnnEnn - Title.mkv') will ERROR with 'compliance "
        "unfixed: filename is …' on every encode attempt until the circuit "
        "breaker parks them as flagged_corrupt."
    )
