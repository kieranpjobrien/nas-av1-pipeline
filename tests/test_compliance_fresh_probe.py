"""Pin the 2026-05-13 fresh-probe fix in the compliance loop.

8 priority-list files (Heads of State, Wild Robot, Happy Gilmore 2,
Superbad, Babygirl, From Russia, Planes Trains, Venom Carnage) sat
in ERROR with "compliance unfixed: N foreign sub track(s) survived
strip" — the SAME violation re-firing every encode attempt. The
merged-drop refactor was producing correct mkvmerge commands and
the proof-of-work guard was passing (the drop worked). But the
gate's residual check kept saying the violations were still there.

Root cause: ``_run_compliance`` captured ``output_probe`` and
``_output_size`` at line 1082/1254 of full_gamut.py — ONE call up
front, before the fixers ran. The closure reused those cached
values for the post-fix residual run. So even when the fixer
correctly modified dest_path, the residual check saw the pre-fix
probe and found the same violations.

Post-fix: ``_run_compliance`` re-probes dest_path every invocation.
Each compliance run sees the actual current state of the file on
disk.
"""

from __future__ import annotations

import inspect


def test_run_compliance_re_probes_every_invocation():
    """Static check: the _run_compliance closure must call _probe_full
    inside its body, not reuse a captured value. If someone reverts
    to the cached-probe pattern, this fails."""
    import pipeline.full_gamut as fg
    src = inspect.getsource(fg)
    # Locate the _run_compliance definition
    needle = "def _run_compliance() -> list:"
    idx = src.find(needle)
    assert idx >= 0, "_run_compliance closure not found in full_gamut.py"
    # Grab the function body (next 30 lines should cover it)
    body = src[idx:idx + 1500]
    assert "fresh_probe = _probe_full(dest_path)" in body, (
        "_run_compliance must re-probe dest_path every call. If it captures "
        "output_probe from outside the closure, post-fix residual checks "
        "will see stale data and reject all fixes."
    )
    assert "fresh_size = os.path.getsize(dest_path)" in body, (
        "_run_compliance must also re-stat dest_path so output_size_bytes "
        "reflects the post-fix size"
    )
    # And the obvious negative: it must NOT pass the outer cached name
    # output_probe directly. (The closure's local is `fresh_probe`.)
    # Use word-boundary-ish check.
    assert "output_probe=output_probe" not in body, (
        "stale-cache pattern detected: output_probe should be the local "
        "fresh_probe inside the closure"
    )
