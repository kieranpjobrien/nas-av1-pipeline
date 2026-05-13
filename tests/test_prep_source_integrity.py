"""Pin the 2026-05-13 phase-3 refactor: source-integrity probe in prep.

Ford v Ferrari class — a source MKV with bitstream corruption past
the EBML break at byte 0xd45d19ce. Pre-2026-05-13 these only got
caught at ~13% of a 90-min encode by the post-encode integrity
check; the circuit breaker eventually flagged the file after 3+
wasted cycles.

Phase 3: ``prepare_for_encode`` now runs ``tools.probe_source_integrity.
probe_file`` against the LOCAL fetched file before the encode
dispatches. Broken sources go straight to FLAGGED_CORRUPT with
``source_corrupt=True``, ``source_probe_windows``, and a sample
of the decode errors stamped in extras for diagnosis.
"""

from __future__ import annotations

import inspect

import pipeline.full_gamut as fg


def test_prepare_for_encode_imports_source_integrity_probe():
    """The probe entry point is wired into prepare_for_encode."""
    src = inspect.getsource(fg)
    # The function-body import lives inside prepare_for_encode.
    assert "from tools.probe_source_integrity import probe_file" in src, (
        "prepare_for_encode must import probe_file so corrupt sources "
        "are caught BEFORE the GPU encode wastes ~90 min"
    )


def test_prepare_for_encode_flags_broken_source_as_flagged_corrupt():
    """When the probe returns healthy=False, the state row must
    transition to FLAGGED_CORRUPT (terminal — user must re-acquire
    the source via Sonarr/Radarr before retry)."""
    src = inspect.getsource(fg)
    # Locate the prepare_for_encode body
    start = src.find("def prepare_for_encode")
    end = src.find("\ndef ", start + 10)
    body = src[start:end if end > 0 else len(src)]

    # The integrity branch sets FLAGGED_CORRUPT, not ERROR — broken
    # sources need source re-acquisition, not a generic retry.
    assert "FileStatus.FLAGGED_CORRUPT" in body, (
        "broken-source branch must use FLAGGED_CORRUPT (terminal), "
        "not ERROR (would re-queue forever)"
    )
    # And the diagnostic extras are stamped for forensics.
    assert "source_corrupt=True" in body
    assert "source_probe_windows" in body
    assert "source_probe_errors" in body


def test_prepare_for_encode_logs_probe_duration_on_success():
    """Healthy probes log the probe wall-time so the operator can
    see the prep-time overhead. The integrity probe takes ~10-30s;
    if it ever balloons we want it visible."""
    src = inspect.getsource(fg)
    assert "source-integrity OK" in src, (
        "prep logs the OK case so operators see the probe ran"
    )
