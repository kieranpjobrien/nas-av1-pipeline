"""Pin the 2026-05-24 retry-on-hits hardening in
``tools.probe_source_integrity._decode_window``.

Pre-fix the probe trusted a single ffmpeg decode pass — if stderr
happened to contain a HARD_ERROR_PATTERN match on the first attempt,
the file was flagged. A transient SMB hiccup mid-read could cause
a one-shot hard-error line that didn't reproduce; a burst of 6
healthy films got falsely flagged_corrupt within a 14-minute window
on 2026-05-23 morning because of exactly this.

Post-fix the helper retries once on hard-error hits (just like it
already retried on subprocess timeout). Real bitstream defects are
deterministic and still flag; transient blips wash out.
"""

from __future__ import annotations

import sys
import subprocess
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.probe_source_integrity import _decode_window


def _result(stderr: str, returncode: int = 0):
    """Build a subprocess.CompletedProcess stand-in."""
    return subprocess.CompletedProcess(args=[], returncode=returncode,
                                       stdout="", stderr=stderr)


HARD_ERROR_LINE = "[hevc @ 0x0] Could not find ref with POC 151"
CLEAN_STDERR = "Application provided invalid, non monotonically increasing dts"


def test_hard_error_retried_then_clean_passes_as_healthy():
    """First attempt hits HARD_ERROR_PATTERN; second attempt is clean.
    Must return ok=True (transient SMB blip — not real corruption)."""
    side_effects = [_result(HARD_ERROR_LINE), _result(CLEAN_STDERR)]
    with patch("tools.probe_source_integrity.subprocess.run", side_effect=side_effects):
        ok, msgs = _decode_window("/fake/path.mkv", 0, 60, timeout=180, max_retries=1)
    assert ok is True, (
        f"transient hard-error hit must wash out on retry, got ok={ok!r} msgs={msgs!r}"
    )


def test_hard_error_persists_across_retries_flags_as_corrupt():
    """Both attempts hit hard-error patterns. Real bitstream corruption
    is deterministic — must flag (ok=False) with the hit signature."""
    side_effects = [_result(HARD_ERROR_LINE), _result(HARD_ERROR_LINE)]
    with patch("tools.probe_source_integrity.subprocess.run", side_effect=side_effects):
        ok, msgs = _decode_window("/fake/path.mkv", 0, 60, timeout=180, max_retries=1)
    assert ok is False, "persistent hard-error must flag as corrupt"
    assert any("Could not find ref with POC 151" in m for m in msgs), (
        f"hit signature must surface in msgs, got {msgs!r}"
    )


def test_clean_first_attempt_short_circuits_no_retry():
    """First attempt is clean — must NOT retry (saves ~60s per window
    on the happy path which dominates)."""
    calls = [_result(CLEAN_STDERR)]
    with patch("tools.probe_source_integrity.subprocess.run", side_effect=calls) as mock_run:
        ok, msgs = _decode_window("/fake/path.mkv", 0, 60, timeout=180, max_retries=1)
    assert ok is True
    assert mock_run.call_count == 1, (
        f"clean first attempt should not retry; got {mock_run.call_count} call(s)"
    )


def test_timeout_then_clean_passes():
    """Existing 2026-05-12 Up-class behaviour — first attempt times out,
    retry succeeds. Must still pass (regression guard)."""
    side_effects = [
        subprocess.TimeoutExpired(cmd=[], timeout=180),
        _result(CLEAN_STDERR),
    ]
    with patch("tools.probe_source_integrity.subprocess.run", side_effect=side_effects):
        ok, msgs = _decode_window("/fake/path.mkv", 0, 60, timeout=180, max_retries=1)
    assert ok is True


def test_timeout_then_hard_error_flags():
    """Pathological case: first attempt times out, retry decodes but
    hits hard-error. The retry budget was already used by the timeout
    — no further retries available — flag."""
    side_effects = [
        subprocess.TimeoutExpired(cmd=[], timeout=180),
        _result(HARD_ERROR_LINE),
    ]
    with patch("tools.probe_source_integrity.subprocess.run", side_effect=side_effects):
        ok, msgs = _decode_window("/fake/path.mkv", 0, 60, timeout=180, max_retries=1)
    assert ok is False
