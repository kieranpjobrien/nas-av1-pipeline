"""Coverage for the source-corruption pre-scan tool.

The tool exists because Ford v Ferrari (2026-05-03 .. 2026-05-12) burned
10 encode cycles on a source MKV with a broken EBML container at byte
0xd45d19ce. The circuit breaker eventually caught it, but each cycle
cost ~90 min of GPU time. The pre-scan probes each source at start /
middle / end via ffmpeg's null muxer; if any window emits a hard
decode-error signature ("Invalid data found", "Could not find ref with
POC", "Error submitting packet to decoder", etc.), the file is flagged
BEFORE we waste the GPU.

These tests cover the signature matcher, the decision logic, and the
state-DB write path. Subprocess calls are mocked — the empirical
validation on real Ford v Ferrari / Titanic is in the session
transcript, not a unit test (would require a real corrupt MKV fixture).
"""

from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path

import pytest

from tools import probe_source_integrity as psi


class FakeRun:
    """Substitute for subprocess.run that returns scripted outputs based
    on the command, in call order."""

    def __init__(self, scripts: list[tuple[str, int, str]]):
        """scripts is a list of (matcher_substring, returncode, stderr)."""
        self.scripts = scripts
        self.calls: list[list[str]] = []

    def __call__(self, cmd, capture_output=False, text=False, timeout=None, encoding=None, errors=None):
        self.calls.append(list(cmd))
        cmd_str = " ".join(cmd)
        for match, rc, err in self.scripts:
            if match in cmd_str:
                return subprocess.CompletedProcess(cmd, rc, "", err)
        return subprocess.CompletedProcess(cmd, 0, "", "")


def test_signature_matcher_catches_ford_v_ferrari_class():
    """The exact stderr lines from Ford v Ferrari decode must trigger."""
    examples = [
        "[matroska,webm @ 0x123] 0x00 at pos 3562871246 invalid as first byte of an EBML number",
        "[hevc @ 0xabc] Could not find ref with POC 7",
        "[hevc @ 0xabc] Error constructing the frame RPS.",
        "[aist#0:1/truehd] Error submitting packet to decoder: Invalid data found when processing input",
        "Non-existing PPS 0 referenced",
        "missing picture in access unit with size 12345",
    ]
    for line in examples:
        assert any(p.search(line) for p in psi.HARD_ERROR_PATTERNS), (
            f"signature regex missed a real-world error line: {line!r}"
        )


def test_signature_matcher_does_not_false_positive_on_soft_warnings():
    """Soft seek-related warnings on healthy files must NOT trip."""
    benign = [
        "[mp3 @ 0x1] Header missing",  # AAC headers from PMT, normal
        "Application provided invalid, non monotonically increasing dts to muxer in stream 0",
        "[h264 @ 0x1] no frame!",   # normal at -ss seek to non-keyframe
    ]
    for line in benign:
        assert not any(p.search(line) for p in psi.HARD_ERROR_PATTERNS), (
            f"signature regex false-positive on benign line: {line!r}"
        )


def test_probe_file_reports_broken_for_decode_errors(monkeypatch, tmp_path):
    """A file that emits a hard-error signature in ANY window must report
    healthy=False with the failing window noted."""
    fp = tmp_path / "fake.mkv"
    fp.write_bytes(b"x" * 1000)  # exists check only
    runner = FakeRun([
        ("format=duration",
         0, ""),  # duration probe — empty stderr, returncode 0
        # First window: stderr contains hard error
        ("-ss 0.0",
         0, "[hevc] Could not find ref with POC 12\n"),
        # Subsequent windows: clean
        ("-ss",
         0, ""),
    ])
    # Override duration return
    monkeypatch.setattr(psi.subprocess, "run", runner)
    monkeypatch.setattr(psi, "_probe_duration", lambda *a, **kw: 3600.0)

    result = psi.probe_file(str(fp))
    assert result.healthy is False
    assert "start" in result.windows_failed
    assert any("POC" in e for e in result.sample_errors)


def test_probe_file_reports_clean_when_all_windows_decode(monkeypatch, tmp_path):
    """All three windows produce clean stderr → healthy=True."""
    fp = tmp_path / "clean.mkv"
    fp.write_bytes(b"x" * 1000)
    monkeypatch.setattr(psi.subprocess, "run",
                        FakeRun([("-i ", 0, "")]))
    monkeypatch.setattr(psi, "_probe_duration", lambda *a, **kw: 3600.0)

    result = psi.probe_file(str(fp))
    assert result.healthy is True
    assert result.windows_failed == []


def test_probe_file_short_file_uses_full_decode(monkeypatch, tmp_path):
    """Files shorter than 3 minutes get a single full-duration decode
    rather than 3 windows of 60s."""
    fp = tmp_path / "short.mkv"
    fp.write_bytes(b"x" * 1000)
    runner = FakeRun([("-i ", 0, "")])
    monkeypatch.setattr(psi.subprocess, "run", runner)
    monkeypatch.setattr(psi, "_probe_duration", lambda *a, **kw: 90.0)  # 1.5 min

    result = psi.probe_file(str(fp))
    assert result.healthy is True
    # Only ffmpeg decode calls (skip the duration call which we mocked separately)
    decode_calls = [c for c in runner.calls if c[0] == psi.FFMPEG]
    assert len(decode_calls) == 1, (
        f"expected one decode call for <3min file, got {len(decode_calls)}"
    )


def test_probe_file_missing_returns_fatal(tmp_path):
    """File that doesn't exist → healthy=False with fatal=missing."""
    result = psi.probe_file(str(tmp_path / "does_not_exist.mkv"))
    assert result.healthy is False
    assert "missing" in (result.fatal or "")


def test_probe_file_zero_duration_returns_fatal(monkeypatch, tmp_path):
    """Duration probe returning 0 means corrupt container — fatal."""
    fp = tmp_path / "fake.mkv"
    fp.write_bytes(b"x" * 100)
    monkeypatch.setattr(psi, "_probe_duration", lambda *a, **kw: 0.0)
    result = psi.probe_file(str(fp))
    assert result.healthy is False
    assert "duration" in (result.fatal or "")


# --------------------------------------------------------------------------
# Streaming output — bug we hit live in this session
# --------------------------------------------------------------------------


def test_main_streams_progress_to_stderr_in_json_mode(monkeypatch, tmp_path, capsys):
    """In --json mode the tool MUST still write per-file progress to
    stderr as each file finishes. Pre-2026-05-12 progress was suppressed
    whenever --json was set, so a long-running probe looked dead. The
    test pins:
      * stderr emits one line per file with the basename
      * stdout emits one JSONL record per file as it completes (not
        a single dump at the end)
      * a final summary JSONL line carries 'summary': true
    """
    import json as _json

    # Two fake targets, both clean
    targets = [str(tmp_path / "a.mkv"), str(tmp_path / "b.mkv")]
    for fp in targets:
        Path(fp).write_bytes(b"x")
    monkeypatch.setattr(psi, "_files_from_state", lambda *a, **kw: targets)
    monkeypatch.setattr(psi, "_probe_duration", lambda *a, **kw: 3600.0)
    monkeypatch.setattr(psi.subprocess, "run",
                        FakeRun([("-i ", 0, "")]))  # clean decode

    rc = psi.main(["--from-state", "--json"])
    captured = capsys.readouterr()

    # Progress on stderr — one line per file with the basename
    assert "[1/2] a.mkv" in captured.err
    assert "[2/2] b.mkv" in captured.err

    # JSONL on stdout — one record per file + a summary line
    lines = [l for l in captured.out.splitlines() if l.strip()]
    assert len(lines) == 3, (
        f"expected 2 result + 1 summary JSONL line, got {len(lines)}: {lines}"
    )
    rec_a = _json.loads(lines[0])
    rec_b = _json.loads(lines[1])
    summary = _json.loads(lines[2])
    assert rec_a["filepath"] == targets[0]
    assert rec_b["filepath"] == targets[1]
    assert summary.get("summary") is True
    assert summary.get("probed") == 2
    assert summary.get("broken") == 0
    assert rc == 0


def test_decode_window_retries_on_timeout(monkeypatch):
    """A flaky-SMB timeout must NOT immediately fail — retry once. If the
    second attempt succeeds, return (ok, []). This is the Up (2009) class:
    the first attempt overran the 180s wall clock on a network blip, but
    the file is fine on retry."""
    import subprocess as _sp
    call_count = {"n": 0}

    def fake_run(cmd, capture_output=False, text=False, timeout=None, encoding=None, errors=None):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise _sp.TimeoutExpired(cmd, timeout or 180)
        # Second call: clean decode
        return _sp.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(psi.subprocess, "run", fake_run)
    ok, errs = psi._decode_window("/fake.mkv", 0.0, 60)
    assert ok is True
    assert errs == []
    assert call_count["n"] == 2, "expected exactly one retry after timeout"


def test_decode_window_persistent_timeout_fails(monkeypatch):
    """If BOTH attempts time out, the window is reported as failed.
    Two-attempts-then-fail is the safety floor — we still flag persistent
    flakiness as corruption rather than spinning forever."""
    import subprocess as _sp
    call_count = {"n": 0}

    def always_timeout(cmd, **kw):
        call_count["n"] += 1
        raise _sp.TimeoutExpired(cmd, kw.get("timeout") or 180)

    monkeypatch.setattr(psi.subprocess, "run", always_timeout)
    ok, errs = psi._decode_window("/fake.mkv", 0.0, 60)
    assert ok is False
    # 2026-05-24: post-retry-on-hits hardening, last_msgs is the
    # per-attempt buffer (overwritten, not accumulated). Final message
    # describes the LAST attempt's outcome — that's what matters when
    # both attempts agree.
    assert len(errs) >= 1
    assert any("timeout" in e for e in errs)
    assert call_count["n"] == 2


def test_decode_window_hard_error_retried_then_failed(monkeypatch):
    """2026-05-24: BOTH attempts hit hard-error stderr → flag corrupt.
    Real bitstream corruption is deterministic and reproduces across
    retries; transient SMB blips wash out (see
    test_probe_retry_on_hits.test_hard_error_retried_then_clean_passes).

    Pre-hardening this test asserted call_count == 1 ('no retry on
    hits'). That's been replaced by retry-on-hits which catches the
    burst-of-false-positives pattern from 2026-05-23 morning."""
    import subprocess as _sp
    call_count = {"n": 0}

    def both_calls_return_error(cmd, **kw):
        call_count["n"] += 1
        return _sp.CompletedProcess(cmd, 0, "",
                                    "[hevc] Could not find ref with POC 7\n")

    monkeypatch.setattr(psi.subprocess, "run", both_calls_return_error)
    ok, errs = psi._decode_window("/fake.mkv", 0.0, 60)
    assert ok is False
    assert any("POC" in e for e in errs)
    assert call_count["n"] == 2, "must retry once on hard decode errors before flagging"


def test_main_streams_progress_to_stderr_in_text_mode(monkeypatch, tmp_path, capsys):
    """Without --json, stderr still gets per-file progress."""
    targets = [str(tmp_path / "x.mkv")]
    Path(targets[0]).write_bytes(b"x")
    monkeypatch.setattr(psi, "_files_from_state", lambda *a, **kw: targets)
    monkeypatch.setattr(psi, "_probe_duration", lambda *a, **kw: 3600.0)
    monkeypatch.setattr(psi.subprocess, "run", FakeRun([("-i ", 0, "")]))

    psi.main(["--from-state"])
    captured = capsys.readouterr()
    assert "[1/1] x.mkv" in captured.err
    # Final summary on stderr too
    assert "0/1 sources are corrupt" in captured.err
    # stdout should NOT have JSONL records when --json not set
    assert captured.out.strip() == ""
