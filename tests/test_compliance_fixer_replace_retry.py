"""Pin the 2026-05-13 atomic-replace retry in _mkvmerge_drop_streams.

Observed live (2026-05-13 18:59:32) — the strip operation against
the LOCAL fetched file hit:

  PermissionError: [WinError 5] Access is denied:
    'F:\\...\\From Russia with Love.mkv.compliance_tmp.mkv' ->
    'F:\\...\\From Russia with Love.mkv'

Windows briefly locks the destination file during ``os.replace`` —
typically the antivirus scanning the freshly-written tmp file, or
another worker holding a read handle. Resolves in 1-3 seconds. Same
transient class as the SMB WinError 59 case ``robust_copy`` already
handles for fetch/upload.

Fix: ``_mkvmerge_drop_streams`` wraps the final ``os.replace`` in a
short backoff retry loop. Up to 4 attempts (0.5s, 1s, 2s, 4s) before
giving up + cleaning the tmp file. Non-PermissionError OSErrors
(ENOENT, disk full, etc.) propagate immediately without wasting
retry budget.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from unittest import mock

import pytest

import pipeline.compliance_fixers as cf


def _setup_minimal_mocks(monkeypatch, tmp_path):
    """Mock _probe_full, subprocess.run, file metadata so the fixer
    reaches the os.replace call without needing real media."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"x" * 10000)
    tmp_out = src.with_suffix(src.suffix + ".compliance_tmp.mkv")
    tmp_out.write_bytes(b"y" * 10000)

    src_probe = {
        "video": {"codec": "av1"},
        "audio": [{}, {}],
        "subs": [{}, {}, {}],
    }
    # Output after a sub drop of [0,1] → 1 sub remains
    out_probe = {**src_probe, "subs": [{}]}

    call_count = {"n": 0}
    def fake_probe(path):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return src_probe
        return out_probe
    monkeypatch.setattr("pipeline.full_gamut._probe_full", fake_probe)

    class FakeCompleted:
        returncode = 0
        stderr = b""
    monkeypatch.setattr(cf.subprocess, "run",
                        lambda cmd, **kw: FakeCompleted())
    return str(src), str(tmp_out)


def test_replace_retries_on_permission_error(monkeypatch, tmp_path):
    """First os.replace raises WinError 5; second succeeds. Must
    transparently retry — the file is briefly locked, not broken."""
    src, _ = _setup_minimal_mocks(monkeypatch, tmp_path)

    call_count = {"n": 0}
    real_replace = os.replace
    def flaky_replace(a, b):
        call_count["n"] += 1
        if call_count["n"] == 1:
            err = PermissionError(5, "Access is denied")
            err.winerror = 5
            raise err
        return real_replace(a, b)
    monkeypatch.setattr(cf.os, "replace", flaky_replace)
    monkeypatch.setattr(cf.time, "sleep", lambda _: None)  # don't slow tests

    ok = cf._mkvmerge_drop_streams(src, drop_sub_indices=[0, 1])
    assert ok is True
    assert call_count["n"] == 2, "expected one retry after WinError 5"


def test_replace_exhausts_retries_then_fails(monkeypatch, tmp_path):
    """If EVERY attempt hits PermissionError, give up cleanly and
    return False after the retry budget. Don't loop forever.

    Post-2026-05-13 the retry budget went from 4 attempts (7.5s
    patience) to 8 attempts (~91s patience) after Into the Woods
    exhausted the shorter budget on a real run."""
    src, _ = _setup_minimal_mocks(monkeypatch, tmp_path)
    call_count = {"n": 0}
    def always_locked(a, b):
        call_count["n"] += 1
        err = PermissionError(5, "Access is denied")
        err.winerror = 5
        raise err
    monkeypatch.setattr(cf.os, "replace", always_locked)
    monkeypatch.setattr(cf.time, "sleep", lambda _: None)

    ok = cf._mkvmerge_drop_streams(src, drop_sub_indices=[0, 1])
    assert ok is False
    assert call_count["n"] == 8, (
        f"expected 8 attempts (extended budget for slow antivirus / "
        f"file-cache flush), got {call_count['n']}"
    )


def test_replace_does_not_retry_on_enoent(monkeypatch, tmp_path):
    """ENOENT / disk-full / other permanent OSErrors propagate
    immediately. Retry budget only applies to PermissionError —
    everything else fails the fixer cleanly on the first try."""
    src, _ = _setup_minimal_mocks(monkeypatch, tmp_path)
    call_count = {"n": 0}
    def enoent(a, b):
        call_count["n"] += 1
        raise FileNotFoundError(2, "no such file")
    monkeypatch.setattr(cf.os, "replace", enoent)
    sleep_count = {"n": 0}
    monkeypatch.setattr(cf.time, "sleep", lambda _: sleep_count.__setitem__("n", sleep_count["n"] + 1))

    ok = cf._mkvmerge_drop_streams(src, drop_sub_indices=[0, 1])
    assert ok is False
    assert call_count["n"] == 1, "ENOENT must propagate on first attempt"
    assert sleep_count["n"] == 0, "no backoff sleep on permanent error"


def test_replace_happy_path_no_retry(monkeypatch, tmp_path):
    """Normal case — first os.replace succeeds, no retry or sleep."""
    src, _ = _setup_minimal_mocks(monkeypatch, tmp_path)
    replaces = {"n": 0}
    sleeps = {"n": 0}
    real_replace = os.replace
    def counting_replace(a, b):
        replaces["n"] += 1
        return real_replace(a, b)
    monkeypatch.setattr(cf.os, "replace", counting_replace)
    monkeypatch.setattr(cf.time, "sleep", lambda _: sleeps.__setitem__("n", sleeps["n"] + 1))

    ok = cf._mkvmerge_drop_streams(src, drop_sub_indices=[0, 1])
    assert ok is True
    assert replaces["n"] == 1, "happy path is one replace, no retry"
    assert sleeps["n"] == 0, "no sleep on happy path"
