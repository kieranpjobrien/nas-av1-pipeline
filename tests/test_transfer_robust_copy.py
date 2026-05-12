"""Coverage for the SMB-transient-error retry helper.

Pre-2026-05-12 the fetch and upload paths called ``shutil.copy2`` with
no retry budget. A single WinError 59 ("an unexpected network error
occurred") sent the file straight to ERROR — Babygirl, Final
Destination Bloodlines, A Bronx Tale, Shaun of the Dead, and Snatch
all hit this on 2026-05-12. The errors resolve on retry in seconds —
they're SMB transients, not real failures.

These tests pin ``robust_copy``:
  * retries on WinError 59 / 64 / 53 / 67 / 121 / 1231
  * does NOT retry on permanent errors (ENOENT, EACCES)
  * succeeds when a retry succeeds
  * exhausts the budget and re-raises after max_retries
  * cleans up partial output between retries (so the next attempt is
    a clean copy, not an append)
"""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest import mock

import pytest

from pipeline import transfer


def _make_winerror(code, msg=""):
    """Build an OSError with a Windows-style winerror code attached."""
    err = OSError(code, msg or f"WinError {code}")
    err.winerror = code  # OSError supports this on Windows
    return err


def test_robust_copy_retries_on_winerror_59(monkeypatch, tmp_path):
    """The Babygirl-class transient. First attempt fails with WinError 59;
    second succeeds. robust_copy must transparently retry."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"payload")
    dst = tmp_path / "dst.mkv"

    call_count = {"n": 0}
    real_copy = shutil.copy2

    def flaky_copy(s, d):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise _make_winerror(59, "an unexpected network error occurred")
        return real_copy(s, d)

    monkeypatch.setattr(transfer.shutil, "copy2", flaky_copy)
    # Avoid sleep wall-clock cost in tests.
    monkeypatch.setattr(transfer.time, "sleep", lambda _: None)

    transfer.robust_copy(str(src), str(dst))
    assert dst.read_bytes() == b"payload"
    assert call_count["n"] == 2


@pytest.mark.parametrize("winerr", [53, 59, 64, 67, 121, 1231])
def test_robust_copy_retries_on_each_transient_class(monkeypatch, tmp_path, winerr):
    """Every WinError in the retryable set must trigger a retry."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"x")
    dst = tmp_path / "dst.mkv"

    call_count = {"n": 0}
    real_copy = shutil.copy2

    def flaky_copy(s, d):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise _make_winerror(winerr)
        return real_copy(s, d)

    monkeypatch.setattr(transfer.shutil, "copy2", flaky_copy)
    monkeypatch.setattr(transfer.time, "sleep", lambda _: None)

    transfer.robust_copy(str(src), str(dst))
    assert call_count["n"] == 2, f"WinError {winerr} must be retried"


def test_robust_copy_does_not_retry_on_permanent_error(monkeypatch, tmp_path):
    """ENOENT / EACCES / disk-full are permanent. No retry budget should
    be wasted — propagate on attempt #1."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"x")
    dst = tmp_path / "dst.mkv"

    call_count = {"n": 0}

    def always_enoent(s, d):
        call_count["n"] += 1
        # ENOENT = 2 (POSIX), NOT in the retryable set
        raise FileNotFoundError(2, "no such file")

    monkeypatch.setattr(transfer.shutil, "copy2", always_enoent)
    monkeypatch.setattr(transfer.time, "sleep", lambda _: None)

    with pytest.raises(FileNotFoundError):
        transfer.robust_copy(str(src), str(dst))
    assert call_count["n"] == 1, "permanent errors must NOT be retried"


def test_robust_copy_exhausts_budget_then_reraises(monkeypatch, tmp_path):
    """When every attempt hits a transient, give up after max_retries
    and re-raise the LAST error so the caller sees the WinError."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"x")
    dst = tmp_path / "dst.mkv"

    call_count = {"n": 0}

    def always_flaky(s, d):
        call_count["n"] += 1
        raise _make_winerror(59)

    monkeypatch.setattr(transfer.shutil, "copy2", always_flaky)
    monkeypatch.setattr(transfer.time, "sleep", lambda _: None)

    with pytest.raises(OSError) as ei:
        transfer.robust_copy(str(src), str(dst), max_retries=3)
    assert getattr(ei.value, "winerror", None) == 59
    assert call_count["n"] == 3


def test_robust_copy_removes_partial_between_retries(monkeypatch, tmp_path):
    """If a transient interrupts mid-copy and leaves a partial file at
    ``dst``, robust_copy must delete it before retrying — otherwise the
    next shutil.copy2 might not overwrite cleanly on every platform."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"full payload")
    dst = tmp_path / "dst.mkv"

    call_count = {"n": 0}
    real_copy = shutil.copy2

    def flaky_then_succeed(s, d):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Leave a partial file behind, then fail with a transient
            Path(d).write_bytes(b"partial")
            raise _make_winerror(64)
        return real_copy(s, d)

    monkeypatch.setattr(transfer.shutil, "copy2", flaky_then_succeed)
    monkeypatch.setattr(transfer.time, "sleep", lambda _: None)

    transfer.robust_copy(str(src), str(dst))
    # Final content matches src — not the partial
    assert dst.read_bytes() == b"full payload"
    assert call_count["n"] == 2


def test_robust_copy_succeeds_first_try_no_retries(monkeypatch, tmp_path):
    """Happy path — when copy2 works first try, no retry, no sleep."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"payload")
    dst = tmp_path / "dst.mkv"

    call_count = {"n": 0}
    sleep_count = {"n": 0}
    real_copy = shutil.copy2

    def good_copy(s, d):
        call_count["n"] += 1
        return real_copy(s, d)

    def slept(_):
        sleep_count["n"] += 1

    monkeypatch.setattr(transfer.shutil, "copy2", good_copy)
    monkeypatch.setattr(transfer.time, "sleep", slept)

    transfer.robust_copy(str(src), str(dst))
    assert call_count["n"] == 1
    assert sleep_count["n"] == 0, "no sleep on happy path"
    assert dst.read_bytes() == b"payload"
