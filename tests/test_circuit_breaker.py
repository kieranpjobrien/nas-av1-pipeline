"""Tests for pipeline/circuit_breaker.py — threshold-open, cooldown-halfopen state machine."""

from __future__ import annotations

import logging
import threading

import pytest

from pipeline import circuit_breaker as cb_module
from pipeline.circuit_breaker import CircuitBreaker, CircuitBreakerOpen


class TestBasicStateMachine:
    """Closed -> open -> half_open -> closed transitions."""

    def test_closes_after_consecutive_failures(self) -> None:
        """record(False) threshold times flips the breaker to OPEN."""
        cb = CircuitBreaker(threshold=5, cooldown_secs=60, name="t1")
        for _ in range(5):
            cb.record(False)
        assert cb.is_open() is True
        assert cb.state() == "open"
        assert cb.consecutive_failures() == 5

    def test_opens_at_threshold_not_before(self) -> None:
        """Breaker with threshold=3 opens exactly on the third failure, not the second."""
        # threshold=3, two failures -> still closed
        cb = CircuitBreaker(threshold=3, cooldown_secs=60, name="t2a")
        cb.record(False)
        cb.record(False)
        assert cb.is_open() is False
        assert cb.state() == "closed"

        # threshold=3, three failures -> open
        cb2 = CircuitBreaker(threshold=3, cooldown_secs=60, name="t2b")
        cb2.record(False)
        cb2.record(False)
        cb2.record(False)
        assert cb2.is_open() is True

    def test_recovery_on_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """After OPEN, once cooldown elapses and a success is recorded, breaker CLOSES."""
        fake_now = [1_000.0]

        def fake_monotonic() -> float:
            return fake_now[0]

        monkeypatch.setattr(cb_module.time, "monotonic", fake_monotonic)

        cb = CircuitBreaker(threshold=2, cooldown_secs=30, name="t3")
        cb.record(False)
        cb.record(False)
        assert cb.state() == "open"

        # Advance past cooldown
        fake_now[0] += 31.0
        # is_open() observation promotes to HALF_OPEN
        assert cb.is_open() is False
        assert cb.state() == "half_open"

        cb.record(True)
        assert cb.state() == "closed"
        assert cb.consecutive_failures() == 0

    def test_half_open_failure_re_opens(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A failure while HALF_OPEN slams the breaker back to OPEN with a fresh cooldown."""
        fake_now = [2_000.0]

        def fake_monotonic() -> float:
            return fake_now[0]

        monkeypatch.setattr(cb_module.time, "monotonic", fake_monotonic)

        cb = CircuitBreaker(threshold=2, cooldown_secs=30, name="t4")
        cb.record(False)
        cb.record(False)
        assert cb.state() == "open"

        # Cooldown elapses -> half_open on observation
        fake_now[0] += 31.0
        assert cb.is_open() is False
        assert cb.state() == "half_open"

        # Trial failure -> back to open
        cb.record(False)
        assert cb.is_open() is True
        assert cb.state() == "open"

        # And the new cooldown must restart from now
        fake_now[0] += 10.0
        assert cb.is_open() is True  # still too soon
        fake_now[0] += 25.0
        assert cb.is_open() is False  # now elapsed


class TestWaitIfOpen:
    """wait_if_open blocks while OPEN and honours shutdown events."""

    def test_wait_if_open_honours_shutdown(self) -> None:
        """With the breaker open and shutdown set, wait_if_open raises."""
        cb = CircuitBreaker(threshold=1, cooldown_secs=600, name="t5")
        cb.record(False)
        assert cb.is_open()

        shutdown = threading.Event()
        shutdown.set()
        with pytest.raises(CircuitBreakerOpen):
            cb.wait_if_open(poll_secs=0.1, shutdown=shutdown)

    def test_wait_if_open_returns_when_closed(self) -> None:
        """A closed breaker's wait_if_open returns immediately."""
        cb = CircuitBreaker(threshold=5, cooldown_secs=60, name="t5b")
        # Never tripped, so wait returns without blocking.
        cb.wait_if_open(poll_secs=0.01)


class TestThreadSafety:
    """Concurrent record() calls must not lose updates."""

    def test_thread_safe_concurrent_record(self) -> None:
        """20 threads x 100 failures each -> consecutive_failures is consistent."""
        # Huge threshold so the breaker stays closed and we can simply
        # count the failures across all threads.
        cb = CircuitBreaker(threshold=10_000_000, cooldown_secs=60, name="t6")

        threads_n = 20
        per_thread = 100

        def hammer() -> None:
            for _ in range(per_thread):
                cb.record(False)

        threads = [threading.Thread(target=hammer) for _ in range(threads_n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert cb.consecutive_failures() == threads_n * per_thread


class TestLogging:
    """Observability: OPEN/HALF_OPEN/CLOSED transitions must be logged."""

    def test_state_transitions_logged(
        self,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Tripping, recovering to half_open, and closing each emit a log line."""
        fake_now = [3_000.0]

        def fake_monotonic() -> float:
            return fake_now[0]

        monkeypatch.setattr(cb_module.time, "monotonic", fake_monotonic)

        caplog.set_level(logging.INFO, logger="pipeline.circuit_breaker")
        cb = CircuitBreaker(threshold=2, cooldown_secs=30, name="logtest")

        # Trip -> OPEN (should WARN)
        cb.record(False)
        cb.record(False)

        # Advance cooldown and observe -> HALF_OPEN (should INFO)
        fake_now[0] += 31.0
        _ = cb.is_open()

        # Success in HALF_OPEN -> CLOSED (should INFO)
        cb.record(True)

        messages = [r.getMessage() for r in caplog.records]
        joined = " | ".join(messages)
        assert "-> OPEN" in joined
        assert "HALF_OPEN" in joined
        assert "CLOSED" in joined

        # And the OPEN transition is at WARNING or higher.
        open_records = [r for r in caplog.records if "-> OPEN" in r.getMessage()]
        assert any(r.levelno >= logging.WARNING for r in open_records)


class TestEdgeCases:
    """Guard rails around constructor and public API."""

    def test_threshold_must_be_positive(self) -> None:
        with pytest.raises(ValueError):
            CircuitBreaker(threshold=0)

    def test_cooldown_must_be_non_negative(self) -> None:
        with pytest.raises(ValueError):
            CircuitBreaker(cooldown_secs=-1)

    def test_success_resets_consecutive_failures_when_closed(self) -> None:
        """A success while CLOSED zeroes the failure counter."""
        cb = CircuitBreaker(threshold=10, cooldown_secs=60, name="reset")
        for _ in range(5):
            cb.record(False)
        assert cb.consecutive_failures() == 5
        cb.record(True)
        assert cb.consecutive_failures() == 0
        assert cb.state() == "closed"

    def test_wait_if_open_poll_secs_validation(self) -> None:
        cb = CircuitBreaker(threshold=1, cooldown_secs=1, name="poll")
        with pytest.raises(ValueError):
            cb.wait_if_open(poll_secs=0)


def test_smoke_quick_open_close_cycle() -> None:
    """A tiny realistic cycle: open after 3 failures, then recover on success.

    cooldown_secs=0 means the next observation (``is_open`` / ``state``) will
    flip OPEN -> HALF_OPEN immediately; a success then closes the breaker.
    """
    cb = CircuitBreaker(threshold=3, cooldown_secs=0, name="smoke")
    cb.record(False)
    cb.record(False)
    cb.record(False)
    # cooldown=0 means the very next observation promotes to half_open
    assert cb.is_open() is False
    assert cb.state() == "half_open"
    cb.record(True)
    assert cb.state() == "closed"
