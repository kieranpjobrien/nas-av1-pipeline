"""Thread-safe circuit breaker for remote-operation loops.

Used by orchestrator/worker loops that call into fragile remote systems
(SSH + docker exec on the NAS, mkvmerge, mkvpropedit, ffprobe over SMB)
to pause when consecutive failures cross a threshold — preventing the
overnight 2026-04-23 pattern where each failure spawned another SSH +
docker exec against a dying NAS.

State machine
-------------
The breaker moves between three states:

    CLOSED  -- normal operation. Failures are counted. Once the running
               count of consecutive failures hits ``threshold``, the
               breaker transitions to OPEN.

    OPEN    -- all calls are refused (``is_open()`` returns True). After
               ``cooldown_secs`` have elapsed since entry, the breaker
               transitions to HALF_OPEN on the next state observation.

    HALF_OPEN -- a single trial is permitted. The very next ``record()``
                 determines the next state:
                   * success -> CLOSED, consecutive_failures reset to 0
                   * failure -> OPEN, cooldown starts again

Transitions are logged: WARNING on CLOSED -> OPEN, INFO on OPEN ->
HALF_OPEN and HALF_OPEN -> CLOSED (recovery). All public methods are
safe to call from multiple threads concurrently.
"""

from __future__ import annotations

import logging
import threading
import time

_LOG = logging.getLogger(__name__)

_STATE_CLOSED = "closed"
_STATE_OPEN = "open"
_STATE_HALF_OPEN = "half_open"


class CircuitBreakerOpen(Exception):
    """Raised when a call is blocked by an open breaker."""


class CircuitBreaker:
    """Thread-safe circuit breaker with threshold-open + cooldown-halfopen semantics.

    Attributes:
        threshold: Consecutive failures required to trip the breaker from
            CLOSED to OPEN.
        cooldown_secs: Seconds the breaker stays OPEN before moving to
            HALF_OPEN on the next state observation.
        name: Human-readable name used in log messages for observability.
    """

    def __init__(
        self,
        threshold: int = 5,
        cooldown_secs: int = 300,
        name: str = "breaker",
    ) -> None:
        """Initialise a closed breaker.

        Args:
            threshold: Consecutive failures to trip. Must be >= 1.
            cooldown_secs: Seconds before an OPEN breaker becomes HALF_OPEN.
            name: Identifier used in log messages.
        """
        if threshold < 1:
            raise ValueError("threshold must be >= 1")
        if cooldown_secs < 0:
            raise ValueError("cooldown_secs must be >= 0")
        self._threshold = threshold
        self._cooldown_secs = cooldown_secs
        self._name = name

        self._lock = threading.Lock()
        self._state = _STATE_CLOSED
        self._consecutive_failures = 0
        # Wall-clock time (time.monotonic) at which the breaker entered OPEN.
        # 0.0 while CLOSED or HALF_OPEN.
        self._opened_at = 0.0

    # ------------------------------------------------------------------
    # Internal helpers (caller must hold self._lock)
    # ------------------------------------------------------------------

    def _maybe_enter_half_open_locked(self) -> None:
        """If OPEN and the cooldown has elapsed, transition to HALF_OPEN."""
        if self._state != _STATE_OPEN:
            return
        if time.monotonic() - self._opened_at < self._cooldown_secs:
            return
        self._state = _STATE_HALF_OPEN
        _LOG.info("circuit breaker %s: OPEN -> HALF_OPEN (cooldown elapsed, allowing trial)", self._name)

    def _trip_to_open_locked(self) -> None:
        """Transition to OPEN and stamp the cooldown start."""
        self._state = _STATE_OPEN
        self._opened_at = time.monotonic()
        _LOG.warning(
            "circuit breaker %s: -> OPEN (consecutive_failures=%d threshold=%d cooldown=%ds)",
            self._name,
            self._consecutive_failures,
            self._threshold,
            self._cooldown_secs,
        )

    def _close_locked(self) -> None:
        """Transition to CLOSED and reset the failure counter."""
        was = self._state
        self._state = _STATE_CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        if was != _STATE_CLOSED:
            _LOG.info("circuit breaker %s: %s -> CLOSED (recovery)", self._name, was.upper())

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record(self, success: bool) -> None:
        """Record a single operation outcome.

        Rules:
          * CLOSED + failure: increment counter; trip to OPEN at threshold.
          * CLOSED + success: reset counter.
          * HALF_OPEN + success: close the breaker (recovery).
          * HALF_OPEN + failure: re-open the breaker; cooldown restarts.
          * OPEN + anything: if cooldown has elapsed we drop into
            HALF_OPEN first, then apply the outcome per above. Otherwise
            the outcome only updates the counter (for observability).

        Args:
            success: True if the operation succeeded, False otherwise.
        """
        with self._lock:
            # Give OPEN a chance to age into HALF_OPEN before applying the outcome.
            self._maybe_enter_half_open_locked()

            if success:
                if self._state == _STATE_HALF_OPEN:
                    self._close_locked()
                elif self._state == _STATE_CLOSED:
                    self._consecutive_failures = 0
                # If still OPEN (cooldown not elapsed), ignore a success signal
                # — the breaker should not close mid-cooldown without a trial.
                return

            # Failure path.
            self._consecutive_failures += 1

            if self._state == _STATE_HALF_OPEN:
                # Trial failed; slam it shut again.
                self._trip_to_open_locked()
                return

            if self._state == _STATE_CLOSED and self._consecutive_failures >= self._threshold:
                self._trip_to_open_locked()

    def is_open(self) -> bool:
        """True if currently open (will refuse calls).

        Observing ``is_open()`` is what ages an OPEN breaker into
        HALF_OPEN once the cooldown has expired, so callers that poll
        this method also drive state progression.
        """
        with self._lock:
            self._maybe_enter_half_open_locked()
            return self._state == _STATE_OPEN

    def wait_if_open(
        self,
        poll_secs: float = 2.0,
        shutdown: threading.Event | None = None,
    ) -> None:
        """Block until the breaker is no longer OPEN.

        Polls ``is_open()`` on an interval. If a ``shutdown`` event is
        supplied and fires while the breaker is still OPEN, raises
        ``CircuitBreakerOpen`` rather than continuing to spin.

        Args:
            poll_secs: Interval between state checks.
            shutdown: Optional event signalling the worker should stop.

        Raises:
            CircuitBreakerOpen: If ``shutdown`` fires while the breaker
                is OPEN.
        """
        if poll_secs <= 0:
            raise ValueError("poll_secs must be > 0")
        while self.is_open():
            if shutdown is not None and shutdown.is_set():
                raise CircuitBreakerOpen(
                    f"circuit breaker {self._name} is OPEN and shutdown requested"
                )
            if shutdown is not None:
                # Event.wait returns True immediately if set; False on timeout.
                if shutdown.wait(timeout=poll_secs):
                    raise CircuitBreakerOpen(
                        f"circuit breaker {self._name} is OPEN and shutdown requested"
                    )
            else:
                time.sleep(poll_secs)

    def state(self) -> str:
        """Return one of 'closed' / 'open' / 'half_open' for observability."""
        with self._lock:
            self._maybe_enter_half_open_locked()
            return self._state

    def consecutive_failures(self) -> int:
        """Return the current run of back-to-back failures."""
        with self._lock:
            return self._consecutive_failures
