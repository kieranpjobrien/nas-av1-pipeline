"""A doomed job must be detected early, not once it has wasted the bandwidth.

2026-08-03: the queue was full of jobs sitting at 16-23% for days while the SAB
log streamed "Article ... unavailable on all servers, discarding". The absolute
mbmissing threshold missed them, because mbmissing only counts articles SAB has
already TRIED - a job 16% in has only tested 16% of the payload. Rick and Morty
S03E06 read as 47 MB missing (under any sane absolute threshold) while actually
having lost 13% of everything fetched, i.e. heading for ~260 MB by completion.
"""

from tools.kill_doomed import DEFAULT_LOSS_PCT, DEFAULT_THRESHOLD_MB, MIN_FETCHED_MB, judge


def _slot(total_gb, pct, missing_mb):
    return {"mb": total_gb * 1024, "percentage": pct, "mbmissing": missing_mb}


def _judge(slot):
    return judge(slot, DEFAULT_THRESHOLD_MB, DEFAULT_LOSS_PCT)


class TestProportionalLoss:
    def test_the_rick_and_morty_s03e06_case(self):
        """47 MB missing on a 2 GB job at 18% - 13% of what was fetched is dead."""
        bad, reason, loss = _judge(_slot(2.0, 18, 47.2))
        assert bad
        assert 12 < loss < 14
        assert "fetched payload dead" in reason

    def test_a_healthy_job_survives(self):
        bad, _, loss = _judge(_slot(44.0, 76, 0.0))
        assert not bad
        assert loss == 0.0

    def test_loss_within_par2_range_is_left_alone(self):
        """~1% loss is comfortably inside par2 recovery - do not bin it."""
        bad, _, _ = _judge(_slot(2.1, 16, 3.0))
        assert not bad


class TestSmallSampleNoise:
    def test_tiny_sample_does_not_trip_the_ratio(self):
        """One bad article early on must not condemn the job."""
        slot = _slot(0.1, 20, 1.5)  # 20 MB fetched, under MIN_FETCHED_MB
        assert (0.1 * 1024) * 0.20 < MIN_FETCHED_MB
        bad, _, _ = _judge(slot)
        assert not bad

    def test_zero_percent_job_is_not_judged(self):
        bad, _, loss = _judge(_slot(50.0, 0, 0.0))
        assert not bad
        assert loss == 0.0


class TestAbsoluteThresholdStillApplies:
    def test_large_absolute_loss_caught_even_when_ratio_is_small(self):
        """A 60 GB job at 99% with 200 MB missing is under 1% but still dead."""
        bad, reason, _ = _judge(_slot(60.0, 99, 200.0))
        assert bad
        assert "confirmed missing" in reason


class TestMalformedInput:
    def test_non_numeric_percentage_does_not_raise(self):
        bad, _, loss = _judge({"mb": 2048, "percentage": "", "mbmissing": 5})
        assert not bad
        assert loss == 0.0
