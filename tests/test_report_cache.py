"""Tests for the mtime-invalidating media_report cache in server.helpers."""

import json
import os
import threading
import time

import pytest


@pytest.fixture(autouse=True)
def _clean_cache():
    """Clear the module-level cache before every test to prevent bleed-through."""
    from server.helpers import invalidate_report_cache

    invalidate_report_cache()
    yield
    invalidate_report_cache()


@pytest.fixture()
def report_on_disk(sample_report):
    """Write ``sample_report`` to the configured ``MEDIA_REPORT`` path and return it."""
    from paths import MEDIA_REPORT

    MEDIA_REPORT.parent.mkdir(parents=True, exist_ok=True)
    MEDIA_REPORT.write_text(json.dumps(sample_report), encoding="utf-8")
    yield MEDIA_REPORT
    try:
        MEDIA_REPORT.unlink()
    except OSError:
        pass


class TestReportCacheHitMiss:
    """mtime-keyed cache returns the same instance on HIT, re-reads on MISS."""

    def test_cache_hit_returns_same_dict_instance_when_mtime_unchanged(self, report_on_disk):
        """Two reads without a mtime change return the same object reference."""
        from server.helpers import read_report_cached

        first = read_report_cached(report_on_disk)
        second = read_report_cached(report_on_disk)

        assert first is second, "expected cache HIT to return the same dict instance"
        assert isinstance(first, dict)
        assert len(first.get("files", [])) > 0

    def test_cache_miss_after_mtime_change(self, report_on_disk):
        """Touching the file's mtime forces a fresh read."""
        from server.helpers import read_report_cached

        first = read_report_cached(report_on_disk)

        # Advance mtime beyond the filesystem timestamp resolution.
        new_mtime = os.path.getmtime(report_on_disk) + 10
        os.utime(report_on_disk, (new_mtime, new_mtime))

        second = read_report_cached(report_on_disk)
        assert second is not first, "expected cache MISS after mtime change"
        # Contents still identical — the cache just shouldn't reuse the stale entry.
        assert second == first

    def test_returns_none_when_file_missing(self, tmp_path):
        """A non-existent path returns None, matching ``read_json_safe`` semantics."""
        from server.helpers import read_report_cached

        ghost = tmp_path / "does_not_exist.json"
        assert read_report_cached(ghost) is None


class TestInvalidate:
    """invalidate_report_cache forces the next read to repopulate."""

    def test_invalidate_clears_cache(self, report_on_disk):
        """Explicit invalidate triggers a re-read even when mtime is unchanged."""
        from server.helpers import invalidate_report_cache, read_report_cached

        first = read_report_cached(report_on_disk)
        invalidate_report_cache()
        second = read_report_cached(report_on_disk)

        assert second is not first, "expected fresh parse after invalidate"
        assert second == first


class TestConcurrentReads:
    """Concurrent readers de-duplicate the underlying file parse."""

    def test_threaded_concurrent_reads_do_not_duplicate_parse(self, report_on_disk, monkeypatch):
        """10 threads racing on a cold cache should only trigger a handful of real reads.

        We can't guarantee exactly one read without serialising every caller around
        the cache lock (which we deliberately don't), but we CAN assert that the
        first completed read populates the cache so later racers hit it — the call
        count must be strictly less than the thread count.
        """
        from server import helpers
        from tools import report_lock as rl

        call_count = 0
        original = rl.read_report

        def counting_read():
            nonlocal call_count
            call_count += 1
            # Simulate a slow 50 MB parse so threads pile up on the cold miss.
            time.sleep(0.05)
            return original()

        monkeypatch.setattr(rl, "read_report", counting_read)

        results: list = []
        errors: list = []

        def worker():
            try:
                results.append(helpers.read_report_cached(report_on_disk))
            except Exception as e:  # pragma: no cover — defensive
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"unexpected worker errors: {errors}"
        assert len(results) == 10
        # All results should be equivalent data (same dict content).
        for r in results:
            assert r == results[0]
        # Strict: at least one thread's parse MUST have been served from the
        # populated cache, i.e. fewer parses than threads.
        assert call_count < 10, f"expected cache to absorb racers, got {call_count} parses"
