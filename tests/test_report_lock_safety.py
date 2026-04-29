"""Regression tests for the media_report cascade-of-loss bug (2026-04-29).

Background: ``_read_or_empty`` in tools/report_lock.py used to silently
return an empty skeleton on any read failure. The first time a write
produced corrupt JSON, the next ``patch_report`` would patch into that
empty dict and write it back — wiping every file from the report in a
single round-trip. We lost 8,679 files this way.

The fix:
  * Maintain a rolling ``.last_good`` backup of the previous valid report
  * On read failure, transparently restore from backup
  * If both primary and backup are unreadable, raise ReportCorruptError
    rather than silently substituting an empty dict
  * Validate report shape before writing — refuse to commit malformed data

These tests pin each guarantee.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def tmp_report_paths(tmp_path: Path, monkeypatch):
    """Point report_lock at a tmp media_report.json + .last_good + .lock."""
    primary = tmp_path / "media_report.json"
    lock = tmp_path / "media_report.lock"
    import paths
    monkeypatch.setattr(paths, "MEDIA_REPORT", primary)
    monkeypatch.setattr(paths, "MEDIA_REPORT_LOCK", lock)
    import tools.report_lock as report_lock
    monkeypatch.setattr(report_lock, "MEDIA_REPORT", primary)
    monkeypatch.setattr(report_lock, "MEDIA_REPORT_LOCK", lock)
    return primary, tmp_path / "media_report.json.last_good"


def test_first_write_creates_primary(tmp_report_paths):
    primary, backup = tmp_report_paths
    from tools.report_lock import write_report

    write_report({"files": [{"filepath": "/a/b.mkv"}]})

    assert primary.exists()
    with primary.open(encoding="utf-8") as f:
        assert json.load(f)["files"] == [{"filepath": "/a/b.mkv"}]


def test_second_write_promotes_first_to_backup(tmp_report_paths):
    """write_report copies the prior good state into .last_good before replacing."""
    primary, backup = tmp_report_paths
    from tools.report_lock import write_report

    first = {"files": [{"filepath": "/first.mkv"}]}
    second = {"files": [{"filepath": "/second.mkv"}]}
    write_report(first)
    write_report(second)

    assert primary.exists()
    assert backup.exists()
    assert json.loads(primary.read_text(encoding="utf-8"))["files"][0]["filepath"] == "/second.mkv"
    # The backup should be the FIRST write, since the second promotes
    # whatever was on disk before it (=first) into .last_good.
    assert json.loads(backup.read_text(encoding="utf-8"))["files"][0]["filepath"] == "/first.mkv"


def test_corrupt_primary_recovers_from_backup(tmp_report_paths):
    """If the primary is unparseable, read_report returns the backup transparently."""
    primary, backup = tmp_report_paths
    from tools.report_lock import write_report, read_report

    write_report({"files": [{"filepath": "/orig.mkv"}]})
    write_report({"files": [{"filepath": "/orig2.mkv"}]})  # creates .last_good

    # Now corrupt the primary
    primary.write_text("not valid json {", encoding="utf-8")

    recovered = read_report()
    # We should get the backup contents (the previous good state)
    assert recovered["files"][0]["filepath"] == "/orig.mkv"


def test_both_corrupt_raises(tmp_report_paths):
    """When both primary and backup are unreadable, refuse to silently empty.

    This is the cascade-of-loss guard: previous behaviour returned {} which
    a downstream patch_report would write back, wiping everything.
    """
    primary, backup = tmp_report_paths
    from tools.report_lock import read_report, ReportCorruptError

    primary.write_text("garbage", encoding="utf-8")
    backup.write_text("also garbage", encoding="utf-8")

    with pytest.raises(ReportCorruptError):
        read_report()


def test_refuse_to_write_malformed_report(tmp_report_paths):
    """write_report must reject obviously-bad shapes — they're the source of cascades."""
    from tools.report_lock import write_report

    with pytest.raises(ValueError):
        write_report("not a dict")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        write_report({"files": "not a list"})


def test_patch_report_round_trips_under_corruption(tmp_report_paths):
    """patch_report must NOT cascade-wipe when primary is corrupt and backup is good."""
    primary, backup = tmp_report_paths
    from tools.report_lock import write_report, patch_report

    write_report({"files": [{"filepath": "/a.mkv"}, {"filepath": "/b.mkv"}]})
    write_report({"files": [{"filepath": "/a.mkv"}, {"filepath": "/b.mkv"}, {"filepath": "/c.mkv"}]})
    # Corrupt primary
    primary.write_text("partial json {", encoding="utf-8")

    # Patch tries to add /d.mkv — should restore from backup, then patch, then write
    def _patch(report):
        report["files"].append({"filepath": "/d.mkv"})

    patch_report(_patch)

    with primary.open(encoding="utf-8") as f:
        result = json.load(f)
    paths_now = [f["filepath"] for f in result["files"]]
    # The backup contained a/b. Restored, then we appended d. Two files + one new = 3.
    assert "/a.mkv" in paths_now
    assert "/b.mkv" in paths_now
    assert "/d.mkv" in paths_now


def test_first_run_no_existing_files_returns_empty_skeleton(tmp_report_paths):
    """A fresh install (no primary, no backup) returns the empty skeleton — only this case."""
    from tools.report_lock import read_report

    result = read_report()
    assert result == {"files": [], "scan_date": "", "total_files": 0}


def test_partial_truncation_validated_as_corrupt(tmp_report_paths):
    """A JSON file that parses but lacks 'files' is rejected."""
    primary, _ = tmp_report_paths
    primary.write_text(json.dumps({"unrelated": "data"}), encoding="utf-8")

    from tools.report_lock import read_report, ReportCorruptError

    # No backup → should raise (this is exactly the dangerous shape that
    # the previous behaviour silently treated as empty)
    with pytest.raises(ReportCorruptError):
        read_report()
