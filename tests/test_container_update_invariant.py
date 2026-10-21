"""The stack must be provably receiving security updates.

watchtower failed 234 consecutive scheduled runs between 2026-06-06 and
2026-10-22 while reporting "Failed=0 Updated=0". Nothing off that box could see
it, so it went unnoticed for five months - straight through the in-the-wild
exploitation of the Plex2Shell RCE. Plex was patched by luck of timing, not by
the update mechanism working.
"""

import json
from datetime import datetime, timedelta

import pytest

import tools.invariants as inv


@pytest.fixture
def status_file(tmp_path, monkeypatch):
    p = tmp_path / "docker_update.json"
    monkeypatch.setattr(inv, "CONTAINER_UPDATE_STATUS", str(p))

    def write(**over):
        body = {
            "when": datetime.now().isoformat(),
            "services": 11,
            "pulled_ok": 11,
            "pull_failed": 0,
            "failed_services": "",
            "blind_containers": 0,
        }
        body.update(over)
        p.write_text(json.dumps(body), encoding="utf-8")
        return p

    return write


class TestHealthy:
    def test_fresh_clean_run_passes(self, status_file):
        status_file()
        assert inv.check_container_updates_running().passed

    def test_message_says_when_and_how_many(self, status_file):
        status_file()
        r = inv.check_container_updates_running()
        assert "image(s) current" in r.message


class TestFailures:
    def test_stale_run_fails(self, status_file):
        """Two missed nights means the updater is not running."""
        status_file(when=(datetime.now() - timedelta(hours=72)).isoformat())
        r = inv.check_container_updates_running()
        assert not r.passed
        assert "ago" in r.message

    def test_one_missed_night_is_tolerated(self, status_file):
        status_file(when=(datetime.now() - timedelta(hours=26)).isoformat())
        assert inv.check_container_updates_running().passed

    def test_failed_pull_fails(self, status_file):
        """The exact watchtower shape: it ran, but updated nothing."""
        status_file(pull_failed=3, failed_services="plex sonarr radarr")
        r = inv.check_container_updates_running()
        assert not r.passed
        assert "plex" in r.message

    def test_blind_container_fails(self, status_file):
        """A container that cannot see the NAS runs and reports healthy anyway."""
        status_file(blind_containers=2)
        r = inv.check_container_updates_running()
        assert not r.passed
        assert "cannot see the NAS" in r.message

    def test_never_run_fails(self, tmp_path, monkeypatch):
        monkeypatch.setattr(inv, "CONTAINER_UPDATE_STATUS", str(tmp_path / "nope.json"))
        r = inv.check_container_updates_running()
        assert not r.passed
        assert "never" in r.message

    def test_timestampless_status_fails(self, status_file):
        p = status_file()
        p.write_text(json.dumps({"pulled_ok": 11}), encoding="utf-8")
        assert not inv.check_container_updates_running().passed


class TestDoesNotCryWolf:
    def test_unreadable_file_is_skipped_not_failed(self, status_file):
        """An SMB blip must not look like a security lapse."""
        p = status_file()
        p.write_text("{ this is not json", encoding="utf-8")
        r = inv.check_container_updates_running()
        assert r.passed
        assert "skipped" in r.message

    def test_it_is_registered(self):
        """An invariant nobody runs is the same as no invariant."""
        assert inv.check_container_updates_running in inv._invariant_runners(skip_ssh=False)
