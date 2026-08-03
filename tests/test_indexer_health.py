"""An indexer that mostly serves dead posts must be caught automatically.

2026-08-03: Nzb.su sat at joint-top priority serving cross-posted reposts of
dead articles - 85% of its grabs failed over a 3-hour window, 25% over the
settled history against altHUB's 10%. 38% of ALL TV grabs were failing.
Sonarr re-searched each failure and grabbed another dead xpost from the same
source. Nothing surfaced it because Prowlarr scores indexers on response
time, never on whether the grab survives contact with the servers.
"""

import tools.indexer_health as ih


def _hist(events):
    return {"records": events}


def _grab(did, indexer, title="Show.S01E01.1080p"):
    return {"eventType": "grabbed", "downloadId": did, "sourceTitle": title, "data": {"indexer": indexer}}


def _fail(did, indexer=None):
    return {
        "eventType": "downloadFailed",
        "downloadId": did,
        "sourceTitle": "",
        "data": {"indexer": indexer} if indexer else {},
    }


class TestOutcomes:
    def test_joins_grabs_to_failures(self, monkeypatch):
        events = [_grab("a", "Nzb.su"), _fail("a"), _grab("b", "altHUB")]
        monkeypatch.setattr(ih, "arr", lambda api, path, *a, **k: _hist(events if "page=1" in path else []))
        grabs, fails, _ = ih.outcomes(ih.SONARR)
        assert grabs["Nzb.su"] == 1
        assert fails["Nzb.su"] == 1
        assert fails["altHUB"] == 0

    def test_unattributable_failure_is_excluded_from_both_sides(self, monkeypatch):
        """A failure whose grab predates the window has no denominator.

        Counting it as a failure without counting a matching resolved grab
        would inflate the rate of whichever indexer happened to be named.
        """
        events = [_fail("orphan", "Nzb.su")]
        monkeypatch.setattr(ih, "arr", lambda api, path, *a, **k: _hist(events if "page=1" in path else []))
        resolved, fails, _ = ih.outcomes(ih.SONARR)
        assert fails["Nzb.su"] == 0
        assert resolved["Nzb.su"] == 0

    def test_import_counts_as_a_resolved_success(self, monkeypatch):
        events = [
            _grab("a", "altHUB"),
            {"eventType": "downloadFolderImported", "downloadId": "a", "data": {}},
        ]
        monkeypatch.setattr(ih, "arr", lambda api, path, *a, **k: _hist(events if "page=1" in path else []))
        resolved, fails, _ = ih.outcomes(ih.SONARR)
        assert resolved["altHUB"] == 1
        assert fails["altHUB"] == 0

    def test_a_pending_grab_is_not_yet_counted(self, monkeypatch):
        """The bug this metric fixes: an unresolved grab is not a success."""
        events = [_grab("a", "Nzb.su")]
        monkeypatch.setattr(ih, "arr", lambda api, path, *a, **k: _hist(events if "page=1" in path else []))
        resolved, _, _ = ih.outcomes(ih.SONARR)
        assert resolved["Nzb.su"] == 0

    def test_counts_xposts(self, monkeypatch):
        events = [_grab("a", "Nzb.su", "Show.S01E01.1080p.WEB-xpost"), _grab("b", "Nzb.su", "Show.S01E02.1080p")]
        monkeypatch.setattr(ih, "arr", lambda api, path, *a, **k: _hist(events if "page=1" in path else []))
        _, _, xpost = ih.outcomes(ih.SONARR)
        assert xpost["Nzb.su"] == 1


def _row(resolved, fails):
    return {"resolved": resolved, "fails": fails, "rate": fails / resolved, "xpost": 0}


class TestDemotionThreshold:
    def test_the_nzb_su_case_is_flagged(self):
        assert ih.failing({"Nzb.su": _row(66, 56)}) == ["Nzb.su"]

    def test_a_healthy_indexer_is_left_alone(self):
        assert ih.failing({"altHUB": _row(43, 7)}) == []

    def test_small_sample_is_not_condemned(self):
        """Three failures out of three is noise, not evidence."""
        assert ih.failing({"NewIndexer": _row(3, 3)}) == []


class TestRelativeTest:
    def test_flags_an_indexer_far_worse_than_its_peers(self):
        """The real 2026-08-03 numbers: 25% vs 10% on the same library.

        Neither trips the 40% absolute threshold, but one is plainly worse.
        """
        data = {"Nzb.su": _row(495, 126), "altHUB": _row(344, 33), "NinjaCentral": _row(291, 55)}
        assert ih.failing(data) == ["Nzb.su"]

    def test_does_not_punish_a_uniformly_bad_night(self):
        """If everyone is failing equally it is usenet, not the indexer."""
        data = {"a": _row(100, 30), "b": _row(100, 28), "c": _row(100, 32)}
        assert ih.failing(data) == []

    def test_low_rates_are_never_flagged_relatively(self):
        """2% vs 8% is 4x but both are fine - don't demote on noise."""
        data = {"a": _row(200, 4), "b": _row(200, 16)}
        assert ih.failing(data) == []

    def test_a_flawless_indexer_does_not_condemn_everyone_else(self):
        """best rate of 0 must not make every other indexer 'infinitely worse'."""
        data = {"perfect": _row(100, 0), "ok": _row(100, 5)}
        assert ih.failing(data) == []


class TestProwlarrNameMapping:
    def test_strips_the_prowlarr_suffix(self):
        assert ih.prowlarr_key("Nzb.su (Prowlarr)") == "Nzb.su"

    def test_leaves_a_plain_name_alone(self):
        assert ih.prowlarr_key("altHUB") == "altHUB"


class TestDemote:
    def test_demotes_by_matching_name(self, monkeypatch):
        seen = {}

        def fake(api, path, method="GET", body=None):
            if method == "GET":
                return [{"id": 2, "name": "Nzb.su", "priority": 2}]
            seen["body"] = body
            return {}

        monkeypatch.setattr(ih, "arr", fake)
        assert "45" in ih.demote("Nzb.su (Prowlarr)")
        assert seen["body"]["priority"] == ih.DEMOTED_PRIORITY

    def test_is_idempotent(self, monkeypatch):
        monkeypatch.setattr(ih, "arr", lambda *a, **k: [{"id": 2, "name": "Nzb.su", "priority": ih.DEMOTED_PRIORITY}])
        assert ih.demote("Nzb.su (Prowlarr)") == "already demoted"

    def test_unknown_indexer_reports_rather_than_raising(self, monkeypatch):
        monkeypatch.setattr(ih, "arr", lambda *a, **k: [{"id": 1, "name": "altHUB", "priority": 2}])
        assert ih.demote("Ghost") == "not found in Prowlarr"
