"""The *arr apps must keep rejecting releases that are not the original language.

Six episodes of Nobody Wants This S03 arrived French-only on 2026-10-22, and
the sweep that followed found 92 more files across the library playing in a
language nobody in this house speaks - 50 episodes of Game of Thrones in German
among them.

The defence that failed was a banned-word list over release names: plain
``FRENCH`` was never on it, and the German pattern was case-sensitive so
``GERMAN.DL`` walked straight past. The replacement ignores names and keys on
the ``Language`` = Original metadata instead.

The specific regression this guards is subtler than the format being deleted.
Radarr already had an ``nc-Reject-Foreign-Language`` format while the dubs were
arriving - it was scored 0, so it annotated releases and rejected nothing. A
present-but-unscored format is indistinguishable from no format at all, so
these tests pin the score, not the existence.
"""

import pytest

import tools.invariants as inv

GUARD = inv.LANGUAGE_GUARD_FORMAT


@pytest.fixture
def arr_stub(monkeypatch):
    """Stand in for tools.grab_best.arr with per-app custom formats + profiles."""

    def install(
        sonarr_score=-10000,
        radarr_score=-10000,
        *,
        sonarr_present=True,
        radarr_present=True,
        sonarr_raises=False,
        profiles=("Any", "Quality+"),
    ):
        cfg = {
            "http://sonarr": (sonarr_present, sonarr_score, sonarr_raises),
            "http://radarr": (radarr_present, radarr_score, False),
        }

        def fake_arr(api, path, method="GET", body=None):
            base = api[0]
            present, score, raises = cfg[base]
            if raises:
                raise OSError("connection refused")
            if path == "/api/v3/customformat":
                return [{"id": 15, "name": GUARD}] if present else [{"id": 3, "name": "nc-HEVC"}]
            if path == "/api/v3/qualityprofile":
                return [
                    {
                        "id": i,
                        "name": nm,
                        "formatItems": ([{"format": 15, "name": GUARD, "score": score}] if score is not None else []),
                    }
                    for i, nm in enumerate(profiles, 1)
                ]
            raise AssertionError(f"unexpected path {path}")

        import tools.grab_best as gb

        monkeypatch.setattr(gb, "arr", fake_arr)
        monkeypatch.setattr(gb, "SONARR", ("http://sonarr", "k"))
        monkeypatch.setattr(gb, "RADARR", ("http://radarr", "k"))

    return install


class TestArmed:
    def test_both_apps_scored_to_reject_passes(self, arr_stub):
        arr_stub()
        assert inv.check_foreign_dub_guard_armed().passed

    def test_message_names_the_guard(self, arr_stub):
        arr_stub()
        assert "guard armed" in inv.check_foreign_dub_guard_armed().message

    def test_score_exactly_at_threshold_passes(self, arr_stub):
        arr_stub(sonarr_score=inv.LANGUAGE_GUARD_MAX_SCORE, radarr_score=inv.LANGUAGE_GUARD_MAX_SCORE)
        assert inv.check_foreign_dub_guard_armed().passed


class TestUnarmed:
    def test_format_scored_zero_fails(self, arr_stub):
        """The exact state Radarr sat in while German dubs were arriving."""
        arr_stub(radarr_score=0)
        res = inv.check_foreign_dub_guard_armed()
        assert not res.passed
        assert any("radarr" in v for v in res.violations)

    def test_format_missing_fails(self, arr_stub):
        arr_stub(sonarr_present=False)
        res = inv.check_foreign_dub_guard_armed()
        assert not res.passed
        assert any("missing" in v for v in res.violations)

    def test_format_not_attached_to_profile_fails(self, arr_stub):
        arr_stub(sonarr_score=None)
        res = inv.check_foreign_dub_guard_armed()
        assert not res.passed
        assert any("absent" in v for v in res.violations)

    def test_a_weak_negative_score_still_fails(self, arr_stub):
        """-50 annotates but will not outweigh the positive quality formats."""
        arr_stub(radarr_score=-50)
        assert not inv.check_foreign_dub_guard_armed().passed

    def test_violation_names_every_unarmed_profile(self, arr_stub):
        arr_stub(radarr_score=0, profiles=("Any", "Quality+", "1080p"))
        res = inv.check_foreign_dub_guard_armed()
        assert "3 profile(s)" in " ".join(res.violations)


class TestUnreachable:
    def test_unreachable_app_does_not_fail_the_invariant(self, arr_stub):
        """Rule 12: "cannot read" must never be reported as "is broken"."""
        arr_stub(sonarr_raises=True)
        res = inv.check_foreign_dub_guard_armed()
        assert res.passed
        assert "unreachable" in str(res.details["sonarr"])

    def test_unreachable_app_still_checks_the_other(self, arr_stub):
        arr_stub(sonarr_raises=True, radarr_score=0)
        assert not inv.check_foreign_dub_guard_armed().passed


class TestRegistration:
    def test_runs_as_part_of_the_suite(self):
        names = [fn.__name__ if hasattr(fn, "__name__") else "" for fn in inv._invariant_runners(skip_ssh=True)]
        assert "check_foreign_dub_guard_armed" in names

    def test_severity_is_high(self, arr_stub):
        arr_stub()
        assert inv.check_foreign_dub_guard_armed().severity == "HIGH"
