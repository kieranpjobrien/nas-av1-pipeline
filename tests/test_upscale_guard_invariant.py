"""The upscale guard must be checked by behaviour, not by its presence.

On 2026-11-02 Radarr grabbed ``About.Time.-2013-.BluRay.2160p.AI.Upscale...``
for a film that has no 4K master at all - all 233 genuine candidates were
1080p - while carrying a custom format named "Upscale (reject)" scored -10000
in both profiles. The format had never matched anything: its pattern was
``(?i)\\x08(upscale|...)\\x08``, where the ``\\x08`` are literal backspace
characters produced by writing ``"\\b"`` in a double-quoted string instead of
the two characters a regex word boundary needs.

So the failure mode these tests exist for is a guard that looks perfect to any
check that only asks "does it exist and is it scored". The pattern has to be
compiled and exercised, in both directions:

  * it must match the upscale titles that actually got through;
  * it must NOT match genuine remuxes, because a guard that rejects everything
    would starve the library while still reporting itself armed.
"""

import pytest

import tools.invariants as inv

DEAD_PATTERN = "(?i)\x08(upscale|upscaled|ai[ ._-]?upscale)\x08"
GOOD_PATTERN = r"\b(ai[ ._-]?upscale|upscaled?|topaz|gigapixel)\b"


def _format(pattern, name=None, fid=16):
    return {
        "id": fid,
        "name": name or inv.UPSCALE_GUARD_FORMAT,
        "specifications": [
            {
                "implementation": "ReleaseTitleSpecification",
                "fields": [{"name": "value", "value": pattern}],
            }
        ],
    }


def _profile(fid=16, score=-10000, name="Any", include=True):
    items = [{"format": fid, "name": inv.UPSCALE_GUARD_FORMAT, "score": score}] if include else []
    return {"id": 1, "name": name, "formatItems": items}


@pytest.fixture
def arrs(monkeypatch):
    """Stub both *arr apps with the given formats/profiles."""

    def install(formats, profiles, fail=()):
        import sys
        import types

        mod = types.ModuleType("tools.grab_best")
        mod.SONARR = ("http://sonarr", "k")
        mod.RADARR = ("http://radarr", "k")

        def arr(api, path, *a, **kw):
            label = "sonarr" if "sonarr" in api[0] else "radarr"
            if label in fail:
                raise OSError("unreachable")
            return formats[label] if "customformat" in path else profiles[label]

        mod.arr = arr
        monkeypatch.setitem(sys.modules, "tools.grab_best", mod)

    return install


class TestDeadPattern:
    def test_backspace_pattern_is_a_violation(self, arrs):
        """The exact live state on 2026-11-02: scored -10000, matching nothing."""
        f = [_format(DEAD_PATTERN)]
        p = [_profile()]
        arrs({"sonarr": f, "radarr": f}, {"sonarr": p, "radarr": p})
        res = inv.check_upscale_guard_armed()
        assert not res.passed
        assert any("matches no upscale marker" in v for v in res.violations)

    def test_correct_score_does_not_excuse_a_dead_pattern(self, arrs):
        """Score was never the problem - the check must not be satisfied by it."""
        f = [_format(DEAD_PATTERN)]
        p = [_profile(score=-10000)]
        arrs({"sonarr": f, "radarr": f}, {"sonarr": p, "radarr": p})
        assert not inv.check_upscale_guard_armed().passed

    def test_uncompilable_pattern_is_reported_not_raised(self, arrs):
        f = [_format("(?i)[unclosed")]
        p = [_profile()]
        arrs({"sonarr": f, "radarr": f}, {"sonarr": p, "radarr": p})
        res = inv.check_upscale_guard_armed()
        assert not res.passed
        assert any("does not compile" in v for v in res.violations)

    def test_pattern_that_rejects_genuine_releases_is_a_violation(self, arrs):
        """A guard matching everything starves the library but looks armed."""
        f = [_format(r"\b(remux|upscale)\b")]
        p = [_profile()]
        arrs({"sonarr": f, "radarr": f}, {"sonarr": p, "radarr": p})
        res = inv.check_upscale_guard_armed()
        assert not res.passed
        assert any("genuine release" in v for v in res.violations)


class TestMissingOrUnscored:
    def test_missing_format_is_a_violation(self, arrs):
        """Sonarr's state before the fix: no upscale format at all."""
        p = [_profile()]
        arrs({"sonarr": [], "radarr": [_format(GOOD_PATTERN)]}, {"sonarr": p, "radarr": p})
        res = inv.check_upscale_guard_armed()
        assert not res.passed
        assert any("is missing" in v and "sonarr" in v for v in res.violations)

    def test_zero_score_is_a_violation(self, arrs):
        f = [_format(GOOD_PATTERN)]
        p = [_profile(score=0)]
        arrs({"sonarr": f, "radarr": f}, {"sonarr": p, "radarr": p})
        res = inv.check_upscale_guard_armed()
        assert not res.passed
        assert any("not scored" in v for v in res.violations)

    def test_absent_from_a_profile_is_a_violation(self, arrs):
        f = [_format(GOOD_PATTERN)]
        p = [_profile(include=False)]
        arrs({"sonarr": f, "radarr": f}, {"sonarr": p, "radarr": p})
        res = inv.check_upscale_guard_armed()
        assert not res.passed
        assert any("absent" in v for v in res.violations)


class TestHealthy:
    def test_repaired_guard_passes(self, arrs):
        f = [_format(GOOD_PATTERN)]
        p = [_profile()]
        arrs({"sonarr": f, "radarr": f}, {"sonarr": p, "radarr": p})
        res = inv.check_upscale_guard_armed()
        assert res.passed, res.violations
        assert res.details["radarr"]["pattern_ok"] is True

    def test_unreachable_app_is_not_a_failure(self, arrs):
        """Rule 12: a read failure must never be reported as a broken guard."""
        f = [_format(GOOD_PATTERN)]
        p = [_profile()]
        arrs({"sonarr": f, "radarr": f}, {"sonarr": p, "radarr": p}, fail=("sonarr",))
        res = inv.check_upscale_guard_armed()
        assert res.passed
        assert "unreachable" in str(res.details["sonarr"])


class TestRegistration:
    def test_runs_as_part_of_the_suite(self):
        names = [getattr(fn, "__name__", "") for fn in inv._invariant_runners(skip_ssh=True)]
        assert "check_upscale_guard_armed" in names
