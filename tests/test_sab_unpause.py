"""A stale disk-error pause must not sit there for nine hours.

2026-08-06: SAB auto-paused three times on FileNotFoundError writing into a
job's own incomplete folder, and stayed paused 6h, 47m and 9h30m because
nothing was watching. The cause was duplicate queue entries sharing one
incomplete directory - when one twin was cleaned up it removed the folder out
from under the other. The trigger is momentary; the pause is permanent.

The risk in automating this is resuming a pause that IS legitimate, so these
pin the refusals as hard as the resume.
"""

from tools.sab_unpause import MIN_FREE_GB, diagnose


def _q(paused=True, free_gb=2700.0, pause_int="0", status="Paused"):
    return {"paused": paused, "diskspace1": free_gb, "pause_int": pause_int, "status": status}


class TestResumes:
    def test_stale_pause_with_plenty_of_room_is_resumed(self):
        ok, reason = diagnose(_q())
        assert ok
        assert "2700" in reason

    def test_just_above_the_floor_resumes(self):
        ok, _ = diagnose(_q(free_gb=MIN_FREE_GB + 1))
        assert ok


class TestRefusals:
    def test_a_full_disk_stays_paused(self):
        """The pause is doing its job - resuming would just re-fill the disk."""
        ok, reason = diagnose(_q(free_gb=5.0))
        assert not ok
        assert "legitimate" in reason

    def test_exactly_at_the_floor_stays_paused(self):
        ok, _ = diagnose(_q(free_gb=MIN_FREE_GB - 0.01))
        assert not ok

    def test_a_deliberate_timed_pause_is_never_overridden(self):
        """'Pause for 30 minutes' is an operator decision, not a fault."""
        ok, reason = diagnose(_q(pause_int="0:29:41"))
        assert not ok
        assert "deliberate" in reason

    def test_not_paused_is_a_no_op(self):
        ok, reason = diagnose(_q(paused=False, status="Downloading"))
        assert not ok
        assert reason == "not paused"

    def test_unreadable_free_space_does_not_resume(self):
        """No evidence the disk is healthy means no resume."""
        ok, reason = diagnose({"paused": True, "diskspace1": "n/a", "pause_int": "0"})
        assert not ok
        assert "could not read" in reason

    def test_missing_free_space_field_does_not_resume(self):
        ok, _ = diagnose({"paused": True, "pause_int": "0"})
        assert not ok
