"""A stale disk-error pause must not sit there for nine hours.

2026-08-06: SAB auto-paused three times on FileNotFoundError writing into a
job's own incomplete folder, and stayed paused 6h, 47m and 9h30m because
nothing was watching. The cause was duplicate queue entries sharing one
incomplete directory - when one twin was cleaned up it removed the folder out
from under the other. The trigger is momentary; the pause is permanent.

The risk in automating this is resuming a pause that IS legitimate, so these
pin the refusals as hard as the resume.
"""

import pytest

import tools.sab_unpause as su
from tools.sab_unpause import MIN_FREE_GB, diagnose, find_husks, is_husk, paused_by_disk_error


def _q(paused=True, free_gb=2700.0, pause_int="0", status="Paused"):
    return {"paused": paused, "diskspace1": free_gb, "pause_int": pause_int, "status": status}


FAULT_LOG = "ERROR::[downloader:750] Fatal error in Downloader\nINFO::[downloader:438] Pausing"


@pytest.fixture
def after_disk_error(monkeypatch):
    """Simulate a pause that a write failure actually caused."""
    monkeypatch.setattr(su, "recent_sab_log", lambda *a, **k: FAULT_LOG)


class TestResumes:
    def test_stale_pause_with_plenty_of_room_is_resumed(self, after_disk_error):
        ok, reason = diagnose(_q())
        assert ok
        assert "2700" in reason

    def test_just_above_the_floor_resumes(self, after_disk_error):
        ok, _ = diagnose(_q(free_gb=MIN_FREE_GB + 1))
        assert ok

    def test_manual_pause_is_left_alone_even_with_room(self, monkeypatch):
        """The whole point: free disk is NOT sufficient reason to resume."""
        monkeypatch.setattr(su, "recent_sab_log", lambda *a, **k: "INFO::[downloader:438] Pausing")
        ok, reason = diagnose(_q(free_gb=2700.0))
        assert not ok
        assert "manual pause" in reason


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


class TestHuskDetection:
    """The 2026-08-06 23:38 pause: a leftover dir with no payload and no
    nzo_data. SAB won't list it as an orphan (nothing to resume) but it owns
    the name, so the re-grab dies on FileExistsError creating __ADMIN__."""

    def _mk(self, tmp_path, name, *, nzo=False, payload=False, admin=True):
        d = tmp_path / name
        d.mkdir()
        if admin:
            a = d / "__ADMIN__"
            a.mkdir()
            (a / "SABnzbd_attrib").write_text("x")
            if nzo:
                (a / "SABnzbd_nzo_data").write_text("x")
        if payload:
            (d / "part.par2").write_bytes(b"x" * 100)
        return d

    def test_the_felicity_case_is_a_husk(self, tmp_path):
        d = self._mk(tmp_path, "Felicity.S03.NTSC.DVD.REMUX-MNeRD")
        assert is_husk(str(d))

    def test_a_resumable_job_is_not_a_husk(self, tmp_path):
        d = self._mk(tmp_path, "Show.S01E01", nzo=True)
        assert not is_husk(str(d)), "has nzo_data - SAB can resume it, do not delete"

    def test_a_dir_with_real_data_is_not_a_husk(self, tmp_path):
        d = self._mk(tmp_path, "Show.S01E02", payload=True)
        assert not is_husk(str(d)), "holds downloaded bytes - deleting loses them"

    def test_partial_download_without_nzo_data_is_kept(self, tmp_path):
        """Payload but no nzo_data still must not be binned - the bytes are real."""
        d = self._mk(tmp_path, "Show.S01E03", nzo=False, payload=True)
        assert not is_husk(str(d))


class TestFindHusks:
    def test_skips_dirs_owned_by_a_live_job(self, tmp_path):
        for n in ("LiveJob", "DeadHusk"):
            (tmp_path / n / "__ADMIN__").mkdir(parents=True)
        found = find_husks(str(tmp_path), live_names={"LiveJob"})
        assert [f.split("\\")[-1].split("/")[-1] for f in found] == ["DeadHusk"]

    def test_missing_base_dir_is_not_an_error(self):
        """Running from a machine that isn't the download server."""
        assert find_husks("/definitely/not/here") == []


class TestManualPauseIsRespected:
    """2026-08-08 00:00: the operator paused SAB by hand to let post-processing
    drain. This watchdog resumed it at 00:10 - an indefinite manual pause and a
    disk-error pause are identical through the API. The fix is to require
    positive evidence of a write failure, not merely the absence of a reason."""

    def test_bare_pause_is_not_ours_to_undo(self):
        log = "\n".join(
            [
                "2026-08-08 00:00:48,001::INFO::[misc:1] Nothing wrong here",
                "2026-08-08 00:00:49,773::INFO::[downloader:438] Pausing",
            ]
        )
        assert not paused_by_disk_error(log)

    def test_disk_error_pause_is_ours(self):
        log = "\n".join(
            [
                "2026-08-06 23:38:35,047::ERROR::[downloader:750] Fatal error in Downloader",
                "2026-08-06 23:38:35,062::INFO::[downloader:438] Pausing",
            ]
        )
        assert paused_by_disk_error(log)

    def test_filenotfound_pause_is_ours(self):
        log = "FileNotFoundError: [Errno 2] No such file\n...::[downloader:438] Pausing"
        assert paused_by_disk_error(log)

    def test_old_error_does_not_authorise_a_later_manual_pause(self):
        """An error hours ago says nothing about THIS pause."""
        log = "Fatal error in Downloader\n" + ("filler line\n" * 900) + "::[downloader:438] Pausing"
        assert not paused_by_disk_error(log)

    def test_no_pause_line_at_all(self):
        assert not paused_by_disk_error("just some ordinary log output")

    def test_unreadable_log_does_not_authorise_a_resume(self):
        """If we cannot read the log we cannot prove a fault - so do nothing."""
        assert not paused_by_disk_error("")
