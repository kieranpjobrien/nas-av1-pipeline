"""A file we could not READ must never be recorded as a file we found BROKEN.

2026-08-03: 218 files across 19 series sat in flagged_corrupt - a terminal
state - after the prep probe reported "duration probe failed (corrupt
container?)". 14 of 14 sampled re-probed perfectly fine, including the Veep
episode the pipeline died on. They were flagged during bulk-fetch windows
when the NAS share was saturated.

Two defects combined:
  * ``_decode_window`` was hardened against flaky SMB in 2026-05-24, but
    ``_probe_duration`` - which GATES it, and whose 0.0 condemns the file
    before a single frame is decoded - had no retry at all.
  * A probe that could not run was mapped onto FLAGGED_CORRUPT, which is
    terminal. Rule 12: never substitute a failure for a verdict.
"""

import inspect

import pipeline.full_gamut as fg
import tools.probe_source_integrity as psi


class TestDurationProbeRetries:
    def test_retries_before_giving_up(self, monkeypatch):
        calls = []

        class _Out:
            returncode = 0
            stdout = "1680.5"
            stderr = ""

        def flaky(cmd, **kw):
            calls.append(1)
            if len(calls) < 3:
                raise __import__("subprocess").TimeoutExpired(cmd, 30)
            return _Out()

        monkeypatch.setattr(psi.subprocess, "run", flaky)
        monkeypatch.setattr(psi.time, "sleep", lambda *_: None)
        assert psi._probe_duration("x.mkv") == 1680.5
        assert len(calls) == 3, "must retry a timing-out duration probe, not condemn on the first"

    def test_retries_on_nonzero_returncode(self, monkeypatch):
        calls = []

        class _Bad:
            returncode = 1
            stdout = ""
            stderr = "Input/output error"

        class _Good:
            returncode = 0
            stdout = "900"
            stderr = ""

        def flaky(cmd, **kw):
            calls.append(1)
            return _Bad() if len(calls) == 1 else _Good()

        monkeypatch.setattr(psi.subprocess, "run", flaky)
        monkeypatch.setattr(psi.time, "sleep", lambda *_: None)
        assert psi._probe_duration("x.mkv") == 900.0

    def test_gives_up_eventually(self, monkeypatch):
        class _Bad:
            returncode = 1
            stdout = ""
            stderr = ""

        monkeypatch.setattr(psi.subprocess, "run", lambda *a, **kw: _Bad())
        monkeypatch.setattr(psi.time, "sleep", lambda *_: None)
        assert psi._probe_duration("x.mkv") == 0.0


class TestUnreadableIsNotCorrupt:
    def test_failed_duration_probe_is_marked_unreadable(self, monkeypatch, tmp_path):
        fp = tmp_path / "fake.mkv"
        fp.write_bytes(b"x" * 100)
        monkeypatch.setattr(psi, "_probe_duration", lambda *a, **kw: 0.0)
        r = psi.probe_file(str(fp))
        assert not r.healthy
        assert r.unreadable is True
        assert "not a corruption verdict" in r.fatal

    def test_missing_file_is_unreadable_not_corrupt(self, tmp_path):
        r = psi.probe_file(str(tmp_path / "nope.mkv"))
        assert r.unreadable is True

    def test_real_decode_errors_are_still_a_corruption_verdict(self, monkeypatch, tmp_path):
        """The Ford v Ferrari class must still flag - don't over-correct."""
        fp = tmp_path / "real.mkv"
        fp.write_bytes(b"x" * 100)
        monkeypatch.setattr(psi, "_probe_duration", lambda *a, **kw: 1800.0)
        monkeypatch.setattr(psi, "_decode_window", lambda *a, **kw: (False, ["Invalid NAL unit size"]))
        r = psi.probe_file(str(fp))
        assert not r.healthy
        assert r.unreadable is False, "we decoded frames and found them broken - that IS a verdict"


class TestPrepParksRatherThanFlags:
    def test_prep_returns_unreadable_to_pending(self):
        src = inspect.getsource(fg)
        marker = "def _prepare_for_encode_locked"
        start = src.find(marker) if marker in src else src.find("def prepare_for_encode")
        body = src[start : start + 20000]
        assert "probe_result.unreadable" in body, "prep must branch on unreadable before flagging corrupt"
        idx_unreadable = body.find("probe_result.unreadable")
        idx_flag = body.find("FileStatus.FLAGGED_CORRUPT")
        assert idx_unreadable < idx_flag, "the unreadable branch must be checked BEFORE the corrupt branch"
        window = body[idx_unreadable : idx_unreadable + 1200]
        assert "FileStatus.PENDING" in window, "an unreadable source goes back to PENDING for retry"
