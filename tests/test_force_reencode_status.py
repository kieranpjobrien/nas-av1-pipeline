"""Stamping force_reencode must never leave a terminal status in place.

flagged_corrupt reached 94 rows on 2026-10-20 and almost none were corrupt. A
FLAGGED_CORRUPT row reaching _stamp_force_reencode kept its corrupt status while
its reason was overwritten with "force_reencode set by categorise_entry:
finished-video audio needs transcode to eac-3", so 42 files whose only fault was
AAC or Opus audio were reported to the operator as corrupt.

Stamping force_reencode means "we intend to re-encode this". That is inherently
non-terminal.
"""

import pytest

from pipeline.__main__ import _stamp_force_reencode
from pipeline.state import TERMINAL_STATUSES, FileStatus


class FakeState:
    def __init__(self):
        self.writes = []

    def set_file(self, filepath, status, **kw):
        self.writes.append((filepath, status, kw))


@pytest.mark.parametrize("terminal", sorted(TERMINAL_STATUSES, key=lambda s: s.value))
def test_terminal_statuses_are_reset_to_pending(terminal):
    st = FakeState()
    _stamp_force_reencode(st, "X.mkv", {"status": terminal.value}, reason="audio needs transcode to eac-3")
    _fp, status, _kw = st.writes[0]
    assert status == FileStatus.PENDING, f"{terminal.value} survived the stamp"


def test_flagged_corrupt_specifically_does_not_survive():
    """The exact shape of the bug: audio-transcode reason, corrupt status."""
    st = FakeState()
    _stamp_force_reencode(
        st,
        "X.mkv",
        {"status": FileStatus.FLAGGED_CORRUPT.value},
        reason="finished-video audio needs transcode to eac-3",
    )
    _fp, status, kw = st.writes[0]
    assert status == FileStatus.PENDING
    assert "audio needs transcode" in kw["reason"]


@pytest.mark.parametrize(
    "active",
    [FileStatus.PENDING, FileStatus.QUALIFYING, FileStatus.FETCHING, FileStatus.PROCESSING, FileStatus.UPLOADING],
)
def test_non_terminal_statuses_are_preserved(active):
    """An in-flight row must keep its place in the workflow."""
    st = FakeState()
    _stamp_force_reencode(st, "X.mkv", {"status": active.value}, reason="priority.json membership")
    _fp, status, _kw = st.writes[0]
    assert status == active


def test_no_existing_row_becomes_pending():
    st = FakeState()
    _stamp_force_reencode(st, "X.mkv", None, reason="cq off-target")
    assert st.writes[0][1] == FileStatus.PENDING


def test_unrecognised_status_becomes_pending():
    st = FakeState()
    _stamp_force_reencode(st, "X.mkv", {"status": "something_invented"}, reason="cq off-target")
    assert st.writes[0][1] == FileStatus.PENDING


def test_the_stamp_itself_still_happens():
    """Guard the original purpose: without force_reencode the AV1-source guard
    silently marks the file DONE without encoding (2026-05-22, 183 files)."""
    st = FakeState()
    _stamp_force_reencode(st, "X.mkv", {"status": FileStatus.FLAGGED_CORRUPT.value}, reason="r")
    _fp, _status, kw = st.writes[0]
    assert kw["force_reencode"] is True
    assert kw["force_source"] == "auto"
