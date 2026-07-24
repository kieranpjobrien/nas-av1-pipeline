"""Regression: a requeue to PENDING must scrub the stale prep cache.

Carrying prep_done/prep_data forward through an internal requeue (categorise
auto-reset, force-stamp, finalize retry) handed ffmpeg a .stripped.mkv the
restart cleanup had deleted (exit 4294967294, 10 files) and skipped the strip
so foreign/commentary tracks survived to verify (4 files) -- both 2026-07-24.
The dashboard requeue already cleared it; set_file must clear it on every
PENDING write so the internal paths are covered too.
"""

from pipeline.state import FileStatus, PipelineState


def test_pending_scrubs_stale_prep_cache(tmp_path):
    st = PipelineState(str(tmp_path / "scrub.db"))
    st.set_file(
        "X.mkv",
        FileStatus.DONE,
        prep_done=True,
        prep_data={"actual_input": "gone.stripped.mkv"},
        detected_audio=[{"codec": "eac3"}],
        detected_subs=[{"lang": "eng"}],
        pre_processed=True,
    )
    done = st.get_file("X.mkv")
    assert done.get("prep_done") is True and done.get("prep_data")

    # Requeue -> PENDING must drop the whole prep cache.
    st.set_file("X.mkv", FileStatus.PENDING)
    row = st.get_file("X.mkv")
    assert not row.get("prep_done")
    assert not row.get("prep_data")
    assert not row.get("detected_audio")
    assert not row.get("detected_subs")
    assert not row.get("pre_processed")
    st.close()


def test_pending_preserves_explicit_prep_from_caller(tmp_path):
    """A caller that re-supplies prep on the same PENDING write is not clobbered
    (guards a legitimate prep-worker write)."""
    st = PipelineState(str(tmp_path / "scrub2.db"))
    st.set_file(
        "Y.mkv",
        FileStatus.PENDING,
        prep_done=True,
        prep_data={"actual_input": "y.stripped.mkv"},
    )
    row = st.get_file("Y.mkv")
    assert row.get("prep_done") is True
    assert (row.get("prep_data") or {}).get("actual_input") == "y.stripped.mkv"
    st.close()


def test_non_pending_transition_keeps_prep(tmp_path):
    """A transition to a non-pending status must NOT scrub prep -- the prep
    worker stores prep_done while the row is processing/prepped."""
    st = PipelineState(str(tmp_path / "scrub3.db"))
    st.set_file("Z.mkv", FileStatus.PROCESSING, prep_done=True,
                prep_data={"actual_input": "z.stripped.mkv"})
    st.set_file("Z.mkv", FileStatus.PROCESSING, stage="encoding")
    row = st.get_file("Z.mkv")
    assert row.get("prep_done") is True
    assert (row.get("prep_data") or {}).get("actual_input") == "z.stripped.mkv"
    st.close()
