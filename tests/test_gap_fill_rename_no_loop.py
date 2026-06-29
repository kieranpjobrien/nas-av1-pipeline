"""Regression: gap_fill must not loop on a file that was already renamed.

A file that gets both a track-strip AND a filename-clean in one gap_fill pass
ends up with TWO media_report entries: the stale pre-rename path and the new
clean one. analyse_gaps on the stale entry keeps asking for a strip; gap_fill
finds the old path gone, redirects to the real (clean) file, strips -> no-op,
and -- pre-fix -- left the stale path's state row non-terminal, so the
orchestrator re-picked it every pass. Mrs Maisel S05E07 ran 553x overnight
2026-06-28.

Fix: when gap_fill redirects from a missing old path to its existing
clean-named sibling, it terminalises the old path's state row so it stops being
re-queued.
"""

from pipeline.gap_filler import GapAnalysis, gap_fill
from pipeline.state import FileStatus, PipelineState


def test_gap_fill_terminalises_stale_old_path_on_rename(tmp_path, monkeypatch):
    import pipeline.report as report

    # The re-probe before the DONE short-circuit is a best-effort sanity check;
    # stub it so the test stays hermetic (no ffprobe on a dummy file).
    monkeypatch.setattr(report, "probe_file", lambda *a, **k: None)

    clean = tmp_path / "Show S01E01.mkv"  # the real, already-renamed file
    clean.write_bytes(b"x")
    old = tmp_path / "Show. S01E01.mkv"  # stale pre-rename path -- does NOT exist

    st = PipelineState(str(tmp_path / "state.db"))
    st.set_file(str(old), FileStatus.PROCESSING, mode="gap_filler", stage="gap_fill")

    gaps = GapAnalysis()
    gaps.clean_name = "Show S01E01.mkv"
    gaps.needs_filename_clean = True
    entry = {
        "filepath": str(old),
        "filename": "Show. S01E01.mkv",
        "library_type": "series",
        "audio_streams": [],
        "subtitle_streams": [],
        "tmdb": {"id": 1},
    }

    ok = gap_fill(str(old), entry, gaps, {}, st)

    assert ok is True
    old_row = st.get_file(str(old))
    assert old_row is not None
    # The stale old path must be terminal so the pass + queue builder skip it.
    assert old_row["status"] == FileStatus.DONE.value
    assert "supersed" in (old_row.get("reason") or "").lower()


def test_gap_fill_terminalises_missing_file_with_no_clean_name(tmp_path):
    """A queued file that is simply gone (renamed/deleted, no clean-name
    candidate) must be terminalised, not returned un-marked -- the un-marked
    return looped 2887x on a stale 'My Neighbor Totoro- (1988).mkv' entry whose
    real file had lost the trailing dash the report still carried (2026-06-30).
    """
    missing = tmp_path / "Gone (1999).mkv"  # never created on disk
    st = PipelineState(str(tmp_path / "state2.db"))
    st.set_file(str(missing), FileStatus.PROCESSING, mode="gap_filler", stage="gap_fill")

    gaps = GapAnalysis()
    gaps.needs_track_removal = True  # there is "work", but the file is gone
    entry = {
        "filepath": str(missing),
        "filename": "Gone (1999).mkv",
        "library_type": "movie",
        "audio_streams": [],
        "subtitle_streams": [],
        "tmdb": {"id": 1},
    }

    ok = gap_fill(str(missing), entry, gaps, {}, st)

    assert ok is True
    row = st.get_file(str(missing))
    assert row is not None
    assert row["status"] == FileStatus.DONE.value  # terminalised -> won't re-loop
