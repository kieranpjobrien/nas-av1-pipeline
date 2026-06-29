"""Regression: convert→replace must not leave a stale old-extension entry.

When the encoder converts a ``.mp4`` source to a ``.mkv`` AV1 output it
replaces the file on disk under a DIFFERENT name (``final_path != filepath``).
Pre-fix, ``finalize_upload`` recorded DONE under the original ``.mp4`` path and
left that path's media_report entry in place. On the next restart
``categorise_entry`` saw the old non-AV1 report entry, ffprobed the now-missing
``.mp4``, couldn't confirm AV1, and auto-reset it to PENDING+force_reencode;
``fetch_file`` then hit ``SOURCE_MISSING`` and ``_remove_missing_source``
mis-flagged it ``flagged_corrupt`` — a phantom duplicate on the dashboard.

Observed 2026-06-29 on 9 already-converted files (Sneakers, Thief, Jurassic
Park, Rush, Twisters, No Country for Old Men, Guardians of the Galaxy, Diamonds
Are Forever, 3-10 to Yuma). Same class as the gap_filler rename loop fixed in
ce9e767, via the convert→replace path.

Fix: ``finalize_upload`` keys the DONE row on ``final_path`` and
``_purge_stale_source_path`` drops the dead original path's media_report entry +
state row (single-writer paths, rules 12/13), guarded on the original genuinely
being gone from disk (rule 8).
"""

from __future__ import annotations

import inspect
import os
from pathlib import Path

import pytest

from pipeline.full_gamut import _purge_stale_source_path
from pipeline.state import FileStatus, PipelineState


@pytest.fixture
def tmp_report_paths(tmp_path: Path, monkeypatch):
    """Point report_lock at a tmp media_report.json + .last_good + .lock."""
    primary = tmp_path / "media_report.json"
    lock = tmp_path / "media_report.lock"
    import paths

    monkeypatch.setattr(paths, "MEDIA_REPORT", primary)
    monkeypatch.setattr(paths, "MEDIA_REPORT_LOCK", lock)
    import tools.report_lock as report_lock

    monkeypatch.setattr(report_lock, "MEDIA_REPORT", primary)
    monkeypatch.setattr(report_lock, "MEDIA_REPORT_LOCK", lock)
    return primary


def _seed_report(mp4_path: str, mkv_path: str) -> None:
    """Write a media_report holding BOTH the stale .mp4 and the new .mkv entry.

    This mirrors the on-disk state right after the replace step + the
    ``update_entry(final_path)`` call: the .mkv has been added, the .mp4 entry
    is the leftover the purge must remove.
    """
    from tools.report_lock import write_report

    write_report(
        {
            "files": [
                {
                    "filepath": mp4_path,
                    "filename": os.path.basename(mp4_path),
                    "library_type": "movie",
                    "video": {"codec": "H.264", "codec_raw": "h264"},
                    "file_size_bytes": 8_000_000_000,
                },
                {
                    "filepath": mkv_path,
                    "filename": os.path.basename(mkv_path),
                    "library_type": "movie",
                    "video": {"codec": "AV1", "codec_raw": "av1"},
                    "file_size_bytes": 3_000_000_000,
                },
            ]
        }
    )


def _report_paths() -> list[str]:
    from tools.report_lock import read_report

    return [f.get("filepath") for f in read_report().get("files", [])]


def test_purge_removes_stale_mp4_entry_and_state_row(tmp_path, tmp_report_paths):
    """The core fix: a .mp4→.mkv convert drops the dead .mp4 from BOTH stores."""
    mp4 = str(tmp_path / "Sneakers (1992).mp4")  # NOT created — replaced on disk
    mkv = str(tmp_path / "Sneakers (1992).mkv")
    _seed_report(mp4, mkv)

    st = PipelineState(str(tmp_path / "state.db"))
    # State as finalize_upload leaves it just before the DONE block: the .mp4
    # carried the encode through to UPLOADING; the .mkv DONE row is set first.
    st.set_file(mp4, FileStatus.UPLOADING, stage="upload")
    st.set_file(mkv, FileStatus.DONE, final_path=mkv, mode="full_gamut")

    purged = _purge_stale_source_path(mp4, mkv, st)

    assert purged is True
    # media_report: only the .mkv survives.
    assert _report_paths() == [mkv]
    # state: the dead .mp4 row is gone; the .mkv DONE row stays.
    assert st.get_file(mp4) is None
    mkv_row = st.get_file(mkv)
    assert mkv_row is not None and mkv_row["status"] == FileStatus.DONE.value


def test_purge_noop_when_path_unchanged(tmp_path, tmp_report_paths):
    """A normal .mkv→.mkv re-encode (final_path == filepath) must touch nothing."""
    mkv = str(tmp_path / "Inception (2010).mkv")
    Path(mkv).write_bytes(b"x")  # the file stays at the same path on disk
    _seed_report(mkv, mkv)  # single entry under the one path

    st = PipelineState(str(tmp_path / "state.db"))
    st.set_file(mkv, FileStatus.DONE, final_path=mkv, mode="full_gamut")

    purged = _purge_stale_source_path(mkv, mkv, st)

    assert purged is False
    assert mkv in _report_paths()
    assert st.get_file(mkv) is not None


def test_purge_noop_when_original_still_on_disk(tmp_path, tmp_report_paths):
    """If the original path still exists (e.g. a skipped backup rename left it
    in place), leave its records for the scanner — never drop a live file's
    entry (rule 8: probe before touching)."""
    mp4 = str(tmp_path / "Thief (1981).mp4")
    mkv = str(tmp_path / "Thief (1981).mkv")
    Path(mp4).write_bytes(b"still here")  # original NOT gone — guard must hold
    _seed_report(mp4, mkv)

    st = PipelineState(str(tmp_path / "state.db"))
    st.set_file(mp4, FileStatus.UPLOADING, stage="upload")

    purged = _purge_stale_source_path(mp4, mkv, st)

    assert purged is False
    assert mp4 in _report_paths()
    assert st.get_file(mp4) is not None


def test_finalize_upload_keys_done_on_final_path_and_purges():
    """Wiring: finalize_upload keys the DONE row on final_path (via done_key)
    and runs the purge AFTER that transition. If DONE were keyed on the raw
    filepath, or the purge ran before DONE, the stale entry would survive a
    crash window between the two writes."""
    import pipeline.full_gamut as fg

    src = inspect.getsource(fg.finalize_upload)

    assert "done_key = final_path if renamed else filepath" in src, (
        "DONE must be keyed on final_path when the encode renamed the file, "
        "not on the now-dead original path."
    )
    assert "done_key," in src, "the DONE state.set_file must use done_key as its filepath arg"

    done_idx = src.find("FileStatus.DONE")
    purge_idx = src.find("_purge_stale_source_path(filepath, final_path, state)")
    assert done_idx >= 0, "DONE transition not found"
    assert purge_idx >= 0, "purge call not found in finalize_upload"
    assert done_idx < purge_idx, (
        "the stale-path purge must run AFTER the DONE transition so a crash "
        "between them can never leave the real file un-recorded."
    )
