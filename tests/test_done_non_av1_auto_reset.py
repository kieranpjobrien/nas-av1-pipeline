"""Pin the 2026-05-24 DONE-consistency auto-reset.

Background: 28 state-DB rows on 2026-05-24 morning were marked DONE
but the file on disk was h264/hevc — not AV1. Three causes mixed in:

  * priority-API auto-seed (server/routers/pipeline.py) inserted
    rows as 'pending' with force_reencode=true; they transitioned
    to DONE without an actual encode. The 'auto-seeded by priority
    API' reason persisted because state.set_file's stale-reason
    scrub only fires on failure-flavoured keywords.
  * Sonarr / qbittorrent / manual restore replaced our AV1 output
    post-encode (file mtime hours/days after the DONE timestamp).
  * cq_resync flipped some DONE rows around the silent-DONE bug
    period without verifying the file was actually re-encoded.

Pre-fix the categoriser's terminal-skip block returned 'skip' for
ANY DONE/REPLACED row — those files stayed on the done list forever
in the dashboard even though they were h264/hevc on disk. The user
saw 'H.264 + done' rows in Library and was rightly furious.

Post-fix: DONE/REPLACED + on-disk codec != AV1 triggers auto-reset
to PENDING with force_reencode=true so the pipeline picks the file
up and encodes it properly. AV1 DONE rows are left alone.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.__main__ import categorise_entry
from pipeline.control import PipelineControl
from pipeline.state import FileStatus, PipelineState


def _state(tmp_path) -> PipelineState:
    return PipelineState(str(tmp_path / "state.db"))


def _control(tmp_path) -> PipelineControl:
    return PipelineControl(str(tmp_path))


def _entry(filepath: str, codec_raw: str = "h264") -> dict:
    return {
        "filepath": filepath,
        "filename": filepath.split("\\")[-1],
        "library_type": "movie",
        "file_size_bytes": 5_000_000_000,
        "file_mtime": 1_700_000_000.0,
        "video": {"codec_raw": codec_raw},
        "audio_streams": [{"codec_raw": "eac3", "language": "eng", "channels": 6}],
        "subtitle_streams": [],
        "tmdb": {"original_language": "en", "title": "Test"},
    }


def test_done_h264_resets_to_pending(tmp_path):
    """The Gilmore Girls class: state says DONE but on-disk codec is h264.
    Must auto-reset to PENDING + force_reencode=true and route to full_gamut."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Series\Gilmore Girls\Season 1\Gilmore Girls S01E02.mkv"

    state.set_file(fp, FileStatus.DONE, reason="auto-seeded by priority API")
    entry = _entry(fp, codec_raw="h264")

    cat, item = categorise_entry(entry, {}, state, control)

    assert cat == "full_gamut", (
        f"non-AV1 DONE row must be re-routed for encode; got cat={cat!r}"
    )
    row = state.get_file(fp)
    assert row["status"] == "pending"
    assert row.get("force_reencode") is True, (
        "auto-reset must stamp force_reencode=true so the full_gamut AV1 "
        "guard doesn't block the re-encode"
    )


def test_done_hevc_also_resets(tmp_path):
    """Same class for HEVC sources (Dune, Killers of the Flower Moon, etc.)."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Movies\Dune (2021)\Dune (2021).mkv"

    state.set_file(fp, FileStatus.DONE, reason="already compliant")
    entry = _entry(fp, codec_raw="hevc")

    cat, _ = categorise_entry(entry, {}, state, control)
    assert cat == "full_gamut"


def test_done_av1_stays_done(tmp_path):
    """The genuine compliant case: state says DONE and on-disk codec IS av1.
    Must NOT auto-reset — that would re-encode every successfully done file."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Series\Bluey\Season 1\Bluey S01E11 Bike.mkv"

    state.set_file(fp, FileStatus.DONE, reason="compression ratio 35.2%")
    entry = _entry(fp, codec_raw="av1")

    cat, _ = categorise_entry(entry, {}, state, control)
    # AV1 entries are handled by the av1 branch below the terminal-skip;
    # they should return "skip" because AV1 + no audit + no priority +
    # no other gaps = compliant.
    assert cat == "skip", (
        f"AV1 DONE must stay DONE; got cat={cat!r}"
    )


def test_flagged_manual_stays_skipped(tmp_path):
    """flagged_manual is the user's explicit park button — must still
    require manual clearing, NOT auto-reset on codec mismatch."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\\NAS\Movies\ManualPark\ManualPark.mkv"

    state.set_file(fp, FileStatus.FLAGGED_MANUAL, reason="parked by user")
    entry = _entry(fp, codec_raw="h264")

    cat, _ = categorise_entry(entry, {}, state, control)
    assert cat == "skip"
    assert state.get_file(fp)["status"] == "flagged_manual"
