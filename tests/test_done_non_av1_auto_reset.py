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

import os
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


# ---------------------------------------------------------------------------
# 2026-06-01: ffprobe verification before trusting report's stale codec data
# ---------------------------------------------------------------------------


def test_done_with_stale_report_codec_does_not_reset_when_ffprobe_says_av1(tmp_path, monkeypatch):
    """The 2026-06-01 incident: media_report had codec_raw='hevc' on 15
    DONE rows where ffprobe confirmed the file was actually AV1 (the
    report was stale post-re-encode). My auto-reset rule wrongly kicked
    them back into the queue → fetch/encode failed → 15 spurious error
    rows in the dashboard.

    Fix: ffprobe the file before trusting the report. When the report's
    codec disagrees with ffprobe, ffprobe wins (it's the live truth)."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\NAS\Movies\StaleReport\StaleReport.mkv"

    state.set_file(fp, FileStatus.DONE, reason="real prior encode")
    entry = _entry(fp, codec_raw="hevc")  # report claims hevc (STALE)

    # Stub ffprobe to return av1 (the truth)
    from pipeline import __main__ as main_mod
    monkeypatch.setattr(main_mod, "_ffprobe_video_codec", lambda fp, **kw: "av1")

    cat, _ = categorise_entry(entry, {}, state, control)
    assert cat == "skip", (
        f"stale report codec must NOT trigger reset when ffprobe confirms "
        f"the file is already AV1; got cat={cat!r}"
    )
    # Status should be unchanged
    assert state.get_file(fp)["status"] == "done"


def test_done_with_report_codec_hevc_resets_when_ffprobe_confirms_hevc(tmp_path, monkeypatch):
    """The genuine case: report says hevc AND ffprobe confirms hevc.
    The reset should still fire — both agree the file needs work."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\NAS\Movies\GenuineHevc\GenuineHevc.mkv"

    state.set_file(fp, FileStatus.DONE, reason="hevc-stale-DONE class")
    entry = _entry(fp, codec_raw="hevc")

    from pipeline import __main__ as main_mod
    monkeypatch.setattr(main_mod, "_ffprobe_video_codec", lambda fp, **kw: "hevc")

    cat, _ = categorise_entry(entry, {}, state, control)
    assert cat == "full_gamut", (
        f"genuine hevc DONE must reset for re-encode; got cat={cat!r}"
    )
    row = state.get_file(fp)
    assert row["status"] == "pending"
    assert row.get("force_reencode") is True


def test_done_ffprobe_failure_falls_back_to_report(tmp_path, monkeypatch):
    """If ffprobe can't read the file (returns None), trust the report.
    Conservative: don't skip-reset just because the probe failed —
    that would mask genuine codec mismatches."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\NAS\Movies\ProbeFail\ProbeFail.mkv"

    state.set_file(fp, FileStatus.DONE, reason="probe-fail class")
    entry = _entry(fp, codec_raw="h264")

    from pipeline import __main__ as main_mod
    monkeypatch.setattr(main_mod, "_ffprobe_video_codec", lambda fp, **kw: None)

    cat, _ = categorise_entry(entry, {}, state, control)
    # Probe failed → trust report → h264 → reset to pending
    assert cat == "full_gamut"


# ---------------------------------------------------------------------------
# 2026-06-01: force_reencode on a DONE row must survive terminal-skip + prune
# (the "7 AV1 re-encodes keep vanishing from priority" bug)
# ---------------------------------------------------------------------------


def test_done_av1_with_force_reencode_routes_to_full_gamut(tmp_path, monkeypatch):
    """A DONE AV1 row with force_reencode=true (operator prioritised it for
    a colour-tag / black-level / CQ re-encode) must route to full_gamut,
    NOT skip. Pre-fix the terminal-skip block returned skip before the
    priority/force routing ever ran."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\NAS\Movies\DoneAV1\DoneAV1.mkv"

    state.set_file(fp, FileStatus.DONE, force_reencode=True)
    entry = _entry(fp, codec_raw="av1")

    from pipeline import __main__ as main_mod
    monkeypatch.setattr(main_mod, "_ffprobe_video_codec", lambda fp, **kw: "av1")

    cat, item = categorise_entry(entry, {}, state, control, priority_paths={fp})
    assert cat == "full_gamut", (
        f"done AV1 + force_reencode must re-encode; got {cat!r}"
    )


def test_done_av1_without_force_reencode_still_skips(tmp_path, monkeypatch):
    """Negative control: a plain DONE AV1 row (no force_reencode) must
    still skip — we don't want to re-encode every completed file."""
    state = _state(tmp_path)
    control = _control(tmp_path)
    fp = r"\NAS\Movies\PlainDone\PlainDone.mkv"

    state.set_file(fp, FileStatus.DONE)
    entry = _entry(fp, codec_raw="av1")

    from pipeline import __main__ as main_mod
    monkeypatch.setattr(main_mod, "_ffprobe_video_codec", lambda fp, **kw: "av1")

    cat, _ = categorise_entry(entry, {}, state, control)
    assert cat == "skip", f"plain done AV1 must skip; got {cat!r}"


def test_prune_keeps_done_with_force_reencode(tmp_path):
    """_prune_done_from_priority must KEEP a DONE row that has
    force_reencode=true — it's an active re-encode request, not a
    completed item. Pre-fix the prune removed it within 10s, which is
    why the 7 AV1 re-encodes kept vanishing from priority.json."""
    import json as _json
    from pipeline.__main__ import _prune_done_from_priority

    state = _state(tmp_path)
    os.makedirs(tmp_path / "control", exist_ok=True)
    fp = r"\NAS\Movies\DoneAV1\DoneAV1.mkv"
    state.set_file(fp, FileStatus.DONE, force_reencode=True)

    prio = tmp_path / "control" / "priority.json"
    prio.write_text(_json.dumps({"paths": [fp], "force": [], "patterns": []}))

    removed = _prune_done_from_priority(staging_dir=str(tmp_path), state=state)
    after = _json.loads(prio.read_text())
    assert removed == 0, f"force_reencode row must NOT be pruned; removed={removed}"
    assert fp in after["paths"]


def test_prune_removes_plain_done(tmp_path):
    """Negative control: a plain DONE row (no force_reencode) must still
    be pruned — that's the legitimate self-cleaning behaviour."""
    import json as _json
    from pipeline.__main__ import _prune_done_from_priority

    state = _state(tmp_path)
    os.makedirs(tmp_path / "control", exist_ok=True)
    fp = r"\NAS\Movies\PlainDone\PlainDone.mkv"
    state.set_file(fp, FileStatus.DONE)

    prio = tmp_path / "control" / "priority.json"
    prio.write_text(_json.dumps({"paths": [fp], "force": [], "patterns": []}))

    removed = _prune_done_from_priority(staging_dir=str(tmp_path), state=state)
    after = _json.loads(prio.read_text())
    assert removed == 1, f"plain done row must be pruned; removed={removed}"
    assert fp not in after["paths"]
