"""Regression tests for the orchestrator's mid-session queue refresh.

The user expects: a Sonarr/Radarr drop-in that's smaller than anything left
in the queue should jump to the top (smallest-first) and become the next
item the fetch worker pulls — without waiting for a pipeline restart. These
tests cover ``Orchestrator._merge_new_files`` and ``categorise_entry``.

Tests deliberately exercise ``_merge_new_files`` directly rather than the
full ``_refresh_worker`` thread — the worker is just a polling loop on
top of merge; verifying merge correctness covers the meaningful logic.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from pipeline.__main__ import categorise_entry
from pipeline.control import PipelineControl
from pipeline.orchestrator import Orchestrator
from pipeline.state import FileStatus, PipelineState


def _orch(tmp_path) -> Orchestrator:
    state = PipelineState(str(tmp_path / "state.db"))
    control = PipelineControl(str(tmp_path))
    config = {
        "gpu_concurrency": 1,
        "fetch_concurrency": 1,
        "queue_refresh_interval_secs": 0,  # never start the polling loop in tests
    }
    with patch("pipeline.orchestrator.signal") as fake_signal:
        fake_signal.SIGTERM = 0
        fake_signal.SIGINT = 0
        fake_signal.signal = MagicMock()
        return Orchestrator(config, state, str(tmp_path), control)


def _h264_entry(filepath: str, size_bytes: int, filename: str | None = None) -> dict:
    return {
        "filepath": filepath,
        "filename": filename or filepath.rsplit("/", 1)[-1].rsplit("\\", 1)[-1],
        "library_type": "movie",
        "video": {"codec": "H.264", "codec_raw": "h264", "resolution_class": "1080p"},
        "audio_streams": [{"codec_raw": "eac3", "channels": 6, "language": "eng"}],
        "subtitle_streams": [],
        "file_size_bytes": size_bytes,
        "duration_seconds": 6000,
        "overall_bitrate_kbps": 2700,
    }


def _av1_entry(filepath: str, size_bytes: int) -> dict:
    return {
        "filepath": filepath,
        "filename": filepath.rsplit("/", 1)[-1].rsplit("\\", 1)[-1],
        "library_type": "movie",
        "video": {"codec": "AV1", "codec_raw": "av1", "resolution_class": "1080p"},
        # gap_filler kicks in only if there's something to do — give it a
        # non-EAC3 lossless track so analyse_gaps reports needs_audio_transcode.
        "audio_streams": [{"codec_raw": "flac", "channels": 6, "language": "eng"}],
        "subtitle_streams": [],
        "file_size_bytes": size_bytes,
        "duration_seconds": 6000,
        "overall_bitrate_kbps": 1800,
    }


def _write_report(tmp_path, files: list[dict]):
    p = tmp_path / "media_report.json"
    p.write_text(json.dumps({"generated": "test", "files": files}), encoding="utf-8")
    return str(p)


class TestCategoriseEntry:
    def test_h264_lands_in_full_gamut(self, tmp_path):
        orch = _orch(tmp_path)
        entry = _h264_entry(r"\\NAS\Movies\A.mkv", 2_000_000_000)
        category, item = categorise_entry(entry, {}, orch.state, orch.control)
        assert category == "full_gamut"
        assert item["filepath"] == r"\\NAS\Movies\A.mkv"

    def test_av1_with_gaps_lands_in_gap_filler(self, tmp_path):
        from pipeline.config import build_config

        orch = _orch(tmp_path)
        entry = _av1_entry(r"\\NAS\Movies\A.mkv", 2_000_000_000)
        category, item = categorise_entry(entry, build_config(), orch.state, orch.control)
        assert category == "gap_filler"
        assert item is entry  # gap filler keeps the original entry shape

    def test_terminal_status_is_skipped(self, tmp_path):
        orch = _orch(tmp_path)
        fp = r"\\NAS\Movies\Already.mkv"
        orch.state.set_file(fp, FileStatus.DONE)
        entry = _h264_entry(fp, 2_000_000_000)
        category, _ = categorise_entry(entry, {}, orch.state, orch.control)
        assert category == "skip"

    def test_unprobeable_flagged_corrupt(self, tmp_path):
        orch = _orch(tmp_path)
        fp = r"\\NAS\Movies\Broken.mkv"
        entry = {
            "filepath": fp,
            "filename": "Broken.mkv",
            "library_type": "movie",
            "video": {"codec_raw": None},
            "audio_streams": [],
            "subtitle_streams": [],
            "file_size_bytes": 4_000_000_000,
        }
        category, item = categorise_entry(entry, {}, orch.state, orch.control)
        assert category == "skip"
        assert item is None
        assert orch.state.get_file(fp)["status"] == FileStatus.FLAGGED_CORRUPT.value

    def test_av1_with_force_reencode_lands_in_full_gamut(self, tmp_path):
        """Pin the 2026-05-07 fix: when the dashboard's "Queue re-encode"
        button stamps ``force_reencode=true`` on an already-AV1 row, the
        queue builder must route to full_gamut. Pre-fix, AV1 files were
        unconditionally routed to gap_filler (audio strip / sub stamp
        only) or skip — so the user's queue action was a silent no-op.
        """
        from pipeline.config import build_config

        orch = _orch(tmp_path)
        fp = r"\\NAS\Movies\GradeNonOptimal.mkv"
        # Already encoded — would normally be skipped by the AV1 branch.
        # Compliant audio (eac3 5.1, eng) so analyse_gaps reports nothing
        # to do — the only thing keeping this file in the queue is the
        # force_reencode flag we set below.
        entry = {
            "filepath": fp,
            "filename": "GradeNonOptimal.mkv",
            "library_type": "movie",
            "video": {"codec": "AV1", "codec_raw": "av1", "resolution_class": "1080p"},
            "audio_streams": [{"codec_raw": "eac3", "channels": 6, "language": "eng"}],
            "subtitle_streams": [],
            "file_size_bytes": 2_000_000_000,
            "duration_seconds": 6000,
            "overall_bitrate_kbps": 1800,
        }
        # Simulate the requeue endpoint: status=pending + force_reencode=true.
        orch.state.set_file(fp, FileStatus.PENDING, force_reencode=True)

        category, item = categorise_entry(entry, build_config(), orch.state, orch.control)
        assert category == "full_gamut", "AV1 + force_reencode must route to full_gamut"
        assert item["filepath"] == fp
        assert item["video_codec"] == "AV1"  # codec carried through

    def test_av1_without_force_reencode_never_full_gamut(self, tmp_path):
        """Sanity: a pending-status AV1 row WITHOUT force_reencode must NOT
        land in full_gamut. The flag is the only thing that overrides the
        AV1 codec branch — without it, the existing gap_filler / skip
        path applies regardless of pending status.
        """
        from pipeline.config import build_config

        orch = _orch(tmp_path)
        # _av1_entry triggers gap_filler (flac audio); use it because the
        # exact downstream category doesn't matter — what matters is that
        # full_gamut is never the answer for AV1 without the flag.
        entry = _av1_entry(r"\\NAS\Movies\NoFlag.mkv", 2_000_000_000)
        orch.state.set_file(entry["filepath"], FileStatus.PENDING)

        category, _ = categorise_entry(entry, build_config(), orch.state, orch.control)
        assert category != "full_gamut", (
            "AV1 file with status=pending must not fall into full_gamut "
            "without an explicit force_reencode flag"
        )

    def test_force_reencode_overrides_gap_filler(self, tmp_path):
        """If a file has BOTH a force_reencode flag AND gap-filler-eligible
        gaps (e.g. a non-EAC-3 audio stream), force_reencode wins. The
        full encode rebuilds audio/subs from scratch, so doing a separate
        gap_filler pass first would just be wasted I/O.
        """
        from pipeline.config import build_config

        orch = _orch(tmp_path)
        # _av1_entry has flac audio → analyse_gaps reports needs_audio_transcode.
        entry = _av1_entry(r"\\NAS\Movies\GapAndForce.mkv", 2_000_000_000)
        orch.state.set_file(entry["filepath"], FileStatus.PENDING, force_reencode=True)

        category, item = categorise_entry(entry, build_config(), orch.state, orch.control)
        assert category == "full_gamut"
        assert item["filepath"] == entry["filepath"]

    def test_full_gamut_item_carries_tmdb(self, tmp_path):
        """Pin the 2026-05-08 grade-mismatch fix: the queue item must
        carry the tmdb blob through to the encoder. Without it,
        resolve_encode_params calls derive_grade(item) and gets
        GRADE_DEFAULT for everything (including blockbuster /
        sitcom / animation), so re-encodes use the wrong target CQ.
        """
        from pipeline.config import build_config

        orch = _orch(tmp_path)
        entry = {
            "filepath": r"\\NAS\Movies\Avengers - Infinity War.mkv",
            "filename": "Avengers - Infinity War.mkv",
            "library_type": "movie",
            "video": {"codec": "AV1", "codec_raw": "av1", "resolution_class": "4K", "hdr": True},
            "audio_streams": [{"codec_raw": "eac3", "channels": 6, "language": "eng"}],
            "subtitle_streams": [],
            "file_size_bytes": 33_000_000_000,
            "duration_seconds": 9000,
            "overall_bitrate_kbps": 30000,
            "tmdb": {
                "tmdb_id": 299536,
                "original_language": "en",
                "release_date": "2018-04-25",
                "runtime": 149,
                "genres": [{"name": "Action"}, {"name": "Adventure"}, {"name": "Science Fiction"}],
                "keywords": ["superhero", "marvel comic", "based on comic"],
            },
        }
        # Mark force_reencode=true so the AV1 file actually goes to full_gamut.
        orch.state.set_file(entry["filepath"], FileStatus.PENDING, force_reencode=True)

        category, item = categorise_entry(entry, build_config(), orch.state, orch.control)
        assert category == "full_gamut"
        assert "tmdb" in item, "queue item must carry tmdb forward"
        assert item["tmdb"].get("original_language") == "en"
        assert item["tmdb"].get("keywords") == ["superhero", "marvel comic", "based on comic"]

    def test_resolve_encode_params_uses_tmdb_for_grade(self, tmp_path):
        """End-to-end of patch 1: the queue item's tmdb makes its way
        through resolve_encode_params -> target_cq -> derive_grade
        and produces the correct grade for the encoder, not 'default'.
        """
        from pipeline.config import build_config, resolve_encode_params

        orch = _orch(tmp_path)
        entry = {
            "filepath": r"\\NAS\Movies\Avengers - Infinity War.mkv",
            "filename": "Avengers - Infinity War.mkv",
            "library_type": "movie",
            "video": {"codec": "AV1", "codec_raw": "av1", "resolution_class": "4K", "hdr": True},
            "audio_streams": [{"codec_raw": "eac3", "channels": 6, "language": "eng"}],
            "subtitle_streams": [],
            "file_size_bytes": 33_000_000_000,
            "duration_seconds": 9000,
            "overall_bitrate_kbps": 30000,
            "tmdb": {
                "original_language": "en",
                "release_date": "2018-04-25",
                "runtime": 149,
                "genres": [{"name": "Action"}, {"name": "Adventure"}, {"name": "Science Fiction"}],
                "keywords": ["superhero", "marvel comic", "based on comic"],
            },
        }
        orch.state.set_file(entry["filepath"], FileStatus.PENDING, force_reencode=True)
        cfg = build_config()
        category, item = categorise_entry(entry, cfg, orch.state, orch.control)
        assert category == "full_gamut"

        params = resolve_encode_params(cfg, item)
        # 4K_HDR base CQ is 22; blockbuster grade adds +3 -> 25.
        assert params["content_grade"] == "blockbuster", (
            f"expected blockbuster grade, got {params['content_grade']!r} — "
            f"item is missing tmdb data the grader needs"
        )
        assert params["cq"] == 25, f"expected cq=25 (base 22 + 3 offset), got {params['cq']}"
        assert params["base_cq"] == 22
        assert params["cq_offset"] == 3


class TestPriorityBump:
    """The 2026-05-08 priority-paths bump replaces the removed force-stack
    mechanism. Items whose filepath is listed in control/priority.json's
    ``paths`` field jump to the front of the full_gamut queue regardless
    of size; everything else stays in the configured size order."""

    def test_priority_paths_lifted_to_front(self):
        """Sorted output: priority items first (size order within), then
        the rest in size order."""
        from pipeline.__main__ import _sort_full_gamut

        queue = [
            {"filepath": "A", "file_size_bytes": 5_000_000_000},
            {"filepath": "B", "file_size_bytes": 1_000_000_000},  # priority
            {"filepath": "C", "file_size_bytes": 10_000_000_000},
            {"filepath": "D", "file_size_bytes": 2_000_000_000},  # priority
        ]
        _sort_full_gamut(queue, {"encode_queue_order": "largest_first"}, {"B", "D"})
        assert [it["filepath"] for it in queue] == ["D", "B", "C", "A"], (
            "expected priority items first (D=2GB then B=1GB largest-first within priority), "
            "then non-priority by size (C=10GB then A=5GB)"
        )

    def test_no_priority_paths_falls_back_to_size_only(self):
        """Sanity: empty priority set leaves the standard size-only sort."""
        from pipeline.__main__ import _sort_full_gamut

        queue = [
            {"filepath": "A", "file_size_bytes": 5_000_000_000},
            {"filepath": "B", "file_size_bytes": 1_000_000_000},
            {"filepath": "C", "file_size_bytes": 10_000_000_000},
        ]
        _sort_full_gamut(queue, {"encode_queue_order": "largest_first"}, set())
        assert [it["filepath"] for it in queue] == ["C", "A", "B"]

    def test_smallest_first_order_preserved(self):
        """The encode_queue_order=smallest_first override still works
        within both the priority and non-priority groups."""
        from pipeline.__main__ import _sort_full_gamut

        queue = [
            {"filepath": "A", "file_size_bytes": 5_000_000_000},  # priority
            {"filepath": "B", "file_size_bytes": 1_000_000_000},
            {"filepath": "C", "file_size_bytes": 2_000_000_000},  # priority
            {"filepath": "D", "file_size_bytes": 8_000_000_000},
        ]
        _sort_full_gamut(queue, {"encode_queue_order": "smallest_first"}, {"A", "C"})
        assert [it["filepath"] for it in queue] == ["C", "A", "B", "D"], (
            "smallest_first within priority: C(2GB) then A(5GB); then non-priority B(1GB) then D(8GB)"
        )


class TestMergeNewFiles:
    def test_new_largest_file_lands_at_front_with_default_order(self, tmp_path):
        """Default order is largest-first (set 2026-05-02). A new big file
        joining the queue lands at index 0 so the encoder hits it next."""
        from pipeline.config import build_config

        orch = _orch(tmp_path)
        cfg = build_config()
        orch.config = cfg

        small = _h264_entry(r"\\NAS\Series\Show\S01E01.mkv", 200_000_000)
        med = _h264_entry(r"\\NAS\Movies\Med.mkv", 5_000_000_000)
        full_q: list[dict] = []
        gap_q: list[dict] = []
        for entry in (small, med):
            cat, it = categorise_entry(entry, cfg, orch.state, orch.control)
            assert cat == "full_gamut"
            full_q.append(it)
        full_q.sort(key=lambda x: x["file_size_bytes"], reverse=True)

        # Sonarr drops an 8 GB title. Report now has all three.
        big = _h264_entry(r"\\NAS\Movies\Big.mkv", 8_000_000_000)
        report = _write_report(tmp_path, [big, med, small])

        added_full, added_gap = orch._merge_new_files(full_q, gap_q, report)
        assert added_full == 1
        assert added_gap == 0
        assert len(full_q) == 3
        assert full_q[0]["filepath"] == r"\\NAS\Movies\Big.mkv"
        # Largest-first invariant preserved
        sizes = [item["file_size_bytes"] for item in full_q]
        assert sizes == sorted(sizes, reverse=True)

    def test_smallest_first_order_still_works_when_configured(self, tmp_path):
        """Override path: setting encode_queue_order='smallest_first' falls back
        to the previous burn-through-quick-wins ordering."""
        from pipeline.config import build_config

        orch = _orch(tmp_path)
        cfg = build_config({"encode_queue_order": "smallest_first"})
        orch.config = cfg

        big = _h264_entry(r"\\NAS\Movies\Big.mkv", 8_000_000_000)
        med = _h264_entry(r"\\NAS\Movies\Med.mkv", 5_000_000_000)
        full_q: list[dict] = []
        gap_q: list[dict] = []
        for entry in (big, med):
            cat, it = categorise_entry(entry, cfg, orch.state, orch.control)
            assert cat == "full_gamut"
            full_q.append(it)
        full_q.sort(key=lambda x: x["file_size_bytes"])

        small = _h264_entry(r"\\NAS\Series\Show\S01E01.mkv", 200_000_000)
        report = _write_report(tmp_path, [big, med, small])

        added_full, added_gap = orch._merge_new_files(full_q, gap_q, report)
        assert added_full == 1
        assert full_q[0]["filepath"] == r"\\NAS\Series\Show\S01E01.mkv"
        sizes = [item["file_size_bytes"] for item in full_q]
        assert sizes == sorted(sizes)

    def test_already_known_paths_not_duplicated(self, tmp_path):
        from pipeline.config import build_config

        orch = _orch(tmp_path)
        cfg = build_config()
        orch.config = cfg

        existing = _h264_entry(r"\\NAS\Movies\Existing.mkv", 5_000_000_000)
        full_q = [categorise_entry(existing, cfg, orch.state, orch.control)[1]]

        # Report has the same file (no actual new ones).
        report = _write_report(tmp_path, [existing])
        added_full, added_gap = orch._merge_new_files(full_q, [], report)
        assert added_full == 0
        assert len(full_q) == 1

    def test_terminal_status_files_not_added(self, tmp_path):
        """A new entry in the report whose DB row is already DONE / FLAGGED
        must NOT be appended — they're terminal and should stay out of the
        queue. Otherwise we'd re-encode-stripped Bluey episodes into Swedish-
        loss again, the exact incident the discipline contract was written
        to prevent."""
        from pipeline.config import build_config

        orch = _orch(tmp_path)
        cfg = build_config()
        orch.config = cfg

        fp = r"\\NAS\Movies\Done.mkv"
        orch.state.set_file(fp, FileStatus.DONE)
        entry = _h264_entry(fp, 2_000_000_000)
        report = _write_report(tmp_path, [entry])

        added_full, added_gap = orch._merge_new_files([], [], report)
        assert added_full == 0
        assert added_gap == 0

    def test_skip_list_respected(self, tmp_path):
        from pipeline.config import build_config

        orch = _orch(tmp_path)
        cfg = build_config()
        orch.config = cfg

        # User added the path to the skip list via control file.
        skip_path = r"\\NAS\Movies\Skipped.mkv"
        skip_file = tmp_path / "control" / "skip.json"
        skip_file.parent.mkdir(exist_ok=True)
        skip_file.write_text(json.dumps({"paths": [skip_path]}), encoding="utf-8")

        entry = _h264_entry(skip_path, 2_000_000_000)
        report = _write_report(tmp_path, [entry])
        added_full, _ = orch._merge_new_files([], [], report)
        assert added_full == 0

    def test_merge_runs_both_queues_independently(self, tmp_path):
        """A report with one new H.264 + one new AV1-with-gaps adds to BOTH queues."""
        from pipeline.config import build_config

        orch = _orch(tmp_path)
        cfg = build_config()
        orch.config = cfg

        h264_new = _h264_entry(r"\\NAS\Movies\NewH264.mkv", 4_000_000_000)
        av1_new = _av1_entry(r"\\NAS\Movies\NewAV1.mkv", 3_000_000_000)
        report = _write_report(tmp_path, [h264_new, av1_new])

        added_full, added_gap = orch._merge_new_files([], [], report)
        assert added_full == 1
        assert added_gap == 1
