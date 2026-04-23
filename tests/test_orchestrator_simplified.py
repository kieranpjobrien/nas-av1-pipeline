"""Regression tests for the simplified 3-worker-type orchestrator layout.

Post-simplification shape:

    GPU workers  — one per NVENC slot, each encode + upload inline.
    Fetch worker — one SMB fetcher (default fetch_concurrency=1).
    Gap filler   — one SSH heavy worker (SERVER only) + one local quick worker.

Gone: separate upload thread, NAS-vs-SERVER branching, force stack,
per-file profile / gentle / reencode overrides.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from pipeline.control import PipelineControl
from pipeline.orchestrator import Orchestrator
from pipeline.state import PipelineState


def _bare_orchestrator(tmp_path) -> Orchestrator:
    """Build an Orchestrator wired up to temp state + control, no signals live."""
    state = PipelineState(str(tmp_path / "state.db"))
    control = PipelineControl(str(tmp_path))
    config = {"gpu_concurrency": 1, "fetch_concurrency": 1}
    # signal.signal doesn't work in pytest worker threads; patch it out.
    with patch("pipeline.orchestrator.signal") as fake_signal:
        fake_signal.SIGTERM = 0
        fake_signal.SIGINT = 0
        fake_signal.signal = MagicMock()
        orch = Orchestrator(config, state, str(tmp_path), control)
    return orch


class TestGpuWorkerInlineUpload:
    """GPU worker calls finalize_upload inline — no separate upload thread."""

    def test_finalize_upload_called_after_full_gamut_success(self, tmp_path, monkeypatch):
        """When full_gamut returns True, finalize_upload runs in the same thread."""
        orch = _bare_orchestrator(tmp_path)

        # Queue one item; mark pre-fetched so no fetch loop is involved.
        item = {
            "filepath": str(tmp_path / "fake.mkv"),
            "filename": "fake.mkv",
            "file_size_bytes": 0,
            "video": {},
        }
        (tmp_path / "fake.mkv").write_bytes(b"0")
        queue = [item]

        # Track call order: full_gamut first, finalize_upload second.
        calls: list[str] = []

        def fake_full_gamut(fp, it, cfg, st, staging):
            calls.append("full_gamut")
            return True

        def fake_finalize_upload(fp, st, cfg):
            calls.append("finalize_upload")
            return True

        monkeypatch.setattr("pipeline.orchestrator.full_gamut", fake_full_gamut)
        monkeypatch.setattr("pipeline.orchestrator.finalize_upload", fake_finalize_upload)

        # Ask the GPU worker to shut down as soon as it finishes the one item.
        def one_pass():
            orch._gpu_worker(queue, [], worker_id=0)

        # Race: set shutdown after the worker enters the loop to bail after one item.
        import threading

        def stop_soon():
            import time
            time.sleep(0.3)
            orch._shutdown.set()

        threading.Thread(target=stop_soon, daemon=True).start()
        one_pass()

        # Both must be called, and full_gamut must come before finalize_upload.
        assert "full_gamut" in calls
        assert "finalize_upload" in calls
        assert calls.index("full_gamut") < calls.index("finalize_upload")

    def test_finalize_upload_skipped_when_full_gamut_fails(self, tmp_path, monkeypatch):
        """If full_gamut returns False, finalize_upload MUST NOT be called."""
        orch = _bare_orchestrator(tmp_path)

        item = {
            "filepath": str(tmp_path / "bad.mkv"),
            "filename": "bad.mkv",
            "file_size_bytes": 0,
            "video": {},
        }
        (tmp_path / "bad.mkv").write_bytes(b"0")
        queue = [item]

        finalize_called: list[bool] = []
        monkeypatch.setattr("pipeline.orchestrator.full_gamut", lambda *a, **kw: False)
        monkeypatch.setattr(
            "pipeline.orchestrator.finalize_upload",
            lambda *a, **kw: finalize_called.append(True) or True,
        )

        import threading

        def stop_soon():
            import time
            time.sleep(0.3)
            orch._shutdown.set()

        threading.Thread(target=stop_soon, daemon=True).start()
        orch._gpu_worker(queue, [], worker_id=0)

        assert finalize_called == []


class TestGapFillerSingleHeavyWorker:
    """Gap filler spawns one SSH heavy worker + one quick worker, not 3 SRV + 2 NAS + 2 quick."""

    def test_startup_log_reports_single_heavy_plus_quick(self, tmp_path, monkeypatch, caplog):
        """The gap_filler_worker startup log should mention just one heavy + one quick."""
        orch = _bare_orchestrator(tmp_path)

        # Shut down immediately so the workers exit on the first iteration.
        orch._shutdown.set()

        # Force SERVER host to be set so the heavy worker thread is spawned (and exits).
        monkeypatch.setattr("pipeline.nas_worker.SERVER", {"host": "test-srv", "label": "SRV"})

        with caplog.at_level(logging.INFO):
            orch._gap_filler_worker([])  # empty queue → nothing to do

        # Collect messages about worker skip/start. The key thing we assert is that
        # the NAS-workers line is NOT in the log — only SERVER / heavy.
        nas_mentions = [r for r in caplog.records if "NAS workers" in r.getMessage()]
        assert nas_mentions == [], "Simplified gap filler should not spawn NAS-specific workers"

    def test_heavy_worker_skipped_when_server_not_configured(self, tmp_path, monkeypatch, caplog):
        """No SERVER_SSH_HOST -> no heavy worker thread, just the quick worker."""
        orch = _bare_orchestrator(tmp_path)
        orch._shutdown.set()
        monkeypatch.setattr("pipeline.nas_worker.SERVER", {"host": "", "label": "SRV"})

        with caplog.at_level(logging.INFO):
            orch._gap_filler_worker([])

        msgs = [r.getMessage() for r in caplog.records]
        assert any("Heavy worker skipped" in m for m in msgs)


class TestSkipListHonoured:
    """skip.json still filters the queue after the priority/force drop."""

    def test_skip_list_removes_entry_from_apply_queue_overrides(self, tmp_path):
        """PipelineControl.apply_queue_overrides respects skip.json."""
        import json as _json

        ctrl = PipelineControl(str(tmp_path))
        # Seed skip.json with one path.
        skip_path = tmp_path / "control" / "skip.json"
        _json.dump({"paths": [r"\\NAS\Skipped.mkv"]}, open(skip_path, "w"))
        # Bust the read cache.
        ctrl._last_read.pop(str(skip_path), None)

        queue = [
            {"filepath": r"\\NAS\Keep.mkv", "file_size_bytes": 1},
            {"filepath": r"\\NAS\Skipped.mkv", "file_size_bytes": 2},
            {"filepath": r"\\NAS\AlsoKeep.mkv", "file_size_bytes": 3},
        ]
        filtered = ctrl.apply_queue_overrides(queue)
        paths = [e["filepath"] for e in filtered]
        assert r"\\NAS\Skipped.mkv" not in paths
        assert r"\\NAS\Keep.mkv" in paths
        assert r"\\NAS\AlsoKeep.mkv" in paths


class TestPipelineControlSurfaceArea:
    """After the drop, PipelineControl exposes only skip + pause checks."""

    def test_removed_methods_are_gone(self, tmp_path):
        """The force/priority/reencode/gentle/profile methods no longer exist."""
        ctrl = PipelineControl(str(tmp_path))
        for method in (
            "push_force_item",
            "remove_force_item",
            "get_force_items",
            "get_priority_bumps",
            "get_priority_patterns",
            "is_priority",
            "is_force",
            "get_reencode_list",
            "get_reencode_override",
            "remove_reencode",
            "get_gentle_override",
            "get_quality_profile",
        ):
            assert not hasattr(ctrl, method), f"{method} should be removed"

    def test_kept_methods_still_there(self, tmp_path):
        """skip + pause plumbing survives."""
        ctrl = PipelineControl(str(tmp_path))
        for method in (
            "should_skip",
            "is_fetch_paused",
            "is_encode_paused",
            "check_pause",
            "apply_queue_overrides",
        ):
            assert callable(getattr(ctrl, method)), f"{method} should still exist"


class TestConfigDefaults:
    """Defaults post-simplification."""

    def test_priority_tiers_dropped(self):
        """DEFAULT_CONFIG no longer carries priority_tiers."""
        from pipeline.config import DEFAULT_CONFIG

        assert "priority_tiers" not in DEFAULT_CONFIG

    def test_fetch_concurrency_default_is_one(self):
        """One SMB fetch saturates the link; default dropped from 2 to 1."""
        from pipeline.config import DEFAULT_CONFIG

        assert DEFAULT_CONFIG["fetch_concurrency"] == 1

    def test_quality_profiles_dropped(self):
        """QUALITY_PROFILES is no longer exported from pipeline.config."""
        import pipeline.config as cfg

        assert not hasattr(cfg, "QUALITY_PROFILES")
