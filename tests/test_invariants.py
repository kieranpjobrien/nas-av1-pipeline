"""Tests for tools/invariants.py and the /api/health-deep endpoint."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

import psutil
import pytest

from tools import invariants as inv


def _seed_state_db(db_path: str, rows: list[dict[str, Any]]) -> None:
    """Create a pipeline_files table and insert seed rows."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_files (
                filepath TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'pending',
                mode TEXT DEFAULT 'full_gamut',
                added TEXT,
                last_updated TEXT,
                tier TEXT,
                local_path TEXT,
                output_path TEXT,
                dest_path TEXT,
                error TEXT,
                stage TEXT,
                reason TEXT,
                res_key TEXT,
                extras TEXT DEFAULT '{}'
            )
            """
        )
        for r in rows:
            cols = list(r.keys())
            placeholders = ", ".join(["?"] * len(cols))
            conn.execute(
                f"INSERT OR REPLACE INTO pipeline_files ({', '.join(cols)}) VALUES ({placeholders})",
                [r[c] for c in cols],
            )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture()
def patched_paths(tmp_path, monkeypatch):
    """Redirect invariants' MEDIA_REPORT / PIPELINE_STATE_DB / STAGING_DIR into tmp."""
    staging = tmp_path / "staging"
    staging.mkdir()
    report = staging / "media_report.json"
    db = staging / "pipeline_state.db"
    monkeypatch.setattr(inv, "MEDIA_REPORT", report)
    monkeypatch.setattr(inv, "PIPELINE_STATE_DB", db)
    monkeypatch.setattr(inv, "STAGING_DIR", staging)
    return staging, report, db


class TestNoAudiolessAv1:
    """AV1 entries missing audio_streams must be flagged."""

    def test_catches_damage(self, patched_paths):
        _, report, _ = patched_paths
        data = {
            "files": [
                {
                    "filepath": r"\\KieranNAS\Media\Movies\Bad.mkv",
                    "video": {"codec_raw": "av1"},
                    "audio_streams": [],
                },
                {
                    "filepath": r"\\KieranNAS\Media\Movies\Good.mkv",
                    "video": {"codec_raw": "av1"},
                    "audio_streams": [{"codec": "eac3"}],
                },
            ],
            "summary": {"total_files": 2},
        }
        report.write_text(json.dumps(data), encoding="utf-8")
        result = inv.check_no_audioless_av1()
        assert not result.ok
        assert result.value == 1
        assert result.severity == "critical"
        assert "Bad.mkv" in result.message

    def test_passes_when_clean(self, patched_paths):
        _, report, _ = patched_paths
        data = {
            "files": [
                {
                    "filepath": "ok.mkv",
                    "video": {"codec_raw": "av1"},
                    "audio_streams": [{"codec": "eac3"}],
                }
            ],
            "summary": {"total_files": 1},
        }
        report.write_text(json.dumps(data), encoding="utf-8")
        result = inv.check_no_audioless_av1()
        assert result.ok
        assert result.value == 0


class TestNoDoneWithDeferredReason:
    """status=DONE paired with reason like 'deferred'/'skipped' is the 2026-04-23 lie."""

    def test_catches_lie(self, patched_paths):
        _, _, db = patched_paths
        _seed_state_db(
            str(db),
            [
                {
                    "filepath": "liar.mkv",
                    "status": "done",
                    "reason": "deferred strip",
                    "extras": "{}",
                },
                {
                    "filepath": "honest.mkv",
                    "status": "done",
                    "reason": "",
                    "extras": "{}",
                },
            ],
        )
        result = inv.check_no_done_with_deferred_reason()
        assert not result.ok
        assert result.value == 1
        assert result.severity == "critical"
        assert "deferred" in result.message.lower() or "skip" in result.message.lower()

    def test_passes_when_clean(self, patched_paths):
        _, _, db = patched_paths
        _seed_state_db(
            str(db),
            [
                {"filepath": "ok1.mkv", "status": "done", "reason": "", "extras": "{}"},
                {"filepath": "ok2.mkv", "status": "error", "reason": "deferred", "extras": "{}"},
            ],
        )
        result = inv.check_no_done_with_deferred_reason()
        assert result.ok
        assert result.value == 0


class TestNoStaleTmp:
    """Entries with *.tmp suffixes should fail the stale tmp check."""

    def test_catches_stale_tmp(self, patched_paths):
        _, report, _ = patched_paths
        report.write_text(
            json.dumps(
                {
                    "files": [
                        {"filepath": r"\\nas\Media\x.mkv.gapfill_tmp.mkv"},
                        {"filepath": r"\\nas\Media\y.mkv.submux_tmp.mkv"},
                        {"filepath": r"\\nas\Media\z.mkv"},
                    ],
                    "summary": {"total_files": 3},
                }
            ),
            encoding="utf-8",
        )
        result = inv.check_no_stale_tmp_on_nas()
        assert not result.ok
        assert result.value == 2


class TestGhostProcessDetection:
    """Pipeline python processes unknown to the registry are ghosts."""

    def test_ghost_python_process_detection(self, patched_paths, monkeypatch):
        staging, _, _ = patched_paths
        control = staging / "control"
        control.mkdir(parents=True, exist_ok=True)
        reg_path = control / "agents.registry.json"

        # Register a fake entry, then pretend its PID was reaped.
        dead_pid = _find_dead_pid()
        reg_path.write_text(
            json.dumps(
                [
                    {
                        "role": "scanner",
                        "pid": dead_pid,
                        "cmd": ["python", "-m", "tools.scanner"],
                        "started_at": time.time() - 3600,
                        "create_time": time.time() - 3600,
                        "last_heartbeat": time.time() - 3600,
                    }
                ]
            ),
            encoding="utf-8",
        )

        # Reconcile should drop the dead entry so it's no longer "registered".
        from pipeline.process_registry import ProcessRegistry

        reg = ProcessRegistry(reg_path, heartbeat_secs=60)
        removed = reg.reconcile()
        assert removed == ["scanner"]
        assert reg.list_active() == []

        # Fabricate a "ghost" psutil process running a pipeline module.
        class FakeProc:
            info = {
                "pid": 99991,
                "name": "python.exe",
                "cmdline": ["python", "-m", "pipeline", "--foo"],
            }

        def fake_iter(*_args, **_kwargs):
            yield FakeProc()

        monkeypatch.setattr(psutil, "process_iter", fake_iter)

        result = inv.check_no_ghost_python_processes()
        assert not result.ok
        assert result.value >= 1
        assert result.severity == "medium"
        assert "ghost" in result.message.lower() or "pipeline" in result.message.lower()


class TestSummaryMatches:
    def test_drift_detected(self, patched_paths):
        _, report, _ = patched_paths
        report.write_text(
            json.dumps({"files": [{"filepath": "a"}], "summary": {"total_files": 5}}),
            encoding="utf-8",
        )
        result = inv.check_media_report_summary_matches()
        assert not result.ok
        assert result.value == 4


class TestHealthDeepEndpoint:
    """TestClient integration: /api/health-deep returns the right schema."""

    def test_health_deep_endpoint_aggregates(self, test_app):
        resp = test_app.get("/api/health-deep")
        assert resp.status_code == 200
        data = resp.json()
        assert "generated_at" in data
        assert "all_green" in data
        assert isinstance(data["all_green"], bool)
        assert "checks" in data
        assert isinstance(data["checks"], list)
        assert len(data["checks"]) >= 1
        # Every check must carry the advertised schema.
        for c in data["checks"]:
            assert {"name", "severity", "ok", "value", "message"} <= set(c.keys())
            assert c["severity"] in ("critical", "high", "medium", "low")
            assert isinstance(c["ok"], bool)
            assert isinstance(c["value"], int)


class TestExitCode:
    def test_all_green_exits_zero(self):
        results = [inv.CheckResult("a", "critical", True, 0, "ok")]
        assert inv._exit_code(results) == 0

    def test_critical_failure_exits_one(self):
        results = [inv.CheckResult("a", "critical", False, 1, "bad")]
        assert inv._exit_code(results) == 1

    def test_low_only_exits_two(self):
        results = [inv.CheckResult("a", "low", False, 1, "meh")]
        assert inv._exit_code(results) == 2


# --------------------------------------------------------------------------
# Local helper — stub `_find_dead_pid` used in the ghost test. Mirrors the
# helper in test_process_registry.py but duplicated here to keep test files
# independent.
# --------------------------------------------------------------------------


def _find_dead_pid(max_tries: int = 10_000) -> int:
    for candidate in range(2**15 - 1, 2**15 - max_tries - 1, -1):
        if candidate > 0 and not psutil.pid_exists(candidate):
            return candidate
    raise RuntimeError("no dead PID found")


# Sanity: importable alongside test_api conftest.
def test_invariants_module_importable():
    assert hasattr(inv, "run_all")
    assert hasattr(inv, "main")
    assert len(inv._check_functions()) >= 8
    # Keep the `os` import referenced — used by fixture setup.
    assert os is not None
    # Keep pathlib.Path referenced.
    assert Path is not None
