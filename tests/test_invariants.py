"""Regression coverage for ``tools.invariants``.

Focused on the 2026-04-23 audio-loss incident: AV1 files with zero audio
streams must be flagged, and DONE rows paired with a "deferred" reason
must fail CRITICAL. A smoke test runs the full battery with SSH skipped
so CI can exercise the orchestration path without a live NAS.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from tools import invariants

# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


def _report_with_files(*entries: dict) -> dict:
    """Wrap a list of file entries in the media_report.json envelope."""
    return {
        "generated": "2026-04-23T12:00:00",
        "summary": {"total_files": len(entries), "total_size_gb": 0.0},
        "files": list(entries),
    }


def _av1_file(filepath: str, audio_streams: list[dict]) -> dict:
    """Build a media_report entry for an AV1 file with the given audio streams."""
    return {
        "filepath": filepath,
        "filename": Path(filepath).name,
        "video": {"codec": "AV1", "codec_raw": "av1", "width": 1920, "height": 1080},
        "audio_streams": audio_streams,
        "subtitle_streams": [],
        "size_bytes": 1_000_000_000,
    }


def _make_state_db(tmp_path: Path, rows: list[tuple[str, str, "str | None"]]) -> Path:
    """Create a minimal pipeline_state.db matching the schema from pipeline.state.

    Each row is ``(filepath, status, reason)``. The full schema is copied
    verbatim because the invariants SQL looks up direct columns rather
    than going through ``PipelineState`` (which forbids DONE+deferred
    inserts at runtime and would otherwise refuse to seed the fixture).
    """
    db_path = tmp_path / "pipeline_state.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE pipeline_files (
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
    conn.executemany(
        "INSERT INTO pipeline_files (filepath, status, reason) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()
    return db_path


# --------------------------------------------------------------------------
# no_audioless_av1 - the 2026-04-23 signature
# --------------------------------------------------------------------------


def test_no_audioless_av1_passes_on_clean_report():
    """Report full of AV1 files with audio - invariant passes, no violations."""
    report = _report_with_files(
        _av1_file(
            r"\\KieranNAS\Media\Movies\Clean (2023).mkv",
            [{"codec": "EAC3", "codec_raw": "eac3", "channels": 6, "language": "eng"}],
        ),
        _av1_file(
            r"\\KieranNAS\Media\Movies\Also Clean (2024).mkv",
            [{"codec": "AAC", "codec_raw": "aac", "channels": 2, "language": "eng"}],
        ),
    )
    result = invariants.check_no_audioless_av1(report=report)
    assert result.passed is True
    assert result.severity == "CRITICAL"
    assert result.violations == []


def test_no_audioless_av1_fails_on_damaged():
    """Zero-audio AV1 file in the report - invariant fails and lists the path."""
    damaged_path = r"\\KieranNAS\Media\Movies\Damaged (2020).mkv"
    damaged = _av1_file(damaged_path, [])
    clean = _av1_file(
        r"\\KieranNAS\Media\Movies\OK (2021).mkv",
        [{"codec": "AAC", "codec_raw": "aac", "channels": 2, "language": "eng"}],
    )
    report = _report_with_files(damaged, clean)
    result = invariants.check_no_audioless_av1(report=report)
    assert result.passed is False
    assert result.severity == "CRITICAL"
    assert damaged_path in result.violations
    assert "1" in result.message


# --------------------------------------------------------------------------
# no_done_with_deferred_reason - the 65-file antipattern
# --------------------------------------------------------------------------


def test_no_done_with_deferred_reason_passes(tmp_path):
    """No DONE rows carry a 'deferred' reason - invariant passes."""
    db_path = _make_state_db(
        tmp_path,
        [
            (r"\\KieranNAS\Media\Movies\A.mkv", "done", "encoded"),
            (r"\\KieranNAS\Media\Movies\B.mkv", "pending", None),
            (r"\\KieranNAS\Media\Movies\C.mkv", "error", "ffmpeg rc=137"),
        ],
    )
    result = invariants.check_no_done_with_deferred_reason(db_path=db_path)
    assert result.passed is True
    assert result.severity == "CRITICAL"
    assert result.violations == []


def test_no_done_with_deferred_reason_fails(tmp_path):
    """One DONE row has reason containing 'defer' - invariant fails, lists path."""
    offender = r"\\KieranNAS\Media\Movies\TrackStripDeferred.mkv"
    db_path = _make_state_db(
        tmp_path,
        [
            (offender, "done", "local ops done (strip deferred)"),
            (r"\\KieranNAS\Media\Movies\Clean.mkv", "done", "encoded"),
        ],
    )
    result = invariants.check_no_done_with_deferred_reason(db_path=db_path)
    assert result.passed is False
    assert result.severity == "CRITICAL"
    assert offender in result.violations
    assert result.details["reasons"]
    # Details payload must carry the exact reason string so operators see the lie.
    reasons_seen = {d["reason"] for d in result.details["reasons"]}
    assert any("defer" in r.lower() for r in reasons_seen)


# --------------------------------------------------------------------------
# report_db_consistency
# --------------------------------------------------------------------------


def test_report_db_consistency_detects_mismatch(tmp_path):
    """A report entry with no matching DB row counts as 'in_report_but_not_in_db'.

    Complementary to the DONE-but-not-in-report branch - both directions of
    drift should surface in the details payload.
    """
    report = _report_with_files(
        _av1_file(
            r"\\KieranNAS\Media\Movies\OnlyInReport.mkv",
            [{"codec": "AAC", "codec_raw": "aac", "channels": 2, "language": "eng"}],
        ),
    )
    db_path = _make_state_db(
        tmp_path,
        [
            (r"\\KieranNAS\Media\Movies\OnlyInDB.mkv", "done", "encoded"),
        ],
    )
    result = invariants.check_report_db_consistency(report=report, db_path=db_path, tolerance=0)
    # OnlyInDB is DONE but not in the report -> high-severity counter.
    assert result.details["done_in_db_but_not_in_report"] == 1
    # OnlyInReport has no DB row -> soft 'report-missing-from-db' counter.
    assert result.details["in_report_but_not_in_db"] == 1
    # Exceeded tolerance of 0, so the check fails at HIGH severity.
    assert result.passed is False
    assert result.severity == "HIGH"


# --------------------------------------------------------------------------
# Orchestration smoke test
# --------------------------------------------------------------------------


def test_run_all_invariants_smoke(tmp_path, monkeypatch):
    """Orchestration runs every check and returns a list of InvariantResult.

    SSH-backed checks are skipped; the test verifies both that every expected
    invariant name is represented, and that the skipped checks pass with a
    'skipped' message instead of silently vanishing.
    """
    report_path = tmp_path / "media_report.json"
    report_path.write_text(
        json.dumps(
            _report_with_files(
                _av1_file(
                    r"\\KieranNAS\Media\Movies\Smoke.mkv",
                    [{"codec": "AAC", "codec_raw": "aac", "channels": 2, "language": "eng"}],
                )
            )
        ),
        encoding="utf-8",
    )
    db_path = _make_state_db(tmp_path, [])

    monkeypatch.setattr(invariants, "MEDIA_REPORT", report_path)
    monkeypatch.setattr(invariants, "PIPELINE_STATE_DB", db_path)
    monkeypatch.setattr(invariants, "STAGING_DIR", tmp_path)

    results = invariants.run_all_invariants(skip_ssh=True)
    assert isinstance(results, list)
    assert all(isinstance(r, invariants.InvariantResult) for r in results)
    names = {r.name for r in results}
    expected = {
        "no_audioless_av1",
        "no_done_with_deferred_reason",
        "no_done_with_error_reason",
        "no_stale_tmp_on_nas",
        "no_zombie_mkvmerge",
        "no_ghost_python_processes",
        "report_db_consistency",
        "report_file_exists_on_disk",
        "no_banned_ffmpeg_flags_in_log",
    }
    assert expected <= names

    # SSH-backed checks must pass with a 'skipped' message (not silently absent).
    ssh_checks = [r for r in results if r.name in ("no_stale_tmp_on_nas", "no_zombie_mkvmerge")]
    assert len(ssh_checks) == 2
    assert all(r.passed for r in ssh_checks)
    assert all("skipped" in r.message.lower() for r in ssh_checks)

    # Severities must be the advertised uppercase tier labels.
    allowed = {"CRITICAL", "HIGH", "MEDIUM", "LOW"}
    for r in results:
        assert r.severity in allowed, f"{r.name} has invalid severity {r.severity!r}"
