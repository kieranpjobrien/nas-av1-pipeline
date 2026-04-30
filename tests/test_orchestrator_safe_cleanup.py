"""Regression tests for the 2026-04-30 'cleanup wiped 66 staged uploads' bug.

Background: the orchestrator's startup cleanup blindly deleted every file
in ``encoded/`` and ``fetch/`` on launch. The upload worker had died mid-day
on a JSON corruption error, leaving 66 fully-encoded files in ``encoded/``
with state rows at ``status='uploading'``. A pipeline restart wiped all 66
— ~11 hours of GPU work — because the cleanup didn't check whether the
state DB still referenced those paths.

The fix lives in ``Orchestrator._cleanup_staging`` (currently inlined into
``run``): collect ``local_path`` / ``output_path`` / ``actual_input`` from
every non-terminal state row and skip files that match. Anything left is
genuinely orphaned. If state enumeration fails, fall back to age-based
deletion (>24h only) so we never re-create the wipe.

These tests exercise the safety contract directly — terminal vs non-terminal
filtering, path matching, and the age fallback.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest


@pytest.fixture
def staging(tmp_path: Path) -> Path:
    (tmp_path / "fetch").mkdir()
    (tmp_path / "encoded").mkdir()
    return tmp_path


def _write(p: Path, content: str = "x") -> Path:
    p.write_text(content, encoding="utf-8")
    return p


def _run_cleanup(staging_dir: Path, all_files: dict[str, dict]) -> tuple[int, int]:
    """Re-implementation of the orchestrator cleanup — kept in sync with
    pipeline/orchestrator.py. Returns (cleaned, preserved) totals across
    fetch/ + encoded/.
    """
    import json as _json

    live_paths: set[str] = set()
    enumerated = True
    try:
        for _fp, row in all_files.items():
            status = (row.get("status") or "").lower()
            if status in ("done", "skipped", "error", "flagged_undetermined"):
                continue
            for k in ("local_path", "output_path", "actual_input"):
                p = row.get(k)
                if p:
                    live_paths.add(os.path.normcase(os.path.abspath(p)))
            pd = row.get("prep_data")
            if isinstance(pd, dict):
                for k in ("local_path", "output_path", "actual_input"):
                    p = pd.get(k)
                    if p:
                        live_paths.add(os.path.normcase(os.path.abspath(p)))
    except Exception:
        live_paths = set()
        enumerated = False

    now = time.time()
    AGE_FALLBACK_SECS = 24 * 3600

    total_cleaned = 0
    total_preserved = 0
    for subdir in ("fetch", "encoded"):
        d = staging_dir / subdir
        if not d.is_dir():
            continue
        for f in os.listdir(d):
            path = d / f
            norm = os.path.normcase(os.path.abspath(path))
            if norm in live_paths:
                total_preserved += 1
                continue
            if not live_paths and not enumerated:
                # Age fallback — only delete >24h old
                try:
                    if (now - path.stat().st_mtime) < AGE_FALLBACK_SECS:
                        total_preserved += 1
                        continue
                except OSError:
                    pass
            try:
                path.unlink()
                total_cleaned += 1
            except OSError:
                pass
    return total_cleaned, total_preserved


def test_preserves_uploading_files(staging):
    """The 2026-04-30 reproducer: a state row with status='uploading' and
    local_path pointing at encoded/X must NOT have X deleted."""
    f = _write(staging / "encoded" / "abc123_Movie.mkv")
    rows = {
        f.as_posix(): {
            "status": "uploading",
            "local_path": str(f),
        }
    }
    cleaned, preserved = _run_cleanup(staging, rows)
    assert preserved == 1
    assert cleaned == 0
    assert f.exists()


def test_preserves_processing_files(staging):
    """status='processing' (mid-encode) is also non-terminal and must be preserved."""
    f = _write(staging / "encoded" / "def456_Movie.mkv")
    rows = {
        f.as_posix(): {
            "status": "processing",
            "local_path": str(f),
        }
    }
    cleaned, preserved = _run_cleanup(staging, rows)
    assert preserved == 1
    assert f.exists()


def test_preserves_via_prep_data(staging):
    """Paths nested inside prep_data dict are also live."""
    f = _write(staging / "fetch" / "789_Source.mkv")
    rows = {
        f.as_posix(): {
            "status": "pending",
            "prep_data": {"actual_input": str(f)},
        }
    }
    cleaned, preserved = _run_cleanup(staging, rows)
    assert preserved == 1
    assert f.exists()


def test_cleans_genuine_orphan(staging):
    """A file in encoded/ with no matching state row IS orphaned and should be removed."""
    orphan = _write(staging / "encoded" / "nobody_owns_this.mkv")
    rows = {
        # Some other file, not the orphan
        "/somewhere/else.mkv": {"status": "uploading", "local_path": "/somewhere/else.mkv"},
    }
    cleaned, preserved = _run_cleanup(staging, rows)
    assert cleaned == 1
    assert not orphan.exists()


def test_terminal_status_does_not_protect(staging):
    """status='done' is terminal — a stale local_path on a done row should NOT
    save the file. Common case: encoder finished, uploaded, but never cleaned
    the staging file."""
    f = _write(staging / "encoded" / "stale_done.mkv")
    rows = {
        f.as_posix(): {
            "status": "done",
            "local_path": str(f),
        }
    }
    cleaned, preserved = _run_cleanup(staging, rows)
    assert cleaned == 1
    assert not f.exists()


def test_age_fallback_when_state_unreadable(staging):
    """If state enumeration raises, fall back to age-based: keep files <24h old.
    Without this fallback, a corrupt state DB would re-create the original wipe.
    """
    fresh = _write(staging / "encoded" / "fresh_unknown.mkv")
    old = _write(staging / "encoded" / "old_unknown.mkv")
    # Make old file 25h old
    twenty_five_h_ago = time.time() - 25 * 3600
    os.utime(old, (twenty_five_h_ago, twenty_five_h_ago))

    # Pass a rows dict that triggers the except branch
    class BrokenDict(dict):
        def items(self):
            raise RuntimeError("simulated state corruption")

    cleaned, preserved = _run_cleanup(staging, BrokenDict())
    assert fresh.exists(), "fresh files must survive when state is unreadable"
    assert not old.exists(), ">24h files are still cleanable as last-resort"
    assert preserved == 1
    assert cleaned == 1


def test_mass_uploading_backlog_preserved(staging):
    """The 66-file scenario — many uploading files all get preserved."""
    files = []
    rows = {}
    for i in range(66):
        f = _write(staging / "encoded" / f"hash{i:02d}_Movie{i}.mkv")
        files.append(f)
        rows[f.as_posix()] = {"status": "uploading", "local_path": str(f)}
    cleaned, preserved = _run_cleanup(staging, rows)
    assert preserved == 66
    assert cleaned == 0
    for f in files:
        assert f.exists()
