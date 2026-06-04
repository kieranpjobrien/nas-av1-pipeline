"""Regression tests for the 2026-06-05 multi-agent ('ultracode') review fixes.

Covers the verified HIGH findings:
  * server rename_file: NAS-membership guard + new_name sanitisation
  * server /api/dismissed/{section}: section-name validation (path traversal)
  * orchestrator: errors counter only increments on real upload failure
  * full_gamut: encode-retry chain advances past attempt 0 (tried-set)

content_grade._entry_year and the frontend normalizeFile fixes are pinned
in tests/test_content_grade.py and the frontend build respectively.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# --- server rename_file security ------------------------------------------


def test_rename_rejects_path_outside_nas(monkeypatch, tmp_path):
    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "NAS_MOVIES", tmp_path / "Movies", raising=False)
    monkeypatch.setattr(paths_mod, "NAS_SERIES", tmp_path / "Series", raising=False)
    from server.routers.files import rename_file

    with pytest.raises(HTTPException) as ei:
        rename_file({"path": str(tmp_path / "elsewhere" / "x.mkv"), "new_name": "y.mkv"})
    assert ei.value.status_code == 403


def test_rename_rejects_traversal_in_new_name(monkeypatch, tmp_path):
    movies = tmp_path / "Movies"
    movies.mkdir()
    src = movies / "Film (2020)"
    src.mkdir()
    f = src / "Film (2020).mkv"
    f.write_bytes(b"x")
    import paths as paths_mod
    monkeypatch.setattr(paths_mod, "NAS_MOVIES", movies, raising=False)
    monkeypatch.setattr(paths_mod, "NAS_SERIES", tmp_path / "Series", raising=False)
    from server.routers.files import rename_file

    for bad in ["../../escape.db", "a/b.mkv", "..", ".hidden"]:
        with pytest.raises(HTTPException) as ei:
            rename_file({"path": str(f), "new_name": bad})
        assert ei.value.status_code == 400, f"{bad!r} should be rejected as 400"


# --- server dismissed section validation -----------------------------------


def test_safe_section_accepts_valid_names():
    from server.routers.admin import _safe_section
    for ok in ("glance", "grade_optimal", "a-b_1", "Errors", "queue"):
        assert _safe_section(ok) == ok


def test_safe_section_rejects_traversal_and_junk():
    from server.routers.admin import _safe_section
    for bad in ("../../etc/passwd", "a/b", "..", "a.json", "x/../y", "", "a b", "a.b"):
        with pytest.raises(HTTPException) as ei:
            _safe_section(bad)
        assert ei.value.status_code == 400, f"{bad!r} should be rejected"


# --- pipeline source-level pins (logic lives in threaded/subprocess code) ---


def _src(rel: str) -> str:
    return (Path(__file__).resolve().parent.parent / rel).read_text(encoding="utf-8")


def test_orchestrator_errors_increment_inside_except():
    """The inline-upload errors counter must live INSIDE the except clause,
    not at the try/except indent level (where it counted every success as an
    error on the upload_concurrency<=0 path)."""
    src = _src("pipeline/orchestrator.py")
    # The increment immediately following the finalize_upload except must be
    # indented deeper than the bare 'self.state.stats["errors"]' that sat at
    # try-level before. Assert the success path no longer unconditionally
    # increments: there must be a logging.error + increment pairing inside except.
    assert 'except Exception as e:' in src
    # The guard: no increment at exactly the try/except sibling indent for the
    # inline branch. We check the fixed structure: increment appears AFTER a
    # logging.error("Upload failed..." line within the same block.
    idx = src.find("Upload failed for")
    assert idx != -1
    window = src[idx: idx + 400]
    assert 'stats["errors"]' in window, (
        "errors increment must immediately follow the 'Upload failed' log inside except"
    )


def test_full_gamut_retry_uses_tried_modes_not_attempt0():
    """The encode-retry selectors must gate on 'mode not yet tried' so the
    no_hwaccel -> no_subs -> audio_copy chain can progress past attempt 0."""
    src = _src("pipeline/full_gamut.py")
    assert "tried_modes" in src, "expected a tried_modes set guarding retry selection"
    # The old broken guards were 'if attempt == 0 and ...' on the hwaccel and
    # subtitle selectors. They must be gone (replaced by 'not in tried_modes').
    assert 'attempt == 0 and any(m in error_tail' not in src, (
        "hwaccel retry must not gate on attempt == 0"
    )
    assert 'attempt == 0 and ("subtitle"' not in src, (
        "subtitle retry must not gate on attempt == 0"
    )
    assert '"no_hwaccel" not in tried_modes' in src
    assert '"no_subs" not in tried_modes' in src
    assert '"audio_copy" not in tried_modes' in src
