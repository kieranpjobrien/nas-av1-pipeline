"""Regression tests for the 2026-06-05 multi-agent ('ultracode') review fixes.

Covers the verified HIGH findings:
  * server rename_file: NAS-membership guard + new_name sanitisation
  * server /api/dismissed/{section}: section-name validation (path traversal)
  * orchestrator: errors counter only increments on real upload failure
  * full_gamut: encode-retry chain advances past attempt 0 (tried-set)

And the verified MEDIUM findings (second pass):
  * compliance: forced-sub detection uses disposition+title (is_forced_internal),
    not title-only — a disposition-forced untitled track must not be miscounted
    as a regular English sub (extra_eng_subs breaker loop).
  * streams: DTS *core* is lossy; only DTS-HD MA (via profile) is lossless.
  * ws.py: blocking SQLite read + nvidia-smi subprocess are offloaded via
    asyncio.to_thread so they don't stall the event loop.
  * orchestrator: GPU first-pass skips files a prep worker holds in _prepping
    (GPU-vs-prep double-pick race).

The external-sidecar out_idx fix (ffmpeg) is pinned in tests/test_ffmpeg_builder.py.
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


# --- MEDIUM: compliance forced-sub detection (disposition + title) ----------


def test_compliance_forced_sub_detected_by_disposition_not_just_title():
    """A forced sub flagged only by ``disposition.forced`` (empty title) must
    be excluded from the regular-English count, matching the encoder's
    is_forced detection. Pre-fix compliance checked the title only, miscounted
    it as a 2nd regular English sub, and tripped ``extra_eng_subs`` forever
    (the prep circuit-breaker loop)."""
    from pipeline.compliance import check_compliance

    def _run(subs: list[dict]) -> list:
        return check_compliance(
            filepath=r"\\KieranNAS\Test.mkv",
            item={"tmdb": {}, "library_type": "movie",
                  "filename": "Test.mkv", "final_name": "Test.mkv"},
            encode_params={"cq": 22, "content_grade": "default"},
            output_probe={
                "video": {"codec": "av1"},
                "audio": [{"codec": "eac3", "language": "eng", "title": ""}],
                "subs": subs,
            },
            mkv_tags={"ENCODER": "x", "CQ": "22", "CONTENT_GRADE": "default"},
            input_size_bytes=10_000_000_000,
            output_size_bytes=8_000_000_000,
            source_was_av1=False,
            config={"lossless_audio_codecs": []},
        )

    # disposition-forced (untitled) + 1 regular eng → only 1 regular → OK.
    out = _run([
        {"language": "eng", "title": "", "disposition": {"forced": 1}},
        {"language": "eng", "title": ""},
    ])
    assert not any(v.tag == "extra_eng_subs" for v in out), (
        "disposition-forced sub wrongly counted as a regular English sub — "
        "compliance forced detection must use disposition, not title only"
    )

    # Control: two genuine regular English subs SHOULD still trip extra_eng_subs
    # (proves the assertion above can actually fail — guards against a no-op test).
    out2 = _run([
        {"language": "eng", "title": ""},
        {"language": "eng", "title": ""},
    ])
    assert any(v.tag == "extra_eng_subs" for v in out2), (
        "two regular English subs must still be flagged (test sanity check)"
    )


# --- MEDIUM: DTS core is lossy, DTS-HD MA is lossless -----------------------


def test_dts_core_lossy_dts_hd_ma_lossless():
    from pipeline.streams import parse_audio_stream

    # DTS core (no HD MA profile) is LOSSY — must not be marked lossless.
    assert parse_audio_stream({"codec": "dts", "profile": ""}).lossless is False
    assert parse_audio_stream({"codec": "dts"}).lossless is False
    # DTS-HD MA is lossless — detected via the profile string ("hd ma").
    assert parse_audio_stream({"codec": "dts", "profile": "DTS-HD MA"}).lossless is True
    assert parse_audio_stream({"codec": "dts", "profile": "DTS-HD Master Audio"}).lossless is True
    # TrueHD / FLAC / PCM remain lossless.
    assert parse_audio_stream({"codec": "truehd"}).lossless is True
    assert parse_audio_stream({"codec": "flac"}).lossless is True
    assert parse_audio_stream({"codec": "pcm_s24le"}).lossless is True
    # An explicit lossless=True flag from the media report is still honoured.
    assert parse_audio_stream({"codec": "dts", "lossless": True}).lossless is True


def test_is_forced_internal_helper():
    from pipeline.streams import is_forced_internal

    assert is_forced_internal({"disposition": {"forced": 1}, "title": ""}) is True
    assert is_forced_internal({"title": "English (Forced)"}) is True
    assert is_forced_internal({"title": "Foreign Parts Only"}) is True
    assert is_forced_internal({"title": "English"}) is False
    assert is_forced_internal({"title": "", "disposition": {}}) is False


# --- MEDIUM: ws.py offloads blocking calls off the event loop ---------------


def test_ws_offloads_blocking_calls_to_thread():
    """The websocket handler's SQLite read (_get_pipeline_state) and nvidia-smi
    subprocess (_query_gpu) must run via asyncio.to_thread, never bare on the
    event loop (which would freeze every other WS client + async HTTP route
    for the subprocess duration)."""
    src = _src("server/routers/ws.py")
    assert "asyncio.to_thread(_get_pipeline_state)" in src
    assert "asyncio.to_thread(_query_gpu)" in src
    # The bare blocking call forms must be gone from the handler body.
    assert "_get_pipeline_state()" not in src, "bare blocking SQLite read still present"
    assert "_query_gpu()" not in src, "bare blocking nvidia-smi call still present"


# --- MEDIUM: GPU picker skips files a prep worker is mid-prep on ------------


def test_pick_next_locked_skips_prepping_files():
    """`_pick_next_locked`'s first pass must skip files held in `_prepping` by a
    prep worker — otherwise the GPU worker grabs the same PROCESSING+local file
    (it's NOT in _dispatched yet) and both run prep/encode on it."""
    src = _src("pipeline/orchestrator.py")
    start = src.find("def _pick_next_locked(")
    assert start != -1, "_pick_next_locked not found"
    end = src.find("\n    def ", start + 1)
    body = src[start: end if end != -1 else len(src)]
    assert "_prepping" in body, "first pass must consult _prepping"
    prepping_pos = body.find("self._prepping")
    first_return = body.find("return item")
    assert prepping_pos != -1 and first_return != -1 and prepping_pos < first_return, (
        "the _prepping skip must come before the first-pass 'return item'"
    )


# --- LOW pass: latent NameError found during dead-code sweep ----------------


def test_circuit_breaker_logs_use_defined_variable():
    """The compliance + integrity circuit-breaker error logs referenced an
    undefined ``filename`` (only ``filepath`` is in scope at those sites) — a
    latent NameError that would crash the error-recovery path instead of
    parking the file as flagged_corrupt. They must use a defined name. Found
    via ruff F821 while removing dead code; fixed to os.path.basename(filepath)."""
    src = _src("pipeline/full_gamut.py")
    assert "CIRCUIT BREAKER: {filename}" not in src, (
        "circuit-breaker log references undefined `filename` — NameError in the "
        "error-recovery path; use os.path.basename(filepath)"
    )
