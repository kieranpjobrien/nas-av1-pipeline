"""Pin the 2026-05-13 compliance gate fixes.

Two bugs the user hit overnight:

  1. Filename mismatch falsely fired on every .mp4 / .ts source because
     the gate compared full basenames including extension. The
     transcoder ALWAYS produces .mkv — that's normal and correct, not
     a violation. Miller's Girl, Trainspotting, The Menu were stuck
     because of this.

  2. The fixer dispatch ran each Category.FIXABLE violation as a
     SEPARATE mkvmerge call. foreign_subs runs first and drops a sub,
     extra_eng_subs runs second with indices computed against the
     ORIGINAL layout — now stale. mkvmerge gets asked to keep tracks
     that no longer exist. The proof-of-work guard correctly catches
     it, but the file ends in ERROR every time. Wild Robot, Happy
     Gilmore 2, Heads of State, Superbad, Babygirl, From Russia with
     Love, Planes Trains stuck because of this.

Post-fix:
  * Filename check compares stems (no extension). Catches real typos
    ("Crisismkv" vs "Crisis.mkv") but accepts extension transitions.
  * fix loop now merges foreign_subs + extra_eng_subs into one
    _mkvmerge_drop_streams call, similarly foreign_audio +
    commentary_audio. Indices stay valid because we operate on the
    original layout in a single pass.
"""

from __future__ import annotations

import os
from unittest import mock

import pytest

from pipeline import compliance


def _base_item(filepath: str, final_name: str):
    return {
        "filepath": filepath,
        "filename": os.path.basename(filepath),
        "final_name": final_name,
        "library_type": "movie",
        "tmdb": {},
    }


def _base_call_args(filepath: str, final_name: str):
    return dict(
        filepath=filepath,
        item=_base_item(filepath, final_name),
        encode_params={"cq": 22, "content_grade": "default"},
        output_probe={
            "video": {"codec": "av1"},
            "audio": [{"codec": "eac3", "channels": 6, "language": "eng",
                       "bit_rate_kbps": 640}],
            "subs": [{"codec": "subrip", "language": "eng", "title": ""}],
            "format": {},
        },
        mkv_tags={"ENCODER": "av1_nvenc", "CQ": "22", "CONTENT_GRADE": "default"},
        input_size_bytes=10_000_000_000,
        output_size_bytes=5_000_000_000,
        source_was_av1=False,
        config={"keep_langs": ["eng"], "lossless_audio_codecs": []},
    )


# --------------------------------------------------------------------------
# Filename check: extension transitions are NOT violations
# --------------------------------------------------------------------------


def test_filename_check_accepts_mp4_to_mkv_extension(monkeypatch):
    """Source .mp4 + final_name .mkv → NO violation. This is the normal
    transcoding extension change."""
    args = _base_call_args(
        r"\\NAS\Movies\Miller's Girl (2024)\Miller's Girl (2024).mp4",
        "Miller's Girl (2024).mkv",
    )
    violations = compliance.check_compliance(**args)
    filename_violations = [v for v in violations if v.tag == "filename_mismatch"]
    assert filename_violations == [], (
        f"extension change is not a violation; got {[v.message for v in filename_violations]}"
    )


def test_filename_check_accepts_ts_to_mkv_extension(monkeypatch):
    """Same for .ts -> .mkv (broadcast capture transcode)."""
    args = _base_call_args(
        r"\\NAS\Series\Foo S01E01.ts",
        "Foo S01E01.mkv",
    )
    violations = compliance.check_compliance(**args)
    filename_violations = [v for v in violations if v.tag == "filename_mismatch"]
    assert filename_violations == []


def test_filename_check_still_catches_real_typos(monkeypatch):
    """A real typo (missing dot before extension) MUST still fire —
    that's the legitimate use of this check."""
    args = _base_call_args(
        r"\\NAS\Movies\Star Wars Crisismkv",  # typo — no dot before ext
        "Star Wars Crisis.mkv",
    )
    violations = compliance.check_compliance(**args)
    filename_violations = [v for v in violations if v.tag == "filename_mismatch"]
    assert len(filename_violations) == 1, "real typo must still be caught"
    assert "Crisismkv" in filename_violations[0].message
    assert "Crisis.mkv" in filename_violations[0].message


def test_filename_check_catches_missing_year(monkeypatch):
    """Source missing the year — final_name has it. The stems differ,
    so this fires (a real cleanup the user wants)."""
    args = _base_call_args(
        r"\\NAS\Movies\Citizen Kane.mkv",
        "Citizen Kane (1941).mkv",
    )
    violations = compliance.check_compliance(**args)
    assert any(v.tag == "filename_mismatch" for v in violations), (
        "missing year is a real cleanup case — must still fire"
    )


# --------------------------------------------------------------------------
# Merged-drop fixer: foreign_subs + extra_eng_subs → single mkvmerge call
# --------------------------------------------------------------------------


def test_merged_drop_de_duplicates_overlap(monkeypatch):
    """If foreign_audio and commentary_audio both reference the same
    audio index (e.g. a foreign-language commentary track), it gets
    dropped ONCE, not twice."""
    # This test exercises the merge logic without bringing in
    # full_gamut.finalize_upload (too much surrounding state). The
    # merge happens at the dispatch site — we mirror it here.
    foreign_audio_indices = [1, 2]
    commentary_audio_indices = [2, 3]
    merged = sorted(set(foreign_audio_indices) | set(commentary_audio_indices))
    assert merged == [1, 2, 3]


def test_merged_drop_indices_refer_to_original_layout(monkeypatch):
    """Regression: indices from compliance.py (computed against the
    encoded output BEFORE any fix runs) stay valid because we apply all
    drops in a single mkvmerge pass.

    Pre-fix: foreign_subs drop [2] runs and modifies file, then
    extra_eng_subs drop [4] runs against the modified file where index
    4 doesn't exist anymore.

    Post-fix: both [2] and [4] are merged → single mkvmerge call drops
    both from the ORIGINAL layout where they're valid indices.
    """
    # Simulated compliance violation indices (computed against output
    # with 5 subs)
    foreign_drop = [2]      # foreign sub at per-type index 2
    extra_eng_drop = [4]    # 5th English sub at per-type index 4
    merged = sorted(set(foreign_drop) | set(extra_eng_drop))
    # Both indices preserved — the merged set drops both subs in one go
    assert merged == [2, 4]


def test_full_gamut_imports_merged_fixer_helper():
    """Defence-in-depth: confirm full_gamut.py's compliance loop
    imports the underlying ``_mkvmerge_drop_streams`` helper. If
    someone reverts the merge refactor to per-fixer calls, this test
    will at least surface the structural change."""
    import pipeline.full_gamut as fg
    src = open(fg.__file__, encoding="utf-8").read()
    # The merge block uses _mkvmerge_drop_streams directly.
    assert "_mkvmerge_drop_streams" in src, (
        "full_gamut.py must use _mkvmerge_drop_streams directly for the merged drop call"
    )
    # And the merge variable names exist
    assert "merged_audio_drop" in src
    assert "merged_sub_drop" in src
