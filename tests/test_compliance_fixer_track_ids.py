"""Pin the 2026-05-12 mkvmerge track-ID translation in compliance_fixers.

Pre-fix the compliance fixer passed PER-TYPE stream indices (sub index 0
= first subtitle) directly to mkvmerge's ``--audio-tracks`` /
``--subtitle-tracks`` flags. mkvmerge interprets those as GLOBAL track
IDs (video + audio + subtitles consecutively numbered). Result:

    GoodFellas (track IDs 0=video, 1-4=audio, 5-30=subs) — fixer wanted
    to keep per-type sub index 25 → emitted ``--subtitle-tracks 25``,
    which mkvmerge resolved to global ID 25 = per-type sub index 20 (a
    foreign-language sub). The fix returned True, compliance probed the
    output, saw the SAME foreign subs still present, REFUSE. Loop.

That's the bug that drove GoodFellas to ref=6, The Favourite/Toni
Erdmann to ref=4, Mary Poppins/Article 370/2001/Titanic to ref=3 — and
let Mary Poppins/2001/Article 370/Titanic ship with mismatched audio
because the user/system gave up and accepted as-is at some point.

Post-fix the function converts per-type → global by adding the offset
(``n_video`` for audio, ``n_video + n_audio`` for subs). These tests
pin the conversion across a few representative file layouts.
"""

from __future__ import annotations

import subprocess
from unittest import mock

import pytest

import pipeline.compliance_fixers as cf


def _setup_fixer_mocks(monkeypatch, video=1, audio=4, subs=26, mkvmerge_rc=0,
                       out_size_factor=0.95):
    """Patch _probe_full + subprocess.run + os.path so we can inspect the
    mkvmerge command without touching disk or running anything."""
    fake_probe = {
        "video": [{}] * video,
        "audio": [{}] * audio,
        "subs":  [{}] * subs,
    }
    monkeypatch.setattr("pipeline.full_gamut._probe_full", lambda _src: fake_probe)

    captured = {"cmd": None}

    class FakeCompleted:
        returncode = mkvmerge_rc
        stderr = b""

    def fake_run(cmd, capture_output=False, timeout=None):
        captured["cmd"] = cmd
        return FakeCompleted()

    monkeypatch.setattr(cf.subprocess, "run", fake_run)

    # Pretend the source is 30 GB and the output is 95% of that
    monkeypatch.setattr(cf.os.path, "getsize",
                        lambda p: int(30 * 1024**3 * (out_size_factor if "compliance_tmp" in p else 1.0)))
    monkeypatch.setattr(cf.os.path, "exists", lambda p: True)
    monkeypatch.setattr(cf.os, "replace", lambda a, b: None)

    return captured


def test_drop_subs_uses_global_mkvmerge_ids(monkeypatch):
    """GoodFellas layout: keep per-type sub index 25 → must emit global ID 30."""
    captured = _setup_fixer_mocks(monkeypatch, video=1, audio=4, subs=26)

    ok = cf._mkvmerge_drop_streams(
        "//nas/movie.mkv",
        drop_sub_indices=list(range(25)),  # drop sub indices 0..24, keep 25
    )
    assert ok is True
    cmd = captured["cmd"]
    # video=1 + audio=4 = 5; per-type sub 25 → global ID 30
    assert "--subtitle-tracks" in cmd
    i = cmd.index("--subtitle-tracks")
    assert cmd[i + 1] == "30", f"expected global ID 30, got {cmd[i + 1]}"


def test_drop_audio_uses_global_mkvmerge_ids(monkeypatch):
    """File with 1 video + 4 audio. Drop audio per-type index 1 (foreign);
    keep per-type 0, 2, 3 → global IDs 1, 3, 4 (NOT 0, 2, 3)."""
    captured = _setup_fixer_mocks(monkeypatch, video=1, audio=4, subs=2)

    ok = cf._mkvmerge_drop_streams(
        "//nas/movie.mkv",
        drop_audio_indices=[1],
    )
    assert ok is True
    cmd = captured["cmd"]
    assert "--audio-tracks" in cmd
    i = cmd.index("--audio-tracks")
    assert cmd[i + 1] == "1,3,4", (
        f"expected global IDs '1,3,4' (per-type 0,2,3 + offset 1), got {cmd[i + 1]!r}"
    )


def test_drop_subs_and_audio_use_correct_offsets(monkeypatch):
    """Combined drop. Layout: 1 video + 4 audio + 26 subs.
    Audio offset = 1, sub offset = 5."""
    captured = _setup_fixer_mocks(monkeypatch, video=1, audio=4, subs=26)

    ok = cf._mkvmerge_drop_streams(
        "//nas/movie.mkv",
        drop_audio_indices=[0, 1],   # keep audio per-type 2, 3 → global 3, 4
        drop_sub_indices=list(range(24)),  # keep sub per-type 24, 25 → global 29, 30
    )
    assert ok is True
    cmd = captured["cmd"]
    i = cmd.index("--audio-tracks")
    assert cmd[i + 1] == "3,4"
    i = cmd.index("--subtitle-tracks")
    assert cmd[i + 1] == "29,30"


def test_drop_all_subs_emits_no_subtitles_flag(monkeypatch):
    """When every sub is in the drop set, use ``--no-subtitles`` flag
    rather than emitting an empty track-list."""
    captured = _setup_fixer_mocks(monkeypatch, video=1, audio=2, subs=3)

    ok = cf._mkvmerge_drop_streams(
        "//nas/movie.mkv",
        drop_sub_indices=[0, 1, 2],
    )
    assert ok is True
    assert "--no-subtitles" in captured["cmd"]
    assert "--subtitle-tracks" not in captured["cmd"]


def test_refuse_to_drop_all_audio(monkeypatch):
    """Dropping every audio track would produce a silent video — refuse
    before invoking mkvmerge."""
    captured = _setup_fixer_mocks(monkeypatch, video=1, audio=2, subs=0)

    ok = cf._mkvmerge_drop_streams(
        "//nas/movie.mkv",
        drop_audio_indices=[0, 1],
    )
    assert ok is False
    assert captured["cmd"] is None, "must NOT invoke mkvmerge when about to drop all audio"


def test_no_video_layout_no_offset_for_audio(monkeypatch):
    """An audio-only MKV (n_video=0): per-type audio index N stays at
    global ID N (offset=0). Defensive case."""
    captured = _setup_fixer_mocks(monkeypatch, video=0, audio=3, subs=0)

    ok = cf._mkvmerge_drop_streams(
        "//nas/movie.mkv",
        drop_audio_indices=[1],
    )
    assert ok is True
    cmd = captured["cmd"]
    i = cmd.index("--audio-tracks")
    assert cmd[i + 1] == "0,2", f"audio-only file: keep per-type 0,2 → global 0,2, got {cmd[i+1]!r}"
