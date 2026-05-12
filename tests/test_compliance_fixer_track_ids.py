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


def _setup_fixer_mocks(monkeypatch, video=1, audio=4, subs=26,
                       drop_audio_count=0, drop_sub_count=0,
                       mkvmerge_rc=0, out_size_factor=0.95,
                       simulate_drop_failure=False):
    """Patch _probe_full + subprocess.run + os.path so we can inspect the
    mkvmerge command without touching disk or running anything.

    The probe returns ``(video, audio, subs)`` for the SOURCE call and
    ``(video, audio - drop_audio_count, subs - drop_sub_count)`` for the
    OUTPUT call — simulating mkvmerge correctly dropping the requested
    tracks. If ``simulate_drop_failure`` is True, the output probe
    returns the SAME shape as the source (mkvmerge silently kept all
    tracks). The proof-of-work guard must fail in that case.
    """
    # _probe_full returns video as a SINGLE DICT (the first video stream),
    # not a list. Audio and subs are lists. Mirroring this shape exactly is
    # critical — the bug fixed on 2026-05-12 was that the fixer called
    # ``len(probe.get("video"))`` and a dict's len is its KEY COUNT (~9
    # fields), so the audio/sub offsets were 9-too-big. Tests must use the
    # same shape as production or the bug class is invisible.
    def _video_block(n):
        # Real shape: dict with codec/width/height/etc when present, empty
        # dict or missing when not. n=0 means no video stream.
        return {"codec": "av1", "width": 1920, "height": 1080} if n > 0 else None

    src_probe = {
        "video": _video_block(video),
        "audio": [{}] * audio,
        "subs":  [{}] * subs,
    }
    if simulate_drop_failure:
        out_probe = src_probe
    else:
        out_probe = {
            "video": _video_block(video),
            "audio": [{}] * max(0, audio - drop_audio_count),
            "subs":  [{}] * max(0, subs - drop_sub_count),
        }

    call_count = {"n": 0}

    def fake_probe(path):
        # First call is on the source (src), subsequent on the temp output.
        call_count["n"] += 1
        # The fixer probes src once at the top (gets src_probe). After
        # mkvmerge runs it probes the tmp_out for proof-of-work
        # (gets out_probe). Calls after that (test re-validation) repeat.
        if call_count["n"] == 1:
            return src_probe
        return out_probe

    monkeypatch.setattr("pipeline.full_gamut._probe_full", fake_probe)

    captured = {"cmd": None}

    class FakeCompleted:
        returncode = mkvmerge_rc
        stderr = b""

    def fake_run(cmd, capture_output=False, timeout=None):
        captured["cmd"] = cmd
        return FakeCompleted()

    monkeypatch.setattr(cf.subprocess, "run", fake_run)

    monkeypatch.setattr(cf.os.path, "getsize",
                        lambda p: int(30 * 1024**3 * (out_size_factor if "compliance_tmp" in p else 1.0)))
    monkeypatch.setattr(cf.os.path, "exists", lambda p: True)
    monkeypatch.setattr(cf.os, "replace", lambda a, b: None)
    monkeypatch.setattr(cf.os, "remove", lambda p: None)

    return captured


def test_drop_subs_uses_global_mkvmerge_ids(monkeypatch):
    """GoodFellas layout: keep per-type sub index 25 → must emit global ID 30."""
    captured = _setup_fixer_mocks(monkeypatch, video=1, audio=4, subs=26, drop_sub_count=25)

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
    captured = _setup_fixer_mocks(monkeypatch, video=1, audio=4, subs=2, drop_audio_count=1)

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
    captured = _setup_fixer_mocks(
        monkeypatch, video=1, audio=4, subs=26,
        drop_audio_count=2, drop_sub_count=24,
    )

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
    captured = _setup_fixer_mocks(monkeypatch, video=1, audio=2, subs=3, drop_sub_count=3)

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
    captured = _setup_fixer_mocks(monkeypatch, video=0, audio=3, subs=0, drop_audio_count=1)

    ok = cf._mkvmerge_drop_streams(
        "//nas/movie.mkv",
        drop_audio_indices=[1],
    )
    assert ok is True
    cmd = captured["cmd"]
    i = cmd.index("--audio-tracks")
    assert cmd[i + 1] == "0,2", f"audio-only file: keep per-type 0,2 → global 0,2, got {cmd[i+1]!r}"


def test_proof_of_work_rejects_silent_failure(monkeypatch):
    """If mkvmerge exits 0 but the output still has all the original
    tracks (the EXACT class of bug we just fixed), the fixer must
    return False so the breaker eventually catches it rather than the
    compliance-refuse cohort silently expanding."""
    captured = _setup_fixer_mocks(
        monkeypatch, video=1, audio=4, subs=26,
        drop_sub_count=25,
        simulate_drop_failure=True,  # output probe == source probe
    )

    ok = cf._mkvmerge_drop_streams(
        "//nas/movie.mkv",
        drop_sub_indices=list(range(25)),
    )
    assert ok is False, (
        "fixer must return False when mkvmerge silently kept all tracks — "
        "this is the proof-of-work guard"
    )
    # mkvmerge was invoked (the cmd was constructed) but the result was rejected
    assert captured["cmd"] is not None


def test_proof_of_work_accepts_correct_drop(monkeypatch):
    """When mkvmerge correctly drops the requested tracks (output probe
    shows the expected count), the fixer returns True. Positive case."""
    captured = _setup_fixer_mocks(
        monkeypatch, video=1, audio=4, subs=26,
        drop_sub_count=25,
        simulate_drop_failure=False,
    )

    ok = cf._mkvmerge_drop_streams(
        "//nas/movie.mkv",
        drop_sub_indices=list(range(25)),
    )
    assert ok is True


def test_probe_video_is_dict_not_list(monkeypatch):
    """Regression test for the 2026-05-12 bug class: ``_probe_full``
    returns ``video`` as a single dict, not a list. ``len(dict)`` returns
    the KEY COUNT (~9), not 1, which made every global track ID 8 too
    big and mkvmerge errored rc=1 with empty stderr (silent failure).

    This test verifies the fixer correctly counts video=1 even when the
    video probe block is a populated dict. If someone reverts the
    1-if-non-empty-else-0 normalisation, the assertions on global IDs
    will catch it.
    """
    fake_probe = {
        "video": {  # NOT a list — dict with multiple fields
            "codec": "av1",
            "width": 3840,
            "height": 2160,
            "pix_fmt": "yuv420p10le",
            "r_frame_rate": "24000/1001",
            "color_transfer": "smpte2084",
            "color_space": "bt2020nc",
            "bit_rate_kbps": 25000,
            "profile": "Main 10",
        },
        "audio": [{}, {}],   # 2 audio streams
        "subs":  [{}, {}, {}, {}],  # 4 sub streams
    }
    monkeypatch.setattr("pipeline.full_gamut._probe_full", lambda _src: fake_probe)
    monkeypatch.setattr(cf.os.path, "getsize",
                        lambda p: int(30 * 1024**3 * (0.95 if "compliance_tmp" in p else 1.0)))
    monkeypatch.setattr(cf.os.path, "exists", lambda p: True)
    monkeypatch.setattr(cf.os, "replace", lambda a, b: None)
    monkeypatch.setattr(cf.os, "remove", lambda p: None)

    # After mkvmerge succeeds, the same probe is hit for proof-of-work.
    # Simulate correct drop output (1 sub remains after dropping 3).
    call_count = {"n": 0}
    def probe_with_output(_src):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return fake_probe
        # Output: 1 video, 2 audio, 1 sub (dropped 3 of 4)
        return {**fake_probe, "subs": [{}]}
    monkeypatch.setattr("pipeline.full_gamut._probe_full", probe_with_output)

    captured = {"cmd": None}
    class FakeCompleted:
        returncode = 0
        stderr = b""
    monkeypatch.setattr(cf.subprocess, "run",
                        lambda cmd, **kw: (captured.__setitem__("cmd", cmd), FakeCompleted())[1])

    # 1 video + 2 audio + 4 subs, keep sub per-type index 3 (last)
    # Pre-fix: n_video = len(dict) = 9 → sub_id_offset = 11 → global ID 14 (doesn't exist)
    # Post-fix: n_video = 1 → sub_id_offset = 3 → global ID 6 (correct: last sub)
    ok = cf._mkvmerge_drop_streams(
        "//nas/movie.mkv",
        drop_sub_indices=[0, 1, 2],
    )
    assert ok is True
    cmd = captured["cmd"]
    i = cmd.index("--subtitle-tracks")
    assert cmd[i + 1] == "6", (
        f"global ID 6 expected (video=1 + audio=2 + sub_idx=3 = 6). "
        f"If you see 14, the n_video=len(dict)=9 regression is back: {cmd[i + 1]!r}"
    )
