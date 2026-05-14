"""Pin the 2026-05-14 Blue Valentine switch from mkvmerge to ffmpeg
for the pre-encode strip step.

Pre-fix, ``_mkvmerge_drop_streams_to_path`` shelled out to mkvmerge.
On large MKV sources with ``--no-subtitles`` (the Blue Valentine
class — 18.9 GB MKV, 1 video + 1 DTS-HD MA + 5 foreign PGS subs)
mkvmerge non-deterministically terminated mid-mux: four sequential
runs of the identical command produced outputs at 2.4 / 3.5 / 7.7 /
10.8 GB, all with rc=0 but zero tracks visible to mkvmerge
--identify (no writing_application string set, no track headers
flushed). The pipeline's proof-of-work probe caught it (ffprobe
saw 0 audio at probe time) and the file got marked ERROR with no
progress.

Post-fix: the function shells out to ffmpeg's stream-copy mode
instead. ffmpeg completed the same job in one shot on the same
source, output mkvmerge --identify cleanly with tracks + chapters
+ global tags intact. The function name kept ``_mkvmerge_`` for
backward-compat with callers + existing tests.
"""

from __future__ import annotations

import os
import subprocess
from unittest import mock

import pytest

import pipeline.compliance_fixers as cf


def _stub_probe(monkeypatch, src_audio: int, src_sub: int, out_audio: int, out_sub: int):
    """Make _probe_full return src_audio/src_sub for the input path and
    out_audio/out_sub for the output path. Lets tests assert the
    proof-of-work logic without spinning real ffprobe."""
    def _probe(path):
        # The stripper passes src then probes dst — distinguish by
        # whether the path is the dst we expect.
        if path.endswith(".stripped.mkv"):
            return {
                "video": {"codec": "h264"},
                "audio": [{}] * out_audio,
                "subs": [{}] * out_sub,
            }
        return {
            "video": {"codec": "h264"},
            "audio": [{}] * src_audio,
            "subs": [{}] * src_sub,
        }
    monkeypatch.setattr("pipeline.full_gamut._probe_full", _probe)


def test_to_path_invokes_ffmpeg_not_mkvmerge(monkeypatch, tmp_path):
    """The function name keeps ``_mkvmerge_`` for caller compat, but the
    actual subprocess invocation must be ``ffmpeg`` — mkvmerge's
    non-deterministic mid-mux termination is what motivated the
    switch."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"x" * (200 * 1024 * 1024))  # 200 MB, passes the 50% size guard
    dst = tmp_path / "src.mkv.stripped.mkv"

    _stub_probe(monkeypatch, src_audio=1, src_sub=5, out_audio=1, out_sub=0)

    captured = {"cmd": None}

    class _FakeResult:
        returncode = 0
        stdout = b""
        stderr = b""

    def _fake_run(cmd, **kw):
        captured["cmd"] = cmd
        # Write a fake output big enough to pass the size guard.
        dst.write_bytes(b"y" * (180 * 1024 * 1024))
        return _FakeResult()

    monkeypatch.setattr(subprocess, "run", _fake_run)

    ok = cf._mkvmerge_drop_streams_to_path(
        str(src), str(dst), drop_sub_indices=[0, 1, 2, 3, 4]
    )
    assert ok is True

    assert captured["cmd"] is not None, "subprocess.run must have been called"
    binary = os.path.basename(captured["cmd"][0]).lower()
    assert binary.startswith("ffmpeg"), (
        f"strip must shell out to ffmpeg, got {binary!r}. mkvmerge's "
        "non-deterministic mid-mux termination on large MKVs with "
        "--no-subtitles (Blue Valentine class) is the reason we switched."
    )


def test_to_path_maps_kept_audio_and_sub_indices(monkeypatch, tmp_path):
    """ffmpeg uses positive-selection ``-map 0:a:N`` per kept index —
    inverse of mkvmerge's ``--audio-tracks <keep-list>`` negative
    form. Verify the per-keep maps are emitted in source order."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"x" * (200 * 1024 * 1024))
    dst = tmp_path / "src.mkv.stripped.mkv"

    # Source has 3 audio, 5 subs. Drop audio idx 1; drop subs 0,1,3.
    # Expected keep: audio [0, 2], subs [2, 4].
    _stub_probe(monkeypatch, src_audio=3, src_sub=5, out_audio=2, out_sub=2)

    captured = {"cmd": None}

    class _FakeResult:
        returncode = 0
        stdout = b""
        stderr = b""

    def _fake_run(cmd, **kw):
        captured["cmd"] = cmd
        dst.write_bytes(b"y" * (180 * 1024 * 1024))
        return _FakeResult()

    monkeypatch.setattr(subprocess, "run", _fake_run)

    ok = cf._mkvmerge_drop_streams_to_path(
        str(src), str(dst),
        drop_audio_indices=[1],
        drop_sub_indices=[0, 1, 3],
    )
    assert ok is True

    cmd = captured["cmd"]
    # Walk -map pairs and collect specifiers.
    map_specs = [cmd[i + 1] for i in range(len(cmd) - 1) if cmd[i] == "-map"]
    assert "0:v" in map_specs, "video must be mapped"
    assert "0:a:0" in map_specs
    assert "0:a:2" in map_specs
    assert "0:a:1" not in map_specs, "dropped audio idx 1 must NOT be mapped"
    assert "0:s:2" in map_specs
    assert "0:s:4" in map_specs
    assert "0:s:0" not in map_specs
    assert "0:s:1" not in map_specs
    assert "0:s:3" not in map_specs


def test_to_path_drop_all_subs_emits_no_sub_map(monkeypatch, tmp_path):
    """The Blue Valentine shape: drop ALL subs. ffmpeg's positive-selection
    form just emits zero ``-map 0:s:N`` entries; no special flag needed.
    Mkvmerge's ``--no-subtitles`` was what tripped its mid-mux bug —
    ffmpeg has no such mode-switch, so the failure class can't recur."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"x" * (200 * 1024 * 1024))
    dst = tmp_path / "src.mkv.stripped.mkv"

    _stub_probe(monkeypatch, src_audio=1, src_sub=5, out_audio=1, out_sub=0)

    captured = {"cmd": None}

    class _FakeResult:
        returncode = 0
        stdout = b""
        stderr = b""

    def _fake_run(cmd, **kw):
        captured["cmd"] = cmd
        dst.write_bytes(b"y" * (180 * 1024 * 1024))
        return _FakeResult()

    monkeypatch.setattr(subprocess, "run", _fake_run)

    ok = cf._mkvmerge_drop_streams_to_path(
        str(src), str(dst), drop_sub_indices=[0, 1, 2, 3, 4],
    )
    assert ok is True

    cmd = captured["cmd"]
    map_specs = [cmd[i + 1] for i in range(len(cmd) - 1) if cmd[i] == "-map"]
    sub_maps = [s for s in map_specs if s.startswith("0:s")]
    assert sub_maps == [], (
        f"all-subs-dropped must emit zero -map 0:s:N entries; got {sub_maps}. "
        "No mkvmerge --no-subtitles equivalent — that's the whole point of the switch."
    )
    # Audio + video still mapped.
    assert "0:v" in map_specs
    assert "0:a:0" in map_specs


def test_to_path_refuses_drop_all_audio(monkeypatch, tmp_path):
    """Safety: refuse to produce a zero-audio file. The pre-fix mkvmerge
    impl had the same guard; ffmpeg path must too. A silent file would
    just get refused by compliance.check_compliance later anyway."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"x" * (200 * 1024 * 1024))
    dst = tmp_path / "src.mkv.stripped.mkv"

    _stub_probe(monkeypatch, src_audio=2, src_sub=0, out_audio=0, out_sub=0)

    fake_run_called = {"yes": False}

    def _fake_run(cmd, **kw):
        fake_run_called["yes"] = True
        raise AssertionError("subprocess.run must NOT be called for refuse-all-audio")

    monkeypatch.setattr(subprocess, "run", _fake_run)

    ok = cf._mkvmerge_drop_streams_to_path(
        str(src), str(dst), drop_audio_indices=[0, 1],
    )
    assert ok is False
    assert fake_run_called["yes"] is False


def test_to_path_proof_of_work_catches_track_count_mismatch(monkeypatch, tmp_path):
    """If ffmpeg ever produces a torn-mux output (we haven't seen it,
    but defence in depth) the proof-of-work check must catch it. Set
    the output probe to return fewer audio tracks than expected; the
    function must return False and remove the broken dst."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"x" * (200 * 1024 * 1024))
    dst = tmp_path / "src.mkv.stripped.mkv"

    # Source has 1 audio + 5 subs. Expected after drop_sub=[0,1,2,3,4]:
    # 1 audio + 0 subs. Make the output probe return 0 audio + 0 subs
    # (the exact Blue Valentine shape ffprobe saw at proof-of-work time
    # under the old mkvmerge path).
    def _probe(path):
        if path.endswith(".stripped.mkv"):
            return {"video": {"codec": "h264"}, "audio": [], "subs": []}
        return {"video": {"codec": "h264"}, "audio": [{}], "subs": [{}] * 5}
    monkeypatch.setattr("pipeline.full_gamut._probe_full", _probe)

    class _FakeResult:
        returncode = 0
        stdout = b""
        stderr = b""

    def _fake_run(cmd, **kw):
        dst.write_bytes(b"y" * (180 * 1024 * 1024))
        return _FakeResult()

    monkeypatch.setattr(subprocess, "run", _fake_run)

    ok = cf._mkvmerge_drop_streams_to_path(
        str(src), str(dst), drop_sub_indices=[0, 1, 2, 3, 4],
    )
    assert ok is False, "track-count mismatch must trip the proof-of-work guard"
    assert not dst.exists(), "torn-mux output must be removed on proof-of-work fail"


def test_to_path_no_drops_just_copies(tmp_path):
    """When neither audio nor sub drops are requested, the function
    short-circuits to shutil.copy2 — no subprocess at all. This is
    the no-op path callers use to materialise a sibling for downstream
    consumers."""
    src = tmp_path / "src.mkv"
    src.write_bytes(b"x" * 10000)
    dst = tmp_path / "src.mkv.stripped.mkv"

    ok = cf._mkvmerge_drop_streams_to_path(str(src), str(dst))
    assert ok is True
    assert dst.exists()
    assert dst.read_bytes() == src.read_bytes()
