"""Pin the 2026-05-26 SDR colour-tag fix.

Background: pre-fix, build_ffmpeg_cmd emitted -color_primaries /
-color_trc / -colorspace ONLY for HDR or HDR-tonemap paths. SDR
encodes got no colour tags at all. The AV1 stream went out tagged
"unspecified" and players default-guessed the matrix — on 10-bit
SDR content the guess often landed on BT.2020 (because of bit
depth) producing green / purple tints. Confirmed on 1917 (2019,
1080p HEVC re-encoded, green) and The Drama (2026, 4K 10-bit AV1
re-encoded, purple).

Post-fix the SDR branch always emits explicit -color_* flags:
* If the source carries tags, pass them through
* Else fall back to BT.709 (matches virtually all SDR content)
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.config import build_config
from pipeline.ffmpeg import _probe_source_color, build_ffmpeg_cmd


def _sdr_item(**overrides) -> dict:
    """Minimal item dict for a clean SDR encode."""
    item = {
        "filepath": r"\\NAS\Movies\Test\Test.mkv",
        "filename": "Test.mkv",
        "library_type": "movie",
        "file_size_bytes": 5_000_000_000,
        "video_codec": "hevc",
        "resolution": "1080p",
        "hdr": False,
        "bit_depth": 8,
        "audio_streams": [
            {"codec_raw": "eac3", "language": "eng", "channels": 6}
        ],
        "subtitle_streams": [],
        "tmdb": {"original_language": "en", "title": "Test"},
    }
    item.update(overrides)
    return item


def _build_sdr_cmd(source_color_tags: dict[str, str | None] | None = None) -> list[str]:
    """Build a build_ffmpeg_cmd output with the probe stub returning the
    supplied source colour tags (or all-None if not specified)."""
    tags = source_color_tags or {
        "color_primaries": None,
        "color_transfer": None,
        "color_space": None,
    }
    cfg = build_config()
    item = _sdr_item()
    with patch("pipeline.ffmpeg._probe_source_color", return_value=tags):
        cmd = build_ffmpeg_cmd(
            item=item,
            input_path=r"\\NAS\Movies\Test\Test.mkv",
            output_path=r"F:\AV1_Staging\encoded\test.mkv",
            config=cfg,
            external_subs=None,
        )
    return cmd


def _flag_value(cmd: list[str], flag: str) -> str | None:
    """Return the value paired with the first occurrence of ``flag``."""
    for i, tok in enumerate(cmd):
        if tok == flag and i + 1 < len(cmd):
            return cmd[i + 1]
    return None


def test_sdr_no_source_tags_falls_back_to_bt709():
    """The The-Drama / 1917 class: source has no colour tags, we MUST
    emit BT.709 explicitly so the AV1 stream isn't tagged unspecified."""
    cmd = _build_sdr_cmd()  # all tags None
    assert _flag_value(cmd, "-color_primaries") == "bt709", (
        f"missing source tags must default to bt709 primaries; cmd={cmd!r}"
    )
    assert _flag_value(cmd, "-color_trc") == "bt709"
    assert _flag_value(cmd, "-colorspace") == "bt709"


def test_sdr_passes_source_tags_through():
    """When the source IS tagged (e.g. 4K SDR with BT.2020 primaries),
    preserve those tags rather than overwriting with bt709."""
    cmd = _build_sdr_cmd({
        "color_primaries": "bt2020",
        "color_transfer": "bt709",  # SDR transfer, wide primaries
        "color_space": "bt2020nc",
    })
    assert _flag_value(cmd, "-color_primaries") == "bt2020"
    assert _flag_value(cmd, "-color_trc") == "bt709"
    assert _flag_value(cmd, "-colorspace") == "bt2020nc"


def test_sdr_partial_source_tags_mix_with_bt709():
    """Source tags only some of the three — the other two fall back."""
    cmd = _build_sdr_cmd({
        "color_primaries": "bt709",
        "color_transfer": None,  # missing
        "color_space": "bt709",
    })
    assert _flag_value(cmd, "-color_primaries") == "bt709"
    assert _flag_value(cmd, "-color_trc") == "bt709"  # fell back
    assert _flag_value(cmd, "-colorspace") == "bt709"


def test_probe_normalises_unknown_to_none():
    """ffprobe returns 'unknown' / 'reserved' for unset fields. The
    probe helper must normalise those to None so the SDR fallback
    fires for them."""
    fake_stdout = '{"streams":[{"color_primaries":"unknown","color_transfer":"reserved","color_space":""}]}'
    with patch("pipeline.ffmpeg.subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = fake_stdout
        result = _probe_source_color("/fake.mkv")
    assert result == {
        "color_primaries": None,
        "color_transfer": None,
        "color_space": None,
    }


def test_probe_returns_real_tags_unchanged():
    """When ffprobe returns concrete colour names, they pass through."""
    fake_stdout = '{"streams":[{"color_primaries":"bt709","color_transfer":"bt709","color_space":"bt709"}]}'
    with patch("pipeline.ffmpeg.subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = fake_stdout
        result = _probe_source_color("/fake.mkv")
    assert result == {
        "color_primaries": "bt709",
        "color_transfer": "bt709",
        "color_space": "bt709",
    }
