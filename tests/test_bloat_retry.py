"""Regression: source-relative bloat guard (retry tighter, then convert).

NVENC's -cq -rc vbr -b:v 0 treats -maxrate as advisory, so 4K-HDR / grainy
sources can encode LARGER than source. Ant-Man (10.6->13.8 GB) and The Avengers
(13.4->16.6 GB) shipped past the AV1-only grow-refuse (2026-07-24). Policy:
retry at a higher CQ until the output fits under source, then ship; flag + keep
source if even the tightest attempt bloats.
"""

from pipeline.full_gamut import _bloat_retry_plan, _with_cq


def test_with_cq_replaces_value_and_leaves_original():
    cmd = ["ffmpeg", "-i", "x.mkv", "-c:v", "av1_nvenc", "-cq", "22", "-preset", "p7"]
    out = _with_cq(cmd, 28)
    assert out[out.index("-cq") + 1] == "28"
    assert cmd[cmd.index("-cq") + 1] == "22"  # input list not mutated


def test_ship_when_output_not_bigger_than_source():
    assert _bloat_retry_plan(90, 100, 22, 0) == ("ship", None)
    assert _bloat_retry_plan(100, 100, 22, 0) == ("ship", None)  # equal is fine


def test_retry_bumps_cq_cumulatively():
    assert _bloat_retry_plan(130, 100, 22, 0) == ("retry", 28)  # +6
    assert _bloat_retry_plan(130, 100, 22, 1) == ("retry", 34)  # +12


def test_flag_after_max_retries():
    # attempt == max_retries (default 2) -> give up, keep source
    assert _bloat_retry_plan(130, 100, 22, 2) == ("flag", None)
