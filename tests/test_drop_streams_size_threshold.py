"""Pin the 2026-05-23 relaxation of the drop-to-path size threshold.

Background: ``_ffmpeg_drop_streams_to_path`` had a size sanity check that
rejected any output below 50% of source. Foreign-track-heavy files
(Bluey episodes with 24 audio dubs, where stripping 23 of them
legitimately drops the file to ~40% of source) were repeatedly failing
this guard — the track-count proof-of-work check downstream would have
verified the strip was correct, but the size gate rejected first.

The fix relaxes the threshold to 10% (still catches truly-truncated
torn-mux outputs while letting heavy-strip results through).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_drop_streams_size_threshold_relaxed_to_ten_percent():
    """The hard-coded size ratio threshold must allow heavy-strip
    outputs (~40% of source for 23-of-24 audio-track removals) through.
    Pre-2026-05-23 this was 0.5 (50%) and rejected legitimate strips.

    Both _ffmpeg_drop_streams_to_path (prep-time strip) and
    _mkvmerge_drop_streams (post-encode compliance fix) had the same
    50% threshold; both must be relaxed consistently."""
    src = (Path(__file__).resolve().parent.parent / "pipeline" / "compliance_fixers.py").read_text()
    # Two relaxed guards — count occurrences of `src_size * 0.10`.
    assert src.count("src_size * 0.10") == 2, (
        "Expected exactly 2 occurrences of `src_size * 0.10` (one per drop "
        "helper). Got "
        + str(src.count("src_size * 0.10"))
        + ". Was 0.5 (50%) before 2026-05-23 and rejected valid heavy-strip outputs."
    )
    # And the old 0.5 threshold must not silently linger.
    assert "src_size * 0.5" not in src, (
        "Old 50% threshold should not appear in compliance_fixers.py — "
        "re-introducing it would regress the Bluey-class strip failures "
        "from 2026-05-23."
    )
