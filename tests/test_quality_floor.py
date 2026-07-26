"""The pipeline must refuse to encode sources below the quality floor.

2026-07-26: the operator found the pipeline encoding a 400 MB Fargo episode
(8.19 MB/min at 720p, floor 12) and a 400 MB Ally McBeal (9.18 MB/min at
1080p, floor 18). Re-encoding junk does not just waste GPU time — it bakes
the junk permanently into AV1, which is strictly worse than leaving the
source alone until Sonarr/Radarr grab a proper release.

The Sonarr/Radarr Quality Definitions gate ACQUISITION. This gates ENCODING.
"""

import pytest

from pipeline.__main__ import _source_mbmin, _under_quality_floor, categorise_entry
from pipeline.state import FileStatus, PipelineState


class _Control:
    def should_skip(self, _filepath):
        return False


@pytest.fixture
def state(tmp_path):
    return PipelineState(str(tmp_path / "state.db"))


def _entry(*, mb, minutes, res="1080p", codec="h264", path="/nas/Show/S01E01.mkv"):
    return {
        "filepath": path,
        "filename": path.rsplit("/", 1)[-1],
        "file_size_bytes": int(mb * 1_000_000),
        "duration_seconds": minutes * 60,
        "video": {"codec_raw": codec, "codec": codec, "resolution_class": res},
        "audio_streams": [{"codec": "eac3", "language": "eng"}],
    }


def test_mbmin_arithmetic():
    # 400 MB over 47 minutes ~= the real Fargo S02E03 row.
    assert _source_mbmin(_entry(mb=400, minutes=47)) == pytest.approx(8.51, abs=0.01)


def test_mbmin_none_when_duration_unknown():
    assert _source_mbmin(_entry(mb=400, minutes=0)) is None


@pytest.mark.parametrize(
    ("res", "mb", "minutes", "expect_under"),
    [
        ("720p", 400, 47, True),  # Fargo: 8.5 < 12
        ("1080p", 400, 44, True),  # Ally McBeal: 9.1 < 18
        ("1080p", 1200, 44, False),  # 27 MB/min, healthy
        ("480p", 400, 44, False),  # 9.1 > 8 floor for SD
        ("2160p", 900, 44, True),  # 20 < 25
    ],
)
def test_floor_by_resolution(res, mb, minutes, expect_under):
    under, _, _ = _under_quality_floor(_entry(mb=mb, minutes=minutes, res=res))
    assert under is expect_under


def test_unknown_resolution_fails_open():
    """A resolution we have no floor for must never park the file."""
    under, _, _ = _under_quality_floor(_entry(mb=1, minutes=100, res="weird"))
    assert under is False


def test_undersized_source_is_parked_not_queued(state):
    entry = _entry(mb=400, minutes=47, res="720p")
    category, item = categorise_entry(entry, {}, state, _Control())
    assert category == "skip", "junk source must not reach the encoder"
    assert item is None
    row = state.get_file(entry["filepath"])
    assert row["status"] == FileStatus.FLAGGED_UNDERSIZED.value
    assert "below quality floor" in row["reason"]


def test_healthy_source_still_encodes(state):
    entry = _entry(mb=1200, minutes=44, res="1080p")
    category, item = categorise_entry(entry, {}, state, _Control())
    assert category == "full_gamut"
    assert item["filepath"] == entry["filepath"]


def test_priority_overrides_the_floor(state):
    """Operator intent must never be silently ignored."""
    entry = _entry(mb=400, minutes=47, res="720p")
    category, _ = categorise_entry(entry, {}, state, _Control(), priority_paths={entry["filepath"]})
    assert category == "full_gamut"


def test_force_reencode_overrides_the_floor(state):
    entry = _entry(mb=400, minutes=47, res="720p")
    state.set_file(entry["filepath"], FileStatus.PENDING, force_reencode=True)
    category, _ = categorise_entry(entry, {}, state, _Control())
    assert category == "full_gamut"


def test_gate_can_be_disabled_by_config(state):
    entry = _entry(mb=400, minutes=47, res="720p")
    category, _ = categorise_entry(entry, {"enforce_quality_floor": False}, state, _Control())
    assert category == "full_gamut"


def test_animation_gets_a_lower_floor(state):
    """Animation compresses better than live action, so healthy animated
    episodes sit under the live-action floor legitimately.

    Real case 2026-07-26: Bob's Burgers S01 at 15-17 MB/min (1080p) was
    parked by the live-action floor of 18 on the first run. Those are fine.
    """
    live = _entry(mb=350, minutes=22, res="1080p")  # 15.9 MB/min
    under_live, _, _ = _under_quality_floor(live)
    assert under_live is True, "15.9 MB/min is genuinely low for live action"

    toon = dict(live)
    toon["tmdb"] = {"genres": ["Animation", "Comedy"]}
    under_toon, mbmin, floor = _under_quality_floor(toon)
    assert under_toon is False, f"healthy animation wrongly parked ({mbmin:.1f} < {floor:.1f})"


def test_anime_library_counts_as_animation():
    e = _entry(mb=350, minutes=22, res="1080p")
    e["library_type"] = "anime"
    under, _, _ = _under_quality_floor(e)
    assert under is False


def test_genuinely_tiny_animation_is_still_parked(state):
    """The scale must not become a free pass — 5 MB/min is junk either way."""
    e = _entry(mb=110, minutes=22, res="1080p")  # 5.0 MB/min
    e["tmdb"] = {"genres": ["Animation"]}
    under, _, _ = _under_quality_floor(e)
    assert under is True


def test_av1_output_is_never_parked_by_the_floor(state):
    """AV1 output is 40-50% smaller than its source BY DESIGN, so a
    correctly-encoded file sits below the source floor legitimately.

    Applying the floor to AV1 wrongly parked 467 already-encoded files on
    2026-07-26 before this exclusion was added.
    """
    entry = _entry(mb=400, minutes=47, res="1080p", codec="av1")
    category, _ = categorise_entry(entry, {}, state, _Control())
    row = state.get_file(entry["filepath"])
    assert row is None or row["status"] != FileStatus.FLAGGED_UNDERSIZED.value, (
        "AV1 output must never be parked for being below the SOURCE floor"
    )


def test_replacement_release_unparks_the_file(state):
    """The whole point of parking is that a better release replaces it —
    so a newer file_mtime must resurrect the row automatically."""
    entry = _entry(mb=400, minutes=47, res="720p")
    categorise_entry(entry, {}, state, _Control())
    assert state.get_file(entry["filepath"])["status"] == FileStatus.FLAGGED_UNDERSIZED.value

    import time

    better = _entry(mb=1400, minutes=47, res="1080p")
    better["file_mtime"] = time.time() + 3600  # Sonarr dropped a proper release
    category, _ = categorise_entry(better, {}, state, _Control())
    assert category == "full_gamut", "a replacement release must un-park the file"
