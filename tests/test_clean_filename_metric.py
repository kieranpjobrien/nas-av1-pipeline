"""Pin the dashboard clean-filename metric (2026-06-26).

The completion metric used clean_filename(), which over-normalised — it stripped
cosmetic " - " separators (Show - SxxExx - Title), flagged well-named files as dirty,
and even dropped episode titles (South Park - S05E07 - Proper Condom Use -> South Park
S05E07). SCENE_TAG_RE's lone words (PROPER/MULTi) also false-positived on titles. The
metric now scores on genuine scene-release tags only.
"""

import pytest

from server.routers.library import _filename_is_clean


@pytest.mark.parametrize(
    "name",
    [
        "Industry - S02E07 - Lone Wolf and Cub.mkv",  # standard Show - SxxExx - Title
        "Love, Death & Robots - S02E05 - The Tall Grass.mkv",
        "South Park - S05E07 - Proper Condom Use.mkv",  # "Proper" is a title word
        "Star Wars - The Clone Wars S06E13 Final Multi.mkv",  # "Multi" = dup marker, not a tag
        "St. Denis Medical S01E06.mkv",
        "The Matrix (1999).mkv",  # clean movie
        "Mad Max - Fury Road (2015).mkv",  # "Max" not in scene (dot/dash) context
    ],
)
def test_clean_names_pass(name):
    assert _filename_is_clean(name) is True


@pytest.mark.parametrize(
    "name",
    [
        "Bluey.Shop.1080p.DSNP.WEB-DL.AAC2.0.H.264-OldT.mkv",  # full scene name
        "The Matrix 1999 2160p UHD BluRay x265.mkv",
        "Some.Movie.2020.720p.WEBRip.x264-RARBG.mkv",
        "Show.S01E01.HDTV.x264-GROUP.mkv",
    ],
)
def test_scene_names_flagged(name):
    assert _filename_is_clean(name) is False
