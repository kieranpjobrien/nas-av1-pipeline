"""Stale state-DB paths must resolve to the renamed file, or to nothing.

The pipeline cleans filenames during encode and the state row keeps pointing at
the old name. The next pass cannot find it and flags the file corrupt. On
2026-10-19 that accounted for 10 of 88 flagged_corrupt rows - every one of them
sitting on disk, none of them corrupt.
"""

import tools.resolve_state_paths as rsp


def make(tmp_path, *names):
    for n in names:
        (tmp_path / n).write_bytes(b"x")
    return tmp_path


class TestDashedSeriesRenames:
    def test_dashed_episode_resolves(self, tmp_path):
        make(tmp_path, "Love, Death & Robots S02E05 The Tall Grass.mkv")
        stale = str(tmp_path / "Love, Death & Robots - S02E05 - The Tall Grass.mkv")
        assert rsp._resolve_one(stale) == str(tmp_path / "Love, Death & Robots S02E05 The Tall Grass.mkv")

    def test_existing_path_is_returned_unchanged(self, tmp_path):
        make(tmp_path, "Already Fine S01E01 Title.mkv")
        good = str(tmp_path / "Already Fine S01E01 Title.mkv")
        assert rsp._resolve_one(good) == good


class TestFilmRenamesAndRemuxes:
    """The dashed-series pattern covers TV only; films rename differently and
    a remux changes the extension outright."""

    def test_dot_stripped_title(self, tmp_path):
        make(tmp_path, "Dr No (1962).mkv")
        assert rsp._resolve_one(str(tmp_path / "Dr. No (1962).mkv")) == str(tmp_path / "Dr No (1962).mkv")

    def test_initialism_with_dots(self, tmp_path):
        make(tmp_path, "A I Artificial Intelligence (2001).mkv")
        stale = str(tmp_path / "A.I. Artificial Intelligence (2001).mkv")
        assert rsp._resolve_one(stale) == str(tmp_path / "A I Artificial Intelligence (2001).mkv")

    def test_extension_change_from_a_remux(self, tmp_path):
        make(tmp_path, "Moonlight (2016).mkv")
        assert rsp._resolve_one(str(tmp_path / "Moonlight (2016).mp4")) == str(tmp_path / "Moonlight (2016).mkv")

    def test_trailing_dot_in_title(self, tmp_path):
        make(tmp_path, "Emma (2020).mkv")
        assert rsp._resolve_one(str(tmp_path / "Emma. (2020).mkv")) == str(tmp_path / "Emma (2020).mkv")


class TestItRefusesToGuess:
    def test_different_title_is_not_matched(self, tmp_path):
        make(tmp_path, "A Completely Different Film (1999).mkv")
        assert rsp._resolve_one(str(tmp_path / "Dr. No (1962).mkv")) is None

    def test_ambiguous_match_is_refused(self, tmp_path):
        """Two files normalising the same way - picking either is a coin toss."""
        make(tmp_path, "Dr No (1962).mkv", "Dr. No (1962).mp4")
        assert rsp._resolve_one(str(tmp_path / "Dr-No-1962.mkv")) is None

    def test_missing_folder_is_not_an_error(self, tmp_path):
        assert rsp._resolve_one(str(tmp_path / "nope" / "Film (2000).mkv")) is None

    def test_non_media_files_are_ignored(self, tmp_path):
        make(tmp_path, "Dr No (1962).nfo", "Dr No (1962).srt")
        assert rsp._resolve_one(str(tmp_path / "Dr. No (1962).mkv")) is None

    def test_year_difference_is_a_different_film(self, tmp_path):
        make(tmp_path, "The Thing (2011).mkv")
        assert rsp._resolve_one(str(tmp_path / "The Thing (1982).mkv")) is None
