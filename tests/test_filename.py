"""Tests for pipeline/filename.py — scene tag stripping and filename normalisation."""

from pipeline.filename import clean_filename, clean_movie_name, clean_series_name


class TestCleanSeriesName:
    """Series filenames: find SxxExx anchor, keep title + episode title, strip tags."""

    def test_scene_tagged_series(self):
        """Standard scene-tagged series file gets cleaned to 'Show SxxExx'."""
        result = clean_series_name("Show.S01E02.1080p.WEB-DL.x264-GROUP")
        assert result == "Show S01E02"

    def test_scene_tagged_with_episode_title(self):
        """Episode title after SxxExx is preserved."""
        result = clean_series_name("Breaking.Bad.S05E16.Felina.1080p.BluRay.x264-GROUP")
        assert result == "Breaking Bad S05E16 Felina"

    def test_camelcase_split(self):
        """CamelCase show names get split into separate words."""
        result = clean_series_name("TheWhiteLotus.S01E01.Arrivals.1080p.WEB-DL")
        assert result is not None
        assert "The White Lotus" in result
        assert "S01E01" in result

    def test_pnow_atmos_stripped(self):
        """Concatenated pNOWAtmosHLG junk after episode title is stripped."""
        result = clean_series_name("TheWhiteLotus.S01E01.ArrivalspNOWAtmosHLG")
        assert result is not None
        assert "Arrivals" in result
        assert "NOW" not in result
        assert "Atmos" not in result
        assert "HLG" not in result

    def test_multi_episode(self):
        """Multi-episode markers like S01E01E02 are handled."""
        result = clean_series_name("Show.S01E01E02.Pilot.720p.WEB-DL-GROUP")
        assert result is not None
        assert "S01E01E02" in result

    def test_multi_episode_hyphen_range_with_episode_titles(self):
        """Hyphen-separated SxxExx-Eyy ranges preserve marker AND episode titles.

        Regression: cleaner used to match only S01E22 and drop -E24 + titles,
        producing collisions like Puffin.Rock.mkv x N for a multi-episode show.
        """
        result = clean_series_name(
            "Puffin Rock - S01E22-E24 - Finding Bernie + The Foggy Day + Run, Flap, Fly"
        )
        assert result is not None
        assert "Puffin Rock" in result
        assert "S01E22-E24" in result
        # Episode title text after the range must survive
        assert "Finding Bernie" in result
        assert "Foggy Day" in result
        assert "Run, Flap, Fly" in result

    def test_multi_episode_hyphen_short_range(self):
        """Short-form hyphen range S03E05-06 is canonicalised to S03E05-E06."""
        result = clean_series_name("Show - S03E05-06 - Title Goes Here")
        assert result is not None
        assert "S03E05-E06" in result
        assert "Title Goes Here" in result

    def test_multi_episode_dot_separated(self):
        """Dot-separated multi-episode SxxExx.Eyy keeps both episodes."""
        result = clean_series_name("Show.S01E22.E23.Episode.Name.1080p")
        assert result is not None
        assert "S01E22-E23" in result
        assert "Episode Name" in result

    def test_multi_episode_does_not_swallow_resolution(self):
        """A trailing 1080/720 must not be swallowed by the multi-episode regex."""
        # If the regex were greedy, S01E02.1080p could match "S01E02.10".
        result = clean_series_name("Show.S01E02.1080p.WEB-DL.x264-GROUP")
        assert result == "Show S01E02"

    def test_no_episode_marker_returns_none(self):
        """Filenames without SxxExx return None (not a series)."""
        result = clean_series_name("Just.A.Random.Movie.2020.1080p")
        assert result is None

    def test_trailing_bracket_stripped(self):
        """Trailing unclosed bracket is stripped by clean_filename: 'Show S01E01 (' -> 'Show S01E01'."""
        # clean_series_name may leave a trailing '(' but clean_filename strips it
        result = clean_filename(r"C:\path\Show.S01E01.(1080p.WEB-DL.mkv", "series")
        assert result is not None
        assert result.rstrip() == result  # no trailing whitespace
        assert "(" not in result

    def test_release_group_stripped(self):
        """Trailing release group (-GROUP) is stripped from episode title."""
        result = clean_series_name("Show.S01E02.Episode.Title.1080p-CRFW")
        assert result is not None
        assert "CRFW" not in result


class TestCleanMovieName:
    """Movie filenames: find year anchor, keep title + (year), strip tags."""

    def test_scene_tagged_movie(self):
        """Standard scene movie with year gets cleaned to 'Title (YYYY)'."""
        result = clean_movie_name("Movie.2020.1080p.BluRay.x264")
        assert result == "Movie (2020)"

    def test_movie_with_dots_in_title(self):
        """Dots in title are converted to spaces."""
        result = clean_movie_name("The.Shawshank.Redemption.1994.1080p.BluRay")
        assert result == "The Shawshank Redemption (1994)"

    def test_movie_with_edition(self):
        """Edition tags after year are preserved: 'Title (YYYY) Director's Cut'."""
        result = clean_movie_name("Blade.Runner.1982.Directors.Cut.1080p.BluRay")
        assert result is not None
        assert "1982" in result
        assert "Director" in result

    def test_no_year_returns_none(self):
        """Filenames without a recognisable year return None."""
        result = clean_movie_name("NoYear.Movie.1080p.BluRay")
        assert result is None

    def test_parenthesized_year(self):
        """Year in parentheses is handled: 'Movie (2020)'."""
        result = clean_movie_name("Movie.(2020).1080p.BluRay")
        assert result == "Movie (2020)"


class TestCleanFilename:
    """High-level clean_filename: wraps series/movie cleaning + extension handling."""

    def test_series_returns_clean_name_with_extension(self):
        """Series cleaning returns 'Clean Name.mkv' with the original extension."""
        result = clean_filename(r"C:\path\Show.S01E02.1080p.WEB-DL.x264-GROUP.mkv", "series")
        assert result is not None
        assert result.endswith(".mkv")
        assert result == "Show S01E02.mkv"

    def test_movie_returns_clean_name_with_extension(self):
        """Movie cleaning returns 'Title (YYYY).mkv' with the original extension."""
        result = clean_filename(r"C:\path\Movie.2020.1080p.BluRay.x264.mkv", "movie")
        assert result is not None
        assert result == "Movie (2020).mkv"

    def test_no_change_returns_none(self):
        """If the filename is already clean, return None (no rename needed)."""
        result = clean_filename(r"C:\path\Show S01E02.mkv", "series")
        assert result is None

    def test_unknown_library_type_returns_none(self):
        """Unsupported library types return None."""
        result = clean_filename(r"C:\path\test.mkv", "podcast")
        assert result is None

    def test_decimal_preserved_in_movie(self):
        """Decimal numbers in titles are not mangled: 'Naked Gun 2.5' stays as-is."""
        result = clean_movie_name("Naked.Gun.2.5.The.Smell.of.Fear.1991.1080p.BluRay")
        assert result is not None
        assert "2.5" in result

    def test_language_tags_case_sensitive(self):
        """'Italian' in a title is NOT stripped (only scene-case 'iTALiAN' would be)."""
        result = clean_series_name("An.Italian.Dream.S01E01.Pilot.1080p.WEB-DL")
        assert result is not None
        assert "Italian" in result
