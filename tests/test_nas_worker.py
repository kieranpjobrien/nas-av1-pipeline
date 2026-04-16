"""Tests for pipeline/nas_worker.py — UNC path conversion and shell quoting."""

import pytest

from pipeline.nas_worker import _shell_quote, unc_to_container_path


class TestUncToContainerPath:
    """unc_to_container_path converts Windows UNC paths to Docker container /media/ paths."""

    @pytest.mark.parametrize(
        "unc,expected",
        [
            # Standard movie path
            (
                r"\\KieranNAS\Media\Movies\Inception (2010)\Inception (2010).mkv",
                "/media/Movies/Inception (2010)/Inception (2010).mkv",
            ),
            # Series path
            (
                r"\\KieranNAS\Media\Series\The Wire\Season 01\The Wire S01E01.mkv",
                "/media/Series/The Wire/Season 01/The Wire S01E01.mkv",
            ),
            # Lowercase NAS name variant
            (
                r"\\kierannas\Media\Movies\Test.mkv",
                "/media/Movies/Test.mkv",
            ),
            # Lowercase share name
            (
                r"\\kierannas\media\Movies\Test.mkv",
                "/media/Movies/Test.mkv",
            ),
            # Deeply nested path
            (
                r"\\KieranNAS\Media\Series\Show\Season 02\Show S02E05 Episode Title.mkv",
                "/media/Series/Show/Season 02/Show S02E05 Episode Title.mkv",
            ),
        ],
    )
    def test_unc_conversion(self, unc, expected):
        """UNC paths are correctly converted to container-internal /media/ paths."""
        assert unc_to_container_path(unc) == expected

    def test_non_matching_unc_unchanged(self):
        """Paths that don't match the KieranNAS prefix are returned with normalised slashes."""
        path = r"\\OtherServer\Share\file.mkv"
        result = unc_to_container_path(path)
        # Should have forward slashes but no /media prefix
        assert "/media" not in result
        assert "file.mkv" in result


class TestShellQuote:
    """_shell_quote wraps strings safely for SSH shell commands."""

    def test_simple_string(self):
        """A simple path is wrapped in single quotes."""
        assert _shell_quote("/media/Movies/Test.mkv") == "'/media/Movies/Test.mkv'"

    def test_spaces(self):
        """Paths with spaces are safely quoted."""
        result = _shell_quote("/media/Movies/My Movie (2020)/My Movie (2020).mkv")
        assert result.startswith("'")
        assert result.endswith("'")
        assert "My Movie" in result

    def test_single_quotes_escaped(self):
        """Single quotes within the string are properly escaped."""
        result = _shell_quote("/media/Movies/It's a Test.mkv")
        # Expected: '/media/Movies/It'\''s a Test.mkv'
        assert "\\'" in result
        assert result.startswith("'")
        assert result.endswith("'")

    def test_special_chars(self):
        """Special shell characters ($, `, !) inside single quotes are safe."""
        result = _shell_quote("/media/Test $HOME `cmd`.mkv")
        assert result.startswith("'")
        assert "$HOME" in result
        assert "`cmd`" in result

    def test_empty_string(self):
        """Empty string produces empty single quotes."""
        assert _shell_quote("") == "''"

    def test_multiple_single_quotes(self):
        """Multiple single quotes are each independently escaped."""
        result = _shell_quote("it's Bob's file")
        # Each ' becomes '\'' (close quote, escaped quote, reopen quote)
        assert result.count("\\'") == 2
