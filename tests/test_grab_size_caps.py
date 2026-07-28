"""Release selection must have an upper bound, not just a lower one.

2026-07-28: "take the largest available" with no ceiling queued a 124 GB
Game of Thrones episode and House episodes to match - season packs and absurd
remuxes mis-parsed as single episodes. 3,907 jobs / 26 TB were queued before
the operator caught it and told me to kill the lot.

Operator's rule: no TV episode over 20 GB, and that only for the rarest
exceptions; 10 GB is the working maximum.
"""

from tools.grab_best import (
    EPISODE_ABSOLUTE_MAX_GB,
    EPISODE_MAX_GB,
    MOVIE_MAX_GB,
    pick,
    within_size_cap,
)


def _rel(title, gb):
    return {"title": title, "size": int(gb * 1e9), "guid": title, "indexerId": 1}


class TestSizeCap:
    def test_the_124gb_game_of_thrones_case_is_rejected(self):
        assert not within_size_cap(_rel("Game.of.Thrones.S03E07.2160p", 124), is_episode=True)

    def test_a_sane_episode_passes(self):
        assert within_size_cap(_rel("Show.S01E01.2160p", 8), is_episode=True)

    def test_movies_get_a_larger_allowance(self):
        big = _rel("Some.Movie.2019.2160p.REMUX", 60)
        assert within_size_cap(big, is_episode=False)
        assert not within_size_cap(big, is_episode=True)

    def test_movie_cap_still_bites(self):
        assert not within_size_cap(_rel("Some.Movie.2019", MOVIE_MAX_GB + 10), is_episode=False)


class TestPickRespectsCaps:
    def test_picks_under_the_working_maximum_when_available(self):
        rels = [
            _rel("Show.S01E01.1080p.WEB", 2),
            _rel("Show.S01E01.2160p.WEB", 9),
            _rel("Show.S01E01.2160p.REMUX", 45),  # over the hard cap
        ]
        got = pick(rels)
        assert got is not None
        assert (got["size"] / 1e9) <= EPISODE_MAX_GB, "must not exceed the 10 GB working maximum"

    def test_exceeds_working_max_only_when_nothing_smaller_exists(self):
        rels = [
            _rel("Show.S01E01.2160p.A", 14),
            _rel("Show.S01E01.2160p.B", 17),
        ]
        got = pick(rels)
        assert got is not None
        gb = got["size"] / 1e9
        assert EPISODE_MAX_GB < gb <= EPISODE_ABSOLUTE_MAX_GB

    def test_everything_over_the_hard_cap_yields_nothing(self):
        rels = [_rel("Show.S01E01.pack", 124), _rel("Show.S01E01.pack2", 90)]
        assert pick(rels) is None, "a season pack must not be grabbed as one episode"

    def test_still_prefers_real_4k_within_the_cap(self):
        rels = [
            _rel("Show.S01E01.1080p.BluRay.REMUX", 9.5),
            _rel("Show.S01E01.2160p.WEB-DL", 8.0),
        ]
        got = pick(rels)
        assert "2160p" in got["title"], "4K preference still applies below the cap"
