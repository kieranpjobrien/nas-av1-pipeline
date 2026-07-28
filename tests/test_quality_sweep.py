"""The library-wide quality sweep and its remediation matching.

These tools delete files, so their judgement and their path matching both
need pinning. The matching in particular bit once already: on the first run
only 330 of 482 garbage files matched an *arr record, because the pipeline
renames files and the basenames no longer agreed.
"""

from tools.quality_remediate import _ep_key, _slug, lookup
from tools.quality_sweep import TIER_FLOOR_MBMIN, floor_for


class TestFloorFor:
    def test_live_action_1080p(self):
        e = {"video": {"resolution_class": "1080p"}}
        assert floor_for(e) == 50.0

    def test_animation_is_scaled_down(self):
        e = {"video": {"resolution_class": "1080p"}, "tmdb": {"genres": ["Animation"]}}
        floor = floor_for(e)
        assert floor is not None
        assert floor < 50.0, "animation must get a lower floor than live action"
        assert floor > 25.0, "but not so low it waves junk through"

    def test_unknown_resolution_is_unjudgeable(self):
        assert floor_for({"video": {"resolution_class": "potato"}}) is None


class TestTierFloors:
    def test_remux_tier_floor_is_far_above_the_resolution_floor(self):
        """A real 1080p remux is untouched Blu-ray video (150-225 MB/min).
        The tier floor only needs to be high enough to catch a re-encode
        wearing the tag — Brooklyn Nine-Nine sat at 14."""
        assert TIER_FLOOR_MBMIN["Bluray-1080p Remux"] > 50.0

    def test_brooklyn_nine_nine_case_would_be_caught(self):
        assert 14.0 < TIER_FLOOR_MBMIN["Bluray-1080p Remux"]


class TestEpisodeKeyMatching:
    def test_slug_ignores_punctuation_and_case(self):
        assert _slug("Brooklyn Nine-Nine") == _slug("brooklyn nine nine")
        assert _slug("Bob's Burgers") == _slug("bobs burgers")

    def test_key_from_arr_style_path(self):
        p = r"\\NAS\Series\Brooklyn Nine-Nine\Season 5\Brooklyn Nine-Nine - S05E14 - The Box.mkv"
        assert _ep_key(p) == (_slug("Brooklyn Nine-Nine"), 5, 14)

    def test_key_from_pipeline_renamed_path(self):
        """The pipeline strips the ' - ' separators. Same episode, different
        filename — this is exactly what broke basename matching."""
        p = r"\\NAS\Series\Brooklyn Nine-Nine\Season 5\Brooklyn Nine-Nine S05E14 The Box.mkv"
        assert _ep_key(p) == (_slug("Brooklyn Nine-Nine"), 5, 14)

    def test_both_naming_styles_produce_the_same_key(self):
        a = r"\\NAS\Series\The West Wing\Season 2\The West Wing - S02E18 - People.mkv"
        b = r"\\NAS\Series\The West Wing\Season 2\The West Wing S02E18 People.mkv"
        assert _ep_key(a) == _ep_key(b)

    def test_movie_path_has_no_episode_key(self):
        assert _ep_key(r"\\NAS\Movies\Heat (1995)\Heat (1995).mkv") is None

    def test_lookup_falls_back_from_basename_to_episode_key(self):
        rec = ("series", 42, 7)
        by_name = {}  # pipeline renamed it, so the basename is not known
        by_ep = {(_slug("The West Wing"), 2, 18): rec}
        p = r"\\NAS\Series\The West Wing\Season 2\The West Wing S02E18 People.mkv"
        assert lookup(by_name, by_ep, p) == rec

    def test_lookup_prefers_an_exact_basename_hit(self):
        exact = ("series", 1, 1)
        fallback = ("series", 99, 99)
        p = r"\\NAS\Series\Show\Season 1\Show - S01E01 - Pilot.mkv"
        by_name = {"show - s01e01 - pilot.mkv": exact}
        by_ep = {(_slug("Show"), 1, 1): fallback}
        assert lookup(by_name, by_ep, p) == exact

    def test_lookup_returns_none_when_nothing_matches(self):
        assert lookup({}, {}, r"\\NAS\Series\Show\Season 1\Show S01E01.mkv") is None
