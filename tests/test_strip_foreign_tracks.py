"""The strip planner must never lose a track it cannot justify losing.

Three rules, in priority order:
  * a track whose language is unknown is NEVER dropped (operator, 2026-04-29);
  * a file never ends up with zero audio (the 2026-04-22 class of bug);
  * audio is never judged without TMDb original_language - a dub and the real
    thing are indistinguishable without it.
"""

import pytest

from tools.strip_foreign_tracks import allowed_audio_langs, plan_file, stream_lang


def entry(orig="en", audio=(), subs=()):
    return {
        "filepath": r"\\NAS\Media\Movies\X (2000)\X (2000).mkv",
        "tmdb": {"original_language": orig} if orig else {},
        "audio_streams": [dict(a) for a in audio],
        "subtitle_streams": [dict(s) for s in subs],
    }


class TestStreamLang:
    def test_prefers_whisper_over_a_lying_tag(self):
        """Every Bluey track ships tagged 'eng'; whisper heard Chinese."""
        assert stream_lang({"language": "eng", "detected_language": "zh"}) == "zh"

    def test_falls_back_to_the_tag(self):
        assert stream_lang({"language": "fre"}) == "fre"

    def test_ignores_an_empty_detection(self):
        assert stream_lang({"language": "ger", "detected_language": "und"}) == "ger"


class TestAllowedLangs:
    def test_none_when_original_language_unknown(self):
        assert allowed_audio_langs({"tmdb": {}}) is None

    def test_includes_english_and_the_original(self):
        a = allowed_audio_langs({"tmdb": {"original_language": "ja"}})
        assert "eng" in a and "jpn" in a

    def test_iso_variants_resolve_to_the_same_bucket(self):
        """'fre' and 'fra' are both French - Amour must not read as foreign."""
        a = allowed_audio_langs({"tmdb": {"original_language": "fr"}})
        assert "fre" in a and "fra" in a


class TestNeverDropUnknown:
    def test_und_audio_is_kept(self):
        p = plan_file(entry("en", audio=[{"language": "eng"}, {"language": "und"}]))
        assert p["audio_drop"] == []

    def test_und_subs_are_kept(self):
        p = plan_file(entry("en", subs=[{"language": "und"}, {"language": "spa"}]))
        assert p["sub_drop"] == [1]
        assert 0 in p["sub_keep"]

    def test_audio_untouched_without_tmdb(self):
        p = plan_file(entry(None, audio=[{"language": "spa"}, {"language": "ger"}]))
        assert p["audio_drop"] == []


class TestUnjudgeableLanguagesSurvive:
    """'mul' is the one that bit: Joyeux Noel (2005) is French/German/English
    in the same scenes, TMDb calls it 'fr', and its multilingual mix is tagged
    'mul'. A naive foreign-language rule drops the actual film."""

    def test_mul_audio_is_never_dropped(self):
        p = plan_file(entry("fr", audio=[{"language": "mul"}, {"language": "fre"}, {"language": "rus"}]))
        assert 0 in p["audio_keep"]
        assert p["audio_langs_dropped"] == ["rus"]

    def test_mul_subs_are_never_dropped(self):
        p = plan_file(entry("en", subs=[{"language": "mul"}, {"language": "spa"}]))
        assert p["sub_drop"] == [1]

    @pytest.mark.parametrize("code", ["mis", "qaa", "und", "unk", ""])
    def test_other_uncodeable_languages_survive(self, code):
        p = plan_file(entry("en", audio=[{"language": "eng"}, {"language": code}]))
        assert p["audio_drop"] == []


class TestNeverZeroAudio:
    def test_all_foreign_audio_is_flagged_not_stripped(self):
        p = plan_file(entry("en", audio=[{"language": "zho"}, {"language": "tur"}]))
        assert p["audio_keep"] == []
        assert "zero audio" in p["skip"]

    def test_one_english_survivor_is_enough(self):
        p = plan_file(entry("en", audio=[{"language": "zho"}, {"language": "eng"}]))
        assert p["audio_drop"] == [0]
        assert p["audio_keep"] == [1]
        assert p["skip"] == ""


class TestOrdinaryStripping:
    def test_english_film_with_dubs(self):
        p = plan_file(
            entry(
                "en",
                audio=[
                    {"language": "eng"},
                    {"language": "spa"},
                    {"language": "ger"},
                    {"language": "tur"},
                ],
            )
        )
        assert p["audio_keep"] == [0]
        assert p["audio_drop"] == [1, 2, 3]
        assert p["audio_langs_dropped"] == ["spa", "ger", "tur"]

    def test_foreign_film_keeps_its_own_language_and_english(self):
        """Amour is French. Its French audio is the point of the film."""
        p = plan_file(entry("fr", audio=[{"language": "fre"}, {"language": "eng"}, {"language": "ger"}]))
        assert p["audio_drop"] == [2]
        assert set(p["audio_keep"]) == {0, 1}

    def test_subs_are_english_only_regardless_of_original_language(self):
        p = plan_file(entry("fr", subs=[{"language": "eng"}, {"language": "fre"}, {"language": "spa"}]))
        assert p["sub_keep"] == [0]
        assert p["sub_drop"] == [1, 2]

    def test_nothing_foreign_is_skipped(self):
        p = plan_file(entry("en", audio=[{"language": "eng"}], subs=[{"language": "eng"}]))
        assert p["skip"] == "nothing foreign"

    def test_source_sub_count_is_recorded_for_the_verify(self):
        """_post_mux_verify_and_replace uses it as the no-unintended-loss floor."""
        p = plan_file(entry("en", subs=[{"language": "eng"}, {"language": "spa"}]))
        assert p["source_sub_count"] == 2


class TestForeignFilmsKeepSomethingReadable:
    """Amour is French, with French subs and no English ones. 'English subs
    only' applied literally leaves a French film with no subtitles at all."""

    def test_french_film_with_no_english_sub_keeps_its_french_subs(self):
        p = plan_file(entry("fr", audio=[{"language": "fre"}], subs=[{"language": "fre"}]))
        assert p["sub_drop"] == []
        assert "no English sub" in p["note"]

    def test_french_film_WITH_an_english_sub_does_strip_the_french(self):
        p = plan_file(entry("fr", audio=[{"language": "fre"}], subs=[{"language": "eng"}, {"language": "fre"}]))
        assert p["sub_drop"] == [1]
        assert p["note"] == ""

    def test_english_film_still_strips_foreign_subs_with_none_left(self):
        """An English speaker does not need subs on an English film."""
        p = plan_file(entry("en", audio=[{"language": "eng"}], subs=[{"language": "spa"}]))
        assert p["sub_drop"] == [0]

    def test_und_sub_does_not_count_as_an_english_fallback(self):
        """An untagged sub might be anything; it cannot justify binning the rest."""
        p = plan_file(entry("ja", audio=[{"language": "jpn"}], subs=[{"language": "und"}, {"language": "jpn"}]))
        assert p["sub_drop"] == []


class TestAnimationDualAudio:
    """All animated content keeps BOTH original and English (policy 2026-07-11).
    Falls out of the rule automatically, but pin it so nobody 'optimises' it."""

    @pytest.mark.parametrize("orig", ["ja", "fr", "en"])
    def test_english_always_survives(self, orig):
        p = plan_file(entry(orig, audio=[{"language": "eng"}, {"language": "jpn"}]))
        assert 0 in p["audio_keep"]
