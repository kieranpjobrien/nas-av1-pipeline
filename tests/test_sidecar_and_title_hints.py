"""Regressions for two silent subtitle/language-loss paths (found 2026-07-25).

1. subs._FLAG_TOKENS was defined and never consulted, so `.hi` / `.sdh` / `.cc`
   sidecars parsed as LANGUAGES (Hindi, etc). An HI-only English sub then never
   matched ENG_LANGS: never muxed, and deleted by gap_filler as "foreign".
2. pick_english_sidecars ignored is_forced, so `Title.en.forced.srt` beat
   `Title.en.srt` on listdir order and the full English sub was deleted.
3. _TITLE_HINTS matched as a substring, so a track titled "Bengali" was labelled
   English at 0.9 confidence -- which also skips whisper, so nothing corrects it.
"""

import re

from pipeline.language import _TITLE_HINTS
from pipeline.subs import SidecarSub, _parse_language_and_flags, pick_english_sidecars


def _sc(name, language, *, is_forced=False, is_hi=False):
    return SidecarSub(
        path=f"/m/{name}", filename=name, stem="Movie",
        language=language, is_forced=is_forced, is_hi=is_hi,
    )


def test_flag_tokens_are_not_languages():
    assert _parse_language_and_flags("hi") == ("und", ["hi"])
    assert _parse_language_and_flags("sdh") == ("und", ["sdh"])
    assert _parse_language_and_flags("cc") == ("und", ["cc"])
    # A real language code still wins, and flags still register.
    assert _parse_language_and_flags("en.forced") == ("en", ["forced"])
    assert _parse_language_and_flags("eng.sdh") == ("eng", ["sdh"])


def test_full_english_sub_beats_forced_and_forced_is_never_deleted():
    forced = _sc("Movie.en.forced.srt", "en", is_forced=True)
    full = _sc("Movie.en.srt", "en")
    # listdir order puts the forced one first — it must still lose.
    to_mux, to_delete = pick_english_sidecars([forced, full])
    assert to_mux == [full], "the FULL English sub must be the one muxed"
    assert forced not in to_delete, "a forced English sidecar must never be deleted"


def test_forced_english_used_when_it_is_the_only_english():
    forced = _sc("Movie.en.forced.srt", "en", is_forced=True)
    to_mux, _ = pick_english_sidecars([forced])
    assert to_mux == [forced], "better than shipping no subtitle at all"


def _title_hint(title: str):
    tokens = re.findall(r"\w+", title.lower())
    for hint, code in _TITLE_HINTS.items():
        if any(tok.startswith(hint) for tok in tokens):
            return code
    return None


def test_bengali_is_not_english():
    assert _title_hint("Bengali") is None
    assert _title_hint("Bengali (Commentary)") is None


def test_genuine_english_titles_still_detected():
    assert _title_hint("English 5.1") == "en"
    assert _title_hint("ENGSUB") == "en"
    assert _title_hint("Eng") == "en"
    assert _title_hint("Hindi") == "hi"
