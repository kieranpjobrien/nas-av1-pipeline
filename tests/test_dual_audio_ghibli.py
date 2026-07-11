"""Ghibli dual-audio rule: keep BOTH original (Japanese) + English audio.

The default policy strips foreign dubs, including the English dub of a
foreign-origin film. For Studio Ghibli we relax that so younger viewers can
watch the English dub while adults pick the original language + subtitles.

The per-film decision lives in ``should_keep_dual_audio`` and is shared by the
encoder (pipeline.ffmpeg) and the gap-filler (pipeline.gap_filler) so both keep
identical tracks -- the two paths diverging is what caused the 2026-04-23 audio
loss. The selector's keep_english_too behaviour itself is covered by
test_audio_strip_rules / test_ffmpeg_builder; this file pins the decision logic.
Added 2026-06-28.
"""

from pipeline.config import build_config
from pipeline.streams import should_keep_dual_audio


def _entry(director):
    return {"tmdb": {"director": director, "original_language": "ja"}}


def test_ghibli_directors_trigger_dual_audio_under_default_config():
    cfg = build_config({})
    assert should_keep_dual_audio(_entry("Hayao Miyazaki"), cfg) is True
    assert should_keep_dual_audio(_entry("Isao Takahata"), cfg) is True  # Grave of the Fireflies


def test_match_is_case_and_whitespace_insensitive():
    cfg = {"dual_audio_directors": ["Hayao Miyazaki"]}
    assert should_keep_dual_audio(_entry("HAYAO MIYAZAKI"), cfg) is True
    assert should_keep_dual_audio(_entry("  hayao miyazaki  "), cfg) is True


def test_non_ghibli_and_missing_director_do_not_trigger():
    cfg = build_config({})
    assert should_keep_dual_audio(_entry("Christopher Nolan"), cfg) is False
    assert should_keep_dual_audio({"tmdb": {}}, cfg) is False
    assert should_keep_dual_audio({}, cfg) is False


def test_global_flag_forces_dual_audio_for_everything():
    cfg = {"audio_keep_english_with_original": True, "dual_audio_directors": []}
    assert should_keep_dual_audio(_entry("Christopher Nolan"), cfg) is True


# --- Generalised 2026-07-11: ANY animated title keeps both languages ----------


def _animated(director=None, genres=("Animation",), orig="ja"):
    return {"tmdb": {"director": director, "original_language": orig,
                     "genres": [{"name": g} for g in genres]}}


def test_animation_genre_triggers_dual_audio_without_ghibli_director():
    """A non-Ghibli anime keeps both languages purely on the Animation genre."""
    cfg = build_config({})
    assert should_keep_dual_audio(_animated(director="Mamoru Hosoda"), cfg) is True
    assert should_keep_dual_audio(_animated(director=None), cfg) is True


def test_live_action_foreign_does_not_trigger_dual_audio():
    """The generalisation must not sweep in live-action foreign films."""
    cfg = build_config({})
    entry = {"tmdb": {"director": "Bong Joon-ho", "original_language": "ko",
                      "genres": [{"name": "Thriller"}]}}
    assert should_keep_dual_audio(entry, cfg) is False
