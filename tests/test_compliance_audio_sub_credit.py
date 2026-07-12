"""Regression (2026-07-11): the dashboard compliance check (_compliance_for_entry)
must credit TrueHD as passthrough audio (rule 9a — Atmos carrier) and must NOT
demand an English sub for a film whose audio is already English. Before the fix,
159 correctly-TrueHD files showed as "needs EAC-3" and 75 English-audio films
showed as "missing English subs".

Pure-function unit tests — no media_report fixture, so safe to run with the
pipeline live.
"""
from __future__ import annotations

from server.routers.library import _compliance_for_entry


def _entry(audio, subs=None, ext_subs=None, orig_lang="en", codec_raw="av1"):
    return {
        "filepath": r"\\KieranNAS\Media\Movies\X (2024)\X (2024).mkv",
        "filename": "X (2024).mkv",
        "video": {"codec_raw": codec_raw},
        "audio_streams": audio,
        "subtitle_streams": subs or [],
        "external_subtitles": ext_subs or [],
        "tmdb": {"original_language": orig_lang},
    }


def test_truehd_audio_is_compliant():
    c = _compliance_for_entry(_entry([{"codec_raw": "truehd", "language": "eng"}]))
    assert c["audio_ok"] is True, "TrueHD is passthrough per rule 9a, not a transcode target"


def test_eac3_audio_is_compliant():
    c = _compliance_for_entry(_entry([{"codec_raw": "eac3", "language": "eng"}]))
    assert c["audio_ok"] is True


def test_dts_audio_not_compliant():
    c = _compliance_for_entry(_entry([{"codec_raw": "dts", "language": "eng"}]))
    assert c["audio_ok"] is False, "DTS must still transcode to EAC-3"


def test_english_audio_without_sub_is_compliant():
    """English audio → you can hear it → a missing English sub is not a gap."""
    c = _compliance_for_entry(_entry([{"codec_raw": "eac3", "language": "eng"}], subs=[]))
    assert c["subs_ok"] is True
    assert "subs_english_missing" not in c["violations"]


def test_foreign_audio_without_sub_needs_subs():
    """Japanese-audio film with no English sub genuinely needs one."""
    c = _compliance_for_entry(_entry([{"codec_raw": "eac3", "language": "jpn"}], subs=[], orig_lang="ja"))
    assert c["subs_ok"] is False
    assert "subs_english_missing" in c["violations"]


def test_english_audio_with_foreign_sub_still_flagged():
    """A foreign sub is still garbage to strip even when audio is English."""
    c = _compliance_for_entry(
        _entry([{"codec_raw": "eac3", "language": "eng"}], subs=[{"language": "fre", "codec": "subrip"}])
    )
    assert c["subs_ok"] is False, "foreign sub present → still not compliant"


def test_flagged_corrupt_excluded_from_completion(monkeypatch):
    """A broken source (flagged_corrupt) is not compliance work — it's dropped
    from the completion denominator, not counted as non-compliant across every
    metric at once (2026-07-11)."""
    from server.routers import library

    files = [
        {"filepath": "ok.mkv", "video": {"codec_raw": "av1"},
         "audio_streams": [{"codec_raw": "eac3", "language": "eng"}],
         "subtitle_streams": [], "tmdb": {"original_language": "en"}},
        {"filepath": "broken.mkv", "video": {"codec_raw": "hevc"},
         "audio_streams": [{"codec_raw": "dts", "language": "eng"}],
         "subtitle_streams": [], "tmdb": {"original_language": "en"}},
    ]
    monkeypatch.setattr(library, "read_report_cached", lambda _p: {"files": files})
    monkeypatch.setattr(library, "_flagged_corrupt_paths", lambda: {"broken.mkv"})
    library._completion_cache = None

    r = library.get_library_completion()
    assert r["total"] == 1, "broken.mkv (flagged_corrupt) must be excluded from the denominator"
    assert r["needs_video"] == 0, "only ok.mkv (AV1) remains -> no outstanding video work"


# --- Audio-language: dual-audio + language-code variants (2026-07-11) ----------
# The old `all(stream in keepers)` rule flagged the English half of every
# dual-audio film as "foreign audio", contradicting the Ghibli dual-audio policy.
# The fix credits English as an ADDITIONAL track without relaxing "not
# English-only" (a foreign film must still carry its original language).


def test_dual_audio_original_plus_english_is_compliant():
    """Original + English (the dual-audio policy — Ghibli keeps both) is compliant."""
    c = _compliance_for_entry(
        _entry([{"codec_raw": "eac3", "language": "jpn"}, {"codec_raw": "eac3", "language": "eng"}], orig_lang="ja")
    )
    assert c["audio_ok"] is True, "original (jpn) present + English is the dual-audio target, not a violation"
    assert "audio_foreign_language" not in c["violations"]


def test_original_only_still_compliant():
    """Regression: original-only (no English) stays compliant."""
    c = _compliance_for_entry(_entry([{"codec_raw": "eac3", "language": "jpn"}], orig_lang="ja"))
    assert c["audio_ok"] is True


def test_english_only_dub_of_foreign_film_not_compliant():
    """'not English-only' still holds — an English-only dub of a foreign film is
    missing its original and must stay non-compliant. The fix must NOT relax this."""
    c = _compliance_for_entry(_entry([{"codec_raw": "eac3", "language": "eng"}], orig_lang="es"))
    assert c["audio_ok"] is False, "English-only dub of a Spanish film is missing the original"
    assert "audio_foreign_language" in c["violations"]


def test_foreign_dub_track_on_english_film_not_compliant():
    """A genuinely-foreign dub (German) on an English-origin film is junk to strip,
    even though the English track makes it watchable."""
    c = _compliance_for_entry(
        _entry([{"codec_raw": "eac3", "language": "ger"}, {"codec_raw": "eac3", "language": "eng"}], orig_lang="en")
    )
    assert c["audio_ok"] is False, "German dub track is foreign junk to strip (Pocahontas case)"


def test_norwegian_bokmaal_is_the_original():
    """`nob` (Norwegian Bokmål) IS the original for a Norwegian film (orig=no) —
    the variant must be credited, not flagged as foreign (Sentimental Value)."""
    c = _compliance_for_entry(_entry([{"codec_raw": "eac3", "language": "nob"}], orig_lang="no"))
    assert c["audio_ok"] is True, "nob is Norwegian; orig=no must accept it"


def test_forced_foreign_sub_not_counted_as_foreign():
    """Forced foreign-parts subs (any language) are legitimate — the encoder KEEPS
    them (select_sub_keep_indices) — so they must not fail no_foreign_subs, or the
    metric can never hit 100% on a film with a 'French Forced' track (2026-07-13)."""
    c = _compliance_for_entry(_entry(
        [{"codec_raw": "eac3", "language": "eng"}],
        subs=[{"language": "fre", "codec": "subrip", "title": "French Forced"},
              {"language": "eng", "codec": "subrip", "title": ""}],
    ))
    assert c["no_foreign_subs"] is True, "a forced foreign-parts sub is not junk"


def test_full_foreign_sub_still_counted_as_foreign():
    """A non-forced full foreign sub IS junk to strip — must still be flagged."""
    c = _compliance_for_entry(_entry(
        [{"codec_raw": "eac3", "language": "eng"}],
        subs=[{"language": "ger", "codec": "subrip", "title": ""},
              {"language": "eng", "codec": "subrip", "title": ""}],
    ))
    assert c["no_foreign_subs"] is False, "a non-forced German sub is foreign junk"


def test_keeper_langs_include_norwegian_variants():
    """tmdb_keeper_langs must expand Norwegian to its Bokmål/Nynorsk variants."""
    from pipeline.streams import tmdb_keeper_langs

    keepers = tmdb_keeper_langs("no")
    assert "nob" in keepers and "nno" in keepers


# --- Animated content requires dual audio (2026-07-11, Totoro rule generalised) -
# The little one watches the English dub; the original is kept. So a foreign-
# origin animated film must carry BOTH the original AND English.


def _animated(audio, orig_lang="ja"):
    e = _entry(audio, orig_lang=orig_lang)
    e["tmdb"]["genres"] = [{"name": "Animation"}]
    return e


def test_animated_foreign_original_only_needs_english_dub():
    """Totoro with only Japanese (no English dub) is incomplete."""
    c = _compliance_for_entry(_animated([{"codec_raw": "eac3", "language": "jpn"}]))
    assert c["audio_ok"] is False
    assert "audio_animated_missing_english" in c["violations"]


def test_animated_foreign_with_dual_audio_is_compliant():
    c = _compliance_for_entry(
        _animated([{"codec_raw": "eac3", "language": "jpn"}, {"codec_raw": "eac3", "language": "eng"}])
    )
    assert c["audio_ok"] is True
    assert "audio_animated_missing_english" not in c["violations"]


def test_animated_english_origin_compliant_with_english_only():
    """English-origin animation (Disney) is complete with English — 'both' only
    bites for foreign-origin animation."""
    c = _compliance_for_entry(_animated([{"codec_raw": "eac3", "language": "eng"}], orig_lang="en"))
    assert c["audio_ok"] is True


def test_live_action_foreign_original_only_stays_compliant():
    """The dual-audio requirement is animation-only — a live-action foreign film
    with just its original language is still compliant (no genres = not animated)."""
    c = _compliance_for_entry(_entry([{"codec_raw": "eac3", "language": "jpn"}], orig_lang="ja"))
    assert c["audio_ok"] is True
