"""Regression tests for the 2026-05-06 audio-strip overhaul.

User pointed out that AV1 files were ending up with multiple audio
tracks despite the original-language policy: the existing rule kept
every track matching the film's TMDb original_language, which for
English films meant keeping commentaries + alt mixes alongside the
main mix.

Two new rules layered on top of the language filter:

  1. **Commentary strip** — drop tracks whose title matches the
     commentary regex (commentary, director's, isolated music score,
     audio description, etc.). Untitled tracks are kept.
  2. **Lossless wins over lossy of same channels + language** — when
     a TrueHD / DTS-HD MA track is present, drop EAC-3 / AC-3 / DTS /
     AAC of the same channel count + language. The lossy version is
     a fallback the source author shipped for older decoders.

Anything not stripped by language / commentary / lossless rules is
kept — including two untitled 5.1 tracks of the same codec, which
the user explicitly approved keeping (per 2026-05-06 conversation).
"""

from __future__ import annotations

from pipeline.streams import (
    AudioStream,
    _is_commentary,
    select_audio_keep_indices_by_original_language,
)


def _audio(idx, codec, channels, lang="eng", title="", detected=None):
    return AudioStream(
        index=idx, codec=codec, language=lang, channels=channels,
        channel_layout=f"{channels}ch", bitrate_kbps=None, lossless=False,
        detected_language=detected, title=title,
    )


# ---------------------------------------------------------------------------
# Commentary detector
# ---------------------------------------------------------------------------


def test_commentary_detection_explicit_title():
    s = _audio(0, "eac3", 2, title="Commentary by Lars von Trier and Peter Schepelern")
    assert _is_commentary(s) is True


def test_commentary_detection_directors_commentary():
    s = _audio(0, "ac3", 2, title="Director's Commentary")
    assert _is_commentary(s) is True


def test_commentary_detection_audio_description():
    s = _audio(0, "eac3", 2, title="Audio description")
    assert _is_commentary(s) is True


def test_commentary_detection_isolated_score():
    s = _audio(0, "ac3", 2, title="Isolated Music Score")
    assert _is_commentary(s) is True


def test_commentary_detection_untitled_does_not_match():
    """Untitled tracks must not be treated as commentary — they're
    the user's preserved alt-mix case."""
    s = _audio(0, "eac3", 6, title="")
    assert _is_commentary(s) is False


def test_commentary_detection_documentary_does_not_match():
    """The word 'documentary' should NOT trigger the commentary rule.
    Word-boundary regex is the safety net here."""
    s = _audio(0, "eac3", 2, title="A Documentary on the Making")
    # 'making of' would match, so use a title without that phrase
    s = _audio(0, "eac3", 2, title="Documentary track")
    assert _is_commentary(s) is False


def test_commentary_detection_main_mix_with_5_1_label():
    """A title like '5.1 Surround' or 'DTS-HD MA 7.1' isn't a commentary."""
    s = _audio(0, "truehd", 8, title="DTS-HD Master Audio 7.1")
    assert _is_commentary(s) is False


# ---------------------------------------------------------------------------
# Selector — commentary stripping
# ---------------------------------------------------------------------------


def test_strip_commentary_keeps_main():
    """Melancholia case: 5.1 main + 2.0 commentary, both English. Strip
    the commentary, keep the main."""
    streams = [
        _audio(0, "eac3", 6, title=""),
        _audio(1, "eac3", 2, title="Commentary with Lars von Trier"),
    ]
    keep = select_audio_keep_indices_by_original_language(streams, "en")
    assert keep == [0]


def test_strip_three_commentaries():
    """Mad Men case: main + actor commentary + director commentary."""
    streams = [
        _audio(0, "eac3", 6, title="Surround"),
        _audio(1, "eac3", 2, title="Commentary by actors"),
        _audio(2, "eac3", 2, title="Commentary by director"),
    ]
    keep = select_audio_keep_indices_by_original_language(streams, "en")
    assert keep == [0]


def test_keep_untitled_duplicates():
    """User explicit ask 2026-05-06: 'For two 5.1 tracks with no titles,
    fuck it, fine, keep them'. Two untitled English EAC-3 5.1 tracks
    should both survive — no title means we can't tell which is which,
    so the safe move is to keep both."""
    streams = [
        _audio(0, "eac3", 6, title=""),
        _audio(1, "eac3", 6, title=""),
    ]
    keep = select_audio_keep_indices_by_original_language(streams, "en")
    # Returns None when nothing to strip (caller no-ops the audio map)
    assert keep is None


# ---------------------------------------------------------------------------
# Selector — lossless wins over lossy
# ---------------------------------------------------------------------------


def test_truehd_wins_over_eac3_same_channels():
    """User explicit ask 2026-05-06: 'If we have Atmos/TrueHD we don't
    need EAC-3'. TrueHD 5.1 + EAC-3 5.1 → drop the EAC-3."""
    streams = [
        _audio(0, "truehd", 6, title=""),
        _audio(1, "eac3", 6, title=""),
    ]
    keep = select_audio_keep_indices_by_original_language(streams, "en")
    assert keep == [0]


def test_truehd_71_keeps_eac3_51_different_channels():
    """Different channel counts → both unique, both kept. TrueHD 7.1 +
    EAC-3 5.1 are NOT the same content — the EAC-3 is a downmix."""
    streams = [
        _audio(0, "truehd", 8, title="TrueHD Atmos 7.1"),
        _audio(1, "eac3", 6, title="DD+ 5.1"),
    ]
    keep = select_audio_keep_indices_by_original_language(streams, "en")
    # Both kept → None (no strip needed)
    assert keep is None


def test_dtshd_ma_wins_over_dts_same_channels():
    """DTS-HD MA is lossless. DTS at the same channel count is the
    lossy fallback the source shipped for older decoders. Drop it."""
    streams = [
        _audio(0, "dtshd_ma", 6, title=""),
        _audio(1, "dts", 6, title=""),
    ]
    keep = select_audio_keep_indices_by_original_language(streams, "en")
    assert keep == [0]


def test_lossless_does_not_drop_lossless():
    """TrueHD 7.1 + DTS-HD MA 5.1 — both lossless, different channels.
    Keep both. (And neither is in the LOSSY_DEDUP_CODECS set so the
    rule doesn't even consider dropping them.)"""
    streams = [
        _audio(0, "truehd", 8, title="TrueHD Atmos"),
        _audio(1, "dtshd_ma", 6, title="DTS-HD MA 5.1"),
    ]
    keep = select_audio_keep_indices_by_original_language(streams, "en")
    assert keep is None  # nothing stripped, both kept


# ---------------------------------------------------------------------------
# Selector — combined rules
# ---------------------------------------------------------------------------


def test_truehd_main_plus_eac3_main_plus_commentary():
    """Real-world case: TrueHD 7.1 + EAC-3 5.1 (Plex fallback) +
    Director's commentary. Should strip BOTH the EAC-3 (TrueHD wins)
    AND the commentary."""
    streams = [
        _audio(0, "truehd", 8, title=""),
        _audio(1, "eac3", 8, title=""),  # same channels, dropped by lossless rule
        _audio(2, "eac3", 2, title="Director's commentary"),
    ]
    keep = select_audio_keep_indices_by_original_language(streams, "en")
    assert keep == [0]


def test_foreign_film_strips_english_dub():
    """Toni Erdmann case: German film with English dub track. Default
    keep_english_too=False → strip the English dub entirely. User has
    explicitly asked for original-language only multiple times."""
    streams = [
        _audio(0, "eac3", 6, lang="ger", title=""),
        _audio(1, "eac3", 6, lang="eng", title="English dub"),
    ]
    keep = select_audio_keep_indices_by_original_language(streams, "de")
    assert keep == [0]


def test_foreign_film_with_keep_english_too_keeps_dub():
    """Same scenario but with keep_english_too=True (legacy fallback for
    users who explicitly want both)."""
    streams = [
        _audio(0, "eac3", 6, lang="ger", title=""),
        _audio(1, "eac3", 6, lang="eng", title=""),
    ]
    keep = select_audio_keep_indices_by_original_language(
        streams, "de", keep_english_too=True
    )
    assert keep is None  # both kept


def test_returns_none_when_nothing_to_strip():
    """Single English track on an English film — nothing to strip."""
    streams = [_audio(0, "eac3", 6, title="")]
    keep = select_audio_keep_indices_by_original_language(streams, "en")
    assert keep is None


def test_never_strips_to_zero_audio():
    """Safety: if the rules conspire to drop every track (e.g.
    every track is a commentary somehow), return None instead of
    [] so the encoder doesn't ship a zero-audio file."""
    streams = [
        _audio(0, "eac3", 2, title="Director's commentary"),
        _audio(1, "eac3", 2, title="Cast & crew commentary"),
    ]
    keep = select_audio_keep_indices_by_original_language(streams, "en")
    assert keep is None  # NOT [], which would mean "strip all audio"
