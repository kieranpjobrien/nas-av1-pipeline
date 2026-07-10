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
