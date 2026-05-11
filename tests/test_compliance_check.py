"""Pin the compliance contract — pipeline.compliance.check_compliance is
the function the encoder's verify gate runs pre-replace AND the audit
tool runs post-shipping. If they diverge the audit's whole purpose
(catching what verify missed) breaks.

These are unit tests on ``check_compliance`` only — fixers and the
end-to-end finalize_upload integration live in their own files.

Note: distinct from ``test_compliance.py`` which exercises the
``/api/library-completion`` HTTP endpoints (different layer).
"""

from __future__ import annotations

from pipeline.compliance import (
    AV1_GROWTH_TOLERANCE,
    Category,
    check_compliance,
)


def _base_args(**overrides):
    """Default arg set — clean output. Override individual fields per test."""
    args = {
        "filepath": r"\\KieranNAS\Media\Movies\Test (2024)\Test (2024).mkv",
        "item": {
            "tmdb": {"original_language": "en"},
            "library_type": "movie",
            "filename": "Test (2024).mkv",
            "final_name": "Test (2024).mkv",
        },
        "encode_params": {"cq": 22, "content_grade": "default"},
        "output_probe": {
            "video": {"codec": "av1"},
            "audio": [{"codec": "eac3", "language": "eng", "title": ""}],
            "subs": [{"language": "eng", "title": ""}],
            "format": {},
        },
        "mkv_tags": {
            "ENCODER": "av1_nvenc cq=22 preset=p7 multipass=fullres grade=default base_cq=22 offset=+0",
            "CQ": "22",
            "CONTENT_GRADE": "default",
        },
        "input_size_bytes": 10_000_000_000,
        "output_size_bytes": 8_000_000_000,
        "source_was_av1": False,
        "config": {"lossless_audio_codecs": []},
    }
    args.update(overrides)
    return args


def test_clean_output_zero_violations():
    """The default args represent a fully-compliant output."""
    assert check_compliance(**_base_args()) == []


def test_wrong_video_codec_refuses():
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "hevc"},
        "audio": [{"codec": "eac3", "language": "eng"}],
        "subs": [],
    }))
    assert any(v.tag == "video_codec_wrong" and v.category == Category.REFUSE for v in out)


def test_zero_audio_refuses():
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "av1"},
        "audio": [],
        "subs": [],
    }))
    assert any(v.tag == "zero_audio" and v.category == Category.REFUSE for v in out)


def test_non_target_audio_codec_refuses():
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "av1"},
        "audio": [{"codec": "ac3", "language": "eng"}],
        "subs": [],
    }))
    assert any(v.tag == "audio_codec_wrong" and v.category == Category.REFUSE for v in out)


def test_truehd_passthrough_accepted():
    """TrueHD is the Atmos exception — preserved as passthrough, not transcoded."""
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "av1"},
        "audio": [{"codec": "truehd", "channels": 8, "language": "eng"}],
        "subs": [],
    }))
    assert not any(v.tag == "audio_codec_wrong" for v in out)


def test_foreign_audio_jpn_accepted_when_orig_lang_is_ja():
    """Seven Samurai class — original_language=ja means jpn audio is allowed."""
    out = check_compliance(**_base_args(
        item={"tmdb": {"original_language": "ja"}, "library_type": "movie"},
        output_probe={
            "video": {"codec": "av1"},
            "audio": [{"codec": "eac3", "language": "jpn"}],
            "subs": [],
        },
    ))
    assert not any(v.tag == "foreign_audio" for v in out)


def test_foreign_audio_chi_rejected_for_english_film():
    """The Office class — Italian/Chinese audio on an English film must be flagged
    fixable so mkvmerge drops it."""
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "av1"},
        "audio": [
            {"codec": "eac3", "language": "eng"},
            {"codec": "eac3", "language": "chi"},
        ],
        "subs": [],
    }))
    foreign = [v for v in out if v.tag == "foreign_audio"]
    assert foreign and foreign[0].category == Category.FIXABLE
    assert foreign[0].data["indices"] == [1]


def test_commentary_audio_flagged_fixable():
    """Mythic Quest class — commentary track survived strip, must drop in-place."""
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "av1"},
        "audio": [
            {"codec": "eac3", "language": "eng", "title": ""},
            {"codec": "eac3", "language": "eng", "title": "Commentary by B.J. Novak"},
        ],
        "subs": [],
    }))
    comm = [v for v in out if v.tag == "commentary_audio"]
    assert comm and comm[0].category == Category.FIXABLE
    assert comm[0].data["indices"] == [1]


def test_two_english_subs_flagged_fixable():
    """The Office S03E16 class — internal PGS English + external SubRip English."""
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "av1"},
        "audio": [{"codec": "eac3", "language": "eng"}],
        "subs": [
            {"language": "eng", "title": ""},
            {"language": "en", "title": ""},
        ],
    }))
    extra = [v for v in out if v.tag == "extra_eng_subs"]
    assert extra and extra[0].category == Category.FIXABLE
    assert extra[0].data["indices"] == [1]


def test_forced_sub_alongside_regular_eng_is_ok():
    """A forced sub doesn't compete with the regular English slot — both kept."""
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "av1"},
        "audio": [{"codec": "eac3", "language": "eng"}],
        "subs": [
            {"language": "eng", "title": ""},
            {"language": "eng", "title": "Forced"},
        ],
    }))
    assert not any(v.tag == "extra_eng_subs" for v in out)


def test_foreign_sub_flagged_fixable():
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "av1"},
        "audio": [{"codec": "eac3", "language": "eng"}],
        "subs": [
            {"language": "eng", "title": ""},
            {"language": "fre", "title": ""},
        ],
    }))
    foreign = [v for v in out if v.tag == "foreign_subs"]
    assert foreign and foreign[0].category == Category.FIXABLE
    assert foreign[0].data["indices"] == [1]


def test_missing_encode_tags_flagged_fixable():
    out = check_compliance(**_base_args(mkv_tags={}))
    miss = [v for v in out if v.tag == "missing_encode_tags"]
    assert miss and miss[0].category == Category.FIXABLE
    assert set(miss[0].data["tags"]) == {"ENCODER", "CQ", "CONTENT_GRADE"}


def test_cq_tag_mismatch_flagged_fixable():
    out = check_compliance(**_base_args(
        encode_params={"cq": 25, "content_grade": "blockbuster"},
        mkv_tags={"ENCODER": "x", "CQ": "22", "CONTENT_GRADE": "blockbuster"},
    ))
    mm = [v for v in out if v.tag == "cq_mismatch"]
    assert mm and mm[0].category == Category.FIXABLE


def test_grade_tag_mismatch_flagged_fixable():
    out = check_compliance(**_base_args(
        encode_params={"cq": 22, "content_grade": "blockbuster"},
        mkv_tags={"ENCODER": "x", "CQ": "22", "CONTENT_GRADE": "default"},
    ))
    mm = [v for v in out if v.tag == "grade_mismatch"]
    assert mm and mm[0].category == Category.FIXABLE


def test_av1_grew_within_5pct_passes():
    """NVENC second-pass on the same source can vary by ~1-3% between runs.
    A 4% growth must NOT trip the gate — that's encoder noise."""
    out = check_compliance(**_base_args(
        source_was_av1=True,
        input_size_bytes=10_000_000_000,
        output_size_bytes=10_400_000_000,  # +4%
    ))
    assert not any(v.tag == "av1_grew" for v in out)


def test_av1_grew_more_than_5pct_refuses():
    """6% growth crosses the tolerance — refuse to ship."""
    out = check_compliance(**_base_args(
        source_was_av1=True,
        input_size_bytes=10_000_000_000,
        output_size_bytes=10_600_000_000,  # +6%
    ))
    grew = [v for v in out if v.tag == "av1_grew"]
    assert grew and grew[0].category == Category.REFUSE


def test_av1_grew_severely_refuses():
    """Saving Private Ryan class — 18 GB → 47 GB (ratio 2.6). Hard refuse."""
    out = check_compliance(**_base_args(
        source_was_av1=True,
        input_size_bytes=18_000_000_000,
        output_size_bytes=47_000_000_000,
    ))
    grew = [v for v in out if v.tag == "av1_grew"]
    assert grew and grew[0].category == Category.REFUSE
    assert grew[0].data["ratio"] > 2.0


def test_hevc_to_av1_growth_allowed():
    """HEVC → AV1 first-encodes can legitimately grow (user's "same container
    for everything" ask). The growth check is gated on source_was_av1."""
    out = check_compliance(**_base_args(
        source_was_av1=False,
        input_size_bytes=10_000_000_000,
        output_size_bytes=15_000_000_000,
    ))
    assert not any(v.tag == "av1_grew" for v in out)


def test_av1_growth_exactly_at_tolerance_is_ok():
    """Boundary — growth equal to the tolerance ratio passes."""
    out = check_compliance(**_base_args(
        source_was_av1=True,
        input_size_bytes=10_000_000_000,
        output_size_bytes=int(10_000_000_000 * AV1_GROWTH_TOLERANCE),
    ))
    assert not any(v.tag == "av1_grew" for v in out)


def test_filename_mismatch_flagged_fixable():
    out = check_compliance(**_base_args(
        filepath=r"\\KieranNAS\Media\Movies\Test (2024)\Test_dirty_name.mkv",
        item={
            "tmdb": {"original_language": "en"},
            "library_type": "movie",
            "filename": "Test (2024).mkv",
            "final_name": "Test (2024).mkv",
        },
    ))
    fn = [v for v in out if v.tag == "filename_mismatch"]
    assert fn and fn[0].category == Category.FIXABLE
    assert fn[0].data["expected"] == "Test (2024).mkv"


def test_probe_error_unrecoverable():
    out = check_compliance(**_base_args(output_probe={"error": "ffprobe failed"}))
    assert len(out) == 1
    assert out[0].tag == "probe_error"
    assert out[0].category == Category.UNRECOVERABLE
