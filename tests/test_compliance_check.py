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


def test_sdh_sub_alongside_regular_eng_is_ok_title():
    """Pin the 2026-05-14 Slow Horses S05E05 Circus PREP MISS fix.

    Pre-fix, compliance.py counted SDH/HI subs toward the regular-English
    cap. A file with regular + SDH was flagged as ``extra_eng_subs``
    even though ``prep_streams.compute_sub_drop_indices`` correctly
    excluded SDH from its own regular-eng count. The breaker fired
    on every encode of Slow Horses S05E05 Circus (1 forced + 1
    regular + 1 SDH; prep kept all three, compliance refused).

    Post-fix: compliance uses ``streams.is_hi_internal`` (disposition
    + title regex) to identify SDH/HI tracks, matching prep_streams.
    Forced AND SDH each occupy a separate slot from regular English.
    """
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "av1"},
        "audio": [{"codec": "eac3", "language": "eng"}],
        "subs": [
            {"language": "eng", "title": ""},
            # Title-based SDH detection
            {"language": "eng", "title": "SDH"},
        ],
    }))
    assert not any(v.tag == "extra_eng_subs" for v in out), (
        "SDH sub (title='SDH') must not count toward the regular-English cap"
    )


def test_sdh_sub_alongside_regular_eng_is_ok_disposition():
    """Same as above but via the disposition.hearing_impaired flag —
    common on tracks whose title is None or just the language name."""
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "av1"},
        "audio": [{"codec": "eac3", "language": "eng"}],
        "subs": [
            {"language": "eng", "title": ""},
            {"language": "eng", "title": None, "disposition": {"hearing_impaired": 1}},
        ],
    }))
    assert not any(v.tag == "extra_eng_subs" for v in out), (
        "disposition.hearing_impaired sub must not count toward the regular-English cap"
    )


def test_cc_sub_alongside_regular_eng_is_ok():
    """``(CC)`` in the title — Closed Caption variants — are SDH-class.
    Resident Alien S01E07's eng sub was titled ``İngilizce [CC]`` (CC
    token in Turkish); the same detection should treat it as SDH on
    the compliance side too."""
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "av1"},
        "audio": [{"codec": "eac3", "language": "eng"}],
        "subs": [
            {"language": "eng", "title": ""},
            {"language": "eng", "title": "English (CC)"},
        ],
    }))
    assert not any(v.tag == "extra_eng_subs" for v in out)


def test_three_regular_english_still_flagged():
    """Sanity: the SDH carve-out doesn't accidentally exempt genuine
    duplicates. Three non-forced, non-SDH eng subs → flagged."""
    out = check_compliance(**_base_args(output_probe={
        "video": {"codec": "av1"},
        "audio": [{"codec": "eac3", "language": "eng"}],
        "subs": [
            {"language": "eng", "title": ""},
            {"language": "eng", "title": ""},
            {"language": "eng", "title": ""},
        ],
    }))
    extra = [v for v in out if v.tag == "extra_eng_subs"]
    assert extra and extra[0].category == Category.FIXABLE
    assert extra[0].data["indices"] == [1, 2]


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


def test_output_growth_never_violates_compliance():
    """Pin the 2026-05-16 policy: output size is NOT a compliance concern.

    Quality is the goal, size is not. The old AV1→AV1 growth REFUSE rule
    (5% tolerance) and its severe-growth variant are both gone. Any
    output, regardless of growth ratio, must pass compliance on size
    grounds alone. The user re-stated this angrily on 2026-05-12 and
    again 2026-05-16 after Cool Hand Luke (16.7 → 33.8 GB) was refused
    by the now-dead rule.
    """
    # 4% growth — was previously fine
    out = check_compliance(**_base_args(
        source_was_av1=True,
        input_size_bytes=10_000_000_000,
        output_size_bytes=10_400_000_000,
    ))
    assert not any(v.tag == "av1_grew" for v in out)

    # 6% growth — previously refused
    out = check_compliance(**_base_args(
        source_was_av1=True,
        input_size_bytes=10_000_000_000,
        output_size_bytes=10_600_000_000,
    ))
    assert not any(v.tag == "av1_grew" for v in out)

    # Saving Private Ryan / Cool Hand Luke class: 2x+ growth, previously hard refuse
    out = check_compliance(**_base_args(
        source_was_av1=True,
        input_size_bytes=18_000_000_000,
        output_size_bytes=47_000_000_000,
    ))
    assert not any(v.tag == "av1_grew" for v in out)

    # HEVC source → AV1 growing (first-encode) — still fine
    out = check_compliance(**_base_args(
        source_was_av1=False,
        input_size_bytes=10_000_000_000,
        output_size_bytes=15_000_000_000,
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
