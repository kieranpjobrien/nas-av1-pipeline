"""Pin the 2026-05-17 _ISO1_EQUIV reverse-lookup fix.

``_ISO1_EQUIV`` keys its buckets by ISO 639-1 canonical codes ("zh", "no",
"de"...). TMDb sometimes returns alternate codes for the same language —
notably "cn" for Chinese (legacy ISO 639 region code), where TMDb has both
"cn" and "zh" in active use.

Pre-2026-05-17, ``compliance.check_compliance`` did:

    if orig_lang in _ISO1_EQUIV:
        allowed_audio_langs |= _ISO1_EQUIV[orig_lang]
    elif orig_lang:
        allowed_audio_langs.add(orig_lang)

For "In the Mood for Love (2000)" — TMDb original_language='cn' — the
direct ``'cn' in _ISO1_EQUIV`` was False (keyed by 'zh'), so only 'cn'
itself was added. The actual MKV audio tag was 'chi' (not in the
narrow {cn} set), and compliance refused the file as foreign_audio.

Post-fix: ``equivalence_bucket(code)`` does direct + reverse lookup
against the bucket values. ``equivalence_bucket('cn')`` returns the
full {zh, cn, chi, zho, yue, cmn, chinese, ...} set — same as if TMDb
had returned 'zh' directly.
"""

from __future__ import annotations

from pipeline.qualify import _ISO1_EQUIV, equivalence_bucket


def test_canonical_key_lookup_unchanged():
    """The canonical key path still works — 'zh' returns the full Chinese bucket."""
    bucket = equivalence_bucket("zh")
    assert "zh" in bucket and "chi" in bucket and "cmn" in bucket and "yue" in bucket


def test_legacy_cn_alias_finds_chinese_bucket():
    """The In the Mood for Love case: TMDb returned 'cn'. Used to miss
    the lookup entirely; now resolves to the same bucket as 'zh'."""
    bucket = equivalence_bucket("cn")
    assert bucket == _ISO1_EQUIV["zh"], (
        "TMDb's 'cn' must resolve to the canonical Chinese bucket. Without "
        "this, Chinese-origin films get their 'chi'-tagged audio refused "
        "as foreign_audio."
    )


def test_three_letter_code_finds_bucket():
    """639-2/3 codes (chi/zho/yue/cmn) all need to resolve to the bucket
    too — useful for any caller that has the 3-letter code in hand
    rather than the 2-letter ISO 639-1 canonical."""
    for code in ("chi", "zho", "yue", "cmn"):
        bucket = equivalence_bucket(code)
        assert "zh" in bucket, f"{code!r} should resolve to the Chinese bucket"


def test_norwegian_macro_codes_all_resolve():
    """Same class for Norwegian: 'no' is the canonical, 'nob' / 'nno' are
    639-3 splits, MKV files commonly tag 'nor'. All should land in the
    same bucket."""
    canonical = _ISO1_EQUIV["no"]
    for code in ("no", "nor", "nob", "nno", "norwegian"):
        assert equivalence_bucket(code) == canonical, (
            f"{code!r} should resolve to the Norwegian bucket"
        )


def test_unknown_code_returns_singleton():
    """If a code isn't in any bucket, return a singleton set with just that
    code — preserves the old elif fallback semantics (the foreign-audio
    check still allows the orig_lang as-is)."""
    bucket = equivalence_bucket("klingon")
    assert bucket == {"klingon"}


def test_empty_and_none_safe():
    """Empty / None inputs return an empty set — caller decides what to do."""
    assert equivalence_bucket("") == set()
    assert equivalence_bucket("  ") == set()


def test_compliance_uses_reverse_lookup_for_cn():
    """End-to-end: simulate a Chinese-origin film whose TMDb returns 'cn'
    and whose MKV audio is tagged 'chi'. The check_compliance call must
    NOT flag the chi audio as foreign."""
    from pipeline.compliance import check_compliance
    from pipeline.config import build_config

    out = check_compliance(
        filepath=r"\\KieranNAS\Media\Movies\In the Mood for Love (2000)\In the Mood for Love (2000).mkv",
        item={
            "tmdb": {"original_language": "cn", "title": "In the Mood for Love"},
            "library_type": "movie",
            "filename": "In the Mood for Love (2000).mkv",
        },
        encode_params={"cq": 27, "content_grade": "default"},
        output_probe={
            "video": {"codec": "av1"},
            "audio": [{"codec": "eac3", "language": "chi"}],
            "subs": [],
        },
        mkv_tags={"ENCODER": "av1_nvenc cq=27", "CQ": "27", "CONTENT_GRADE": "default"},
        input_size_bytes=10_000_000_000,
        output_size_bytes=8_000_000_000,
        source_was_av1=False,
        config=build_config({}),
    )
    assert not any(v.tag == "foreign_audio" for v in out), (
        "TMDb 'cn' + MKV 'chi' must not trip foreign_audio after the "
        "reverse-lookup fix. The bucket equivalence makes 'chi' an "
        "allowed language when orig_lang='cn'."
    )
