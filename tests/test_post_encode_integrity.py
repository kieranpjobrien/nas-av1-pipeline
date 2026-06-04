"""Regression tests for the 2026-05-02 'pre-replace integrity check' fix.

The 2026-04-13/15 distributed-gap-filler sprint produced 962 AV1 files
that had valid Matroska headers + clean metadata (codec=AV1, audio=EAC-3,
subs OK) but corrupt AV1 streams. Header-only probes missed every one.
The encoder's pre-replace standards check ran and passed; the corrupt
output then replaced the user's clean source.

Fix: after standards compliance passes, decode the first 10 s of the
encoded output via ``ffmpeg -v error -t 10 -f null -``. Any error output
means the file is structurally damaged. Stop the replace, preserve the
corrupt output for inspection, and park in ERROR.

These tests pin the contract — the integrity-check signatures, the
preserve-and-rename behaviour, and the no-replace-on-failure invariant.
"""

from __future__ import annotations

import re


def test_integrity_signatures_present():
    """All 11 known corruption signatures must be in the encoder's
    integrity check. Adding new signatures requires keeping this set in
    sync between scan_corrupt_av1.py and full_gamut.py."""
    src = open("pipeline/full_gamut.py", encoding="utf-8").read()
    block = src.split("integrity_signatures = (", 1)[1].split(")", 1)[0]
    sigs = [s.strip().strip('"').strip("'").strip(",") for s in re.split(r"[,\n]", block) if s.strip()]
    sigs = [s for s in sigs if s and not s.startswith("#")]
    expected = {
        "exceeds containing master element",
        "exceeds max length",
        "unknown-sized element",
        "inside parent with finite size",
        "obu_forbidden_bit out of range",
        "failed to parse temporal unit",
        "unknown obu type",
        "overrun in obu bit buffer",
        "error parsing obu data",
        "invalid data found when processing input",
        "error submitting packet to decoder",
    }
    missing = expected - set(sigs)
    assert not missing, f"missing signatures: {missing}"


def test_corrupt_path_uses_corrupt_suffix():
    """Damaged outputs are preserved as ``<dest>.corrupt`` rather than
    deleted — so a post-mortem can inspect the broken file."""
    src = open("pipeline/full_gamut.py", encoding="utf-8").read()
    assert "dest_path + \".corrupt\"" in src, "preserve-as-.corrupt suffix removed"


def test_integrity_runs_before_replace():
    """The integrity check must sit between the compliance gate and the
    atomic replace. Order matters: compliance proves codecs+langs+tags;
    integrity proves the bytes actually decode. If the order is swapped
    or the check moves below the rename(), the user's source is already
    gone by the time we know the encode is broken.

    Post-2026-05-10 the compliance block is a single call to
    ``check_compliance``; we anchor on that import / function call
    instead of the inline ``violations.append`` markers used by the
    pre-refactor block."""
    src = open("pipeline/full_gamut.py", encoding="utf-8").read()
    integrity_pos = src.find("Stream-level integrity check")
    # rfind: the literal Replace-original section comment may appear in an
    # earlier docstring; we want the actual code location which is later.
    replace_pos = src.rfind("Replace original (crash-safe)")
    # Anchor compliance block on the call site of check_compliance.
    compliance_call = src.find("from pipeline.compliance import")
    assert compliance_call != -1, "compliance gate import missing"
    assert integrity_pos != -1, "integrity check section missing"
    assert replace_pos != -1, "replace section missing"
    assert compliance_call < integrity_pos < replace_pos, (
        f"section order broken: expected compliance({compliance_call}) "
        f"-> integrity({integrity_pos}) -> replace({replace_pos})"
    )


def test_integrity_failure_sets_error_status():
    """On detected corruption the state row must go to ERROR with
    ``corruption_signatures`` extras. The re-queue tooling depends on
    this for triage."""
    src = open("pipeline/full_gamut.py", encoding="utf-8").read()
    # The relevant block: state.set_file(..., FileStatus.ERROR, ...,
    #   stage="integrity", corruption_signatures=hits)
    assert "stage=\"integrity\"" in src
    assert "corruption_signatures=hits" in src


def test_verify_accepts_original_language_audio():
    """2026-05-08 fix: the verify step must accept audio in the file's
    TMDb original_language, not just KEEP_LANGS.

    Pre-2026-05-10 this lived inline in full_gamut.py; the source-string
    test asserted that block existed. After the refactor to
    ``pipeline.compliance.check_compliance`` it lives in compliance.py,
    so the test pivots to assert the actual behavioural contract via
    ``check_compliance`` directly.

    Crouching Tiger (chi), Seven Samurai (jpn), Downfall (ger) all
    correctly KEEP their original-language audio per the strip rule;
    the verify must accept those tracks rather than reject them as
    "not in KEEP_LANGS"."""
    from pipeline.compliance import check_compliance

    cases = [
        ("ja", "jpn"),  # Seven Samurai
        ("de", "ger"),  # Downfall
        ("zh", "chi"),  # Crouching Tiger
    ]
    for orig, mkv_lang in cases:
        out = check_compliance(
            filepath=r"\\KieranNAS\Test.mkv",
            item={"tmdb": {"original_language": orig}, "library_type": "movie",
                  "filename": "Test.mkv", "final_name": "Test.mkv"},
            encode_params={"cq": 22, "content_grade": "default"},
            output_probe={
                "video": {"codec": "av1"},
                "audio": [{"codec": "eac3", "language": mkv_lang, "title": ""}],
                "subs": [],
            },
            mkv_tags={"ENCODER": "x", "CQ": "22", "CONTENT_GRADE": "default"},
            input_size_bytes=10_000_000_000,
            output_size_bytes=8_000_000_000,
            source_was_av1=False,
            config={"lossless_audio_codecs": []},
        )
        assert not any(v.tag == "foreign_audio" for v in out), (
            f"original_language={orig!r} + mkv_lang={mkv_lang!r} must not be flagged foreign"
        )


def test_qualify_nothing_to_do_yields_to_force_reencode():
    """2026-05-09 fix: qualify's NOTHING_TO_DO short-circuit must not
    fire when the row has force_reencode=True. The qualify step only
    inspects codec/audio/sub structure — it doesn't consider CQ
    targets. Pre-fix, the overnight 24-file test produced 0 re-encodes
    because qualify deemed 13 of them 'already compliant' (codec=AV1
    + audio=EAC-3 → looks fine to qualify) and silently set
    force_reencode=False before the encode logic ever saw the flag.

    Anchor on the patched control flow at both qualify-call sites
    (prep stage at line ~333 + encode-time fallback at line ~603).
    Both must check force_reencode BEFORE marking DONE."""
    src = open("pipeline/full_gamut.py", encoding="utf-8").read()
    # Both call sites should now have a force_reencode bypass before the DONE-mark.
    # The literal phrase "force_reencode=true → proceeding with re-encode" or similar
    # should appear at least twice (once per call site) in the patched code.
    bypass_count = src.count("force_reencode=true")
    assert bypass_count >= 2, (
        f"qualify NOTHING_TO_DO bypass missing — expected >=2 'force_reencode=true' "
        f"references in full_gamut.py, found {bypass_count}. "
        f"Without the bypass, qualify silently kills user re-encode requests."
    )
    # The DONE-mark inside the bypass must be inside an else branch (not
    # unconditional), so it only fires when force_reencode is NOT set.
    # 2026-06-05: now exactly 1 — the qualify NOTHING_TO_DO handling lives
    # solely in prepare_for_encode (which _encode_only calls). The second
    # copy was in full_gamut()'s old inline STEP 1-5 block, which was dead
    # by construction (full_gamut returns _encode_only before reaching it)
    # and has now been removed.
    assert src.count("reason=\"already compliant\"") == 1, (
        "expected exactly 1 'reason=already compliant' DONE mark (in "
        "prepare_for_encode); if 0 the bypass was lost, if >1 the dead "
        "inline duplicate is back"
    )


def test_replace_uses_os_replace_for_overwriting_target():
    """2026-05-10 fix: re-encode of an already-AV1 file failed at the
    replace step with WinError 183 ("Cannot create a file when that file
    already exists") because the prior encode's .original.bak survived,
    causing the "move original to backup" step to skip — and then
    os.rename(.av1.tmp, .mkv) crashed because .mkv was still there.

    The replace block must use os.replace (atomic, overwrites on Windows)
    not os.rename (fails when target exists on Windows). Otherwise the
    re-encode produces a perfectly valid output but can't commit it,
    leaving the file stuck in error/replace forever.

    Bad Batch S03E01/S03E07/S03E08/S03E13 hit this exact loop today —
    encodes succeeded with 14-25% reductions but couldn't replace.
    """
    src = open("pipeline/full_gamut.py", encoding="utf-8").read()
    # Anchor on the actual code header (the `# === Replace original ...`
    # comment). The function docstring also contains the phrase but is
    # not the implementation; rfind to get the last occurrence which is
    # the live code block.
    block_start = src.rfind("# === Replace original (crash-safe) ===")
    assert block_start != -1, "replace block header not found"
    block = src[block_start : block_start + 2048]
    # Line we care about: os.replace(dest_path, final_path)
    assert "os.replace(dest_path, final_path)" in block, (
        "atomic replace step must use os.replace (overwrites on Windows), "
        "not os.rename (fails when target exists)"
    )
    # Sanity: the OLD os.rename(dest_path, final_path) must NOT be present
    # in this block.
    assert "os.rename(dest_path, final_path)" not in block, (
        "found os.rename(dest_path, final_path) — this fails on Windows "
        "when the target file already exists (the re-encode case)"
    )


def test_iso1_equiv_covers_grew_files_languages():
    """The iso2 → set-of-equivalents mapping must include every
    language that showed up in production verify failures. If new
    languages get added to the library that we don't cover, those
    files will silently fail verify and end up in ERROR until
    someone notices."""
    from pipeline.qualify import _ISO1_EQUIV

    # 2026-05-08 production verify failures: chi (Crouching Tiger),
    # jpn (Seven Samurai), ger (Downfall), ita (One Battle After
    # Another). The TMDb iso2 codes for these are zh, ja, de, it.
    expected = {
        "zh": {"chi", "zho"},
        "ja": {"jpn"},
        "de": {"deu", "ger"},
        "it": {"ita"},
        "fr": {"fra", "fre"},
        "es": {"spa"},
        "ko": {"kor"},
        "ru": {"rus"},
    }
    for iso2, must_contain in expected.items():
        assert iso2 in _ISO1_EQUIV, f"_ISO1_EQUIV missing iso2 code {iso2!r}"
        bucket = _ISO1_EQUIV[iso2]
        for code in must_contain:
            assert code in bucket, (
                f"_ISO1_EQUIV[{iso2!r}] does not contain {code!r} — "
                f"file verify will reject this language even though "
                f"the strip rule keeps it"
            )
