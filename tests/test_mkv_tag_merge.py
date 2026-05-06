"""Regression tests for the MKV global-tag merge logic.

Pre-2026-05-04, three writers all called ``mkvpropedit --tags global:...``
with only their own tags. Each call replaced the entire global-tag block,
so whichever writer ran last wiped the others. Sample of 50 latest done
encodes pre-fix: 0/50 had a CQ stamp because the TMDb writer (which runs
after the encoder stamp) clobbered it every time.

Fix: each writer declares which tag names it owns. The shared
:func:`pipeline.mkv_tags.merge_global_tags` reads the existing tag block,
drops only the owned names, appends the new values, and writes the
union back. These tests pin that union behaviour so regressions are
caught without spinning up real MKVs.
"""

from __future__ import annotations

from unittest.mock import patch

from pipeline.mkv_tags import merge_global_tags


def test_merge_drops_owned_keeps_others():
    """The owned-name set is the only thing that gets dropped."""
    existing = [
        {"name": "ENCODER", "value": "av1_nvenc cq=28"},
        {"name": "CQ", "value": "28"},
        {"name": "DIRECTOR", "value": "Adrian Lyne"},
        {"name": "GENRE", "value": "Drama"},
    ]
    new_tags = [
        {"name": "DIRECTOR", "value": "Updated Lyne"},
        {"name": "GENRE", "value": "Drama, Romance"},
    ]
    captured: dict = {}

    def fake_read(filepath, *, timeout=60):
        return existing

    def fake_write(filepath, xml_body, *, timeout=60):
        captured["xml"] = xml_body
        return True

    with patch("pipeline.mkv_tags.read_global_tags", fake_read), \
         patch("pipeline.mkv_tags._write_tag_xml", fake_write):
        ok = merge_global_tags(
            "fake.mkv",
            owned_names={"DIRECTOR", "GENRE"},
            new_tags=new_tags,
        )

    assert ok
    xml = captured["xml"]
    # ENCODER + CQ survive (not owned)
    assert "<Name>ENCODER</Name>" in xml
    assert "<String>av1_nvenc cq=28</String>" in xml
    assert "<Name>CQ</Name>" in xml
    # DIRECTOR + GENRE replaced with new values
    assert "<String>Updated Lyne</String>" in xml
    assert "<String>Drama, Romance</String>" in xml
    assert "<String>Adrian Lyne</String>" not in xml


def test_merge_owned_match_is_case_insensitive():
    """Tag names compare case-insensitively — Matroska standard tags are
    upper-case but a hand-written XML might use mixed case."""
    existing = [{"name": "Director", "value": "old"}]
    captured: dict = {}

    def fake_write(filepath, xml_body, *, timeout=60):
        captured["xml"] = xml_body
        return True

    with patch("pipeline.mkv_tags.read_global_tags", lambda f, *, timeout=60: existing), \
         patch("pipeline.mkv_tags._write_tag_xml", fake_write):
        merge_global_tags(
            "fake.mkv",
            owned_names={"DIRECTOR"},  # upper-case
            new_tags=[{"name": "DIRECTOR", "value": "new"}],
        )

    assert "<String>old</String>" not in captured["xml"]
    assert "<String>new</String>" in captured["xml"]


def test_merge_empty_new_tags_clears_only_owned():
    """An empty new_tags list with a populated owned-set is the
    clear-but-preserve-others pattern used by clear_grade_review."""
    existing = [
        {"name": "ENCODER", "value": "av1_nvenc"},
        {"name": "GRADE_REVIEW", "value": "accepted"},
        {"name": "GRADE_REVIEW_AT", "value": "2026-05-03T00:00:00Z"},
    ]
    captured: dict = {}

    def fake_write(filepath, xml_body, *, timeout=60):
        captured["xml"] = xml_body
        return True

    with patch("pipeline.mkv_tags.read_global_tags", lambda f, *, timeout=60: existing), \
         patch("pipeline.mkv_tags._write_tag_xml", fake_write):
        merge_global_tags(
            "fake.mkv",
            owned_names={"GRADE_REVIEW", "GRADE_REVIEW_AT"},
            new_tags=[],
        )

    xml = captured["xml"]
    assert "<Name>ENCODER</Name>" in xml
    assert "<Name>GRADE_REVIEW</Name>" not in xml
    assert "<Name>GRADE_REVIEW_AT</Name>" not in xml


def test_merge_propagates_write_error():
    """Hard mkvpropedit failures bubble up as MkvTagWriteError so callers
    (especially the API layer) can surface the actual reason — generic
    'mkvpropedit failed' is useless when the underlying issue is 'file
    no longer exists' or 'not a valid Matroska file'."""
    import pytest

    from pipeline.mkv_tags import MkvTagWriteError

    def fake_write(filepath, xml_body, *, timeout=60):
        raise MkvTagWriteError(
            "not a Matroska file or it could not be found",
            returncode=2,
            filepath=filepath,
        )

    with patch("pipeline.mkv_tags.read_global_tags", lambda f, *, timeout=60: []), \
         patch("pipeline.mkv_tags._write_tag_xml", fake_write):
        with pytest.raises(MkvTagWriteError) as exc_info:
            merge_global_tags(
                "fake.mkv",
                owned_names={"X"},
                new_tags=[{"name": "X", "value": "y"}],
            )

    assert "not a Matroska file" in str(exc_info.value)
    assert exc_info.value.returncode == 2


def test_merge_xml_escapes_special_chars():
    """Values containing &, <, > must be XML-escaped so mkvpropedit doesn't
    fail to parse them. Movie titles with ampersands ("Indiana Jones and the
    Last Crusade & ...") are real."""
    captured: dict = {}

    def fake_write(filepath, xml_body, *, timeout=60):
        captured["xml"] = xml_body
        return True

    with patch("pipeline.mkv_tags.read_global_tags", lambda f, *, timeout=60: []), \
         patch("pipeline.mkv_tags._write_tag_xml", fake_write):
        merge_global_tags(
            "fake.mkv",
            owned_names={"GENRE"},
            new_tags=[{"name": "GENRE", "value": "Action & Adventure <special>"}],
        )

    xml = captured["xml"]
    # Raw ampersand and angle brackets must NOT appear in the value
    assert "Action &amp; Adventure" in xml
    assert "&lt;special&gt;" in xml


def test_remux_fallback_refuses_to_replace_with_smaller_output(tmp_path):
    """Pin the 2026-05-06 incident guard: when mkvmerge remux produces
    an output substantially smaller than the source (the Million Dollar
    Baby class — valid EBML header but 0 tracks → 4 KB stub), the
    fallback must NOT atomically replace the original. The previous
    version did, destroying the source file.
    """
    from unittest.mock import patch
    from pipeline.mkv_tags import _try_remux_in_place

    # Create a fake "source" file with 1 MB of bytes
    src = tmp_path / "fake_source.mkv"
    src.write_bytes(b"x" * (1024 * 1024))
    src_size = src.stat().st_size

    # Stub mkvmerge to return rc=0 and produce a tiny output (the Million
    # Dollar Baby failure mode — header-only stub).
    def fake_subprocess_run(cmd, **kwargs):
        # cmd is [mkvmerge, '-o', tmp_out, src]
        tmp_out = cmd[2]
        # Write a 4 KB stub
        with open(tmp_out, "wb") as f:
            f.write(b"\x1a\x45\xdf\xa3" + b"x" * 4288)
        from subprocess import CompletedProcess
        return CompletedProcess(cmd, 0, stdout="", stderr="")

    with patch("subprocess.run", side_effect=fake_subprocess_run):
        ok = _try_remux_in_place(str(src))

    assert ok is False, "remux must refuse to replace when output is suspiciously small"
    # Source must be UNTOUCHED
    assert src.exists()
    assert src.stat().st_size == src_size, "source file must not have been replaced"


def test_grade_review_writer_owns_only_review_tags():
    """End-to-end: set_grade_review writes its tags + preserves whatever
    else is in the block. Mocked at the I/O layer."""
    from pipeline.grade_review import set_grade_review

    existing = [
        {"name": "ENCODER", "value": "av1_nvenc cq=28"},
        {"name": "CQ", "value": "28"},
        {"name": "DIRECTOR", "value": "Adrian Lyne"},
    ]
    captured_xml: list[str] = []

    def fake_write(filepath, xml_body, *, timeout=60):
        captured_xml.append(xml_body)
        return True

    with patch("pipeline.mkv_tags.read_global_tags", lambda f, *, timeout=60: existing), \
         patch("pipeline.mkv_tags._write_tag_xml", fake_write):
        ok = set_grade_review("fake.mkv", "accepted")

    assert ok
    xml = captured_xml[0]
    assert "<Name>ENCODER</Name>" in xml
    assert "<Name>CQ</Name>" in xml
    assert "<Name>DIRECTOR</Name>" in xml
    assert "<Name>GRADE_REVIEW</Name>" in xml
    assert "<Name>GRADE_REVIEW_AT</Name>" in xml
