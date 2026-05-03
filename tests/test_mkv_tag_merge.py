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


def test_merge_writes_through_failure():
    """If mkvpropedit reports a hard failure (rc>=2) the merge returns False
    so callers can surface the error."""

    def fake_write(filepath, xml_body, *, timeout=60):
        return False

    with patch("pipeline.mkv_tags.read_global_tags", lambda f, *, timeout=60: []), \
         patch("pipeline.mkv_tags._write_tag_xml", fake_write):
        ok = merge_global_tags(
            "fake.mkv",
            owned_names={"X"},
            new_tags=[{"name": "X", "value": "y"}],
        )

    assert ok is False


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
