"""Regression tests for the 2026-05-03 'curly-apostrophe in PowerShell
single-quoted string' incident.

PowerShell treats every Unicode apostrophe-like glyph as a quote
character, not just U+0027. ``Woe's Hollow.mkv`` (U+2019) closed our
generated string mid-path and crashed a deletion script. The shared
helper ``escape_path_for_ps1_squote`` doubles all five members of the
single-quote family. These tests pin the contract.
"""

from __future__ import annotations

from pathlib import Path

from tools.ps1_helpers import escape_path_for_ps1_squote, write_delete_script


def test_plain_path_unchanged():
    p = r"C:\Movies\Inception (2010).mkv"
    assert escape_path_for_ps1_squote(p) == p


def test_ascii_apostrophe_doubled():
    assert escape_path_for_ps1_squote("Bob's Burgers.mkv") == "Bob''s Burgers.mkv"


def test_right_single_quotation_mark_doubled():
    """The actual repro file from the 2026-05-03 incident."""
    assert escape_path_for_ps1_squote("Woe’s Hollow.mkv") == "Woe’’s Hollow.mkv"


def test_left_single_quotation_mark_doubled():
    assert escape_path_for_ps1_squote("‘something’.mkv") == "‘‘something’’.mkv"


def test_low9_and_high_reversed_9_doubled():
    assert escape_path_for_ps1_squote("a‚b‛c") == "a‚‚b‛‛c"


def test_multiple_apostrophes_in_one_path():
    p = "Bob's Mum's House.mkv"
    assert escape_path_for_ps1_squote(p) == "Bob''s Mum''s House.mkv"


def test_mixed_apostrophe_kinds():
    """A path can have BOTH ASCII and curly apostrophes — both must escape."""
    p = "It's ‘great’ and Bob’s favourite.mkv"
    assert escape_path_for_ps1_squote(p) == \
        "It''s ‘‘great’’ and Bob’’s favourite.mkv"


def test_write_delete_script_is_self_contained(tmp_path):
    """The generated script must produce valid PowerShell with the right
    array structure: opening @(, paths separated by commas, last path
    without trailing comma, closing )."""
    out = tmp_path / "del.ps1"
    paths = [
        r"\\NAS\a.mkv",
        "\\NAS\\Bob's b.mkv",
        "\\NAS\\Woe’s c.mkv",
    ]
    write_delete_script(paths, str(out))
    text = out.read_text(encoding="utf-8")
    # Preamble
    assert "param([switch]$WhatIf)" in text
    assert "$paths = @(" in text
    # Each path appears, with curlies doubled
    assert "'\\\\NAS\\a.mkv'" in text
    assert "Bob''s b.mkv" in text  # ASCII apostrophe doubled
    assert "Woe’’s c.mkv" in text  # curly apostrophe doubled
    # Last entry has no trailing comma, others do
    last_idx = text.rfind("Woe’’s c.mkv")
    after_last = text[last_idx:].split("\n", 1)[0]
    assert not after_last.rstrip().endswith(","), \
        "last array element must not have trailing comma"
    # Footer wiring
    assert "Remove-Item -LiteralPath $p -Force -WhatIf:$WhatIf" in text
    assert "Summary:" in text


def test_write_delete_script_no_bom(tmp_path):
    """Don't emit a UTF-8 BOM. PowerShell 7 reads UTF-8 cleanly without
    one, and PS 5.1 has historically misread BOMs as a stray character."""
    out = tmp_path / "del.ps1"
    write_delete_script(["\\\\NAS\\a.mkv"], str(out))
    raw = out.read_bytes()
    # BOM is 0xEF 0xBB 0xBF
    assert not raw.startswith(b"\xef\xbb\xbf"), "BOM leaked into generated script"
