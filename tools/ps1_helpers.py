"""Helpers for generating PowerShell scripts from Python.

The 2026-05-03 finding: PowerShell's parser treats every Unicode
apostrophe-like glyph as a string delimiter inside single-quoted strings,
not just the ASCII ``'``. A path like
``\\\\NAS\\Series\\Severance\\Woe's Hollow.mkv`` (U+2019 RIGHT SINGLE
QUOTATION MARK) closed our string mid-path and broke a generated
deletion script. The fix is to double-escape every member of the
apostrophe-family the way PowerShell expects, exactly as we already do
for ``'``.

The list of glyphs PowerShell treats as quote characters (per the
language spec under "Quoting Rules") includes:

  U+0027 APOSTROPHE
  U+2018 LEFT SINGLE QUOTATION MARK
  U+2019 RIGHT SINGLE QUOTATION MARK
  U+201A SINGLE LOW-9 QUOTATION MARK
  U+201B SINGLE HIGH-REVERSED-9 QUOTATION MARK

This module exposes a single function so every script-generator in the
project escapes paths the same way. Centralising it removes the
"oh I forgot to escape that one" class of bugs.
"""

from __future__ import annotations

# Code points PowerShell parses as opening / closing single-quote string
# delimiters. Verified against PowerShell 7 source (PSObject parser).
_PS_SINGLE_QUOTES = (
    "'",  # APOSTROPHE
    "‘",  # LEFT SINGLE QUOTATION MARK
    "’",  # RIGHT SINGLE QUOTATION MARK
    "‚",  # SINGLE LOW-9 QUOTATION MARK
    "‛",  # SINGLE HIGH-REVERSED-9 QUOTATION MARK
)


def escape_path_for_ps1_squote(path: str) -> str:
    """Escape ``path`` so it can be safely embedded in a PowerShell
    single-quoted string literal.

    Doubles every apostrophe-like glyph the parser recognises as a
    string delimiter. The caller still wraps the returned value in
    single quotes:

        f"'{escape_path_for_ps1_squote(path)}'"

    Examples (unchanged on the happy path):

        >>> escape_path_for_ps1_squote(r"C:\\foo\\bar.mkv")
        'C:\\\\foo\\\\bar.mkv'

    But apostrophes get doubled:

        >>> escape_path_for_ps1_squote("C:\\\\Bob's Burgers.mkv")
        "C:\\\\Bob''s Burgers.mkv"
        >>> escape_path_for_ps1_squote("C:\\\\Woe’s Hollow.mkv")
        'C:\\\\Woe’’s Hollow.mkv'
    """
    out = path
    for q in _PS_SINGLE_QUOTES:
        out = out.replace(q, q + q)
    return out


def write_delete_script(
    paths: list[str],
    output: str,
    header: str = "# Auto-generated bulk delete script.",
) -> int:
    """Write a self-contained PowerShell deletion script.

    The script accepts ``-WhatIf`` for preview and reports a summary on
    completion. Every path is escaped via :func:`escape_path_for_ps1_squote`
    so curly-apostrophe titles don't break the parser (the 2026-05-03
    Severance "Woe's Hollow" incident).

    Returns the byte count written.
    """
    lines: list[str] = []
    lines.append(header)
    lines.append('# Preview: pwsh <script>.ps1 -WhatIf')
    lines.append('# Real:    pwsh <script>.ps1')
    lines.append('')
    lines.append('param([switch]$WhatIf)')
    lines.append('$paths = @(')
    last = len(paths) - 1
    for i, p in enumerate(paths):
        esc = escape_path_for_ps1_squote(p)
        sep = ',' if i < last else ''
        lines.append(f"  '{esc}'{sep}")
    lines.append(')')
    lines.append('$gone = 0; $missing = 0; $failed = 0')
    lines.append('foreach ($p in $paths) {')
    lines.append('  if (-not (Test-Path -LiteralPath $p)) { $missing++; continue }')
    lines.append('  try { Remove-Item -LiteralPath $p -Force -WhatIf:$WhatIf; if (-not $WhatIf) { $gone++ } }')
    lines.append('  catch { Write-Warning "FAIL: $p"; $failed++ }')
    lines.append('}')
    lines.append('Write-Host ""')
    lines.append('Write-Host "Summary: $gone deleted / $missing missing / $failed failed (of $($paths.Count))"')

    body = "\r\n".join(lines).encode("utf-8")  # no BOM — PS 7 reads UTF-8 fine
    with open(output, "wb") as f:
        f.write(body)
    return len(body)
