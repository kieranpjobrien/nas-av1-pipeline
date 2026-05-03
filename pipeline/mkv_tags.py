"""Shared MKV global-tag read + merge utilities.

mkvpropedit's ``--tags global:file.xml`` flag *replaces* the entire
global-tag block. Multiple writers in this codebase want to own
different parts of the global tag set:

  * ``pipeline.full_gamut._stamp_encode_metadata`` writes
    ``ENCODER`` / ``CQ`` / ``CONTENT_GRADE``
  * ``pipeline.metadata.write_tmdb_to_mkv`` writes
    ``DIRECTOR`` / ``GENRE`` / ``ACTOR`` / ``WRITTEN_BY`` / etc.
  * ``pipeline.grade_review`` writes ``GRADE_REVIEW`` / ``GRADE_REVIEW_AT``

Before this module each writer ran ``mkvpropedit --tags global:...``
with only the tags it cared about, so whichever writer ran *last*
wiped the others. The pre-2026-05-04 sample of 50 latest done encodes
showed 0/50 had CQ stamped — the encoder wrote it, then the TMDb
writer clobbered it, every time.

The fix is :func:`merge_global_tags` — each writer declares which tag
names it ``owns`` (the names it would have written on previous runs).
We read the existing tag block via ``mkvextract tags``, drop entries
whose name is in the owned-set, append the writer's new entries, and
push the union back. Anything outside the owned-set is preserved
verbatim.

Why mkvextract and not mkvmerge --identify: ``mkvmerge --identify``
surfaces global tags only as a count (``global_tags[].num_entries``)
and never the actual names/values. ``mkvextract <file> tags -`` writes
the full Matroska tag XML to stdout, which we parse here.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import xml.sax.saxutils as _xml


def _find_mkvextract() -> str | None:
    """Locate the mkvextract binary on Windows / PATH. Cheap so not cached."""
    exe = shutil.which("mkvextract")
    if exe:
        return exe
    for candidate in (
        r"C:\Program Files\MKVToolNix\mkvextract.exe",
        r"C:\Program Files (x86)\MKVToolNix\mkvextract.exe",
    ):
        if os.path.isfile(candidate):
            return candidate
    return None


def read_global_tags(filepath: str, *, timeout: int = 60) -> list[dict]:
    """Return ``[{'name': str, 'value': str}]`` for every global SimpleTag.

    Returns ``[]`` on tool failure (mkvextract not installed, file
    missing, parse error). Track-level tags (per-stream DURATION,
    per-stream ENCODER) are filtered out — only tags whose Targets
    element is empty or carries the global TargetTypeValue (50) survive.

    Order is preserved so a round-trip read → write doesn't shuffle the
    on-disk layout.
    """
    exe = _find_mkvextract()
    if not exe:
        return []
    try:
        result = subprocess.run(
            [exe, filepath, "tags", "-"],
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    if result.returncode != 0 or not result.stdout:
        return []
    raw = result.stdout
    if raw.startswith("﻿"):
        raw = raw[1:]
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return []

    out: list[dict] = []
    for tag in root.findall("Tag"):
        targets = tag.find("Targets")
        if targets is None:
            is_global = True
        else:
            children = list(targets)
            if not children:
                is_global = True
            elif (
                len(children) == 1
                and targets.find("TargetTypeValue") is not None
                and (targets.find("TargetTypeValue").text or "").strip() == "50"
            ):
                is_global = True
            else:
                is_global = False
        if not is_global:
            continue
        for simple in tag.findall("Simple"):
            name_el = simple.find("Name")
            value_el = simple.find("String")
            if name_el is None:
                continue
            name = (name_el.text or "").strip()
            if not name:
                continue
            value = (value_el.text if value_el is not None else "") or ""
            out.append({"name": name, "value": str(value)})
    return out


def _build_tag_xml(tags: list[dict]) -> str:
    """Render a list of {name, value} tags as mkvpropedit-compatible XML.

    Always emits at TargetTypeValue 50 (movie/episode = global). Empty
    tag list produces an empty <Tags/> block, which clears all global
    tags — useful only as the inner of a clear operation.
    """
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<!DOCTYPE Tags SYSTEM "matroskatags.dtd">',
        "<Tags>",
    ]
    if tags:
        parts.append("  <Tag>")
        parts.append("    <Targets><TargetTypeValue>50</TargetTypeValue></Targets>")
        for t in tags:
            name = _xml.escape(t["name"])
            value = _xml.escape(str(t["value"]))
            parts.append(
                f"    <Simple><Name>{name}</Name><String>{value}</String></Simple>"
            )
        parts.append("  </Tag>")
    parts.append("</Tags>")
    return "\n".join(parts)


def _write_tag_xml(filepath: str, xml_body: str, *, timeout: int = 60) -> bool:
    """Write tag XML via mkvpropedit. Returns True on rc < 2."""
    from pipeline import local_mux  # noqa: PLC0415

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".xml", delete=False, encoding="utf-8"
    ) as f:
        f.write(xml_body)
        xml_path = f.name
    try:
        result = local_mux.local_mkvpropedit(
            filepath, ["--tags", f"global:{xml_path}"], timeout=timeout
        )
        if result.returncode >= 2:
            logging.warning(
                "  mkvpropedit rc=%s on %s: %s",
                result.returncode,
                os.path.basename(filepath),
                ((result.stderr or result.stdout) or "").strip()[:200],
            )
            return False
        return True
    finally:
        try:
            os.remove(xml_path)
        except OSError:
            pass


def merge_global_tags(
    filepath: str,
    *,
    owned_names: set[str] | frozenset[str],
    new_tags: list[dict],
    timeout: int = 60,
) -> bool:
    """Patch the global tag block, preserving tags outside ``owned_names``.

    Args:
        filepath: MKV path (UNC or local).
        owned_names: Tag names the caller claims authority over. Existing
            global tags with these names get dropped before the merge.
            Compared case-insensitively.
        new_tags: ``[{'name', 'value'}]`` to append after the dropouts.
        timeout: mkvpropedit timeout in seconds.

    Returns True on mkvpropedit rc < 2 (success or warnings). False on
    hard failure — caller decides whether to surface or retry.

    Pass ``new_tags=[]`` with a populated ``owned_names`` to *clear*
    those tags without writing replacements (used by
    ``grade_review.clear_grade_review``).
    """
    upper_owned = {n.upper() for n in owned_names}
    existing = read_global_tags(filepath, timeout=timeout)
    preserved = [t for t in existing if t["name"].upper() not in upper_owned]
    merged = preserved + new_tags
    return _write_tag_xml(filepath, _build_tag_xml(merged), timeout=timeout)
