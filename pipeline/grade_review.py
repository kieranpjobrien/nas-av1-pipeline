"""Manual grade-review state stamped into MKV global SimpleTags.

The grade-aware CQ audit (``tools.audit_encode_cq``) uses the encoder's
stamped CQ + the grade rule to bucket each file as
optimal / too_low / too_high. The buckets feed the Grade-Optimised
hero stat on the dashboard.

For the **too_high** bucket the user does manual review — the AV1 was
encoded harsher than the grade rule says it should be, but the user
may decide the visible quality is fine and the file is good enough to
keep. To stop those files from re-appearing in the drill every time
the audit runs, we let the user mark them as accepted. The acceptance
is stored *in the file* (MKV global SimpleTag) so it survives:

  * report regeneration
  * pipeline_state.db rebuilds
  * moves between drives / NAS shares

Tags written:

    GRADE_REVIEW       = "accepted"      (or "rejected" — reserved)
    GRADE_REVIEW_AT    = ISO-8601 UTC timestamp

The audit treats ``GRADE_REVIEW=accepted`` as a hard override:
bucket forces to "optimal" regardless of the CQ comparison.

Why an MKV tag and not a sidecar JSON: rule 12 of the discipline
contract (``CLAUDE.md``) — sidecars get out of sync with the actual
file. Stamping the source-of-truth file means the verdict moves with
the bytes.
"""

from __future__ import annotations

import logging
import os
import tempfile
import xml.sax.saxutils as _xml
from datetime import datetime, timezone
from typing import TypedDict


class GradeReview(TypedDict, total=False):
    """Shape returned by :func:`read_grade_review` when a tag is present."""

    status: str  # "accepted" / "rejected"
    reviewed_at: str  # ISO-8601 timestamp


# Tags we manage. Anything else found in the global tag block is
# preserved verbatim on rewrite (ENCODER, CQ, CONTENT_GRADE, plus any
# ad-hoc tags Plex / Radarr / etc. wrote).
_REVIEW_TAGS = frozenset({"GRADE_REVIEW", "GRADE_REVIEW_AT"})


def _read_global_tags(filepath: str) -> list[dict]:
    """Return a list of {name, value} dicts for every global SimpleTag.

    Why mkvextract and not mkvmerge --identify: ``mkvmerge --identify``
    surfaces global tags as a *count* under ``global_tags[].num_entries``
    but does NOT expose the names/values themselves. ``mkvextract tags
    FILE -`` writes the full Matroska tag XML to stdout. We parse that.

    Returns ``[]`` on tool failure or empty tag block rather than raising.
    Track-level tags (DURATION, codec ENCODER) are filtered out — only
    tags whose Targets element is empty (= global) are returned.
    """
    import os  # noqa: PLC0415
    import shutil  # noqa: PLC0415
    import subprocess  # noqa: PLC0415
    import xml.etree.ElementTree as ET  # noqa: PLC0415

    exe = shutil.which("mkvextract")
    if not exe:
        for candidate in (
            r"C:\Program Files\MKVToolNix\mkvextract.exe",
            r"C:\Program Files (x86)\MKVToolNix\mkvextract.exe",
        ):
            if os.path.isfile(candidate):
                exe = candidate
                break
    if not exe:
        return []
    try:
        result = subprocess.run(
            [exe, filepath, "tags", "-"],
            capture_output=True,
            text=True,
            timeout=60,
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
        is_global = targets is None or len(list(targets)) == 0 or (
            # TargetTypeValue 50 is movie/episode level = global by convention.
            len(list(targets)) == 1
            and targets.find("TargetTypeValue") is not None
            and (targets.find("TargetTypeValue").text or "").strip() == "50"
        )
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


def read_grade_review(filepath: str) -> GradeReview | None:
    """Return the current grade-review state, or ``None`` if no tag.

    Cheap-ish — one ``mkvmerge --identify`` call per file. Audit calls
    this 6,000+ times per run; the existing CQ-tag reader does the same
    so the cost ratio is already baked into the audit budget.
    """
    status = None
    reviewed_at = None
    for t in _read_global_tags(filepath):
        name = t["name"].upper()
        if name == "GRADE_REVIEW":
            status = t["value"]
        elif name == "GRADE_REVIEW_AT":
            reviewed_at = t["value"]
    if not status:
        return None
    out: GradeReview = {"status": status}
    if reviewed_at:
        out["reviewed_at"] = reviewed_at
    return out


def _build_tag_xml(tags: list[dict]) -> str:
    """Render a list of {name, value} tags as an mkvpropedit-compatible XML.

    All tags are written at TargetTypeValue 50 (movie / episode) which
    matches what :func:`pipeline.full_gamut._stamp_encode_metadata` uses.
    Empty tag list → empty <Tags/> block which clears all global tags.
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


def set_grade_review(filepath: str, status: str) -> bool:
    """Stamp GRADE_REVIEW=<status> + GRADE_REVIEW_AT into the MKV.

    Critically: ``mkvpropedit --tags global:...`` *replaces* the entire
    global tag block. We read the existing tags first, drop any prior
    GRADE_REVIEW / GRADE_REVIEW_AT, append the new ones, and write the
    full union back. ENCODER / CQ / CONTENT_GRADE etc. survive.

    Returns ``True`` on success. Logs and returns ``False`` on
    mkvpropedit failure — caller surfaces the error to the UI.
    """
    from pipeline import local_mux  # noqa: PLC0415

    if not status:
        raise ValueError("status must be a non-empty string")

    existing = _read_global_tags(filepath)
    preserved = [t for t in existing if t["name"].upper() not in _REVIEW_TAGS]
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_tags = preserved + [
        {"name": "GRADE_REVIEW", "value": status},
        {"name": "GRADE_REVIEW_AT", "value": now_iso},
    ]
    return _write_tags(filepath, new_tags, local_mux=local_mux)


def clear_grade_review(filepath: str) -> bool:
    """Remove GRADE_REVIEW and GRADE_REVIEW_AT, preserving all other tags."""
    from pipeline import local_mux  # noqa: PLC0415

    existing = _read_global_tags(filepath)
    preserved = [t for t in existing if t["name"].upper() not in _REVIEW_TAGS]
    return _write_tags(filepath, preserved, local_mux=local_mux)


def _write_tags(filepath: str, tags: list[dict], *, local_mux) -> bool:  # noqa: ANN001
    """Render tags to XML and call mkvpropedit. Internal helper."""
    xml_body = _build_tag_xml(tags)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".xml", delete=False, encoding="utf-8"
    ) as f:
        f.write(xml_body)
        xml_path = f.name
    try:
        result = local_mux.local_mkvpropedit(
            filepath, ["--tags", f"global:{xml_path}"], timeout=60
        )
        if result.returncode >= 2:
            logging.warning(
                "  mkvpropedit grade-review rc=%s: %s",
                result.returncode,
                (result.stderr or "").strip()[:200],
            )
            return False
        return True
    finally:
        try:
            os.remove(xml_path)
        except OSError:
            pass
