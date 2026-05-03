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

from datetime import datetime, timezone
from typing import TypedDict


class GradeReview(TypedDict, total=False):
    """Shape returned by :func:`read_grade_review` when a tag is present."""

    status: str  # "accepted" / "rejected"
    reviewed_at: str  # ISO-8601 timestamp


# Tag names this module owns. The merge helper drops existing entries
# with these names before appending new ones — anything else (ENCODER,
# CQ, CONTENT_GRADE, DIRECTOR, GENRE, ...) is preserved verbatim.
_OWNED_NAMES = frozenset({"GRADE_REVIEW", "GRADE_REVIEW_AT"})


def _read_global_tags(filepath: str) -> list[dict]:
    """Backwards-compat shim — :mod:`pipeline.mkv_tags` is the canonical
    place for tag reads now. Audit + tests still import this name."""
    from pipeline.mkv_tags import read_global_tags  # noqa: PLC0415

    return read_global_tags(filepath)


def read_grade_review(filepath: str) -> GradeReview | None:
    """Return the current grade-review state, or ``None`` if no tag."""
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


def set_grade_review(filepath: str, status: str) -> bool:
    """Stamp ``GRADE_REVIEW=<status>`` + ``GRADE_REVIEW_AT`` into the MKV.

    All other existing global tags are preserved by the merge helper.
    Returns True on success, False on mkvpropedit failure.
    """
    from pipeline.mkv_tags import merge_global_tags  # noqa: PLC0415

    if not status:
        raise ValueError("status must be a non-empty string")
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return merge_global_tags(
        filepath,
        owned_names=_OWNED_NAMES,
        new_tags=[
            {"name": "GRADE_REVIEW", "value": status},
            {"name": "GRADE_REVIEW_AT", "value": now_iso},
        ],
    )


def clear_grade_review(filepath: str) -> bool:
    """Remove GRADE_REVIEW + GRADE_REVIEW_AT, preserving all other tags."""
    from pipeline.mkv_tags import merge_global_tags  # noqa: PLC0415

    return merge_global_tags(
        filepath,
        owned_names=_OWNED_NAMES,
        new_tags=[],
    )
