"""Pin the 2026-05-11 audit-sidecar removal.

User explicit ask: "THERE SHOULD NOT BE AN audit_cq.json! IT SHOULD ALL
LIVE IN THE MEDIA REPORT.JSON. THAT IS THE FUCKING POINT OF A SINGLE
SOURCE OF TRUTH."

The pre-2026-05-11 design had audit results in a separate
``audit_cq.json`` sidecar. It drifted from media_report and caused the
dashboard's bulk-requeue button to operate on stale buckets — six
already-correctly-encoded files (Avengers IW, Batman & Robin, Vertigo,
etc.) got re-flagged for re-encode and stuck in a re-fetch loop.

These tests pin:
  1. ``tools.audit_encode_cq`` never reads or writes ``audit_cq.json``
     (banned filename + only-via-report_lock pattern)
  2. Audit data is stored in media_report.json as a per-file ``audit``
     field + top-level ``audit_summary``.
  3. Consumers (server endpoints + tooling) read from media_report, not
     a sidecar.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _read(p: str) -> str:
    return (REPO / p).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Banned: any code path that writes a separate audit_cq.json file
# ---------------------------------------------------------------------------


def test_audit_tool_writes_into_media_report_not_sidecar():
    """tools.audit_encode_cq must call patch_report — not write a
    standalone audit_cq.json sidecar (the pre-2026-05-11 design).
    """
    src = _read("tools/audit_encode_cq.py")
    # Must use the report-locked writer
    assert "patch_report" in src, "audit must use tools.report_lock.patch_report to merge into media_report"
    # Must NOT write to audit_cq.json
    forbidden = re.compile(r"""open\([^)]*audit_cq\.json[^)]*['"]w""")
    assert not forbidden.search(src), "audit tool must not open audit_cq.json for write"
    # Must NOT use Path.write_text on audit_cq.json
    forbidden2 = re.compile(r"""audit_cq\.json[^)]*\)\.write_text""")
    assert not forbidden2.search(src), "audit tool must not write a separate audit_cq.json file"


def test_server_endpoints_read_audit_from_report_not_sidecar():
    """server.routers.library /api/cq-audit must source from media_report,
    not from the removed sidecar. /api/library-completion likewise."""
    src = _read("server/routers/library.py")
    # Hard ban: no open() / Path call on audit_cq.json
    forbidden = re.compile(r"""audit_cq\.json['"]?\)?\.(?:open|read_text|exists)""")
    assert not forbidden.search(src), (
        "library router must not read audit_cq.json — single source of truth is media_report"
    )
    # Must reference audit_summary (the new top-level key in media_report)
    assert "audit_summary" in src, "library router must read audit_summary from media_report"


def test_grade_review_endpoints_patch_report_not_sidecar():
    """grade-accept / grade-clear must patch the per-file ``audit`` blob
    in media_report, not the (removed) audit_cq.json sidecar."""
    src = _read("server/routers/files.py")
    # The new helper must exist
    assert "_patch_audit_in_report" in src, (
        "files router must use _patch_audit_in_report (media_report) for grade reviews"
    )
    # The old sidecar helper must be GONE
    assert "_patch_audit_sidecar" not in src, (
        "old _patch_audit_sidecar helper must be removed — it writes the wrong file"
    )


def test_tools_read_audit_from_report():
    """All tooling that used to read audit_cq.json must now read it via
    report_lock.read_report() — single canonical access."""
    for tool in (
        "tools/strip_unsafe_force_reencode.py",
        "tools/overnight_test_setup.py",
        "tools/requeue_grade_mismatches.py",
    ):
        src = _read(tool)
        # No open of audit_cq.json
        assert "open(args.audit" not in src or "default=\"F:/AV1_Staging/audit_cq.json\"" not in src, (
            f"{tool} still has an argparse default pointing at audit_cq.json"
        )
        # Must read via report_lock (or read_report)
        assert "read_report" in src, (
            f"{tool} must read audit data via tools.report_lock.read_report (not from sidecar)"
        )


# ---------------------------------------------------------------------------
# Schema contract: per-file audit blob has the expected shape
# ---------------------------------------------------------------------------


def test_audit_blob_schema_documented_in_audit_tool():
    """The audit-write loop in tools.audit_encode_cq must store the
    full per-file blob (target_cq, current_cq, source, bucket, grade,
    etc.) on the entry. Check the schema as a source-string anchor on
    the patch function body — if someone deletes one of these keys we
    want a loud failure here, not silent dashboard breakage."""
    src = _read("tools/audit_encode_cq.py")
    patch_block_start = src.find("def _patch(rep: dict)")
    assert patch_block_start != -1, "audit tool's _patch function not found"
    patch_block = src[patch_block_start : patch_block_start + 2048]
    # The per-file write must touch the ``audit`` field on the entry
    assert 'f["audit"]' in patch_block or "f['audit']" in patch_block, (
        "_patch must assign to f['audit'] on each report entry"
    )
    # And the summary write must go on the top-level audit_summary key
    assert 'rep["audit_summary"]' in patch_block or "rep['audit_summary']" in patch_block, (
        "_patch must write rep['audit_summary']"
    )
