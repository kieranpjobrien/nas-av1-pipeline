"""Re-probe a file and return a compliance report.

The verifier is the read-only counterpart to :mod:`naslib.run`. It answers
"does this file still meet the library standards?" by running a fresh
``ffprobe`` and comparing the result against a fixed ruleset. It never
writes to the database and never modifies a file.

This module is intentionally narrow — anything more elaborate belongs in
:mod:`naslib.plan` (which consumes the inventory) or in a future
reporting tool. Verification is the immediate, in-line sanity check
operators reach for after running a plan.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .inventory import INVENTORY_DB, connect, iter_files

# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ComplianceReport:
    """Per-file compliance report returned by :func:`verify_file`."""

    filepath: str
    ok: bool
    codec: str | None
    audio_count: int
    sub_count: int
    issues: list[str] = field(default_factory=list)

    def render(self) -> str:
        """Render a single-line human-readable summary."""
        flag = "OK" if self.ok else "BAD"
        issues = "; ".join(self.issues) if self.issues else "no issues"
        return (
            f"[{flag}] codec={self.codec or '?'} a={self.audio_count} s={self.sub_count} :: {issues} :: {self.filepath}"
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def verify_file(filepath: str) -> ComplianceReport:
    """Probe one file and return a :class:`ComplianceReport`.

    Rules applied:

    * Must have at least one non-attached-picture video stream.
    * Must have at least one audio stream (absence == disaster).
    * If the video codec is ``av1``, the file is considered compliant on
      codec grounds. If it's anything else, we flag it for re-encoding.

    Args:
        filepath: Absolute path to a media file.

    Returns:
        A :class:`ComplianceReport` — ``ok=True`` only if all rules pass.
    """
    if not os.path.exists(filepath):
        return ComplianceReport(
            filepath=filepath,
            ok=False,
            codec=None,
            audio_count=0,
            sub_count=0,
            issues=["file does not exist"],
        )
    probe = _ffprobe(filepath)
    if probe is None:
        return ComplianceReport(
            filepath=filepath,
            ok=False,
            codec=None,
            audio_count=0,
            sub_count=0,
            issues=["ffprobe failed"],
        )
    streams = probe.get("streams") or []
    video_count, audio_count, sub_count = _count_streams(streams)
    first_video = next((s for s in streams if _is_real_video(s)), None)
    codec = str(first_video.get("codec_name") or "").lower() if first_video else None
    issues: list[str] = []
    if video_count < 1:
        issues.append("no video streams")
    if audio_count < 1:
        issues.append("no audio streams (DESTRUCTIVE)")
    if codec and codec != "av1":
        issues.append(f"codec not AV1 (is {codec})")
    return ComplianceReport(
        filepath=filepath,
        ok=not issues,
        codec=codec,
        audio_count=audio_count,
        sub_count=sub_count,
        issues=issues,
    )


def verify_all(
    *,
    av1_only: bool = False,
    db_path: Path | None = None,
) -> tuple[int, int, list[ComplianceReport]]:
    """Walk the inventory and run :func:`verify_file` on every row.

    Args:
        av1_only: If ``True``, restrict the walk to rows whose recorded
            video codec is ``av1``. This is the most useful mode: it
            catches AV1 files that have been corrupted since encoding.
        db_path: Optional override for the SQLite path (tests).

    Returns:
        ``(ok_count, bad_count, bad_reports)``. Non-compliant reports are
        collected; compliant ones are counted but dropped to keep the return
        size small for large libraries.
    """
    ok = 0
    bad: list[ComplianceReport] = []
    with connect(db_path or INVENTORY_DB) as conn:
        rows = list(iter_files(conn, video_codec="av1" if av1_only else None))
    for row in rows:
        report = verify_file(row.filepath)
        if report.ok:
            ok += 1
        else:
            bad.append(report)
    return ok, len(bad), bad


# ---------------------------------------------------------------------------
# Probe helpers (private)
# ---------------------------------------------------------------------------


def _ffprobe(filepath: str) -> dict[str, Any] | None:
    """Run ffprobe with streams-only output. Returns the parsed JSON or None."""
    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_streams",
        filepath,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
            encoding="utf-8",
            errors="replace",
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None
    if result.returncode != 0:
        return None
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _count_streams(streams: list[dict[str, Any]]) -> tuple[int, int, int]:
    """Count (video, audio, subtitle) streams ignoring attached-picture video."""
    v = 0
    a = 0
    s = 0
    for stream in streams:
        ctype = stream.get("codec_type")
        if ctype == "video":
            if _is_real_video(stream):
                v += 1
        elif ctype == "audio":
            a += 1
        elif ctype == "subtitle":
            s += 1
    return v, a, s


def _is_real_video(stream: dict[str, Any]) -> bool:
    """Return True for playable video streams, False for attached pictures."""
    if stream.get("codec_type") != "video":
        return False
    disposition = stream.get("disposition")
    if isinstance(disposition, dict) and disposition.get("attached_pic"):
        return False
    return True
