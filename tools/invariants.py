"""Invariant checks for the NASCleanup pipeline.

Each invariant is a pure function returning ``(ok, violating_count, message)``.
They are cheap enough to run on demand (seconds), and the CLI surfaces the
full battery in a single table so silent data loss becomes visible.

Severity tiers:
  * critical: a lie in the DB (DONE with deferred reason, AV1 with no audio)
  * high: stale temp files on NAS, reason-says-error-but-status-done,
    state/report mismatch
  * medium: ghost python processes, missing-on-disk DONE rows
  * low: summary counts drift, forbidden ffmpeg flags in logs

Exit codes from ``main``:
  0 — all green
  1 — any CRITICAL or HIGH fails
  2 — only MEDIUM/LOW fail
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paths import MEDIA_REPORT, PIPELINE_STATE_DB, STAGING_DIR

# --------------------------------------------------------------------------
# Data classes
# --------------------------------------------------------------------------

Severity = str  # "critical" | "high" | "medium" | "low"


@dataclass
class CheckResult:
    """Result of a single invariant check."""

    name: str
    severity: Severity
    ok: bool
    value: int
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "severity": self.severity,
            "ok": self.ok,
            "value": self.value,
            "message": self.message,
        }


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

_TMP_SUFFIXES: tuple[str, ...] = (
    ".gapfill_tmp.mkv",
    ".submux_tmp.mkv",
    ".audiotrans_tmp.mkv",
    ".av1.tmp",
    ".naslib.tmp",
    ".naslib.tmp.mkv",
)


def _load_media_report() -> Optional[dict]:
    """Load media_report.json, or None if missing/unreadable."""
    if not MEDIA_REPORT.exists():
        return None
    try:
        with open(MEDIA_REPORT, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _open_state_db() -> Optional[sqlite3.Connection]:
    """Open the pipeline state DB read-only. Returns None if missing."""
    if not Path(PIPELINE_STATE_DB).exists():
        return None
    try:
        conn = sqlite3.connect(str(PIPELINE_STATE_DB), timeout=10)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


# --------------------------------------------------------------------------
# Individual invariants
# --------------------------------------------------------------------------


def check_no_audioless_av1(report: Optional[dict] = None) -> CheckResult:
    """AV1 video tracks must always carry at least one audio stream."""
    name = "no_audioless_av1"
    severity = "critical"
    report = report if report is not None else _load_media_report()
    if report is None:
        return CheckResult(name, severity, True, 0, "media_report.json not present — skipped")
    files = report.get("files", []) or []
    offenders: list[str] = []
    for entry in files:
        video = entry.get("video") or {}
        if str(video.get("codec_raw", "")).lower() != "av1":
            continue
        audio = entry.get("audio_streams") or []
        if len(audio) == 0:
            offenders.append(entry.get("filepath") or entry.get("filename") or "<unknown>")
    if offenders:
        sample = offenders[:3]
        return CheckResult(
            name,
            severity,
            False,
            len(offenders),
            f"{len(offenders)} AV1 file(s) with zero audio streams — sample: {sample}",
        )
    return CheckResult(name, severity, True, 0, "no AV1 files have zero audio streams")


def _count_done_with_reason(patterns: tuple[str, ...]) -> tuple[int, list[str]]:
    """Return (count, sample filepaths) for DONE rows whose reason matches any LIKE pattern."""
    conn = _open_state_db()
    if conn is None:
        return 0, []
    try:
        clauses = " OR ".join("LOWER(reason) LIKE ?" for _ in patterns)
        sql = f"SELECT filepath FROM pipeline_files WHERE LOWER(status)='done' AND ({clauses})"
        rows = conn.execute(sql, patterns).fetchall()
        return len(rows), [r[0] for r in rows[:3]]
    except sqlite3.Error:
        return 0, []
    finally:
        conn.close()


def check_no_done_with_deferred_reason() -> CheckResult:
    """DONE + 'deferred' or 'skipped' reason is the 2026-04-23 data-loss signature."""
    name = "no_done_with_deferred_reason"
    severity = "critical"
    count, sample = _count_done_with_reason(("%defer%", "%skip%"))
    if count > 0:
        return CheckResult(
            name,
            severity,
            False,
            count,
            f"{count} DONE row(s) with deferred/skipped reason — sample: {sample}",
        )
    return CheckResult(name, severity, True, 0, "no DONE rows have deferred/skipped reasons")


def check_no_done_with_error_reason() -> CheckResult:
    """DONE + reason 'fail'/'error' is also a lie — should be ERROR status."""
    name = "no_done_with_error_reason"
    severity = "high"
    count, sample = _count_done_with_reason(("%fail%", "%error%"))
    if count > 0:
        return CheckResult(
            name,
            severity,
            False,
            count,
            f"{count} DONE row(s) with fail/error reason — sample: {sample}",
        )
    return CheckResult(name, severity, True, 0, "no DONE rows have fail/error reasons")


def check_no_stale_tmp_on_nas(report: Optional[dict] = None) -> CheckResult:
    """Leftover *.tmp files in the report indicate a crash mid-replace."""
    name = "no_stale_tmp_on_nas"
    severity = "high"
    report = report if report is not None else _load_media_report()
    if report is None:
        return CheckResult(name, severity, True, 0, "media_report.json not present — skipped")
    files = report.get("files", []) or []
    offenders: list[str] = []
    for entry in files:
        fp = str(entry.get("filepath") or entry.get("filename") or "")
        low = fp.lower()
        for suffix in _TMP_SUFFIXES:
            if low.endswith(suffix):
                offenders.append(fp)
                break
    if offenders:
        return CheckResult(
            name,
            severity,
            False,
            len(offenders),
            f"{len(offenders)} stale tmp file(s) on NAS — sample: {offenders[:3]}",
        )
    return CheckResult(name, severity, True, 0, "no stale *.tmp files in media report")


# Role-like patterns used to identify pipeline python processes
_PIPELINE_CMD_PATTERNS: tuple[re.Pattern, ...] = (
    re.compile(r"-m\s+pipeline(\b|\.)", re.IGNORECASE),
    re.compile(r"-m\s+tools\.scanner\b", re.IGNORECASE),
    re.compile(r"-m\s+tools\.mux_external_subs\b", re.IGNORECASE),
    re.compile(r"-m\s+tools\.detect_languages\b", re.IGNORECASE),
)


def check_no_ghost_python_processes() -> CheckResult:
    """Every pipeline python.exe must appear in the process registry.

    Anything running a pipeline module that the registry doesn't know about
    is a ghost from a previous session still hammering the NAS.
    """
    name = "no_ghost_python_processes"
    severity = "medium"
    try:
        import psutil
    except ImportError:
        return CheckResult(name, severity, True, 0, "psutil unavailable — skipped")

    from pipeline.process_registry import ProcessRegistry

    reg_path = Path(STAGING_DIR) / "control" / "agents.registry.json"
    registered_pids: set[int] = set()
    if reg_path.exists():
        try:
            reg = ProcessRegistry(reg_path, heartbeat_secs=60)
            for entry in reg.list_active():
                pid = int(entry.get("pid", 0))
                if pid > 0:
                    registered_pids.add(pid)
        except (ValueError, OSError):
            pass

    ghosts: list[dict[str, Any]] = []
    my_pid = os.getpid()
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            info = proc.info
            pid = int(info.get("pid", 0))
            if pid <= 0 or pid == my_pid:
                continue
            pname = str(info.get("name") or "").lower()
            if not pname.startswith("python"):
                continue
            cmdline_list = info.get("cmdline") or []
            cmdline = " ".join(str(x) for x in cmdline_list)
            if not any(p.search(cmdline) for p in _PIPELINE_CMD_PATTERNS):
                continue
            if pid in registered_pids:
                continue
            ghosts.append({"pid": pid, "cmd": cmdline[:120]})
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    if ghosts:
        return CheckResult(
            name,
            severity,
            False,
            len(ghosts),
            f"{len(ghosts)} ghost pipeline process(es) — sample: {ghosts[:3]}",
        )
    return CheckResult(name, severity, True, 0, "no ghost pipeline python processes")


def check_state_db_report_consistency(report: Optional[dict] = None, tolerance: int = 50) -> CheckResult:
    """Files DONE in the state DB but absent from media_report (or vice versa).

    A small delta is expected (scanner lags encodes), hence the tolerance.
    """
    name = "state_db_report_consistency"
    severity = "high"
    report = report if report is not None else _load_media_report()
    if report is None:
        return CheckResult(name, severity, True, 0, "media_report.json not present — skipped")
    conn = _open_state_db()
    if conn is None:
        return CheckResult(name, severity, True, 0, "state DB not present — skipped")
    try:
        report_paths = {str(e.get("filepath", "")) for e in (report.get("files") or [])}
        report_paths.discard("")
        try:
            rows = conn.execute(
                "SELECT filepath FROM pipeline_files WHERE LOWER(status)='done'"
            ).fetchall()
        except sqlite3.Error:
            return CheckResult(name, severity, True, 0, "state DB schema missing — skipped")
        done_paths = {str(r[0]) for r in rows}
        done_missing_from_report = done_paths - report_paths
        violating = len(done_missing_from_report)
        if violating > tolerance:
            sample = list(done_missing_from_report)[:3]
            return CheckResult(
                name,
                severity,
                False,
                violating,
                f"{violating} DONE files absent from media_report (tolerance {tolerance}) — sample: {sample}",
            )
        return CheckResult(
            name,
            severity,
            True,
            violating,
            f"state/report drift {violating} within tolerance {tolerance}",
        )
    finally:
        conn.close()


def check_media_report_summary_matches(report: Optional[dict] = None) -> CheckResult:
    """summary.total_files should equal len(files)."""
    name = "media_report_summary_matches"
    severity = "low"
    report = report if report is not None else _load_media_report()
    if report is None:
        return CheckResult(name, severity, True, 0, "media_report.json not present — skipped")
    files = report.get("files") or []
    summary = report.get("summary") or {}
    declared = int(summary.get("total_files", 0) or 0)
    actual = len(files)
    drift = abs(declared - actual)
    if drift > 0:
        return CheckResult(
            name,
            severity,
            False,
            drift,
            f"summary.total_files={declared} but len(files)={actual} (drift {drift})",
        )
    return CheckResult(name, severity, True, 0, f"summary.total_files matches len(files)={actual}")


def check_filesystem_has_state_done_files(sample_size: int = 20) -> CheckResult:
    """Sample DONE rows — files should still exist on the NAS."""
    name = "filesystem_has_state_done_files"
    severity = "medium"
    conn = _open_state_db()
    if conn is None:
        return CheckResult(name, severity, True, 0, "state DB not present — skipped")
    try:
        try:
            rows = conn.execute(
                "SELECT filepath FROM pipeline_files WHERE LOWER(status)='done'"
            ).fetchall()
        except sqlite3.Error:
            return CheckResult(name, severity, True, 0, "state DB schema missing — skipped")
        paths = [str(r[0]) for r in rows if r[0]]
        if not paths:
            return CheckResult(name, severity, True, 0, "no DONE rows to sample")
        sample = random.sample(paths, min(sample_size, len(paths)))
        missing = [p for p in sample if not os.path.exists(p)]
        if missing:
            return CheckResult(
                name,
                severity,
                False,
                len(missing),
                f"{len(missing)}/{len(sample)} sampled DONE files missing on disk — sample: {missing[:3]}",
            )
        return CheckResult(
            name,
            severity,
            True,
            0,
            f"all {len(sample)} sampled DONE files present on disk",
        )
    finally:
        conn.close()


def check_no_forbidden_ffmpeg_flags_in_recent_logs(tail_bytes: int = 2_000_000) -> CheckResult:
    """Grep pipeline.log for flags banned by the discipline contract."""
    name = "no_forbidden_ffmpeg_flags_in_recent_logs"
    severity = "low"
    log_path = Path(STAGING_DIR) / "pipeline.log"
    if not log_path.exists():
        return CheckResult(name, severity, True, 0, "pipeline.log not present — skipped")
    try:
        size = log_path.stat().st_size
        offset = max(0, size - tail_bytes)
        with open(log_path, "rb") as f:
            f.seek(offset)
            tail = f.read().decode("utf-8", errors="replace")
    except OSError:
        return CheckResult(name, severity, True, 0, "pipeline.log unreadable — skipped")

    # Global (no :v) -err_detect ignore_err:
    bad_err_detect = re.compile(r"-err_detect\s+ignore_err")
    # Reject only if NOT immediately preceded by :v (i.e. ":v " before ignore_err)
    global_err_hits = 0
    for m in bad_err_detect.finditer(tail):
        start = m.start()
        window = tail[max(0, start - 20) : start]
        if ":v" in window:
            continue
        global_err_hits += 1

    optional_audio_hits = len(re.findall(r"-map\s+0:a\?", tail))
    total = global_err_hits + optional_audio_hits
    if total > 0:
        return CheckResult(
            name,
            severity,
            False,
            total,
            f"forbidden flags in recent log: {global_err_hits}x global -err_detect ignore_err, "
            f"{optional_audio_hits}x -map 0:a?",
        )
    return CheckResult(name, severity, True, 0, "no forbidden ffmpeg flags in recent log tail")


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def _check_functions() -> list[Callable[[], CheckResult]]:
    """Ordered list of zero-arg callables producing a CheckResult each."""
    return [
        check_no_audioless_av1,
        check_no_done_with_deferred_reason,
        check_no_done_with_error_reason,
        check_no_stale_tmp_on_nas,
        check_no_ghost_python_processes,
        check_state_db_report_consistency,
        check_media_report_summary_matches,
        check_filesystem_has_state_done_files,
        check_no_forbidden_ffmpeg_flags_in_recent_logs,
    ]


def run_all() -> list[CheckResult]:
    """Run every invariant and return the full list of results in order."""
    results: list[CheckResult] = []
    for fn in _check_functions():
        try:
            results.append(fn())
        except Exception as e:  # noqa: BLE001 - invariant failures must not crash the CLI
            results.append(
                CheckResult(
                    name=fn.__name__.replace("check_", ""),
                    severity="high",
                    ok=False,
                    value=1,
                    message=f"invariant raised {type(e).__name__}: {str(e)[:160]}",
                )
            )
    return results


# --------------------------------------------------------------------------
# Safe auto-fix helpers (--fix-safe)
# --------------------------------------------------------------------------


def fix_media_report_summary() -> int:
    """Recompute summary.total_files / summary.total_size_gb from files list."""
    report = _load_media_report()
    if report is None:
        return 0
    files = report.get("files") or []
    total_size = sum(int(e.get("file_size_bytes", 0) or 0) for e in files)
    summary = report.setdefault("summary", {})
    declared = int(summary.get("total_files", 0) or 0)
    summary["total_files"] = len(files)
    summary["total_size_gb"] = round(total_size / 1024**3, 2)
    summary["total_size_tb"] = round(total_size / 1024**4, 3)
    tmp = Path(str(MEDIA_REPORT) + ".tmp")
    tmp.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(MEDIA_REPORT)
    return abs(declared - len(files))


def fix_state_db_remove_missing_done(sample_size: int = 200) -> int:
    """Delete state DB rows marked DONE whose on-disk file is missing."""
    conn = _open_state_db()
    if conn is None:
        return 0
    try:
        try:
            rows = conn.execute(
                "SELECT filepath FROM pipeline_files WHERE LOWER(status)='done'"
            ).fetchall()
        except sqlite3.Error:
            return 0
        paths = [str(r[0]) for r in rows if r[0]]
        if not paths:
            return 0
        sample = random.sample(paths, min(sample_size, len(paths)))
        missing = [p for p in sample if not os.path.exists(p)]
        if not missing:
            return 0
        # Reopen in write mode — default sqlite3.connect opens RW anyway.
        for p in missing:
            conn.execute("DELETE FROM pipeline_files WHERE filepath = ?", (p,))
        conn.commit()
        return len(missing)
    finally:
        conn.close()


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _format_table(results: list[CheckResult]) -> str:
    """Render results as a fixed-width table."""
    headers = ("NAME", "SEVERITY", "STATUS", "COUNT", "MESSAGE")
    rows = [
        (r.name, r.severity, "OK" if r.ok else "FAIL", str(r.value), r.message) for r in results
    ]
    widths = [max(len(h), *(len(row[i]) for row in rows)) for i, h in enumerate(headers)]
    # Cap the message column so it doesn't destroy the layout.
    widths[-1] = min(widths[-1], 80)

    def fmt(row: tuple[str, ...]) -> str:
        return "  ".join(
            (cell[: widths[i]] if i == len(widths) - 1 else cell).ljust(widths[i])
            for i, cell in enumerate(row)
        )

    lines = [fmt(headers), fmt(tuple("-" * w for w in widths))]
    lines.extend(fmt(row) for row in rows)
    return "\n".join(lines)


def _exit_code(results: list[CheckResult]) -> int:
    """0 if all green; 1 if any CRITICAL/HIGH failed; 2 if only MEDIUM/LOW."""
    if all(r.ok for r in results):
        return 0
    for r in results:
        if not r.ok and r.severity in ("critical", "high"):
            return 1
    return 2


def main(argv: Optional[list[str]] = None) -> int:
    """Entrypoint for ``python -m tools.invariants``."""
    parser = argparse.ArgumentParser(
        prog="tools.invariants",
        description="Run pipeline invariants. Exit 1 on CRITICAL/HIGH failure, 2 on MEDIUM/LOW.",
    )
    parser.add_argument("--json", action="store_true", help="emit results as JSON")
    parser.add_argument(
        "--fix-safe",
        action="store_true",
        help="auto-repair safe invariants (summary recompute, drop missing DONE rows)",
    )
    args = parser.parse_args(argv)

    if args.fix_safe:
        summary_fixed = fix_media_report_summary()
        state_fixed = fix_state_db_remove_missing_done()
        if not args.json:
            sys.stderr.write(
                f"--fix-safe: summary drift corrected={summary_fixed}, "
                f"missing DONE rows removed={state_fixed}\n"
            )

    results = run_all()

    if args.json:
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "all_green": all(r.ok for r in results),
            "checks": [r.to_dict() for r in results],
        }
        sys.stdout.write(json.dumps(payload, indent=2) + "\n")
    else:
        sys.stdout.write(_format_table(results) + "\n")

    return _exit_code(results)


if __name__ == "__main__":
    raise SystemExit(main())
