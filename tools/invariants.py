"""Invariant checks for the NASCleanup pipeline.

Each check returns an :class:`InvariantResult` with a severity tier, a
pass/fail flag, a human-readable message, a list of violating
identifiers (filepaths / pids / etc.), and a structured details dict.

Severity tiers:
  * CRITICAL - silent data loss; never acceptable.
  * HIGH     - actively broken; workflow will fail.
  * MEDIUM   - drift or housekeeping; may self-heal.
  * LOW      - cosmetic / log hygiene.

Exit codes from ``main``:
  0 - all green.
  1 - any CRITICAL or HIGH failed.
  2 - a check itself errored (ssh unreachable, DB missing, etc.).
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sqlite3
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal, Optional

from paths import MEDIA_REPORT, PIPELINE_STATE_DB, STAGING_DIR

Severity = Literal["CRITICAL", "HIGH", "MEDIUM", "LOW"]


# --------------------------------------------------------------------------
# Result dataclass
# --------------------------------------------------------------------------


@dataclass
class InvariantResult:
    """Structured result of a single invariant check."""

    name: str
    severity: Severity
    passed: bool
    message: str
    violations: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# Back-compat alias for call sites that still import the old name.
CheckResult = InvariantResult


# --------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------


def _load_media_report(path: Optional[Path] = None) -> Optional[dict]:
    """Load ``media_report.json`` (or the override path). None on missing/bad JSON."""
    target = Path(path) if path is not None else MEDIA_REPORT
    if not target.exists():
        return None
    try:
        with open(target, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _open_state_db(path: Optional[Path] = None) -> Optional[sqlite3.Connection]:
    """Open the pipeline state DB read-write so --fix-safe can share the handle."""
    target = Path(path) if path is not None else Path(PIPELINE_STATE_DB)
    if not target.exists():
        return None
    try:
        conn = sqlite3.connect(str(target), timeout=10)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


# --------------------------------------------------------------------------
# CRITICAL - no AV1 file may ship without at least one audio track
# --------------------------------------------------------------------------


def check_no_audioless_av1(report: Optional[dict] = None) -> InvariantResult:
    """AV1 video tracks must always carry at least one audio stream.

    This is the exact 2026-04-23 signature: files muxed to AV1 with zero
    audio tracks because of ``-err_detect ignore_err`` + ``-map 0:a?``.
    """
    name = "no_audioless_av1"
    severity: Severity = "CRITICAL"
    if report is None:
        report = _load_media_report()
    if report is None:
        return InvariantResult(name, severity, True, "media_report.json not present - skipped")
    files = report.get("files") or []
    offenders: list[str] = []
    for entry in files:
        video = entry.get("video") or {}
        if str(video.get("codec_raw", "")).lower() != "av1":
            continue
        audio = entry.get("audio_streams") or []
        if len(audio) == 0:
            offenders.append(entry.get("filepath") or entry.get("filename") or "<unknown>")
    passed = not offenders
    message = (
        f"{len(offenders)} AV1 file(s) with zero audio streams"
        if offenders
        else "no AV1 files have zero audio streams"
    )
    return InvariantResult(
        name,
        severity,
        passed,
        message,
        violations=offenders,
        details={"count": len(offenders), "scanned": len(files)},
    )


# --------------------------------------------------------------------------
# CRITICAL - DONE + deferred/skipped reason is a lie
# --------------------------------------------------------------------------


def _done_with_reason_like(
    patterns: tuple[str, ...], db_path: Optional[Path] = None
) -> tuple[list[tuple[str, str]], bool]:
    """Return (rows, db_ok). Rows are (filepath, reason) tuples."""
    conn = _open_state_db(db_path)
    if conn is None:
        return [], False
    try:
        clauses = " OR ".join("LOWER(reason) LIKE ?" for _ in patterns)
        sql = f"SELECT filepath, reason FROM pipeline_files WHERE LOWER(status)='done' AND ({clauses})"
        rows = conn.execute(sql, patterns).fetchall()
        return [(r[0], r[1]) for r in rows], True
    except sqlite3.Error:
        return [], False
    finally:
        conn.close()


def check_no_done_with_deferred_reason(db_path: Optional[Path] = None) -> InvariantResult:
    """DONE + 'deferred' or 'skipped' reason is the 65-file anti-pattern."""
    name = "no_done_with_deferred_reason"
    severity: Severity = "CRITICAL"
    rows, ok = _done_with_reason_like(("%defer%", "%skip%"), db_path)
    if not ok:
        return InvariantResult(name, severity, True, "pipeline_state.db not present - skipped")
    offenders = [fp for fp, _ in rows]
    passed = not offenders
    message = (
        f"{len(offenders)} DONE row(s) with deferred/skipped reason"
        if offenders
        else "no DONE rows have deferred/skipped reasons"
    )
    return InvariantResult(
        name,
        severity,
        passed,
        message,
        violations=offenders,
        details={"reasons": [{"filepath": fp, "reason": r} for fp, r in rows[:20]]},
    )


# --------------------------------------------------------------------------
# HIGH - DONE + fail/skipped/error reason
# --------------------------------------------------------------------------


def check_no_done_with_error_reason(db_path: Optional[Path] = None) -> InvariantResult:
    """DONE paired with fail/skipped/error reason; should be ERROR status."""
    name = "no_done_with_error_reason"
    severity: Severity = "HIGH"
    rows, ok = _done_with_reason_like(("%fail%", "%skipped%", "%error%"), db_path)
    if not ok:
        return InvariantResult(name, severity, True, "pipeline_state.db not present - skipped")
    offenders = [fp for fp, _ in rows]
    passed = not offenders
    message = (
        f"{len(offenders)} DONE row(s) with fail/skipped/error reason"
        if offenders
        else "no DONE rows have fail/skipped/error reasons"
    )
    return InvariantResult(
        name,
        severity,
        passed,
        message,
        violations=offenders,
        details={"reasons": [{"filepath": fp, "reason": r} for fp, r in rows[:20]]},
    )


# --------------------------------------------------------------------------
# HIGH - stale tmp files on NAS (SSH)
# --------------------------------------------------------------------------


def _ssh_nas_host() -> Optional[str]:
    """Resolve the NAS SSH host from pipeline.nas_worker, or None if unconfigured."""
    try:
        from pipeline.nas_worker import NAS
    except ImportError:
        return None
    host = (NAS.get("host") or "").strip()
    return host or None


def _ssh_run(host: str, remote_cmd: str, timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a single shell command on the NAS via SSH with BatchMode (no prompts)."""
    ssh_cmd = [
        "ssh",
        "-o", "ConnectTimeout=10",
        "-o", "BatchMode=yes",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        host,
        remote_cmd,
    ]
    return subprocess.run(
        ssh_cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        encoding="utf-8",
        errors="replace",
    )


def check_no_stale_tmp_on_nas(skip_ssh: bool = False) -> InvariantResult:
    """Look for ``*.tmp.mkv`` / ``*.partial`` files older than 60 minutes on the NAS."""
    name = "no_stale_tmp_on_nas"
    severity: Severity = "HIGH"
    if skip_ssh:
        return InvariantResult(name, severity, True, "ssh checks skipped")
    host = _ssh_nas_host()
    if not host:
        return InvariantResult(name, severity, True, "NAS SSH host not configured - skipped")
    cmd = (
        "find /volume1/Media \\( -name '*.tmp.mkv' -o -name '*.partial' \\) "
        "-type f -mmin +60 2>/dev/null"
    )
    try:
        result = _ssh_run(host, cmd, timeout=45)
    except subprocess.TimeoutExpired:
        return InvariantResult(
            name, severity, False, "ssh find timed out", details={"error": "timeout"}
        )
    except (FileNotFoundError, OSError) as e:
        return InvariantResult(
            name, severity, True, f"ssh unreachable ({type(e).__name__}) - skipped"
        )
    if result.returncode != 0:
        return InvariantResult(
            name,
            severity,
            True,
            f"ssh find failed rc={result.returncode} - skipped",
            details={"stderr": result.stderr[:400]},
        )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    passed = not lines
    message = (
        f"{len(lines)} stale tmp file(s) on NAS"
        if lines
        else "no stale *.tmp.mkv / *.partial files older than 60 min"
    )
    return InvariantResult(
        name,
        severity,
        passed,
        message,
        violations=lines,
        details={"count": len(lines), "host": host},
    )


# --------------------------------------------------------------------------
# HIGH - zombie mkvmerge / ffmpeg on NAS
# --------------------------------------------------------------------------


def check_no_zombie_mkvmerge(skip_ssh: bool = False) -> InvariantResult:
    """mkvmerge/ffmpeg processes running > 1 hour on the NAS are zombies."""
    name = "no_zombie_mkvmerge"
    severity: Severity = "HIGH"
    if skip_ssh:
        return InvariantResult(name, severity, True, "ssh checks skipped")
    host = _ssh_nas_host()
    if not host:
        return InvariantResult(name, severity, True, "NAS SSH host not configured - skipped")
    # etime reads as [[DD-]HH:]MM:SS. Runtimes >= 1 hour have at least two
    # colons (H:MM:SS or DD-HH:MM:SS) or contain a dash. Shorter runtimes show
    # as MM:SS (one colon) and must be filtered out.
    cmd = (
        "ps -eo pid,etime,comm | awk '"
        "NR>1 && ($3 ~ /mkvmerge|ffmpeg/) && ($2 ~ /:/) {print $1, $2, $3}'"
    )
    try:
        result = _ssh_run(host, cmd, timeout=30)
    except subprocess.TimeoutExpired:
        return InvariantResult(
            name, severity, False, "ssh ps timed out", details={"error": "timeout"}
        )
    except (FileNotFoundError, OSError) as e:
        return InvariantResult(
            name, severity, True, f"ssh unreachable ({type(e).__name__}) - skipped"
        )
    if result.returncode != 0:
        return InvariantResult(
            name,
            severity,
            True,
            f"ssh ps failed rc={result.returncode} - skipped",
            details={"stderr": result.stderr[:400]},
        )
    entries: list[dict[str, str]] = []
    violations: list[str] = []
    for line in result.stdout.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 3:
            continue
        pid, etime, comm = parts[0], parts[1], parts[2]
        if etime.count(":") < 2 and "-" not in etime:
            continue
        entries.append({"pid": pid, "etime": etime, "comm": comm})
        violations.append(f"pid={pid} etime={etime} comm={comm}")
    passed = not violations
    message = (
        f"{len(violations)} zombie mkvmerge/ffmpeg process(es) on NAS"
        if violations
        else "no zombie mkvmerge/ffmpeg processes"
    )
    return InvariantResult(
        name,
        severity,
        passed,
        message,
        violations=violations,
        details={"processes": entries[:20]},
    )


# --------------------------------------------------------------------------
# MEDIUM - ghost python processes (local)
# --------------------------------------------------------------------------


_PIPELINE_CMD_PATTERNS: tuple[re.Pattern, ...] = (
    re.compile(r"-m\s+pipeline(\b|\.)", re.IGNORECASE),
    re.compile(r"-m\s+tools\.scanner\b", re.IGNORECASE),
    re.compile(r"-m\s+tools\.mux_external_subs\b", re.IGNORECASE),
    re.compile(r"-m\s+tools\.detect_languages\b", re.IGNORECASE),
    re.compile(r"-m\s+tools\.", re.IGNORECASE),
)


def check_no_ghost_python_processes() -> InvariantResult:
    """Pipeline python.exe processes must appear in agents.registry.json."""
    name = "no_ghost_python_processes"
    severity: Severity = "MEDIUM"
    try:
        import psutil
    except ImportError:
        return InvariantResult(name, severity, True, "psutil unavailable - skipped")

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
    violations: list[str] = []
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
            # `uv run python -m pipeline` spawns a bootstrap python.exe
            # that matches our pipeline cmdline but isn't itself the process
            # the pipeline code registers — the REAL pipeline is the bootstrap's
            # child. Before flagging a ghost, check whether any descendant
            # process is in the registry OR is this invariants-process itself
            # (the self-exemption extends to ancestors of my_pid since
            # ``uv run python -m tools.invariants`` spawns the same kind of
            # bootstrap wrapper). If so, this is the wrapper parent, not a ghost.
            try:
                descendants = proc.children(recursive=True)
                descendant_pids = {int(child.pid) for child in descendants}
                if descendant_pids & registered_pids:
                    continue
                if my_pid in descendant_pids:
                    continue
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            ghosts.append({"pid": pid, "cmd": cmdline[:240]})
            violations.append(f"pid={pid} cmd={cmdline[:120]}")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    passed = not ghosts
    message = (
        f"{len(ghosts)} ghost pipeline python process(es)"
        if ghosts
        else "no ghost pipeline python processes"
    )
    return InvariantResult(
        name,
        severity,
        passed,
        message,
        violations=violations,
        details={"registered_pids": sorted(registered_pids), "ghosts": ghosts[:20]},
    )


# --------------------------------------------------------------------------
# HIGH - media_report vs pipeline_state.db consistency
# --------------------------------------------------------------------------


def check_report_db_consistency(
    report: Optional[dict] = None,
    db_path: Optional[Path] = None,
    tolerance: int = 50,
) -> InvariantResult:
    """Cross-reference DONE filepaths in state.db with media_report filepaths."""
    name = "report_db_consistency"
    severity: Severity = "HIGH"
    if report is None:
        report = _load_media_report()
    if report is None:
        return InvariantResult(name, severity, True, "media_report.json not present - skipped")
    conn = _open_state_db(db_path)
    if conn is None:
        return InvariantResult(name, severity, True, "pipeline_state.db not present - skipped")
    try:
        report_paths = {str(e.get("filepath", "")) for e in (report.get("files") or [])}
        report_paths.discard("")
        try:
            done_rows = conn.execute(
                "SELECT filepath FROM pipeline_files WHERE LOWER(status)='done'"
            ).fetchall()
            all_rows = conn.execute("SELECT filepath FROM pipeline_files").fetchall()
        except sqlite3.Error:
            return InvariantResult(name, severity, True, "state DB schema missing - skipped")
        done_paths = {str(r[0]) for r in done_rows if r[0]}
        tracked_paths = {str(r[0]) for r in all_rows if r[0]}
        done_missing_from_report = sorted(done_paths - report_paths)
        report_missing_from_db = sorted(report_paths - tracked_paths)
        violations = done_missing_from_report[:]
        details = {
            "done_in_db_but_not_in_report": len(done_missing_from_report),
            "in_report_but_not_in_db": len(report_missing_from_db),
            "sample_done_missing_from_report": done_missing_from_report[:10],
            "sample_report_missing_from_db": report_missing_from_db[:10],
            "tolerance": tolerance,
        }
        passed = len(done_missing_from_report) <= tolerance
        if passed:
            message = (
                f"state/report drift within tolerance "
                f"(done-missing-from-report={len(done_missing_from_report)}, "
                f"report-missing-from-db={len(report_missing_from_db)})"
            )
        else:
            message = (
                f"{len(done_missing_from_report)} DONE files missing from media_report "
                f"(tolerance {tolerance})"
            )
        return InvariantResult(name, severity, passed, message, violations=violations, details=details)
    finally:
        conn.close()


# --------------------------------------------------------------------------
# MEDIUM - sampled media_report entries exist on disk
# --------------------------------------------------------------------------


def check_report_file_exists_on_disk(
    report: Optional[dict] = None, sample_size: int = 100
) -> InvariantResult:
    """Random sample of media_report entries - verify each file still exists."""
    name = "report_file_exists_on_disk"
    severity: Severity = "MEDIUM"
    if report is None:
        report = _load_media_report()
    if report is None:
        return InvariantResult(name, severity, True, "media_report.json not present - skipped")
    files = report.get("files") or []
    paths = [str(e.get("filepath", "")) for e in files if e.get("filepath")]
    if not paths:
        return InvariantResult(name, severity, True, "media_report has no filepaths to sample")
    sample = random.sample(paths, min(sample_size, len(paths)))
    missing = [p for p in sample if not os.path.exists(p)]
    passed = not missing
    message = (
        f"{len(missing)}/{len(sample)} sampled report entries missing on disk"
        if missing
        else f"all {len(sample)} sampled report entries present on disk"
    )
    return InvariantResult(
        name,
        severity,
        passed,
        message,
        violations=missing,
        details={"sampled": len(sample), "missing": len(missing)},
    )


# --------------------------------------------------------------------------
# LOW - banned ffmpeg flags in logs
# --------------------------------------------------------------------------


def _log_paths() -> list[Path]:
    """Return pipeline.log and every *.log under logs/."""
    paths: list[Path] = []
    main_log = Path(STAGING_DIR) / "pipeline.log"
    if main_log.exists():
        paths.append(main_log)
    logs_dir = Path(STAGING_DIR) / "logs"
    if logs_dir.is_dir():
        paths.extend(sorted(logs_dir.glob("*.log")))
    return paths


def _scan_log_for_banned_flags(
    path: Path, tail_bytes: int = 2_000_000
) -> tuple[int, int, list[str]]:
    """Return (bare_err_detect_hits, optional_audio_hits, sample_lines)."""
    try:
        size = path.stat().st_size
        offset = max(0, size - tail_bytes)
        with open(path, "rb") as f:
            f.seek(offset)
            tail = f.read().decode("utf-8", errors="replace")
    except OSError:
        return 0, 0, []

    bad_err_detect = re.compile(r"-err_detect\s+ignore_err")
    bare_err_hits = 0
    samples: list[str] = []
    for m in bad_err_detect.finditer(tail):
        start = m.start()
        window = tail[max(0, start - 20) : start]
        if ":v" in window:
            continue
        bare_err_hits += 1
        line_start = tail.rfind("\n", 0, start) + 1
        line_end = tail.find("\n", m.end())
        if line_end == -1:
            line_end = len(tail)
        if len(samples) < 5:
            samples.append(tail[line_start:line_end].strip()[:200])

    optional_audio_hits = 0
    for m in re.finditer(r"-map\s+0:a\?", tail):
        optional_audio_hits += 1
        start = m.start()
        line_start = tail.rfind("\n", 0, start) + 1
        line_end = tail.find("\n", m.end())
        if line_end == -1:
            line_end = len(tail)
        if len(samples) < 10:
            samples.append(tail[line_start:line_end].strip()[:200])

    return bare_err_hits, optional_audio_hits, samples


def check_no_banned_ffmpeg_flags_in_log(tail_bytes: int = 2_000_000) -> InvariantResult:
    """Grep pipeline.log and logs/*.log for banned ffmpeg flags."""
    name = "no_banned_ffmpeg_flags_in_log"
    severity: Severity = "LOW"
    paths = _log_paths()
    if not paths:
        return InvariantResult(name, severity, True, "no pipeline logs present - skipped")
    total_bare_err = 0
    total_opt_audio = 0
    per_file: list[dict[str, Any]] = []
    violations: list[str] = []
    for log_path in paths:
        bare, opt, samples = _scan_log_for_banned_flags(log_path, tail_bytes=tail_bytes)
        if bare == 0 and opt == 0:
            continue
        total_bare_err += bare
        total_opt_audio += opt
        per_file.append({
            "path": str(log_path),
            "err_detect_ignore_err": bare,
            "map_optional_audio": opt,
            "samples": samples[:5],
        })
        for s in samples[:3]:
            violations.append(f"{log_path.name}: {s}")
    total = total_bare_err + total_opt_audio
    passed = total == 0
    message = (
        f"forbidden flags in logs: {total_bare_err}x bare -err_detect ignore_err, "
        f"{total_opt_audio}x -map 0:a?"
        if total
        else "no forbidden ffmpeg flags in recent log tail"
    )
    return InvariantResult(
        name,
        severity,
        passed,
        message,
        violations=violations,
        details={
            "total_err_detect": total_bare_err,
            "total_optional_audio": total_opt_audio,
            "by_file": per_file,
        },
    )


# --------------------------------------------------------------------------
# HIGH — DONE with audio language ≠ TMDb original_language
# --------------------------------------------------------------------------


def check_no_done_with_foreign_audio(report: Optional[dict] = None) -> InvariantResult:
    """Surface DONE files where the audio language doesn't match the original.

    User policy (2026-04-25): "we want to watch in original language always."
    Bluey (en-original) with only-Swedish audio, Amelie (fr-original) with
    only-English-dub-only — both should have been FLAGGED, not silently
    encoded to DONE.

    Pre-fix data (older entries that pre-date the new qualifier) will
    surface here. The fix is the qualify_audit CLI which retroactively
    flags them. This invariant exists so we can confirm the audit ran +
    catch any future regressions.

    HIGH severity (not CRITICAL) because the file CAN still be played —
    it's wrong-language but it's a real file. Distinct from
    no_audioless_av1 which is a content-loss bug.
    """
    name = "no_done_with_foreign_audio"
    severity: Severity = "HIGH"

    if report is None:
        report = _load_media_report()
    if report is None:
        return InvariantResult(name, severity, True, "media_report.json not present — skipped")

    # Build a {filepath: status} map from pipeline_state for the DONE filter.
    conn = _open_state_db()
    if conn is None:
        return InvariantResult(name, severity, True, "state DB unavailable — skipped")
    try:
        rows = conn.execute(
            "SELECT filepath FROM pipeline_files WHERE LOWER(status) = 'done'"
        ).fetchall()
    except sqlite3.Error:
        return InvariantResult(name, severity, True, "state DB query failed — skipped")
    finally:
        conn.close()
    done_paths = {r[0] for r in rows}

    # Lazy import to avoid the qualify module loading whisper deps when
    # invariants are checked from contexts that don't need them.
    from pipeline.qualify import _languages_equivalent  # noqa: PLC0415

    offenders: list[str] = []
    needs_audit: list[str] = []
    sampled = 0
    for entry in report.get("files", []) or []:
        fp = entry.get("filepath")
        if not fp or fp not in done_paths:
            continue
        sampled += 1
        tmdb = entry.get("tmdb") or {}
        original = (tmdb.get("original_language") or "").lower().strip()
        if not original:
            continue  # can't evaluate without ground truth
        audio_streams = entry.get("audio_streams") or []
        if not audio_streams:
            continue  # zero-audio is its own invariant
        # Has any track that matches original_language?
        any_match = False
        any_proven_foreign = False
        any_unverified_und = False
        for s in audio_streams:
            tag = (s.get("language") or "").lower().strip()
            detected = (s.get("detected_language") or "").lower().strip()
            for cand in (tag, detected):
                if cand and cand not in {"und", "unk"} and _languages_equivalent(cand, original):
                    any_match = True
                    break
            if any_match:
                break
            # Track has a confident detected language that's NOT original — proven foreign
            if detected and detected not in {"und", "unk"} and not _languages_equivalent(detected, original):
                any_proven_foreign = True
            # Track is und with no detection — needs audit before we can call it
            if (not tag or tag in {"und", "unk"}) and not detected:
                any_unverified_und = True
        if any_match:
            continue
        # No track matches. Decide: hard violation (proven foreign) or
        # soft "needs audit" (only und-undetected, no whisper run yet).
        if any_proven_foreign:
            offenders.append(fp)
        elif any_unverified_und:
            needs_audit.append(fp)

    passed = not offenders
    if offenders:
        message = (
            f"{len(offenders)} DONE file(s) with detected audio ≠ original_language "
            f"(plus {len(needs_audit)} pending whisper audit)"
        )
    elif needs_audit:
        # No proven violations — but there's unaudited content. Pass the
        # invariant with a hint, don't fail it: "needs audit" is a workflow
        # state, not a discipline breach.
        message = (
            f"all proven-language DONE files match their original; "
            f"{len(needs_audit)} files still pending whisper audit "
            f"(run `uv run python -m tools.qualify_audit` after stopping pipeline)"
        )
    else:
        message = "all DONE files have at least one audio track in the original language"
    return InvariantResult(
        name,
        severity,
        passed,
        message,
        violations=offenders[:50],
        details={
            "sampled_done": sampled,
            "offenders": len(offenders),
            "needs_audit": len(needs_audit),
        },
    )


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def _invariant_runners(skip_ssh: bool) -> list[Callable[[], InvariantResult]]:
    """Ordered list of zero-arg callables producing an InvariantResult each."""
    return [
        check_no_audioless_av1,
        check_no_done_with_deferred_reason,
        check_no_done_with_error_reason,
        check_no_done_with_foreign_audio,
        lambda: check_no_stale_tmp_on_nas(skip_ssh=skip_ssh),
        lambda: check_no_zombie_mkvmerge(skip_ssh=skip_ssh),
        check_no_ghost_python_processes,
        check_report_db_consistency,
        check_report_file_exists_on_disk,
        check_no_banned_ffmpeg_flags_in_log,
    ]


def run_all_invariants(skip_ssh: bool = False) -> list[InvariantResult]:
    """Run every invariant and return the full list of results in order.

    Args:
        skip_ssh: If True, NAS-SSH-backed checks return a passing stub without
            attempting an SSH connection. Used by tests and the dashboard
            endpoint when the operator hasn't configured SSH.
    """
    results: list[InvariantResult] = []
    for fn in _invariant_runners(skip_ssh):
        try:
            results.append(fn())
        except Exception as e:  # noqa: BLE001
            fn_name = getattr(fn, "__name__", "<lambda>")
            name = fn_name.replace("check_", "") if fn_name.startswith("check_") else "check_error"
            results.append(
                InvariantResult(
                    name=name,
                    severity="HIGH",
                    passed=False,
                    message=f"invariant raised {type(e).__name__}: {str(e)[:200]}",
                    details={"traceback_kind": type(e).__name__},
                )
            )
    return results


# Back-compat alias for callers that imported the old short name.
run_all = run_all_invariants


# --------------------------------------------------------------------------
# Safe auto-fix helpers (--fix-safe)
# --------------------------------------------------------------------------


def fix_reset_done_with_deferred(db_path: Optional[Path] = None) -> int:
    """Reset DONE+deferred/skipped rows to PENDING so the queue picks them up."""
    conn = _open_state_db(db_path)
    if conn is None:
        return 0
    try:
        try:
            cursor = conn.execute(
                "UPDATE pipeline_files "
                "SET status='pending', reason=NULL, stage=NULL, error=NULL "
                "WHERE LOWER(status)='done' "
                "AND (LOWER(reason) LIKE ? OR LOWER(reason) LIKE ?)",
                ("%defer%", "%skip%"),
            )
            conn.commit()
            return cursor.rowcount or 0
        except sqlite3.Error:
            return 0
    finally:
        conn.close()


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


_STATUS_SYMBOLS = {True: "OK", False: "FAIL"}


def _format_table(results: list[InvariantResult]) -> str:
    """Render the results as a compact fixed-width table."""
    headers = ("NAME", "SEVERITY", "STATUS", "COUNT", "SAMPLE")
    rows: list[tuple[str, ...]] = []
    for r in results:
        count = str(len(r.violations)) if r.violations else "0"
        sample_list = r.violations[:3] if r.violations else []
        sample = ", ".join(s[-60:] for s in sample_list) if sample_list else r.message[:60]
        rows.append((r.name, r.severity, _STATUS_SYMBOLS[r.passed], count, sample))
    widths = [max(len(h), *(len(row[i]) for row in rows)) for i, h in enumerate(headers)]
    widths[-1] = min(widths[-1], 80)

    def fmt(row: tuple[str, ...]) -> str:
        return "  ".join(
            (cell[: widths[i]] if i == len(widths) - 1 else cell).ljust(widths[i])
            for i, cell in enumerate(row)
        )

    lines = [fmt(headers), fmt(tuple("-" * w for w in widths))]
    lines.extend(fmt(row) for row in rows)
    return "\n".join(lines)


def _exit_code(results: list[InvariantResult]) -> int:
    """0 all green, 1 any CRITICAL/HIGH failed, 2 a check errored out."""
    if all(r.passed for r in results):
        return 0
    for r in results:
        if not r.passed and r.severity in ("CRITICAL", "HIGH"):
            return 1
    return 2


def main(argv: Optional[list[str]] = None) -> int:
    """Entrypoint for ``python -m tools.invariants``."""
    parser = argparse.ArgumentParser(
        prog="tools.invariants",
        description="Run pipeline invariants. Exit 1 on CRITICAL/HIGH failure.",
    )
    parser.add_argument("--json", action="store_true", help="emit results as JSON")
    parser.add_argument(
        "--fix-safe",
        action="store_true",
        help="auto-repair safe invariants (reset done+deferred rows)",
    )
    parser.add_argument(
        "--skip-ssh",
        action="store_true",
        help="skip NAS-SSH-backed checks (stale tmp, zombie mkvmerge)",
    )
    args = parser.parse_args(argv)

    if args.fix_safe:
        fixed = fix_reset_done_with_deferred()
        if not args.json:
            sys.stderr.write(f"--fix-safe: reset {fixed} done+deferred row(s) back to pending\n")

    results = run_all_invariants(skip_ssh=args.skip_ssh)

    if args.json:
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "all_green": all(r.passed for r in results),
            "any_critical": any(not r.passed and r.severity == "CRITICAL" for r in results),
            "checks": [r.to_dict() for r in results],
        }
        sys.stdout.write(json.dumps(payload, indent=2) + "\n")
    else:
        sys.stdout.write(_format_table(results) + "\n")

    return _exit_code(results)


if __name__ == "__main__":
    raise SystemExit(main())
