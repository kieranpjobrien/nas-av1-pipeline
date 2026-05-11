"""Probe source MKV files for bitstream corruption — catches the Ford v
Ferrari class BEFORE we waste 60+ min of GPU on a doomed encode.

The Ford v Ferrari pattern (2026-05-12):
  * EBML container is broken at a specific byte offset in the file.
  * ffmpeg's matroska demuxer chokes: "0x00 at pos X invalid as first
    byte of an EBML number".
  * The HEVC decoder cascades: "Could not find ref with POC N" / "Error
    constructing the frame RPS" because frames are missing past the
    break.
  * NVENC encode dies at ~13% of the file. Output fails integrity check.
  * The circuit breaker eventually flags the file, but only after 3+
    wasted cycles (~90 min each).

This tool finds those files PROACTIVELY by probing the first, middle,
and last 60 seconds of each source via ffmpeg's null muxer. If any of
those three windows reports a real decode error (not just an "invalid
data" warning on a non-keyframe seek), the file is flagged.

Usage:
    uv run python -m tools.probe_source_integrity <path>           # one file
    uv run python -m tools.probe_source_integrity --from-state     # all
        pending+error rows in pipeline_state.db
    uv run python -m tools.probe_source_integrity --from-report    # all
        files in media_report.json (broader sweep)
    uv run python -m tools.probe_source_integrity ... --json       # JSON output
    uv run python -m tools.probe_source_integrity ... --apply      # mark
        broken files as ``flagged_corrupt`` in state DB (terminal —
        user has to re-acquire source before retry)

The check is FAST: ~5–10 s per file (three 60-second decode windows
with no output, plus container probe). A full 5,400-file library is
about 8 hours but it's a one-shot — repeat on demand when something
weird shows up.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

from paths import MEDIA_REPORT, PIPELINE_STATE_DB

FFPROBE = "ffprobe"
FFMPEG = "ffmpeg"

# Decode-error signatures that indicate genuine bitstream corruption,
# not the soft warnings ffmpeg emits on seek to a non-keyframe.
HARD_ERROR_PATTERNS = (
    re.compile(r"invalid (?:as first byte of an EBML number|data found when processing input)", re.I),
    re.compile(r"error submitting packet to decoder", re.I),
    re.compile(r"could not find ref with POC", re.I),
    re.compile(r"error constructing the frame RPS", re.I),
    re.compile(r"non-existing PPS \d+ referenced", re.I),
    re.compile(r"missing picture in access unit", re.I),
)


@dataclass
class ProbeResult:
    """Outcome of probing one file."""

    filepath: str
    duration_seconds: float = 0.0
    healthy: bool = True
    windows_failed: list[str] = field(default_factory=list)
    sample_errors: list[str] = field(default_factory=list)
    probe_time_secs: float = 0.0
    fatal: Optional[str] = None  # set if the probe itself failed to run

    def to_dict(self) -> dict:
        return asdict(self)


def _probe_duration(filepath: str, timeout: int = 30) -> float:
    """Return the duration in seconds, or 0.0 if probe fails."""
    cmd = [FFPROBE, "-v", "error", "-show_entries", "format=duration",
           "-of", "default=nw=1:nk=1", filepath]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 0.0
    if out.returncode != 0:
        return 0.0
    try:
        return float(out.stdout.strip() or "0")
    except ValueError:
        return 0.0


def _decode_window(filepath: str, start_secs: float, length_secs: float = 60,
                   timeout: int = 180, max_retries: int = 1) -> tuple[bool, list[str]]:
    """Decode ``length_secs`` of video starting at ``start_secs`` via
    ffmpeg's null muxer (no output). Return (ok, error_lines_sample).

    ``ok`` is True if no HARD_ERROR_PATTERN appeared in stderr; soft
    warnings ("Application provided invalid, non monotonically
    increasing dts") are tolerated.

    Timeouts are retried up to ``max_retries`` times before being
    reported as failures — flaky SMB reads can cause a 60s decode
    to overrun a 180s wall clock, and that's NOT bitstream corruption.
    Up (2009) (2026-05-12) tripped the original no-retry version on a
    transient timeout and got falsely flagged_corrupt. A retry on the
    SAME window distinguishes real corruption (deterministic; both
    attempts hit hard-error signatures) from network blips (first
    attempt times out, second succeeds).
    """
    cmd = [FFMPEG, "-v", "error", "-nostdin",
           "-ss", str(start_secs), "-i", filepath,
           "-t", str(length_secs),
           "-f", "null", "-"]
    attempts = max_retries + 1
    timeout_msgs: list[str] = []
    for attempt in range(1, attempts + 1):
        try:
            out = subprocess.run(cmd, capture_output=True, text=True,
                                 timeout=timeout, encoding="utf-8", errors="replace")
        except subprocess.TimeoutExpired:
            timeout_msgs.append(
                f"timeout after {timeout}s at ss={start_secs:.0f}s (attempt {attempt}/{attempts})"
            )
            if attempt < attempts:
                continue  # retry — could be flaky SMB
            return False, timeout_msgs
        # Real ffmpeg exit — examine stderr regardless of return code
        stderr = out.stderr or ""
        hits: list[str] = []
        for line in stderr.splitlines():
            if any(p.search(line) for p in HARD_ERROR_PATTERNS):
                hits.append(line.strip()[:200])
                if len(hits) >= 6:
                    break
        return (not hits), hits
    # Unreachable in practice but keeps type checker happy.
    return False, timeout_msgs


def probe_file(filepath: str) -> ProbeResult:
    """Probe one source file at start / middle / end. Return the result."""
    t0 = time.monotonic()
    if not Path(filepath).exists():
        return ProbeResult(filepath=filepath, healthy=False, fatal="file missing")
    duration = _probe_duration(filepath)
    if duration <= 0:
        return ProbeResult(filepath=filepath, healthy=False,
                           fatal="duration probe failed (corrupt container?)")
    result = ProbeResult(filepath=filepath, duration_seconds=duration)
    # Three windows: start, middle, end. For files < 3 min we just probe
    # the whole thing.
    if duration < 180:
        windows = [("full", 0.0, duration)]
    else:
        windows = [
            ("start",  0.0,             60.0),
            ("middle", duration / 2,    60.0),
            ("end",    max(0, duration - 65),  60.0),
        ]
    for tag, start, length in windows:
        ok, errs = _decode_window(filepath, start, length)
        if not ok:
            result.healthy = False
            result.windows_failed.append(tag)
            result.sample_errors.extend(errs[:2])  # keep payload small
    result.probe_time_secs = time.monotonic() - t0
    return result


def _files_from_state(only_pending_error: bool = True) -> list[str]:
    """Return filepaths from pipeline_state.db. Default: pending + error rows."""
    if not Path(PIPELINE_STATE_DB).exists():
        return []
    con = sqlite3.connect(str(PIPELINE_STATE_DB))
    if only_pending_error:
        rows = con.execute(
            "SELECT filepath FROM pipeline_files "
            "WHERE LOWER(status) IN ('pending', 'error')"
        ).fetchall()
    else:
        rows = con.execute("SELECT filepath FROM pipeline_files").fetchall()
    con.close()
    return [r[0] for r in rows]


def _files_from_report() -> list[str]:
    """Return filepaths from media_report.json.files[]. Source of truth
    for the broader 'all sources' sweep."""
    if not Path(MEDIA_REPORT).exists():
        return []
    with open(MEDIA_REPORT, "r", encoding="utf-8") as f:
        rep = json.load(f)
    return [f.get("filepath") for f in rep.get("files", []) if f.get("filepath")]


def _flag_broken_in_state(filepath: str, result: ProbeResult) -> None:
    """Mark a broken source as ``flagged_corrupt`` in state DB so the
    queue builder skips it. The user has to re-acquire (Radarr/Sonarr)
    and force_flagged=true the requeue endpoint to retry."""
    from pipeline import state as st
    ps = st.PipelineState(str(PIPELINE_STATE_DB))
    reason = (
        f"source corruption detected at windows={','.join(result.windows_failed)}: "
        f"{result.sample_errors[0][:120] if result.sample_errors else 'no signature'}"
    )
    ps.set_file(
        filepath,
        status=st.FileStatus.FLAGGED_CORRUPT,
        reason=reason,
        force_reencode=False,
        source_corrupt=True,
        source_probe_at=time.time(),
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.probe_source_integrity",
        description="Probe MKV sources for bitstream corruption (Ford v Ferrari class).",
    )
    src_group = parser.add_mutually_exclusive_group(required=True)
    src_group.add_argument("path", nargs="?", help="probe a single file")
    src_group.add_argument("--from-state", action="store_true",
                           help="probe all pending+error rows in pipeline_state.db")
    src_group.add_argument("--from-report", action="store_true",
                           help="probe all files in media_report.json (slow sweep)")
    parser.add_argument("--json", action="store_true",
                        help="emit each result as a JSONL line on stdout, "
                             "final summary at end. Progress on stderr regardless.")
    parser.add_argument("--apply", action="store_true",
                        help="mark broken files as flagged_corrupt in state DB")
    parser.add_argument("--limit", type=int, default=0,
                        help="cap the number of files probed (0 = no cap)")
    args = parser.parse_args(argv)

    if args.path:
        targets = [args.path]
    elif args.from_state:
        targets = _files_from_state(only_pending_error=True)
    else:
        targets = _files_from_report()

    if args.limit > 0:
        targets = targets[: args.limit]

    if not targets:
        sys.stderr.write("no targets to probe\n")
        return 0

    results: list[ProbeResult] = []
    broken: list[ProbeResult] = []
    sys.stderr.write(f"probing {len(targets)} sources (apply={args.apply})\n")
    sys.stderr.flush()
    for i, fp in enumerate(targets, 1):
        sys.stderr.write(f"[{i}/{len(targets)}] {Path(fp).name} ... ")
        sys.stderr.flush()
        r = probe_file(fp)
        results.append(r)
        tag = "OK" if r.healthy else ("BROKEN " + (r.fatal or ",".join(r.windows_failed)))
        sys.stderr.write(
            f"{tag}  dur={r.duration_seconds:.0f}s  took={r.probe_time_secs:.1f}s\n"
        )
        sys.stderr.flush()
        if not r.healthy:
            broken.append(r)
            if args.apply and r.fatal is None:
                try:
                    _flag_broken_in_state(fp, r)
                    sys.stderr.write(f"  -> marked flagged_corrupt in state DB\n")
                    sys.stderr.flush()
                except Exception as e:  # noqa: BLE001
                    sys.stderr.write(f"  -> apply failed: {e}\n")
                    sys.stderr.flush()
        # In --json mode, emit JSONL so callers can tail the stream.
        # The final summary still appears at the end.
        if args.json:
            sys.stdout.write(json.dumps(r.to_dict()) + "\n")
            sys.stdout.flush()

    if args.json:
        # Trailing summary line as JSONL so the final state is greppable.
        sys.stdout.write(json.dumps({
            "summary": True,
            "probed": len(results),
            "broken": len(broken),
        }) + "\n")
        sys.stdout.flush()

    sys.stderr.write(f"\n=== {len(broken)}/{len(results)} sources are corrupt ===\n")
    for r in broken:
        sys.stderr.write(
            f"  BROKEN  duration={r.duration_seconds:.0f}s  "
            f"windows={','.join(r.windows_failed) or 'fatal'}  "
            f"{Path(r.filepath).name}\n"
        )
        if r.fatal:
            sys.stderr.write(f"    fatal: {r.fatal}\n")
        for err in r.sample_errors[:2]:
            sys.stderr.write(f"    {err}\n")
    sys.stderr.flush()

    # Exit 1 if any broken files were found (so CI / scripts can react).
    return 1 if broken else 0


if __name__ == "__main__":
    raise SystemExit(main())
