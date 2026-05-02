"""Scan AV1 library files for the 'Social Network 2010' corruption class.

The 2026-05-02 finding: a 5 GB AV1 file in the library was unplayable due
to a mix of Matroska container damage and AV1 stream damage. mkvmerge
--identify and ffprobe header-only both said the file was fine; only
``ffmpeg -t 10 -f null -`` with ``-v error`` caught it (in ~330 ms).

Signatures we look for in the first 10 s of a file:

  Container-level (Matroska):
    "Element at X ending at Y exceeds containing master element"
    "Length N indicated by an EBML number's first byte ... exceeds max length"
    "Unknown-sized element ... inside parent with finite size"
    "EBML header parsing failed"

  AV1 stream-level (libdav1d):
    "obu_forbidden_bit out of range"
    "Failed to parse temporal unit"
    "Unknown OBU type"
    "Overrun in OBU bit buffer"
    "Error parsing OBU data"
    "Decoding error: Invalid data found when processing input"
    "Error submitting packet to decoder"

A file matching one or more of these in 10 s is almost certainly damaged
and should be re-encoded from source (or deleted if no source).

Usage:

    # Scan every AV1 file in media_report.json
    uv run python -m tools.scan_corrupt_av1

    # Faster — limit to first N files (smoke test)
    uv run python -m tools.scan_corrupt_av1 --limit 50

    # More workers (default 4) for parallel SMB reads
    uv run python -m tools.scan_corrupt_av1 --workers 6

    # Write report to JSON for follow-up
    uv run python -m tools.scan_corrupt_av1 --output corrupt_av1.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")


# Substring matches inside the lower-cased ffmpeg stderr output.
_CONTAINER_SIGS = (
    "exceeds containing master element",
    "exceeds max length",
    "unknown-sized element",
    "inside parent with finite size",
    "ebml header parsing failed",
    "invalid ebml",
)
_STREAM_SIGS = (
    "obu_forbidden_bit out of range",
    "failed to parse temporal unit",
    "unknown obu type",
    "overrun in obu bit buffer",
    "error parsing obu data",
    "invalid data found when processing input",
    "error submitting packet to decoder",
)
_ALL_SIGS = _CONTAINER_SIGS + _STREAM_SIGS


def _classify(stderr: str) -> tuple[bool, list[str]]:
    """Return (is_corrupt, hit_signatures). Empty stderr = clean."""
    if not stderr:
        return False, []
    lo = stderr.lower()
    hits = [sig for sig in _ALL_SIGS if sig in lo]
    return (len(hits) > 0, hits)


def scan_one(filepath: str, sample_secs: int = 10, timeout: int = 60) -> dict:
    """Run a fast ffmpeg null-output scan of the first ``sample_secs`` seconds.

    Returns a dict: ``{filepath, ok, hits, elapsed_ms, error}``.
    """
    if not os.path.exists(filepath):
        return {"filepath": filepath, "ok": False, "missing": True, "hits": [], "elapsed_ms": 0}

    cmd = [
        "ffmpeg", "-v", "error", "-hide_banner",
        "-i", filepath,
        "-t", str(sample_secs),
        "-f", "null", "-",
    ]
    t0 = time.monotonic()
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
            encoding="utf-8", errors="replace",
        )
        stderr = result.stderr or ""
    except subprocess.TimeoutExpired:
        return {
            "filepath": filepath, "ok": False, "hits": ["scan_timeout"],
            "elapsed_ms": int((time.monotonic() - t0) * 1000),
        }
    except Exception as e:
        return {
            "filepath": filepath, "ok": False, "hits": [f"scan_failed: {type(e).__name__}"],
            "elapsed_ms": int((time.monotonic() - t0) * 1000),
        }

    is_corrupt, hits = _classify(stderr)
    return {
        "filepath": filepath,
        "ok": not is_corrupt,
        "hits": hits,
        "elapsed_ms": int((time.monotonic() - t0) * 1000),
        # Include a small stderr excerpt for the corrupt cases (truncated)
        "stderr_excerpt": stderr[:400] if is_corrupt else "",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan AV1 library for corruption signatures.")
    parser.add_argument("--report", default=None, help="Path to media_report.json (default: paths.MEDIA_REPORT)")
    parser.add_argument("--limit", type=int, default=0, help="Stop after N files (0 = all)")
    parser.add_argument("--workers", type=int, default=4, help="Parallel SMB readers (default 4)")
    parser.add_argument("--sample-secs", type=int, default=10, help="Seconds of media to decode (default 10)")
    parser.add_argument("--timeout", type=int, default=60, help="Per-file timeout in seconds (default 60)")
    parser.add_argument("--output", default=None, help="Write full results JSON here")
    args = parser.parse_args()

    if args.report is None:
        from paths import MEDIA_REPORT
        report_path = str(MEDIA_REPORT)
    else:
        report_path = args.report

    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)

    files = report.get("files", [])
    av1_files = [
        f["filepath"] for f in files
        if (f.get("video") or {}).get("codec_raw") == "av1"
    ]
    if args.limit:
        av1_files = av1_files[: args.limit]

    if not av1_files:
        logging.info("No AV1 files in the report.")
        return 0

    logging.info(f"Scanning {len(av1_files)} AV1 file(s) with {args.workers} workers...")
    t_start = time.monotonic()
    results: list[dict] = []
    corrupt: list[dict] = []
    missing: list[str] = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(scan_one, fp, args.sample_secs, args.timeout): fp
            for fp in av1_files
        }
        for i, fut in enumerate(as_completed(futures), 1):
            r = fut.result()
            results.append(r)
            if r.get("missing"):
                missing.append(r["filepath"])
            elif not r["ok"]:
                corrupt.append(r)
                logging.warning(f"  CORRUPT: {os.path.basename(r['filepath'])}: {', '.join(r['hits'][:3])}")
            if i % 50 == 0 or i == len(av1_files):
                elapsed = time.monotonic() - t_start
                rate = i / max(elapsed, 0.001) * 60
                logging.info(
                    f"  Progress: {i}/{len(av1_files)}  ({rate:.0f} files/min, "
                    f"{len(corrupt)} corrupt so far)"
                )

    elapsed = time.monotonic() - t_start
    logging.info("")
    logging.info(f"Scan complete in {elapsed:.0f}s")
    logging.info(f"  total scanned   : {len(results)}")
    logging.info(f"  clean           : {len(results) - len(corrupt) - len(missing)}")
    logging.info(f"  CORRUPT         : {len(corrupt)}")
    logging.info(f"  missing on disk : {len(missing)}")

    if corrupt:
        logging.info("")
        logging.info("=== CORRUPT FILES ===")
        for r in corrupt:
            logging.info(f"  {r['filepath']}")
            logging.info(f"    signatures: {', '.join(r['hits'][:5])}")

    if args.output:
        out_path = Path(args.output)
        out_path.write_text(
            json.dumps(
                {
                    "scanned": len(results),
                    "corrupt_count": len(corrupt),
                    "missing_count": len(missing),
                    "elapsed_secs": int(elapsed),
                    "corrupt": corrupt,
                    "missing": missing,
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        logging.info(f"Full results: {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
