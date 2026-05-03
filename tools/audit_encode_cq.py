"""Audit AV1 files against the current grade-aware CQ matrix.

For every AV1 file in the library, read its stamped CQ from the MKV
global tags (written by the encoder post-replace as of 2026-05-03) or
the state DB, then compare to what the current
``pipeline.content_grade.target_cq`` rule says it should be.

Three buckets:

  * **optimal**       — stamped CQ matches the rule's target (within
                        the configured tolerance)
  * **too_low**       — stamped CQ < target. The file was encoded
                        gentler than the new rule wants. Re-encoding
                        from the existing AV1 will save space (and
                        AV1→AV1 generation loss at the new harsher CQ
                        is negligible for the typical sitcom case).
  * **too_high**      — stamped CQ > target. The file is rougher than
                        the rule wants. We can't recover detail by
                        re-encoding the AV1; the user has to decide
                        whether to re-download from source.
  * **unknown**       — no stamp, no state DB row. Always re-queue
                        with the grade-aware target.

Reading CQ:
  1. ``mkvmerge --identify --identification-format json`` → look at
     ``container.properties.tags`` for SimpleTag with name "CQ"
  2. Fall back to ``pipeline_state.db`` ``encode_params_used.cq`` if
     no tag

Writes JSON output: ``F:/AV1_Staging/audit_cq.json`` by default.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")


# CQ tolerance: a difference of 0 is exact match, 1 is rounding, etc.
# Default 0 means strict — every encode MUST hit the rule exactly. Tunable
# from the CLI for users who want a wider acceptance band.
DEFAULT_TOLERANCE = 0


def _read_mkv_cq_tag(filepath: str, mkvmerge: str | None = None) -> int | None:
    """Return the integer CQ from the MKV global tags, or None if absent.

    Important: ``mkvmerge --identify`` reports only a *count* of global
    tags (``global_tags[].num_entries``) and does not surface the
    contents. To get the actual tag names/values we use mkvextract.
    Earlier versions of this function read ``container.properties.tags``
    which is always None for global tags — every call silently returned
    None and the audit fell through to state_db / bitrate inference for
    100% of files. After 2026-05-03 mkvextract is the source of truth.
    """
    from pipeline.grade_review import _read_global_tags

    for t in _read_global_tags(filepath):
        if t["name"].upper() == "CQ":
            try:
                return int(t["value"])
            except (TypeError, ValueError):
                continue
    return None


def _build_bitrate_to_cq_table(state_db: str, files_by_path: dict) -> dict:
    """Walk pipeline_state for files with a known stamped CQ, group their
    bitrates by (grade, res_key, cq), and return a median table for each
    bucket. Used as a 3rd-tier fallback for files with no MKV tag and no
    state row — instead of writing them off as "unknown" we look up the
    closest-CQ bucket for their (grade, res) and assign that.

    Built once per audit (cheap — single SELECT, no per-file probing).
    Returns {(grade, res_key): {cq: median_bitrate_kbps}}.
    """
    from collections import defaultdict
    from pipeline.content_grade import derive_grade

    raw: dict[tuple, dict[int, list[int]]] = defaultdict(lambda: defaultdict(list))
    try:
        conn = sqlite3.connect(state_db)
        cur = conn.cursor()
        cur.execute("SELECT filepath, extras FROM pipeline_files WHERE status='done' AND extras IS NOT NULL")
        for fp, ex in cur.fetchall():
            try:
                e = json.loads(ex)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            cq = (e.get("encode_params_used") or {}).get("cq")
            if not isinstance(cq, int):
                continue
            f = files_by_path.get(fp)
            if not f:
                continue
            grade = derive_grade(f)
            video = f.get("video") or {}
            res = video.get("resolution_class") or "1080p"
            bitrate = video.get("bitrate_kbps") or f.get("overall_bitrate_kbps") or 0
            if bitrate <= 0:
                continue
            res_key = "4K_HDR" if (res == "4K" and video.get("hdr")) else (
                "4K_SDR" if res == "4K" else res
            )
            raw[(grade, res_key)][cq].append(int(bitrate))
        conn.close()
    except sqlite3.Error:
        return {}

    # Reduce to medians; drop buckets with <3 samples (too noisy to trust).
    table: dict[tuple, dict[int, int]] = {}
    for key, per_cq in raw.items():
        m = {}
        for cq, vals in per_cq.items():
            if len(vals) >= 3:
                vals_sorted = sorted(vals)
                m[cq] = vals_sorted[len(vals_sorted) // 2]
        if m:
            table[key] = m
    return table


def _infer_cq_from_bitrate(bitrate_kbps: int, table: dict, grade: str, res_key: str) -> int | None:
    """Map an observed bitrate to the closest CQ in the (grade, res_key)
    bucket. Returns None if we have no calibration for this bucket.

    Confidence is implicit in how close the bitrate is to the bucket's
    median — we don't surface a numeric confidence (callers treat it as
    "best guess, mark with source='bitrate_inferred'"). The audit JSON
    captures the source so the dashboard can render unknowns vs inferred
    vs stamped distinctly.
    """
    bucket = table.get((grade, res_key)) or table.get((grade, "1080p"))
    if not bucket:
        return None
    # Find the CQ whose median bitrate is closest to ours
    best_cq = None
    best_dist = None
    for cq, med_bitrate in bucket.items():
        dist = abs(bitrate_kbps - med_bitrate)
        if best_dist is None or dist < best_dist:
            best_dist = dist
            best_cq = cq
    return best_cq


def _read_db_cq(state_db: str, filepath: str) -> int | None:
    """Return CQ from pipeline_state.db encode_params_used, if present."""
    try:
        conn = sqlite3.connect(state_db)
        cur = conn.cursor()
        cur.execute(
            "SELECT extras FROM pipeline_files WHERE filepath = ?", (filepath,)
        )
        row = cur.fetchone()
        conn.close()
    except sqlite3.Error:
        return None
    if not row or not row[0]:
        return None
    try:
        extras = json.loads(row[0])
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    ep = extras.get("encode_params_used") or {}
    cq = ep.get("cq")
    if isinstance(cq, int):
        return cq
    return None


def _audit_one(entry: dict, state_db: str, base_cq_lookup, bitrate_table: dict | None = None) -> dict:
    """Audit a single file. Returns {filepath, target_cq, current_cq, source, bucket, grade}."""
    from pipeline.content_grade import target_cq as compute_target
    from pipeline.grade_review import read_grade_review

    fp = entry.get("filepath", "")
    library_type = entry.get("library_type", "movie")
    content_type = "series" if library_type in ("series", "show", "tv", "anime") else "movie"
    res = (entry.get("video") or {}).get("resolution_class") or entry.get("resolution") or "1080p"
    is_hdr = (entry.get("video") or {}).get("hdr") or entry.get("hdr") or False

    if res == "4K" and is_hdr:
        res_key = "4K_HDR"
    elif res == "4K":
        res_key = "4K_SDR"
    elif res in ("1080p", "720p", "480p", "SD"):
        res_key = res
    else:
        res_key = "SD"

    base_cq = base_cq_lookup(content_type, res_key)
    target, grade, offset = compute_target(base_cq, entry)

    # Read current CQ in three tiers (most reliable to least):
    #   1. MKV ENCODER tag (post-2026-05-03 encodes have it)
    #   2. pipeline_state.db encode_params_used (recent encodes pre-stamp)
    #   3. Bitrate inference against the calibration table (any AV1 file with
    #      a known bitrate, no matter how old; ±2 CQ fuzziness is the cost)
    current = _read_mkv_cq_tag(fp)
    source = "tag" if current is not None else None
    if current is None:
        current = _read_db_cq(state_db, fp)
        if current is not None:
            source = "state_db"
    if current is None and bitrate_table:
        bitrate = (entry.get("video") or {}).get("bitrate_kbps") or entry.get("overall_bitrate_kbps") or 0
        if bitrate > 0:
            inferred = _infer_cq_from_bitrate(int(bitrate), bitrate_table, grade, res_key)
            if inferred is not None:
                current = inferred
                source = "bitrate_inferred"
    if current is None:
        source = "unknown"

    if current is None:
        bucket = "unknown"
    elif current == target:
        bucket = "optimal"
    elif current < target:
        bucket = "too_low"  # encoded gentler than the rule wants — re-encode candidate
    else:
        bucket = "too_high"  # encoded harsher than rule wants — manual review

    # Manual override: if the user has stamped GRADE_REVIEW=accepted, force
    # the bucket to optimal regardless of CQ comparison. This is how a user
    # signs off on a too_high file ("I've watched it, it's fine") so the
    # dashboard stops nagging them about it on every audit re-run.
    review = read_grade_review(fp)
    review_status = review.get("status") if review else None
    if review_status == "accepted":
        bucket = "optimal"

    return {
        "filepath": fp,
        "target_cq": target,
        "current_cq": current,
        "source": source,
        "bucket": bucket,
        "grade": grade,
        "offset": offset,
        "res_key": res_key,
        "content_type": content_type,
        "review_status": review_status,
        "review_at": (review or {}).get("reviewed_at") if review else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit AV1 files against the grade-aware CQ matrix.")
    parser.add_argument("--report", default=None, help="media_report.json path (default paths.MEDIA_REPORT)")
    parser.add_argument("--state-db", default=None, help="pipeline_state.db path (default paths.PIPELINE_STATE_DB)")
    parser.add_argument("--workers", type=int, default=4, help="Parallel mkvmerge calls (default 4)")
    parser.add_argument("--limit", type=int, default=0, help="Stop after N AV1 files (0 = all)")
    parser.add_argument("--output", default=None, help="JSON output path (default F:/AV1_Staging/audit_cq.json)")
    args = parser.parse_args()

    if args.report is None:
        from paths import MEDIA_REPORT
        report_path = str(MEDIA_REPORT)
    else:
        report_path = args.report
    if args.state_db is None:
        from paths import PIPELINE_STATE_DB
        state_db = str(PIPELINE_STATE_DB)
    else:
        state_db = args.state_db
    if args.output is None:
        out_path = "F:/AV1_Staging/audit_cq.json"
    else:
        out_path = args.output

    from pipeline.config import build_config
    cfg = build_config()

    def _base_cq(content_type: str, res_key: str) -> int:
        return cfg["cq"].get(content_type, {}).get(res_key, 30)

    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)
    files = report.get("files", [])
    av1 = [f for f in files if (f.get("video") or {}).get("codec_raw") == "av1"]
    if args.limit:
        av1 = av1[: args.limit]

    # Build the bitrate-to-CQ inference table once. This pulls every
    # known-CQ encode from pipeline_state, groups by (grade, res_key, cq),
    # and stores median bitrates. Used as the 3rd-tier fallback for files
    # with no MKV tag and no state row.
    files_by_path = {f["filepath"]: f for f in files}
    bitrate_table = _build_bitrate_to_cq_table(state_db, files_by_path)
    logging.info(
        f"Bitrate calibration table built from {sum(len(v) for v in bitrate_table.values())} "
        f"(grade,res,cq) buckets across {len(bitrate_table)} (grade,res) groups"
    )

    logging.info(f"Auditing {len(av1)} AV1 file(s) with {args.workers} workers...")
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = [pool.submit(_audit_one, e, state_db, _base_cq, bitrate_table) for e in av1]
        for i, fut in enumerate(as_completed(futs), 1):
            results.append(fut.result())
            if i % 100 == 0 or i == len(av1):
                logging.info(f"  Progress: {i}/{len(av1)}")

    # Tally
    from collections import Counter
    buckets = Counter(r["bucket"] for r in results)
    grades = Counter(r["grade"] for r in results)
    sources = Counter(r["source"] for r in results)

    logging.info("")
    logging.info("=== CQ AUDIT SUMMARY ===")
    for k in ("optimal", "too_low", "too_high", "unknown"):
        logging.info(f"  {k:12s} : {buckets[k]}")
    logging.info("")
    logging.info("Grades:")
    for g, n in grades.most_common():
        logging.info(f"  {g:20s} : {n}")
    logging.info("")
    logging.info("CQ source:")
    for s, n in sources.most_common():
        logging.info(f"  {s:12s} : {n}")

    payload = {
        "scanned": len(results),
        "buckets": dict(buckets),
        "grades": dict(grades),
        "sources": dict(sources),
        "results": results,
    }
    Path(out_path).write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    logging.info("")
    logging.info(f"Full results: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
