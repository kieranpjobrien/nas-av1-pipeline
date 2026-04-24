"""One-off ingester — read agent-produced score files into the taste_scores DB.

Reads every ``chunk_*_scored.json`` + ``series_scored.json`` from the staging
directory, validates each entry, and upserts into ``taste_scores``.

Design:
* Uses seed_version from the current taste_seeds.json so future rescores
  (via the CLI or UI) auto-refresh when seeds change.
* Idempotent — re-running overwrites existing rows rather than duplicating.
* Emits a summary: N movies ingested, N series, any dropped entries.

This is a one-off for the initial bulk-score. Going forward, the
``tools.upgrades taste-rescore`` CLI is the supported entry point (uses the
Anthropic API for programmatic rescoring on seed changes).
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import sys
import time

from tools.upgrades import db as updb
from tools.upgrades.taste_scorer import load_seeds, seed_version

MODEL_TAG = "claude-opus-4-7-via-claude-code-agent"
CHUNK_GLOB_MOVIES = "F:/AV1_Staging/_taste_chunks/chunk_*_scored.json"
SERIES_PATH = "F:/AV1_Staging/_taste_chunks/series_scored.json"


logger = logging.getLogger(__name__)


def _normalise_entry(entry: dict, kind: str) -> dict | None:
    """Validate + coerce one entry from an agent output file.

    Returns a dict ready for persist_score, or None if the entry is garbage
    (missing title, non-integer score, out-of-range, etc.). We discard
    garbage rather than crash — agents can and do occasionally produce
    malformed rows.
    """
    title = (entry.get("title") or "").strip()
    if not title:
        return None
    year = entry.get("year")
    if year is not None:
        try:
            year = int(year)
        except (TypeError, ValueError):
            return None
    try:
        score = int(entry.get("score"))
    except (TypeError, ValueError):
        return None
    if not (0 <= score <= 10):
        return None
    rationale = (entry.get("rationale") or "").strip()
    if len(rationale) < 4:
        rationale = f"(rationale missing; score {score})"
    if len(rationale) > 900:
        rationale = rationale[:897] + "..."
    return {
        "title": title,
        "year": year,
        "score": score,
        "rationale": rationale,
        "kind": kind,
    }


def _persist_row(conn, entry: dict, seed_ver: int) -> None:
    """Direct INSERT OR REPLACE — bypasses the scorer's wrapper so we can
    tag the model as 'via claude-code-agent' (not the production API path).
    """
    conn.execute(
        """
        INSERT OR REPLACE INTO taste_scores
            (title, year, library_type, score, rationale, model, seed_version, scored_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            entry["title"],
            entry["year"],
            entry["kind"],  # 'movie' or 'series'
            entry["score"],
            entry["rationale"],
            MODEL_TAG,
            seed_ver,
            time.time(),
        ),
    )


def ingest(paths: list[str], kind: str, conn, seed_ver: int) -> tuple[int, int]:
    """Ingest all entries from the given files. Returns (ok, dropped)."""
    ok = 0
    dropped = 0
    for path in paths:
        try:
            with open(path, encoding="utf-8") as f:
                rows = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.error("couldn't read %s (%s) — skipping", path, e)
            continue
        if not isinstance(rows, list):
            logger.error("%s doesn't contain a JSON array — skipping", path)
            continue
        file_ok = 0
        for r in rows:
            if not isinstance(r, dict):
                dropped += 1
                continue
            norm = _normalise_entry(r, kind)
            if not norm:
                dropped += 1
                continue
            _persist_row(conn, norm, seed_ver)
            ok += 1
            file_ok += 1
        logger.info("%s -> %d/%d ingested", path.split("/")[-1], file_ok, len(rows))
    conn.commit()
    return ok, dropped


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Ingest agent-produced score files into taste_scores.")
    p.add_argument("--movies-glob", default=CHUNK_GLOB_MOVIES)
    p.add_argument("--series-path", default=SERIES_PATH)
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    seeds = load_seeds()
    seed_ver = seed_version(seeds)
    logger.info("seed_version=%d", seed_ver)

    conn = updb.connect()
    try:
        movie_files = sorted(glob.glob(args.movies_glob))
        logger.info("movies: %d chunk files", len(movie_files))
        m_ok, m_drop = ingest(movie_files, "movie", conn, seed_ver)

        import os
        series_files = [args.series_path] if os.path.exists(args.series_path) else []
        logger.info("series: %d file(s)", len(series_files))
        s_ok, s_drop = ingest(series_files, "series", conn, seed_ver)

        total = conn.execute("SELECT COUNT(*) FROM taste_scores").fetchone()[0]
        dist = dict(conn.execute(
            "SELECT score, COUNT(*) FROM taste_scores GROUP BY score ORDER BY score"
        ).fetchall())
    finally:
        conn.close()

    print(json.dumps({
        "movies_ingested": m_ok,
        "movies_dropped": m_drop,
        "series_ingested": s_ok,
        "series_dropped": s_drop,
        "total_rows": total,
        "score_distribution": dist,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
