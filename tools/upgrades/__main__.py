"""CLI for the Atmos/TrueHD upgrade recommender.

Subcommands
-----------

``refresh``
    Walk the current media_report, fetch bluray.com data for titles we
    don't yet have (or whose cache is older than 7 days), match against
    scraper results, score, and upsert into ``upgrade_info``. Honours a
    strict 1 req/s rate limit and the SQLite scraper cache.

``top``
    Print the top-N rows from ``upgrade_info`` ordered by ``upgrade_score
    DESC``. Empty DB prints a friendly note instead of an empty table.

``show PATH``
    Pretty-print a single row (JSON) for one library filepath.

Run via ``uv run python -m tools.upgrades <command> [args]``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

from tools.upgrades import db as updb
from tools.upgrades import matcher, scorer
from tools.upgrades.scrapers import bluray_com

logger = logging.getLogger("tools.upgrades")


def _configure_logging(verbose: bool) -> None:
    """INFO by default, DEBUG when ``--verbose``."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )


# ---------- Media report integration ----------


def _iter_report_entries(library_type: str | None = None) -> list[dict[str, Any]]:
    """Read the current media report and return entries we care about.

    Filters to entries that have enough info to be worth checking:
    a filename, a non-empty library_type, and at least one audio stream.
    """
    try:
        from tools.report_lock import read_report
    except ImportError as exc:  # pragma: no cover
        logger.error("report_lock unavailable: %s", exc)
        return []

    report = read_report()
    files = report.get("files", []) or []
    out: list[dict[str, Any]] = []
    for e in files:
        if not e.get("filepath") or not e.get("filename"):
            continue
        if library_type and e.get("library_type") != library_type:
            continue
        if not e.get("audio_streams"):
            continue
        out.append(e)
    return out


def _derive_current_state(entry: dict[str, Any]) -> dict[str, Any]:
    """Flatten a media_report entry into upgrade_info 'current_*' fields."""
    video = entry.get("video") or {}
    audio_streams = entry.get("audio_streams") or []
    # First English-ish or first-listed audio stream; heuristic, good enough.
    preferred = next(
        (s for s in audio_streams if (s.get("language") or "").lower().startswith("en")),
        audio_streams[0] if audio_streams else {},
    )
    codec_raw = (preferred.get("codec_raw") or preferred.get("codec") or "").lower()
    # Heuristic: TrueHD always carries Atmos capability, EAC-3-JOC is
    # the other common carrier. Treat both as "has_atmos" for gating.
    has_atmos = codec_raw in {"truehd"} or (
        codec_raw in {"eac3", "eac-3"} and "joc" in (preferred.get("codec") or "").lower()
    )

    width = video.get("width") or 0
    height = video.get("height") or 0
    if height >= 2000 or width >= 3800:
        res = "4K"
    elif height >= 1000:
        res = "1080p"
    elif height >= 700:
        res = "720p"
    else:
        res = entry.get("resolution") or ""

    tmdb = entry.get("tmdb") or {}

    # Prefer TMDb title if enriched; else parse "(YYYY)" cleanly off the filename
    # using the same parser the rest of the pipeline uses.
    from pipeline.metadata import parse_movie_filename
    parsed_title, parsed_year = parse_movie_filename(entry.get("filename", ""))
    clean_title = tmdb.get("title") or parsed_title

    return {
        "filepath": entry["filepath"],
        "tmdb_id": tmdb.get("id") or tmdb.get("tmdb_id"),
        "title": clean_title,
        "year": tmdb.get("release_year") or parsed_year,
        "library_type": entry.get("library_type"),
        "current_video_codec": video.get("codec"),
        "current_video_res": res,
        "current_audio_codec": preferred.get("codec"),
        "current_audio_channels": preferred.get("channels"),
        "current_has_atmos": 1 if has_atmos else 0,
        "tmdb_popularity": tmdb.get("popularity"),
        "tmdb_vote_average": tmdb.get("vote_average"),
    }


# ---------- refresh ----------


def cmd_refresh(args: argparse.Namespace) -> int:
    """Walk media_report, scrape+score missing titles, upsert to DB."""
    conn = updb.connect()
    try:
        entries = _iter_report_entries(library_type=args.library_type)
        if args.limit:
            entries = entries[: args.limit]

        logger.info("refresh: %d candidate files", len(entries))
        processed = 0
        hits = 0
        now_iso = datetime.now(timezone.utc).isoformat()

        for entry in entries:
            current = _derive_current_state(entry)
            title = current.get("title") or ""
            year = current.get("year")
            if not title or not year:
                logger.warning("refresh: skipping %s (no title/year)", entry.get("filepath"))
                continue

            # Movies only for Phase 1; bluray.com has TV boxsets but they
            # rarely change our encoding plan and are higher risk to mismatch.
            if entry.get("library_type") != "movie":
                continue

            try:
                candidates = bluray_com.search_title(title, year, conn=conn)
            except Exception as exc:  # noqa: BLE001
                logger.error("refresh: search failed for %s (%s): %s", title, year, exc)
                candidates = []

            match = matcher.best_match(title, year, candidates)
            if match is None:
                logger.warning("refresh: no match for %s (%s)", title, year)
                updb.upsert(conn, {
                    **current,
                    "last_checked": now_iso,
                    "upgrade_score": 0,
                    "upgrade_reasons": [],
                    "confidence": "unknown",
                })
                processed += 1
                continue

            try:
                product = bluray_com.fetch_product_page(match["url"], conn=conn)
            except Exception as exc:  # noqa: BLE001
                logger.error("refresh: product fetch failed %s: %s", match.get("url"), exc)
                product = {"editions": []}

            best = bluray_com.summarise_best(product.get("editions", []))
            s, reasons = scorer.score(current, best)

            row = {
                **current,
                "last_checked": now_iso,
                **best,
                "upgrade_score": s,
                "upgrade_reasons": reasons,
                "confidence": match.get("_match_confidence") or "fuzzy",
            }
            updb.upsert(conn, row)
            processed += 1
            if s > 0:
                hits += 1

            if processed % 10 == 0:
                logger.info("refresh: progress %d (score>0: %d)", processed, hits)

        logger.info("refresh: done — %d processed, %d with score > 0", processed, hits)
        return 0
    finally:
        conn.close()


# ---------- top ----------


def cmd_top(args: argparse.Namespace) -> int:
    """Print the N highest-scoring upgrade candidates."""
    conn = updb.connect()
    try:
        rows = updb.iter_top(conn, limit=args.limit)
    finally:
        conn.close()

    if not rows:
        if args.json:
            print("[]")
        else:
            print("no upgrade data yet — run refresh")
        return 0

    if args.json:
        print(json.dumps([updb.row_to_public_dict(r) for r in rows], indent=2, default=str))
        return 0

    print(f"{'Score':>5}  {'Conf':<14}  {'Title':<45}  {'Year':<5}  Reasons")
    print("-" * 110)
    for r in rows:
        reasons = r.get("upgrade_reasons") or ""
        title = (r.get("title") or "")[:45]
        year = r.get("year") or ""
        print(f"{r.get('upgrade_score') or 0:>5}  {r.get('confidence') or '':<14}  "
              f"{title:<45}  {str(year):<5}  {reasons}")
    return 0


# ---------- show ----------


def cmd_show(args: argparse.Namespace) -> int:
    """Pretty-print a single row for ``args.path``."""
    conn = updb.connect()
    try:
        row = updb.read(conn, args.path)
    finally:
        conn.close()
    if not row:
        print(f"no upgrade info for: {args.path}", file=sys.stderr)
        return 1
    print(updb.dump_json(row))
    return 0


# ---------- taste-rescore (LLM pass) ----------


def cmd_taste_rescore(args: argparse.Namespace) -> int:
    """Score every movie in the library for 'home-cinema upgrade worthiness'.

    Uses Claude (``tools.upgrades.taste_scorer``) with prompt caching — the
    system prompt (instructions + seed calibration points) is stable and
    reused across all calls, so the cost scales mostly with the per-film
    query (~150 tokens) and output (~100 tokens), not the full prompt.

    Skips films whose cached score already matches the current seed version,
    unless ``--force`` is set.
    """
    try:
        import anthropic
        from tools.upgrades import taste_scorer
    except ImportError as exc:
        logger.error("taste-rescore requires anthropic + pydantic (see pyproject.toml): %s", exc)
        return 2

    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.error("ANTHROPIC_API_KEY not set — export it before running taste-rescore")
        return 2

    client = anthropic.Anthropic()
    conn = updb.connect()
    try:
        seeds = taste_scorer.load_seeds()
        seed_ver = taste_scorer.seed_version(seeds)
        system_prompt = taste_scorer.build_system_prompt(seeds)
        logger.info("taste-rescore: seed_version=%d, %d high + %d low seeds",
                    seed_ver, len(seeds.get("high", [])), len(seeds.get("low", [])))

        # Walk report for distinct (title, year) combinations.
        entries = _iter_report_entries(library_type=args.library_type)
        seen: set[tuple[str, int | None]] = set()
        to_score: list[dict[str, Any]] = []
        for entry in entries:
            current = _derive_current_state(entry)
            title = current.get("title") or ""
            year = current.get("year")
            if not title:
                continue
            key = (title, year)
            if key in seen:
                continue
            seen.add(key)
            # Build director + genre context from TMDb
            tmdb = entry.get("tmdb") or {}
            directors = tmdb.get("directors") or tmdb.get("director") or []
            if isinstance(directors, list):
                director = ", ".join(directors) if directors else None
            else:
                director = str(directors) if directors else None
            genres = tmdb.get("genres") or []
            overview = tmdb.get("overview") or ""
            to_score.append({
                "title": title, "year": year, "director": director,
                "genres": genres if isinstance(genres, list) else [],
                "overview": overview,
            })

        if args.limit:
            to_score = to_score[: args.limit]

        total = len(to_score)
        logger.info("taste-rescore: %d distinct films to consider", total)

        scored = 0
        skipped_cached = 0
        errors = 0
        cache_hits = 0
        total_input = 0
        total_output = 0
        total_cache_read = 0
        total_cache_creation = 0

        for i, film in enumerate(to_score, 1):
            if not args.force:
                cached = taste_scorer.fetch_score(
                    conn, film["title"], film["year"], seed_ver
                )
                if cached:
                    skipped_cached += 1
                    continue

            try:
                result = taste_scorer.score_film(
                    client,
                    title=film["title"],
                    year=film["year"],
                    director=film["director"],
                    genres=film["genres"],
                    overview=film["overview"],
                    system_prompt=system_prompt,
                )
            except Exception as exc:  # noqa: BLE001 — LLM errors are varied
                logger.error("  [%d/%d] %s (%s): %s",
                             i, total, film["title"], film["year"], exc)
                errors += 1
                continue

            taste_scorer.persist_score(
                conn,
                title=film["title"],
                year=film["year"],
                result=result,
                seed_ver=seed_ver,
            )
            scored += 1
            total_input += result.input_tokens
            total_output += result.output_tokens
            total_cache_read += result.cache_read_tokens
            total_cache_creation += result.cache_creation_tokens
            if result.cache_hit:
                cache_hits += 1
            logger.info(
                "  [%d/%d] %s (%s) -> %d  (cache: %s)",
                i, total, film["title"], film["year"], result.score,
                "HIT" if result.cache_hit else "WRITE" if result.cache_creation_tokens else "miss",
            )

        logger.info(
            "taste-rescore: done — %d scored, %d skipped (cached), %d errors",
            scored, skipped_cached, errors,
        )
        if scored:
            logger.info(
                "  cache hit rate: %.0f%% (%d/%d)",
                (100 * cache_hits / scored), cache_hits, scored,
            )
            logger.info(
                "  tokens — input: %d, output: %d, cache read: %d, cache write: %d",
                total_input, total_output, total_cache_read, total_cache_creation,
            )
        return 0 if errors == 0 else 1
    finally:
        conn.close()


# ---------- taste-score-one ----------


def cmd_taste_score_one(args: argparse.Namespace) -> int:
    """Score a single film by title+year, for testing / UI triggers."""
    try:
        import anthropic
        from tools.upgrades import taste_scorer
    except ImportError as exc:
        logger.error("taste-score-one requires anthropic + pydantic: %s", exc)
        return 2
    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.error("ANTHROPIC_API_KEY not set")
        return 2

    client = anthropic.Anthropic()
    conn = updb.connect()
    try:
        seeds = taste_scorer.load_seeds()
        seed_ver = taste_scorer.seed_version(seeds)
        system_prompt = taste_scorer.build_system_prompt(seeds)

        result = taste_scorer.score_film(
            client,
            title=args.title,
            year=args.year,
            director=args.director,
            genres=args.genres.split(",") if args.genres else [],
            overview=args.overview or "",
            system_prompt=system_prompt,
        )
        taste_scorer.persist_score(
            conn, title=args.title, year=args.year,
            result=result, seed_ver=seed_ver,
        )
        print(json.dumps({
            "title": args.title, "year": args.year,
            "score": result.score, "rationale": result.rationale,
            "cache_hit": result.cache_hit,
            "tokens": {
                "input": result.input_tokens,
                "output": result.output_tokens,
                "cache_read": result.cache_read_tokens,
                "cache_creation": result.cache_creation_tokens,
            },
        }, indent=2))
        return 0
    finally:
        conn.close()


# ---------- ranked (combined upgrade_gap × taste) ----------


def cmd_ranked(args: argparse.Namespace) -> int:
    """Print the combined-ranked candidate list.

    Joins upgrade_info (gap) × taste_scores (worthiness). Final score is
    ``upgrade_score * (taste_score / 10)`` so a high-gap low-taste film
    (Fast 5 with an Atmos BluRay gap) scores below a high-gap high-taste
    film (Heat with the same gap).
    """
    conn = updb.connect()
    try:
        cur = conn.execute("""
            SELECT
                u.filepath,
                u.title,
                u.year,
                u.library_type,
                u.current_video_res,
                u.current_audio_codec,
                u.current_has_atmos,
                u.has_atmos_available,
                u.has_4k_hdr_available,
                u.upgrade_score,
                u.upgrade_reasons,
                u.confidence,
                t.score AS taste_score,
                t.rationale AS taste_rationale,
                CAST(COALESCE(u.upgrade_score, 0) AS REAL)
                    * (CAST(COALESCE(t.score, 5) AS REAL) / 10.0) AS combined_score
            FROM upgrade_info u
            LEFT JOIN taste_scores t
                ON t.title = u.title AND (t.year = u.year OR (t.year IS NULL AND u.year IS NULL))
            WHERE u.upgrade_score IS NOT NULL
            ORDER BY combined_score DESC, u.title ASC
            LIMIT ?
        """, (int(args.limit),))
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    if not rows:
        if args.json:
            print("[]")
        else:
            print("no ranking data yet — run 'refresh' then 'taste-rescore'")
        return 0

    if args.json:
        # Convert reasons CSV to list for JSON consumers
        for r in rows:
            reasons = r.get("upgrade_reasons") or ""
            r["upgrade_reasons"] = [x for x in reasons.split(",") if x]
        print(json.dumps(rows, indent=2, default=str))
        return 0

    print(f"{'Combined':>8}  {'Gap':>3}  {'Taste':>5}  {'Title':<42}  {'Year':<5}  Why")
    print("-" * 110)
    for r in rows:
        combined = r.get("combined_score") or 0.0
        gap = r.get("upgrade_score") or 0
        taste = r.get("taste_score")
        title = (r.get("title") or "")[:42]
        year = r.get("year") or ""
        reasons = r.get("upgrade_reasons") or ""
        print(
            f"{combined:>8.1f}  {gap:>3}  "
            f"{'?' if taste is None else taste:>5}  {title:<42}  {str(year):<5}  {reasons}"
        )
    return 0


# ---------- Argparse wiring ----------


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser. Split out so tests can exercise it directly."""
    p = argparse.ArgumentParser(
        prog="python -m tools.upgrades",
        description="Atmos/TrueHD upgrade recommender — bluray.com scrape + SQLite.",
    )
    p.add_argument("--verbose", action="store_true", help="Enable DEBUG logging.")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_refresh = sub.add_parser("refresh", help="Scrape + score missing titles, update DB.")
    p_refresh.add_argument("--limit", type=int, default=0,
                           help="Process at most N files (0 = all). Useful for smoke tests.")
    p_refresh.add_argument("--library-type", default="movie",
                           choices=["movie", "series"],
                           help="Restrict to one library type (default: movie).")
    p_refresh.set_defaults(func=cmd_refresh)

    p_top = sub.add_parser("top", help="Show the top-N upgrade candidates.")
    p_top.add_argument("--limit", type=int, default=50)
    p_top.add_argument("--json", action="store_true", help="Emit JSON instead of a table.")
    p_top.set_defaults(func=cmd_top)

    p_show = sub.add_parser("show", help="Show the row for a single filepath.")
    p_show.add_argument("path", help="Full library filepath (as stored in upgrade_info).")
    p_show.set_defaults(func=cmd_show)

    p_taste = sub.add_parser(
        "taste-rescore",
        help="Score every film for upgrade worthiness using Claude (LLM-based taste scorer).",
    )
    p_taste.add_argument(
        "--limit", type=int, default=0,
        help="Score at most N films (0 = all). Useful for smoke tests / budget control.",
    )
    p_taste.add_argument(
        "--library-type", default="movie", choices=["movie", "series"],
        help="Restrict to one library type (default: movie). Series scoring is experimental.",
    )
    p_taste.add_argument(
        "--force", action="store_true",
        help="Rescore even films whose cached score matches the current seed version.",
    )
    p_taste.set_defaults(func=cmd_taste_rescore)

    p_taste1 = sub.add_parser(
        "taste-score-one",
        help="Score a single film — used by the UI to rescore on demand.",
    )
    p_taste1.add_argument("--title", required=True)
    p_taste1.add_argument("--year", type=int)
    p_taste1.add_argument("--director")
    p_taste1.add_argument("--genres", help="Comma-separated genre list")
    p_taste1.add_argument("--overview", help="TMDb synopsis text")
    p_taste1.set_defaults(func=cmd_taste_score_one)

    p_ranked = sub.add_parser(
        "ranked",
        help="Print combined ranking (upgrade_gap × taste_score).",
    )
    p_ranked.add_argument("--limit", type=int, default=50)
    p_ranked.add_argument("--json", action="store_true")
    p_ranked.set_defaults(func=cmd_ranked)

    return p


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns a POSIX-style exit code."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    _configure_logging(getattr(args, "verbose", False))
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
