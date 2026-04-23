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

    return {
        "filepath": entry["filepath"],
        "tmdb_id": tmdb.get("id") or tmdb.get("tmdb_id"),
        "title": tmdb.get("title") or entry.get("filename", "").rsplit(".", 1)[0],
        "year": tmdb.get("release_year") or _year_from_filename(entry.get("filename", "")),
        "library_type": entry.get("library_type"),
        "current_video_codec": video.get("codec"),
        "current_video_res": res,
        "current_audio_codec": preferred.get("codec"),
        "current_audio_channels": preferred.get("channels"),
        "current_has_atmos": 1 if has_atmos else 0,
        "tmdb_popularity": tmdb.get("popularity"),
        "tmdb_vote_average": tmdb.get("vote_average"),
    }


def _year_from_filename(filename: str) -> int | None:
    """Extract the first ``(YYYY)`` group from a filename; None if absent."""
    import re
    m = re.search(r"\((\d{4})\)", filename)
    return int(m.group(1)) if m else None


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

    return p


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns a POSIX-style exit code."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    _configure_logging(getattr(args, "verbose", False))
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
