"""Recommend titles in the library that likely benefit from TrueHD-Atmos upgrade.

Criteria (inference, not authoritative — real Atmos master availability
varies by release, studio, and region):

* Currently NOT carrying TrueHD or EAC-3-JOC. (If you already have either,
  the Atmos layer is already preserved.)
* TMDb genre suggests immersive sound-design ROI: Action, Adventure,
  Animation, Fantasy, Science Fiction, War, Thriller, Horror, Music.
  Quiet-dialogue genres (Drama, Documentary, History, Romance) are
  deprioritised.
* Release year >= 2014 (Atmos theatrical rollout) with stronger scoring
  for 2016+ (home Atmos / UHD BluRay era).
* Popularity / vote_average > threshold — higher-profile releases are
  more likely to have an Atmos master available.
* Studio hints boost: Disney / Marvel / Pixar / A24 / Warner / Universal
  / DreamWorks / Illumination / Legendary / Blumhouse. Studio is
  inferred from TMDb `networks` (series) or `production_companies`
  (movies), neither of which we store — so we score on title keyword
  heuristics as a fallback.

Usage::

    uv run python -m tools.atmos_candidates              # top 50 movies
    uv run python -m tools.atmos_candidates --limit 100
    uv run python -m tools.atmos_candidates --series     # series-only
    uv run python -m tools.atmos_candidates --json       # machine-readable
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from typing import Any

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

from tools.report_lock import read_report

# Genres where immersive surround + overhead audio genuinely matters.
HIGH_VALUE_GENRES: frozenset[str] = frozenset(
    {
        "Action",
        "Adventure",
        "Animation",
        "Fantasy",
        "Horror",
        "Music",
        "Mystery",
        "Science Fiction",
        "Sci-Fi & Fantasy",
        "Thriller",
        "War",
        "War & Politics",
    }
)

# Genres where the difference is mostly inaudible (dialogue-led).
LOW_VALUE_GENRES: frozenset[str] = frozenset(
    {
        "Documentary",
        "History",
        "News",
        "Reality",
        "Romance",
        "Talk",
    }
)

# Studio/franchise keyword hints — presence in title or collection boosts score.
STUDIO_FRANCHISE_HINTS: tuple[tuple[str, int], ...] = (
    ("marvel", 3),
    ("avengers", 3),
    ("star wars", 3),
    ("dune", 3),
    ("mission impossible", 3),
    ("mission: impossible", 3),
    ("john wick", 3),
    ("mad max", 3),
    ("blade runner", 3),
    ("batman", 2),
    ("spider-man", 2),
    ("spiderman", 2),
    ("godzilla", 2),
    ("kong", 2),
    ("fast", 2),
    ("jurassic", 2),
    ("transformers", 2),
    ("pixar", 2),
    ("how to train your dragon", 2),
    ("kung fu panda", 2),
    ("shrek", 1),
    ("toy story", 2),
    ("incredibles", 2),
    ("inside out", 2),
    ("frozen", 2),
    ("moana", 2),
    ("encanto", 2),
    ("lion king", 2),
    ("tenet", 3),
    ("inception", 3),
    ("interstellar", 3),
    ("dunkirk", 2),
    ("nope", 2),
    ("get out", 1),
    ("a quiet place", 3),
    ("everything everywhere", 2),
    ("gravity", 2),
    ("the matrix", 3),
    ("top gun", 2),
)


@dataclass
class Candidate:
    """One recommended title."""

    filepath: str
    title: str
    library_type: str  # "movie" | "series"
    year: int | None
    current_audio: str  # e.g. "eac3 5.1 640kbps"
    genres: list[str]
    score: int
    reasons: list[str] = field(default_factory=list)


def _has_truehd_or_joc(f: dict[str, Any]) -> bool:
    """True if any audio track is TrueHD or EAC-3 (which may carry JOC/Atmos)."""
    for a in f.get("audio_streams") or []:
        codec = (a.get("codec_raw") or a.get("codec") or "").lower()
        if codec == "truehd":
            return True
        # EAC-3 with profile mentioning Atmos/JOC — already preserved bit-exact.
        profile = (a.get("profile") or "").lower()
        if codec in ("eac3", "e-ac-3") and ("atmos" in profile or "joc" in profile):
            return True
    return False


def _describe_current_audio(f: dict[str, Any]) -> str:
    """Human-readable current audio track summary."""
    streams = f.get("audio_streams") or []
    if not streams:
        return "no audio"
    a = streams[0]
    codec = (a.get("codec_raw") or a.get("codec") or "?").lower()
    ch = a.get("channels") or 0
    br = a.get("bitrate_kbps")
    layout = a.get("channel_layout") or f"{ch}ch"
    br_s = f" {br}kbps" if br else ""
    return f"{codec} {layout}{br_s}"


def _score_candidate(f: dict[str, Any]) -> tuple[int, list[str]]:
    """Return (score, reasons) for a file. Higher = better Atmos-upgrade candidate."""
    score = 0
    reasons: list[str] = []
    tmdb = f.get("tmdb") or {}
    filename_low = (f.get("filename") or "").lower()
    title_low = (tmdb.get("title") or tmdb.get("name") or "").lower()

    # Genre scoring
    genres: list[str] = tmdb.get("genres") or []
    high_hits = [g for g in genres if g in HIGH_VALUE_GENRES]
    low_hits = [g for g in genres if g in LOW_VALUE_GENRES]
    if high_hits:
        score += 3 * len(high_hits)
        reasons.append(f"immersive-genre: {', '.join(high_hits)}")
    if low_hits:
        score -= 2 * len(low_hits)
        reasons.append(f"dialogue-led: {', '.join(low_hits)}")

    # Year scoring — Atmos availability curve
    year = tmdb.get("release_year") or tmdb.get("first_air_year")
    if isinstance(year, int):
        if year >= 2018:
            score += 3
            reasons.append(f"modern release ({year})")
        elif year >= 2016:
            score += 2
            reasons.append(f"UHD-era release ({year})")
        elif year >= 2014:
            score += 1
        elif year < 2010:
            score -= 1
            reasons.append(f"pre-Atmos era ({year})")

    # Vote / popularity scoring — higher profile = more likely to have Atmos master
    vote = tmdb.get("vote_average") or 0
    popularity = tmdb.get("popularity") or 0
    if vote >= 8.0:
        score += 2
        reasons.append(f"high rating ({vote:.1f})")
    elif vote >= 7.0:
        score += 1
    if popularity >= 50:
        score += 2
        reasons.append(f"popular ({popularity:.0f})")
    elif popularity >= 20:
        score += 1

    # Franchise / studio keyword hints in title or filename
    hay = f"{title_low} {filename_low}"
    for keyword, boost in STUDIO_FRANCHISE_HINTS:
        if keyword in hay:
            score += boost
            reasons.append(f"franchise: {keyword}")
            break  # only one boost per file

    # Collection hint (e.g. belongs_to_collection)
    collection = tmdb.get("collection")
    if collection and isinstance(collection, str):
        coll_low = collection.lower()
        for keyword, boost in STUDIO_FRANCHISE_HINTS:
            if keyword in coll_low:
                score += 1
                reasons.append(f"collection: {collection}")
                break

    # Resolution bonus — 4K remuxes are where Atmos lives
    video = f.get("video") or {}
    res_class = (video.get("resolution_class") or "").lower()
    if "4k" in res_class or "2160" in res_class:
        score += 2
        reasons.append("4K source")

    # HDR bonus — Atmos + HDR often ship together on UHD
    if video.get("hdr"):
        score += 1
        reasons.append("HDR")

    # Surround source is a soft precondition — stereo-only sources have no
    # Atmos layer to gain. Don't hard-filter (upgraded sources may appear),
    # but penalise stereo-only current state.
    streams = f.get("audio_streams") or []
    if streams and all((a.get("channels") or 0) <= 2 for a in streams):
        score -= 3
        reasons.append("stereo-only today (no Atmos on stereo)")

    return score, reasons


def recommend(
    files: list[dict[str, Any]],
    library_type: str | None = None,
    min_score: int = 3,
    limit: int = 50,
) -> list[Candidate]:
    """Walk the file list and return ranked upgrade candidates."""
    out: list[Candidate] = []
    for f in files:
        lt = f.get("library_type") or ""
        if library_type and lt != library_type:
            continue
        if _has_truehd_or_joc(f):
            continue  # already carrying Atmos — nothing to upgrade

        score, reasons = _score_candidate(f)
        if score < min_score:
            continue

        tmdb = f.get("tmdb") or {}
        title = tmdb.get("title") or tmdb.get("name") or f.get("filename") or ""
        year = tmdb.get("release_year") or tmdb.get("first_air_year")
        out.append(
            Candidate(
                filepath=f.get("filepath") or "",
                title=str(title),
                library_type=lt,
                year=year if isinstance(year, int) else None,
                current_audio=_describe_current_audio(f),
                genres=tmdb.get("genres") or [],
                score=score,
                reasons=reasons,
            )
        )

    out.sort(key=lambda c: (-c.score, c.title.lower()))
    return out[:limit]


def _format_row(c: Candidate) -> str:
    year = f"({c.year})" if c.year else ""
    return (
        f"  {c.score:3d}  {c.title[:50]:<50} {year:<6} "
        f"{c.current_audio[:30]:<30}  {'; '.join(c.reasons[:3])[:70]}"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__ or "")
    ap.add_argument("--limit", type=int, default=50, help="Top-N candidates (default 50)")
    ap.add_argument("--series", action="store_true", help="Series only")
    ap.add_argument("--movies", action="store_true", help="Movies only")
    ap.add_argument("--json", action="store_true", help="JSON output instead of table")
    ap.add_argument("--min-score", type=int, default=3, help="Minimum score cutoff")
    args = ap.parse_args()

    library_type: str | None = None
    if args.series and not args.movies:
        library_type = "series"
    elif args.movies and not args.series:
        library_type = "movie"

    report = read_report()
    files = report.get("files", []) or []
    candidates = recommend(
        files,
        library_type=library_type,
        min_score=args.min_score,
        limit=args.limit,
    )

    if args.json:
        print(
            json.dumps(
                [
                    {
                        "filepath": c.filepath,
                        "title": c.title,
                        "year": c.year,
                        "library_type": c.library_type,
                        "score": c.score,
                        "current_audio": c.current_audio,
                        "genres": c.genres,
                        "reasons": c.reasons,
                    }
                    for c in candidates
                ],
                indent=2,
                ensure_ascii=False,
            )
        )
        return 0

    # Human table
    scope = library_type or "movies + series"
    print(f"Top {len(candidates)} Atmos-upgrade candidates ({scope}):\n")
    print(f"  {'Scr':>3s}  {'Title':<50} {'Year':<6} {'Current audio':<30}  Reasons")
    print("  " + "-" * 108)
    for c in candidates:
        print(_format_row(c))
    print(
        f"\n({sum(1 for f in files if _has_truehd_or_joc(f))} titles already carry "
        f"TrueHD or EAC-3-Atmos — those are untouched.)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
