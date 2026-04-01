"""
TMDb Metadata Enrichment
=========================
Queries The Movie Database (TMDb) API to enrich media_report.json entries
with metadata: genres, cast, crew, ratings, content ratings, etc.

Usage:
    python -m tools.tmdb                    # enrich media_report.json
    python -m tools.tmdb --force            # re-enrich even if tmdb data exists
    python -m tools.tmdb --file "path.mkv"  # single file
"""

import argparse
import json
import logging
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from paths import MEDIA_REPORT, TMDB_API_KEY

TMDB_BASE = "https://api.themoviedb.org/3"

# ---------- Rate limiter (global, thread-safe) ----------

_rate_lock = threading.Lock()
_request_timestamps: list[float] = []
_MAX_RPS = 38  # stay comfortably under the 40/s hard limit


def _rate_limit() -> None:
    """Sleep if necessary to stay under the TMDb rate limit."""
    with _rate_lock:
        now = time.monotonic()
        # Purge timestamps older than 1 second
        cutoff = now - 1.0
        while _request_timestamps and _request_timestamps[0] < cutoff:
            _request_timestamps.pop(0)
        if len(_request_timestamps) >= _MAX_RPS:
            sleep_for = _request_timestamps[0] - cutoff + 0.02
            time.sleep(max(sleep_for, 0.02))
        _request_timestamps.append(time.monotonic())


# ---------- Low-level API ----------


def _api_get(path: str, params: dict[str, str] | None = None) -> dict | list | None:
    """Make a GET request to the TMDb API. Returns parsed JSON or None on error."""
    _rate_limit()
    params = params or {}
    params["api_key"] = TMDB_API_KEY
    url = f"{TMDB_BASE}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError) as exc:
        print(f"  TMDb API error for {path}: {exc}", file=sys.stderr)
        return None


# ---------- Filename parsing ----------

# Movie: "Movie Name (2024).mkv" or with edition suffix "Movie Name (2024) Extended Cut.mkv"
_MOVIE_RE = re.compile(r"^(.+?)\s*\((\d{4})\)\s*(.*)$")
# Scene: "Gran.Torino.2008.1080p.BluRay..." — dots as separators, year in the middle
_SCENE_RE = re.compile(r"^(.+?)[.\s](\d{4})[.\s](?:\d{3,4}p|BluRay|WEB|HDR|German|DL)", re.IGNORECASE)
# Series: "Show Name S01E05 Episode Title.mkv" (various separators)
_SERIES_RE = re.compile(r"^(.+?)\s*[.\-_ ]*[Ss](\d{1,2})[Ee](\d{1,3})")
# Edition suffixes to strip from titles
_EDITION_SUFFIXES = re.compile(
    r"\s*[-–—]?\s*(?:Director'?s?\s*Cut|Extended\s*(?:Cut|Edition)?|Unrated|IMAX|"
    r"Theatrical\s*Cut|Remastered|EXTENDED|UNRATED)\s*$",
    re.IGNORECASE,
)


def parse_movie_filename(filename: str) -> tuple[str, int | None]:
    """Extract (title, year) from a movie filename.

    Handles: clean names, edition suffixes, scene release naming, dots-as-separators.
    """
    name = Path(filename).stem

    # Standard: "Movie Name (2024) Extended Cut"
    m = _MOVIE_RE.match(name)
    if m:
        title = m.group(1).strip().replace(".", " ")
        title = _EDITION_SUFFIXES.sub("", title).strip()
        return title, int(m.group(2))

    # Scene: "Gran.Torino.2008.1080p.BluRay.x264-OFT"
    m = _SCENE_RE.match(name)
    if m:
        title = m.group(1).replace(".", " ").strip()
        return title, int(m.group(2))

    # Fallback: strip extension, replace dots/underscores
    cleaned = name.replace(".", " ").replace("_", " ").strip()
    return cleaned, None


def parse_series_filename(filename: str) -> tuple[str, int | None, int | None]:
    """Extract (show_name, season, episode) from a series filename.

    Args:
        filename: Base filename like "Breaking Bad S05E16 Felina.mkv".

    Returns:
        Tuple of (show name, season number, episode number).
        Season/episode are None if the pattern doesn't match.
    """
    name = Path(filename).stem
    m = _SERIES_RE.match(name)
    if m:
        show = m.group(1).strip().replace(".", " ").replace("_", " ").rstrip(" -")
        return show, int(m.group(2)), int(m.group(3))
    # Fallback: try to get show name from the parent path pattern
    cleaned = name.replace(".", " ").replace("_", " ").strip()
    return cleaned, None, None


# ---------- Search ----------


def search_movie(title: str, year: int | None = None) -> list[dict]:
    """Search TMDb for a movie by title and optional year."""
    params: dict[str, str] = {"query": title}
    if year:
        params["year"] = str(year)
    data = _api_get("/search/movie", params)
    if data and "results" in data:
        return data["results"]
    return []


def search_tv(title: str) -> list[dict]:
    """Search TMDb for a TV show by title."""
    data = _api_get("/search/tv", {"query": title})
    if data and "results" in data:
        return data["results"]
    return []


# ---------- Details (with appended credits/keywords/release_dates) ----------


def get_movie_details(tmdb_id: int) -> dict | None:
    """Fetch full movie details with credits, keywords, and release dates in one call."""
    return _api_get(f"/movie/{tmdb_id}", {"append_to_response": "credits,keywords,release_dates"})


def get_tv_details(tmdb_id: int) -> dict | None:
    """Fetch full TV details with credits, keywords, and content ratings in one call."""
    return _api_get(f"/tv/{tmdb_id}", {"append_to_response": "credits,keywords,content_ratings"})


def get_movie_credits(tmdb_id: int) -> dict | None:
    """Fetch movie credits (cast and crew)."""
    return _api_get(f"/movie/{tmdb_id}/credits")


def get_tv_credits(tmdb_id: int) -> dict | None:
    """Fetch TV credits (cast and crew)."""
    return _api_get(f"/tv/{tmdb_id}/credits")


# ---------- Data extraction ----------


def _extract_au_content_rating(release_dates: dict) -> str:
    """Pull the Australian (or US fallback) content rating from release_dates."""
    results = release_dates.get("results", [])
    for pref in ("AU", "US", "GB"):
        for entry in results:
            if entry.get("iso_3166_1") == pref:
                for rd in entry.get("release_dates", []):
                    cert = rd.get("certification", "").strip()
                    if cert:
                        return cert
    return ""


def _extract_au_tv_rating(content_ratings: dict) -> str:
    """Pull the Australian (or US fallback) TV content rating."""
    results = content_ratings.get("results", [])
    for pref in ("AU", "US", "GB"):
        for entry in results:
            if entry.get("iso_3166_1") == pref:
                rating = entry.get("rating", "").strip()
                if rating:
                    return rating
    return ""


def extract_movie_metadata(details: dict) -> dict:
    """Extract the fields we care about from a TMDb movie details response.

    Args:
        details: Full movie details dict (with appended credits/keywords/release_dates).

    Returns:
        Flat dict of extracted metadata.
    """
    credits = details.get("credits", {})
    crew = credits.get("crew", [])
    cast = credits.get("cast", [])

    directors = [p["name"] for p in crew if p.get("job") == "Director"]
    writers = [p["name"] for p in crew if p.get("department") == "Writing"][:5]

    keywords_data = details.get("keywords", {}).get("keywords", [])
    keyword_names = [k["name"] for k in keywords_data[:5]]

    collection = details.get("belongs_to_collection")
    collection_name = collection["name"] if collection else None

    content_rating = ""
    if "release_dates" in details:
        content_rating = _extract_au_content_rating(details["release_dates"])

    release_date = details.get("release_date", "")
    release_year = int(release_date[:4]) if release_date and len(release_date) >= 4 else None

    return {
        "tmdb_id": details.get("id"),
        "imdb_id": details.get("imdb_id"),
        "original_language": details.get("original_language"),
        "genres": [g["name"] for g in details.get("genres", [])],
        "release_year": release_year,
        "director": directors[0] if directors else None,
        "cast": [p["name"] for p in cast[:10]],
        "writers": writers,
        "vote_average": details.get("vote_average"),
        "popularity": details.get("popularity"),
        "runtime_minutes": details.get("runtime"),
        "poster_path": details.get("poster_path"),
        "collection": collection_name,
        "content_rating": content_rating,
        "keywords": keyword_names,
    }


def extract_tv_metadata(details: dict) -> dict:
    """Extract the fields we care about from a TMDb TV details response.

    Args:
        details: Full TV details dict (with appended credits/keywords/content_ratings).

    Returns:
        Flat dict of extracted metadata.
    """
    credits = details.get("credits", {})
    cast = credits.get("cast", [])

    created_by = [p["name"] for p in details.get("created_by", [])]
    networks = [n["name"] for n in details.get("networks", [])]

    keywords_data = details.get("keywords", {}).get("results", [])
    keyword_names = [k["name"] for k in keywords_data[:5]]

    content_rating = ""
    if "content_ratings" in details:
        content_rating = _extract_au_tv_rating(details["content_ratings"])

    first_air = details.get("first_air_date", "")
    first_air_year = int(first_air[:4]) if first_air and len(first_air) >= 4 else None

    return {
        "tmdb_id": details.get("id"),
        "original_language": details.get("original_language"),
        "genres": [g["name"] for g in details.get("genres", [])],
        "first_air_year": first_air_year,
        "created_by": created_by,
        "cast": [p["name"] for p in cast[:10]],
        "vote_average": details.get("vote_average"),
        "popularity": details.get("popularity"),
        "number_of_seasons": details.get("number_of_seasons"),
        "number_of_episodes": details.get("number_of_episodes"),
        "status": details.get("status"),
        "networks": networks,
        "poster_path": details.get("poster_path"),
        "content_rating": content_rating,
        "keywords": keyword_names,
    }


# ---------- Match scoring ----------


def _pick_best_movie(results: list[dict], title: str, year: int | None, duration_secs: float) -> dict | None:
    """Score search results and return the best match.

    Validates year (within 1 year tolerance) and optionally cross-checks runtime.
    """
    if not results:
        return None

    title_lower = title.lower()
    best, best_score = None, -1

    for r in results[:10]:
        score = 0.0
        r_title = (r.get("title") or "").lower()

        # Title similarity (exact > partial)
        if r_title == title_lower:
            score += 10
        elif title_lower in r_title or r_title in title_lower:
            score += 5

        # Year match
        r_date = r.get("release_date", "")
        r_year = int(r_date[:4]) if r_date and len(r_date) >= 4 else None
        if year and r_year:
            diff = abs(r_year - year)
            if diff == 0:
                score += 8
            elif diff == 1:
                score += 4
            else:
                score -= 5  # penalise wrong-year matches

        # Popularity tiebreaker
        score += min((r.get("popularity") or 0) / 100, 2)

        if score > best_score:
            best_score = score
            best = r

    if best and best_score < 3:
        return None  # too low confidence
    return best


def _pick_best_tv(results: list[dict], title: str) -> dict | None:
    """Score TV search results and return the best match."""
    if not results:
        return None

    # Normalise for comparison: strip colons, dashes, lowercase
    def norm(s: str) -> str:
        return re.sub(r"[:\-–—]", "", s).lower().strip()

    title_norm = norm(title)
    best, best_score = None, -1

    for r in results[:10]:
        score = 0.0
        r_name = (r.get("name") or "")
        r_norm = norm(r_name)

        if r_norm == title_norm:
            score += 10
        elif title_norm in r_norm or r_norm in title_norm:
            score += 5

        score += min((r.get("popularity") or 0) / 100, 2)

        if score > best_score:
            best_score = score
            best = r

    if best and best_score < 3:
        return None
    return best


# ---------- Per-file enrichment ----------

# Cache show lookups so we don't re-query every episode of the same show.
_tv_cache_lock = threading.Lock()
_tv_cache: dict[str, dict | None] = {}


def _enrich_movie(entry: dict) -> dict | None:
    """Look up and return TMDb metadata for a single movie entry."""
    title, year = parse_movie_filename(entry["filename"])
    if not title or title.startswith("tmp"):
        return None

    # Strip leftover edition suffixes the regex didn't catch
    title = _EDITION_SUFFIXES.sub("", title).strip().rstrip(" -")

    results = search_movie(title, year)
    match = _pick_best_movie(results, title, year, entry.get("duration_seconds", 0))

    # Retry: year might be part of the title (e.g. "Wonder Woman 1984")
    if not match and year:
        title_with_year = f"{title} {year}"
        results2 = search_movie(title_with_year)
        match = _pick_best_movie(results2, title_with_year, None, entry.get("duration_seconds", 0))

    # Retry: without year (broader search)
    if not match and year:
        results3 = search_movie(title)
        match = _pick_best_movie(results3, title, None, entry.get("duration_seconds", 0))

    if not match:
        return None

    details = get_movie_details(match["id"])
    if not details:
        return None

    meta = extract_movie_metadata(details)

    # Runtime cross-check: if TMDb runtime differs by >20 min, it might be wrong match
    tmdb_runtime = meta.get("runtime_minutes") or 0
    file_runtime = entry.get("duration_seconds", 0) / 60
    if tmdb_runtime and file_runtime and abs(tmdb_runtime - file_runtime) > 20:
        # Try without year constraint
        results2 = search_movie(title)
        match2 = _pick_best_movie(results2, title, year, entry.get("duration_seconds", 0))
        if match2 and match2["id"] != match["id"]:
            details2 = get_movie_details(match2["id"])
            if details2:
                meta2 = extract_movie_metadata(details2)
                rt2 = meta2.get("runtime_minutes") or 0
                if rt2 and abs(rt2 - file_runtime) < abs(tmdb_runtime - file_runtime):
                    return meta2

    return meta


def _clean_show_name(raw: str) -> str:
    """Clean up a show folder name for TMDb search.

    Handles: year suffix "Archer (2009)", country suffix "Euphoria (US)",
    dash-for-colon "Star Wars - The Clone Wars" → "Star Wars: The Clone Wars".
    """
    cleaned = re.sub(r"\s*\(\d{4}\)\s*$", "", raw).strip()  # strip year
    cleaned = re.sub(r"\s*\([A-Z]{2,3}\)\s*$", "", cleaned).strip()  # strip country code
    cleaned = re.sub(r"\s+-\s+", ": ", cleaned)  # dash → colon (NAS naming convention)
    return cleaned if cleaned else raw


def _enrich_series(entry: dict) -> dict | None:
    """Look up and return TMDb metadata for a single series entry."""
    # Try to get show name from directory path first (more reliable than filename)
    filepath = entry.get("filepath", "")
    show_name = None
    parts = Path(filepath).parts
    for i, p in enumerate(parts):
        if p.lower().startswith("season") or re.match(r"[Ss]\d+", p):
            if i > 0:
                show_name = _clean_show_name(parts[i - 1])
            break

    if not show_name:
        show_name, _, _ = parse_series_filename(entry["filename"])
        if show_name:
            show_name = _clean_show_name(show_name)

    if not show_name:
        return None

    cache_key = show_name.lower().strip()

    with _tv_cache_lock:
        if cache_key in _tv_cache:
            return _tv_cache[cache_key]

    results = search_tv(show_name)
    match = _pick_best_tv(results, show_name)
    if not match:
        with _tv_cache_lock:
            _tv_cache[cache_key] = None
        return None

    details = get_tv_details(match["id"])
    if not details:
        with _tv_cache_lock:
            _tv_cache[cache_key] = None
        return None

    meta = extract_tv_metadata(details)

    with _tv_cache_lock:
        _tv_cache[cache_key] = meta

    return meta


def _enrich_one(entry: dict, force: bool = False) -> tuple[str, dict | None]:
    """Enrich a single file entry. Returns (filepath, metadata_or_None)."""
    if not force and entry.get("tmdb"):
        return entry["filepath"], None  # already enriched

    lib = entry.get("library_type", "")
    if lib == "movie":
        meta = _enrich_movie(entry)
    elif lib == "series":
        meta = _enrich_series(entry)
    else:
        meta = None

    return entry["filepath"], meta


# ---------- Bulk enrichment ----------


def enrich_report(report: dict, workers: int = 4, force: bool = False) -> dict:
    """Enrich all files in the report with TMDb metadata.

    Args:
        report: The full media_report dict (must contain "files" key).
        workers: Number of threads for concurrent lookups.
        force: If True, re-enrich files that already have tmdb data.

    Returns:
        The modified report dict (mutated in place).
    """
    files = report.get("files", [])
    to_process = files if force else [f for f in files if not f.get("tmdb")]

    if not to_process:
        print("All files already have TMDb metadata. Use --force to re-enrich.")
        return report

    print(f"Enriching {len(to_process)} files with TMDb metadata ({workers} workers)...")

    completed = 0
    enriched = 0
    failed = 0

    # Build a path -> index lookup for fast updates
    path_to_idx: dict[str, int] = {}
    for i, f in enumerate(files):
        path_to_idx[f["filepath"]] = i

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_enrich_one, entry, force): entry for entry in to_process}
        for future in as_completed(futures):
            completed += 1
            filepath, meta = future.result()

            if meta:
                idx = path_to_idx.get(filepath)
                if idx is not None:
                    files[idx]["tmdb"] = meta
                enriched += 1
            else:
                if not futures[future].get("tmdb"):
                    failed += 1

            if completed % 50 == 0 or completed == len(to_process):
                print(f"  Progress: {completed}/{len(to_process)} — {enriched} enriched, {failed} unmatched")

    print(f"\nTMDb enrichment complete: {enriched} enriched, {failed} unmatched out of {len(to_process)}")
    return report


# ---------- CLI ----------


def _find_mkvpropedit() -> str | None:
    """Find mkvpropedit binary."""
    import shutil
    found = shutil.which("mkvpropedit")
    if found:
        return found
    for path in (r"C:\Program Files\MKVToolNix\mkvpropedit.exe",
                 r"C:\Program Files (x86)\MKVToolNix\mkvpropedit.exe"):
        if os.path.isfile(path):
            return path
    return None


def write_tmdb_to_mkv(filepath: str, tmdb: dict) -> bool:
    """Write TMDb metadata into an MKV file's global tags via mkvpropedit.

    Sets title, date, genre, director, actors as MKV global segment tags.
    These are read by media players and metadata tools.
    """
    import subprocess
    import tempfile

    mkvprop = _find_mkvpropedit()
    if not mkvprop:
        return False

    if not filepath.lower().endswith(".mkv"):
        return False

    # Build MKV XML tags
    tags = []

    def add_tag(name: str, value: str) -> None:
        tags.append(f'    <Simple><Name>{name}</Name><String>{_xml_escape(value)}</String></Simple>')

    if tmdb.get("director"):
        add_tag("DIRECTOR", tmdb["director"])
    if tmdb.get("genres"):
        add_tag("GENRE", ", ".join(tmdb["genres"]))
    if tmdb.get("cast"):
        for actor in tmdb["cast"][:10]:
            add_tag("ACTOR", actor)
    if tmdb.get("writers"):
        for writer in tmdb["writers"][:5]:
            add_tag("WRITTEN_BY", writer)
    if tmdb.get("release_year"):
        add_tag("DATE_RELEASED", str(tmdb["release_year"]))
    if tmdb.get("content_rating"):
        add_tag("LAW_RATING", tmdb["content_rating"])
    if tmdb.get("imdb_id"):
        add_tag("IMDB", tmdb["imdb_id"])
    if tmdb.get("tmdb_id"):
        add_tag("TMDB", str(tmdb["tmdb_id"]))
    if tmdb.get("collection"):
        add_tag("COLLECTION", tmdb["collection"])
    if tmdb.get("original_language"):
        add_tag("ORIGINAL_LANGUAGE", tmdb["original_language"])
    if tmdb.get("vote_average"):
        add_tag("RATING", str(tmdb["vote_average"]))
    if tmdb.get("keywords"):
        add_tag("KEYWORDS", ", ".join(tmdb["keywords"]))
    # Series-specific
    if tmdb.get("created_by"):
        for creator in tmdb["created_by"]:
            add_tag("WRITTEN_BY", creator)
    if tmdb.get("networks"):
        add_tag("PUBLISHER", ", ".join(tmdb["networks"]))

    if not tags:
        return False

    xml = ('<?xml version="1.0" encoding="UTF-8"?>\n'
           '<Tags>\n'
           '  <Tag>\n'
           '    <Targets><TargetTypeValue>50</TargetTypeValue></Targets>\n'
           + "\n".join(tags) + "\n"
           '  </Tag>\n'
           '</Tags>\n')

    # Write XML to temp file, run mkvpropedit
    tmp_xml = os.path.join(tempfile.gettempdir(), f"tmdb_tags_{os.getpid()}.xml")
    try:
        with open(tmp_xml, "w", encoding="utf-8") as f:
            f.write(xml)

        result = subprocess.run(
            [mkvprop, filepath, "--tags", f"global:{tmp_xml}"],
            capture_output=True, text=True, timeout=30,
        )
        return result.returncode == 0
    except Exception:
        return False
    finally:
        if os.path.exists(tmp_xml):
            try:
                os.remove(tmp_xml)
            except OSError:
                pass


def _xml_escape(s: str) -> str:
    """Escape XML special characters."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def apply_tmdb_to_files(report: dict) -> tuple[int, int]:
    """Write TMDb metadata to all MKV files that have tmdb data in the report.

    Returns (success_count, fail_count).
    """
    success = 0
    failed = 0
    files = [f for f in report.get("files", []) if f.get("tmdb")]
    logging.info(f"Writing TMDb metadata to {len(files)} files...")

    for i, entry in enumerate(files):
        if (i + 1) % 100 == 0 or i + 1 == len(files):
            logging.info(f"  Progress: {i + 1}/{len(files)}")
        ok = write_tmdb_to_mkv(entry["filepath"], entry["tmdb"])
        if ok:
            success += 1
        else:
            failed += 1

    logging.info(f"  Done: {success} written, {failed} failed")
    return success, failed


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich media report with TMDb metadata")
    parser.add_argument("--report", type=str, default=str(MEDIA_REPORT), help="Path to media_report.json")
    parser.add_argument("--force", action="store_true", help="Re-enrich files that already have tmdb data")
    parser.add_argument("--apply", action="store_true", help="Write TMDb metadata into MKV file tags")
    parser.add_argument("--file", type=str, default=None, help="Enrich a single file by path")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    args = parser.parse_args()

    from tools.report_lock import read_report, write_report

    try:
        report = read_report()
    except FileNotFoundError:
        print(f"media_report.json not found", file=sys.stderr)
        sys.exit(1)

    if args.file:
        # Single-file mode
        target = None
        for entry in report.get("files", []):
            if entry.get("filepath") == args.file or entry.get("filename") == args.file:
                target = entry
                break
        if not target:
            print(f"File not found in report: {args.file}", file=sys.stderr)
            sys.exit(1)

        filepath, meta = _enrich_one(target, force=True)
        if meta:
            target["tmdb"] = meta
            print(f"Enriched: {target['filename']}")
            print(json.dumps(meta, indent=2))
        else:
            print(f"No TMDb match found for: {target['filename']}")
            sys.exit(0)
    else:
        report = enrich_report(report, workers=args.workers, force=args.force)

    write_report(report)
    print(f"Saved: {MEDIA_REPORT}")

    # Apply to MKV files if requested
    if args.apply:
        apply_tmdb_to_files(report)


if __name__ == "__main__":
    main()
