"""Rewatchables podcast → Plex collection sync.

Parses The Rewatchables podcast RSS feed, matches movies to the local
library via TMDb, and maintains a Plex collection.

Usage:
    python -m tools.rewatchables                    # update collection
    python -m tools.rewatchables --dry-run          # show what would change
    python -m tools.rewatchables --list             # list all movies and library status
"""

import argparse
import json
import re
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from paths import MEDIA_REPORT, PLEX_TOKEN, PLEX_URL, STAGING_DIR
from pipeline.metadata import search_movie

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RSS_URLS = [
    "https://feeds.megaphone.fm/the-rewatchables",
    "https://www.theringer.com/rss/the-rewatchables.xml",
]

COLLECTION_NAME = "The Rewatchables"
CONTROL_DIR = STAGING_DIR / "control"
CACHE_FILE = CONTROL_DIR / "rewatchables_cache.json"
STATE_FILE = CONTROL_DIR / "rewatchables.json"

# Episodes whose titles don't contain a movie name
_SKIP_PATTERNS = re.compile(
    r"(top\s+\d+|draft|mailbag|preview|recap|best of|year.end|round.?table|"
    r"mega.?pod|hottest take|big picture|ringer.verse|fantasy|nfl|nba|"
    r"bill simmons.s (?:top|mount rushmore|most)|all.?time|ranking|"
    r"rewatchables awards|oscars|which movie)",
    re.IGNORECASE,
)

# Match text inside single (curly or straight) quotes
_QUOTE_RE = re.compile(r"[\u2018\u2019''](.+?)[\u2018\u2019'']")


# ---------------------------------------------------------------------------
# RSS parsing
# ---------------------------------------------------------------------------


def fetch_rss() -> ElementTree.Element:
    """Fetch and parse the RSS feed, trying multiple URLs."""
    last_err: Exception | None = None
    for url in RSS_URLS:
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0 (rewatchables-sync)"})
            with urlopen(req, timeout=30) as resp:
                return ElementTree.fromstring(resp.read())
        except (URLError, HTTPError, ElementTree.ParseError) as exc:
            last_err = exc
            continue
    raise RuntimeError(f"Failed to fetch RSS feed from any source: {last_err}")


def _extract_movie_title(episode_title: str) -> str | None:
    """Extract a movie title from a podcast episode title.

    Looks for text in single quotes first, then falls back to cleaning
    the raw title.  Returns None for non-movie episodes.
    """
    if _SKIP_PATTERNS.search(episode_title):
        return None

    # Primary: text in single/curly quotes
    m = _QUOTE_RE.search(episode_title)
    if m:
        return m.group(1).strip()

    # Fallback: strip common suffixes like "With Bill Simmons and ..."
    cleaned = re.sub(r"\s+[Ww]ith\s+.*$", "", episode_title)
    cleaned = re.sub(r"\s*\|.*$", "", cleaned)
    cleaned = re.sub(r"^The Rewatchables:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip(" -–—")

    if not cleaned or len(cleaned) < 2:
        return None
    return cleaned


def parse_rss(root: ElementTree.Element) -> list[dict]:
    """Parse RSS XML into a list of episode dicts with extracted movie titles."""
    episodes: list[dict] = []
    seen_titles: set[str] = set()

    for item in root.iter("item"):
        ep_title_el = item.find("title")
        pub_date_el = item.find("pubDate")
        if ep_title_el is None or ep_title_el.text is None:
            continue

        ep_title = ep_title_el.text.strip()
        movie_title = _extract_movie_title(ep_title)
        if not movie_title:
            continue

        # Deduplicate — some movies have multiple episodes
        title_key = movie_title.lower()
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)

        episodes.append(
            {
                "title": movie_title,
                "episode_title": ep_title,
                "pub_date": pub_date_el.text.strip() if pub_date_el is not None and pub_date_el.text else "",
            }
        )

    return episodes


# ---------------------------------------------------------------------------
# TMDb matching
# ---------------------------------------------------------------------------


def _load_cache() -> dict:
    """Load the TMDb lookup cache."""
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_cache(cache: dict) -> None:
    """Write cache atomically."""
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(CACHE_FILE)


def match_tmdb(episodes: list[dict]) -> list[dict]:
    """Enrich episodes with tmdb_id by searching TMDb.

    Uses a persistent cache to avoid re-searching known titles.
    Returns the episodes list with added tmdb_id and tmdb_title fields.
    """
    cache = _load_cache()
    dirty = False

    for ep in episodes:
        title_key = ep["title"].lower()
        if title_key in cache:
            ep["tmdb_id"] = cache[title_key].get("tmdb_id")
            ep["tmdb_title"] = cache[title_key].get("tmdb_title", "")
            ep["tmdb_year"] = cache[title_key].get("tmdb_year")
            continue

        results = search_movie(ep["title"])
        if results:
            best = results[0]
            ep["tmdb_id"] = best.get("id")
            ep["tmdb_title"] = best.get("title", "")
            year_str = (best.get("release_date") or "")[:4]
            ep["tmdb_year"] = int(year_str) if year_str.isdigit() else None
        else:
            ep["tmdb_id"] = None
            ep["tmdb_title"] = ""
            ep["tmdb_year"] = None

        cache[title_key] = {
            "tmdb_id": ep["tmdb_id"],
            "tmdb_title": ep.get("tmdb_title", ""),
            "tmdb_year": ep.get("tmdb_year"),
        }
        dirty = True

    if dirty:
        _save_cache(cache)

    return episodes


# ---------------------------------------------------------------------------
# Library matching
# ---------------------------------------------------------------------------


def _load_library_tmdb_ids() -> set[int]:
    """Load all TMDb IDs from media_report.json."""
    if not MEDIA_REPORT.exists():
        print(f"Warning: {MEDIA_REPORT} not found. Run scanner + TMDb enrichment first.", file=sys.stderr)
        return set()

    report = json.loads(MEDIA_REPORT.read_text(encoding="utf-8"))
    ids: set[int] = set()
    files = report if isinstance(report, list) else report.get("files", [])
    for f in files:
        tmdb = f.get("tmdb") or {}
        tmdb_id = tmdb.get("tmdb_id")
        if tmdb_id is not None:
            ids.add(int(tmdb_id))
    return ids


def match_library(episodes: list[dict]) -> list[dict]:
    """Flag episodes whose tmdb_id appears in the local library."""
    library_ids = _load_library_tmdb_ids()
    for ep in episodes:
        ep["in_library"] = ep.get("tmdb_id") is not None and ep["tmdb_id"] in library_ids
    return episodes


# ---------------------------------------------------------------------------
# Plex collection management
# ---------------------------------------------------------------------------


def _plex_get(endpoint: str) -> ElementTree.Element:
    """Authenticated GET to Plex."""
    url = f"{PLEX_URL}{endpoint}"
    req = Request(url, headers={"X-Plex-Token": PLEX_TOKEN, "Accept": "application/xml"})
    with urlopen(req, timeout=30) as resp:
        return ElementTree.fromstring(resp.read())


def _plex_put(endpoint: str) -> None:
    """Authenticated PUT to Plex."""
    url = f"{PLEX_URL}{endpoint}"
    req = Request(url, headers={"X-Plex-Token": PLEX_TOKEN}, method="PUT")
    with urlopen(req, timeout=30):
        pass


def _plex_post(endpoint: str, data: bytes = b"") -> ElementTree.Element | None:
    """Authenticated POST to Plex."""
    url = f"{PLEX_URL}{endpoint}"
    req = Request(url, headers={"X-Plex-Token": PLEX_TOKEN}, method="POST", data=data)
    with urlopen(req, timeout=30) as resp:
        body = resp.read()
        if body:
            return ElementTree.fromstring(body)
    return None


def _plex_delete(endpoint: str) -> None:
    """Authenticated DELETE to Plex."""
    url = f"{PLEX_URL}{endpoint}"
    req = Request(url, headers={"X-Plex-Token": PLEX_TOKEN}, method="DELETE")
    with urlopen(req, timeout=30):
        pass


def _find_movie_section() -> str | None:
    """Find the first movie library section key."""
    root = _plex_get("/library/sections")
    for d in root.findall(".//Directory"):
        if d.get("type") == "movie":
            return d.get("key")
    return None


def _get_plex_movies(section_key: str) -> dict[int, dict]:
    """Get all movies keyed by TMDb ID (from Plex's guid field).

    Returns {tmdb_id: {rating_key, title, year}}.
    """
    root = _plex_get(f"/library/sections/{section_key}/all?includeGuids=1")
    movies: dict[int, dict] = {}
    for video in root.findall(".//Video"):
        rating_key = video.get("ratingKey")
        title = video.get("title", "")
        year = video.get("year", "")

        # Extract TMDb ID from Guid tags
        for guid in video.findall("Guid"):
            gid = guid.get("id", "")
            if gid.startswith("tmdb://"):
                try:
                    tmdb_id = int(gid.replace("tmdb://", ""))
                    movies[tmdb_id] = {"rating_key": rating_key, "title": title, "year": year}
                except ValueError:
                    pass
                break
    return movies


def _get_collection(section_key: str, name: str) -> tuple[str | None, set[str]]:
    """Find a collection by name.  Returns (ratingKey, {member ratingKeys})."""
    root = _plex_get(f"/library/sections/{section_key}/collections")
    for d in root.findall(".//Directory"):
        if d.get("title") == name:
            coll_key = d.get("ratingKey")
            # Fetch collection members
            members_root = _plex_get(f"/library/collections/{coll_key}/children")
            member_keys = {v.get("ratingKey") for v in members_root.findall(".//Video") if v.get("ratingKey")}
            return coll_key, member_keys
    return None, set()


def _add_to_collection(section_key: str, rating_key: str) -> None:
    """Add a movie to The Rewatchables collection."""
    endpoint = (
        f"/library/sections/{section_key}/all?"
        f"type=1&id={rating_key}"
        f"&collection%5B0%5D.tag.tag={quote(COLLECTION_NAME)}"
        f"&collection.locked=1"
    )
    _plex_put(endpoint)


def _remove_from_collection(section_key: str, rating_key: str) -> None:
    """Remove a movie from The Rewatchables collection."""
    endpoint = (
        f"/library/sections/{section_key}/all?"
        f"type=1&id={rating_key}"
        f"&collection%5B%5D.tag.tag-={quote(COLLECTION_NAME)}"
        f"&collection.locked=1"
    )
    _plex_put(endpoint)


def sync_plex_collection(
    episodes: list[dict],
    dry_run: bool = False,
) -> dict:
    """Create/update the Plex collection to match Rewatchables episodes in library.

    Returns a summary dict with counts and lists of added/removed titles.
    """
    if not PLEX_URL or not PLEX_TOKEN:
        print("Error: PLEX_URL and PLEX_TOKEN required.", file=sys.stderr)
        return {"error": "missing credentials"}

    section_key = _find_movie_section()
    if not section_key:
        print("Error: no movie library section found in Plex.", file=sys.stderr)
        return {"error": "no movie section"}

    # Build the desired set of TMDb IDs (episodes that are in library)
    wanted_tmdb_ids = {ep["tmdb_id"] for ep in episodes if ep.get("in_library") and ep.get("tmdb_id")}

    # Map TMDb IDs → Plex rating keys
    plex_movies = _get_plex_movies(section_key)

    wanted_rating_keys: dict[str, str] = {}  # rating_key → title
    for tmdb_id in wanted_tmdb_ids:
        pm = plex_movies.get(tmdb_id)
        if pm:
            wanted_rating_keys[pm["rating_key"]] = f"{pm['title']} ({pm['year']})"

    # Get current collection members
    coll_key, current_members = _get_collection(section_key, COLLECTION_NAME)

    to_add = {rk: title for rk, title in wanted_rating_keys.items() if rk not in current_members}
    to_remove = current_members - set(wanted_rating_keys.keys())

    added_titles: list[str] = []
    removed_titles: list[str] = []

    if not dry_run:
        for rk, title in sorted(to_add.items(), key=lambda x: x[1]):
            _add_to_collection(section_key, rk)
            added_titles.append(title)
            print(f"  + {title}")

        for rk in to_remove:
            _remove_from_collection(section_key, rk)
            removed_titles.append(rk)
            print(f"  - (ratingKey {rk})")
    else:
        for rk, title in sorted(to_add.items(), key=lambda x: x[1]):
            added_titles.append(title)
            print(f"  would add: {title}")
        for rk in to_remove:
            removed_titles.append(rk)
            print(f"  would remove: ratingKey {rk}")

    return {
        "collection": COLLECTION_NAME,
        "wanted": len(wanted_rating_keys),
        "already_in_collection": len(current_members & set(wanted_rating_keys.keys())),
        "added": len(added_titles),
        "removed": len(removed_titles),
        "added_titles": added_titles,
        "removed_titles": removed_titles,
    }


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def _save_state(episodes: list[dict], sync_result: dict) -> None:
    """Save full state to rewatchables.json."""
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    state = {
        "updated": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "total_episodes": len(episodes),
        "matched_tmdb": sum(1 for e in episodes if e.get("tmdb_id")),
        "in_library": sum(1 for e in episodes if e.get("in_library")),
        "sync": sync_result,
        "episodes": episodes,
    }
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(STATE_FILE)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_list(episodes: list[dict]) -> None:
    """Print all Rewatchables movies and their library status."""
    in_lib = [e for e in episodes if e.get("in_library")]
    not_in_lib = [e for e in episodes if e.get("tmdb_id") and not e.get("in_library")]
    no_match = [e for e in episodes if not e.get("tmdb_id")]

    print(f"\n{'=' * 60}")
    print(f"  The Rewatchables — {len(episodes)} movies extracted")
    print(f"{'=' * 60}")

    print(f"\n  In library ({len(in_lib)}):")
    for e in sorted(in_lib, key=lambda x: x["title"]):
        year = f" ({e['tmdb_year']})" if e.get("tmdb_year") else ""
        print(f"    [x] {e['title']}{year}")

    print(f"\n  Not in library ({len(not_in_lib)}):")
    for e in sorted(not_in_lib, key=lambda x: x["title"]):
        year = f" ({e['tmdb_year']})" if e.get("tmdb_year") else ""
        print(f"    [ ] {e['title']}{year}")

    if no_match:
        print(f"\n  No TMDb match ({len(no_match)}):")
        for e in sorted(no_match, key=lambda x: x["title"]):
            print(f"    [?] {e['title']}  (episode: {e['episode_title'][:60]})")

    print(f"\n  Summary: {len(in_lib)} in library / {len(episodes)} total")


def cmd_sync(episodes: list[dict], dry_run: bool = False) -> None:
    """Sync the Plex collection."""
    label = "DRY RUN — " if dry_run else ""
    print(f"\n{label}Syncing '{COLLECTION_NAME}' Plex collection...")

    result = sync_plex_collection(episodes, dry_run=dry_run)

    if "error" in result:
        print(f"  Error: {result['error']}", file=sys.stderr)
        return

    print("\n  Summary:")
    print(f"    Rewatchables movies in library: {result['wanted']}")
    print(f"    Already in collection:          {result['already_in_collection']}")
    print(f"    Added:                          {result['added']}")
    print(f"    Removed:                        {result['removed']}")

    if not dry_run:
        _save_state(episodes, result)
        print(f"\n  State saved to {STATE_FILE}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    if sys.stdout.encoding != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Rewatchables podcast → Plex collection sync")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without modifying Plex")
    parser.add_argument("--list", action="store_true", dest="list_mode", help="List all movies and library status")
    args = parser.parse_args()

    print("Fetching RSS feed...")
    root = fetch_rss()
    episodes = parse_rss(root)
    print(f"  {len(episodes)} movies extracted from RSS")

    print("Matching via TMDb...")
    episodes = match_tmdb(episodes)
    matched = sum(1 for e in episodes if e.get("tmdb_id"))
    print(f"  {matched}/{len(episodes)} matched")

    print("Checking local library...")
    episodes = match_library(episodes)
    in_lib = sum(1 for e in episodes if e.get("in_library"))
    print(f"  {in_lib} in library")

    if args.list_mode:
        cmd_list(episodes)
    else:
        cmd_sync(episodes, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
