"""Plex collection and genre manager for better library categorisation.

Helps ensure films appear in the right Plex categories by:
1. Auditing genres/collections across libraries
2. Creating smart collections based on rules (studio, genre, keyword)
3. Fixing miscategorised content (e.g. Disney films not in Children's)

Requires PLEX_URL and PLEX_TOKEN environment variables.

Usage:
    python -m tools.plex_collections audit            # Show genre/collection stats
    python -m tools.plex_collections missing-genres    # Find items missing expected genres
    python -m tools.plex_collections apply-rules       # Apply collection rules
    python -m tools.plex_collections apply-rules --dry-run
"""

import argparse
import json
import sys
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from paths import PLEX_TOKEN, PLEX_URL, STAGING_DIR

RULES_FILE = STAGING_DIR / "control" / "plex_rules.json"

# Default rules: studio/keyword patterns → collections to ensure exist
DEFAULT_RULES = {
    "_comment": "Rules for assigning Plex collections. Edit to customise.",
    "studio_collections": {
        "Walt Disney Pictures": ["Disney", "Family"],
        "Walt Disney Animation Studios": ["Disney", "Family", "Animation"],
        "Pixar": ["Pixar", "Family", "Animation"],
        "DreamWorks Animation": ["DreamWorks", "Family", "Animation"],
        "Illumination Entertainment": ["Family", "Animation"],
        "Studio Ghibli": ["Studio Ghibli", "Animation"],
        "Marvel Studios": ["Marvel", "Superhero"],
        "DC Films": ["DC", "Superhero"],
        "DC Entertainment": ["DC", "Superhero"],
        "Lucasfilm Ltd.": ["Star Wars"],
    },
    "genre_collections": {
        "Animation": ["Family"],
        "Family": ["Family"],
    },
    "title_patterns": {
        "Frozen": ["Disney", "Family"],
        "Moana": ["Disney", "Family"],
        "Encanto": ["Disney", "Family"],
        "Tangled": ["Disney", "Family"],
    },
    "genre_aliases": {
        "Children": "Family",
        "Kids": "Family",
        "Sci-Fi": "Science Fiction",
    },
}


def _plex_get(endpoint: str) -> ElementTree.Element:
    """Make an authenticated GET request to the Plex API."""
    url = f"{PLEX_URL}{endpoint}"
    req = Request(url, headers={"X-Plex-Token": PLEX_TOKEN, "Accept": "application/xml"})
    with urlopen(req, timeout=30) as resp:
        return ElementTree.fromstring(resp.read())


def _plex_put(endpoint: str) -> None:
    """Make an authenticated PUT request to the Plex API."""
    url = f"{PLEX_URL}{endpoint}"
    req = Request(url, headers={"X-Plex-Token": PLEX_TOKEN}, method="PUT")
    with urlopen(req, timeout=30):
        pass


def _get_library_sections() -> list[dict]:
    """Get all library sections."""
    root = _plex_get("/library/sections")
    sections = []
    for directory in root.findall(".//Directory"):
        sections.append(
            {
                "key": directory.get("key"),
                "title": directory.get("title"),
                "type": directory.get("type"),
            }
        )
    return sections


def _get_movie_sections() -> list[dict]:
    """Get movie library sections only."""
    return [s for s in _get_library_sections() if s["type"] == "movie"]


def _get_all_movies(section_key: str) -> list[dict]:
    """Get all movies in a library section with metadata."""
    root = _plex_get(f"/library/sections/{section_key}/all")
    movies = []
    for video in root.findall(".//Video"):
        genres = [g.get("tag", "") for g in video.findall("Genre")]
        collections = [c.get("tag", "") for c in video.findall("Collection")]
        studio = video.get("studio", "")

        movies.append(
            {
                "rating_key": video.get("ratingKey"),
                "title": video.get("title", ""),
                "year": video.get("year", ""),
                "studio": studio,
                "genres": genres,
                "collections": collections,
                "content_rating": video.get("contentRating", ""),
            }
        )
    return movies


def _get_existing_collections(section_key: str) -> dict[str, str]:
    """Get existing collections in a section. Returns {name: ratingKey}."""
    root = _plex_get(f"/library/sections/{section_key}/collections")
    return {c.get("title", ""): c.get("ratingKey", "") for c in root.findall(".//Directory")}


def _add_to_collection(section_key: str, rating_key: str, collection_name: str) -> None:
    """Add a movie to a collection (creates the collection if needed)."""
    endpoint = (
        f"/library/sections/{section_key}/all?"
        f"type=1&id={rating_key}"
        f"&collection%5B0%5D.tag.tag={quote(collection_name)}"
        f"&collection.locked=1"
    )
    _plex_put(endpoint)


def _add_genre(section_key: str, rating_key: str, genre_name: str) -> None:
    """Add a genre tag to a movie."""
    endpoint = (
        f"/library/sections/{section_key}/all?"
        f"type=1&id={rating_key}"
        f"&genre%5B0%5D.tag.tag={quote(genre_name)}"
        f"&genre.locked=1"
    )
    _plex_put(endpoint)


def load_rules() -> dict:
    """Load collection rules from control file, or create defaults."""
    if RULES_FILE.exists():
        try:
            return json.loads(RULES_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Warning: Failed to load {RULES_FILE}: {e}", file=sys.stderr)

    # Write defaults
    RULES_FILE.parent.mkdir(parents=True, exist_ok=True)
    RULES_FILE.write_text(
        json.dumps(DEFAULT_RULES, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Created default rules: {RULES_FILE}")
    return DEFAULT_RULES


def cmd_audit() -> None:
    """Audit genres and collections across movie libraries."""
    sections = _get_movie_sections()
    if not sections:
        print("No movie libraries found.")
        return

    for section in sections:
        print(f"\n{'=' * 60}")
        print(f"  Library: {section['title']}")
        print(f"{'=' * 60}")

        movies = _get_all_movies(section["key"])
        print(f"  Total movies: {len(movies)}")

        # Genre stats
        genre_counts: dict[str, int] = {}
        for m in movies:
            for g in m["genres"]:
                genre_counts[g] = genre_counts.get(g, 0) + 1
        no_genre = sum(1 for m in movies if not m["genres"])

        print(f"\n  Genres ({len(genre_counts)} unique, {no_genre} untagged):")
        for genre, count in sorted(genre_counts.items(), key=lambda x: -x[1])[:20]:
            print(f"    {genre:.<30} {count}")

        # Collection stats
        collection_counts: dict[str, int] = {}
        for m in movies:
            for c in m["collections"]:
                collection_counts[c] = collection_counts.get(c, 0) + 1
        no_collection = sum(1 for m in movies if not m["collections"])

        print(f"\n  Collections ({len(collection_counts)} unique, {no_collection} not in any):")
        for coll, count in sorted(collection_counts.items(), key=lambda x: -x[1])[:20]:
            print(f"    {coll:.<30} {count}")

        # Studio stats
        studio_counts: dict[str, int] = {}
        for m in movies:
            if m["studio"]:
                studio_counts[m["studio"]] = studio_counts.get(m["studio"], 0) + 1

        print(f"\n  Studios ({len(studio_counts)} unique):")
        for studio, count in sorted(studio_counts.items(), key=lambda x: -x[1])[:15]:
            print(f"    {studio:.<30} {count}")


def cmd_missing_genres(rules: dict) -> None:
    """Find movies that should have certain genres/collections based on rules."""
    sections = _get_movie_sections()

    for section in sections:
        print(f"\nLibrary: {section['title']}")
        movies = _get_all_movies(section["key"])
        issues = []

        for movie in movies:
            expected_collections = set()
            reasons = []

            # Studio-based rules
            studio_rules = rules.get("studio_collections", {})
            for studio_pattern, colls in studio_rules.items():
                if studio_pattern.lower() in movie["studio"].lower():
                    for c in colls:
                        if c not in movie["collections"]:
                            expected_collections.add(c)
                            reasons.append(f"studio={movie['studio']}")

            # Genre-based rules
            genre_rules = rules.get("genre_collections", {})
            for genre, colls in genre_rules.items():
                if genre in movie["genres"]:
                    for c in colls:
                        if c not in movie["collections"]:
                            expected_collections.add(c)
                            reasons.append(f"genre={genre}")

            # Title-based rules
            title_rules = rules.get("title_patterns", {})
            for pattern, colls in title_rules.items():
                if pattern.lower() in movie["title"].lower():
                    for c in colls:
                        if c not in movie["collections"]:
                            expected_collections.add(c)
                            reasons.append(f"title match '{pattern}'")

            if expected_collections:
                issues.append(
                    {
                        "movie": movie,
                        "missing_collections": sorted(expected_collections),
                        "reasons": list(set(reasons)),
                    }
                )

        if issues:
            print(f"  {len(issues)} movies missing expected collections:")
            for issue in sorted(issues, key=lambda x: x["movie"]["title"]):
                m = issue["movie"]
                colls = ", ".join(issue["missing_collections"])
                print(f"    {m['title']} ({m['year']}) — missing: [{colls}]")
                print(f"      current: genres={m['genres']}, studio={m['studio']}")
        else:
            print("  All movies match rules.")


def cmd_apply_rules(rules: dict, dry_run: bool = True) -> None:
    """Apply collection rules — add missing collections to movies."""
    sections = _get_movie_sections()

    for section in sections:
        print(f"\nLibrary: {section['title']}")
        movies = _get_all_movies(section["key"])
        changes = 0

        for movie in movies:
            collections_to_add = set()

            # Studio-based
            for studio_pattern, colls in rules.get("studio_collections", {}).items():
                if studio_pattern.lower() in movie["studio"].lower():
                    for c in colls:
                        if c not in movie["collections"]:
                            collections_to_add.add(c)

            # Genre-based
            for genre, colls in rules.get("genre_collections", {}).items():
                if genre in movie["genres"]:
                    for c in colls:
                        if c not in movie["collections"]:
                            collections_to_add.add(c)

            # Title-based
            for pattern, colls in rules.get("title_patterns", {}).items():
                if pattern.lower() in movie["title"].lower():
                    for c in colls:
                        if c not in movie["collections"]:
                            collections_to_add.add(c)

            if collections_to_add:
                action = "would add" if dry_run else "adding"
                colls_str = ", ".join(sorted(collections_to_add))
                print(f"  {action}: [{colls_str}] → {movie['title']} ({movie['year']})")

                if not dry_run:
                    for coll in collections_to_add:
                        try:
                            _add_to_collection(section["key"], movie["rating_key"], coll)
                        except (URLError, OSError) as e:
                            print(f"    ERROR adding {coll}: {e}", file=sys.stderr)
                changes += 1

        action_word = "Would update" if dry_run else "Updated"
        print(f"\n  {action_word} {changes} movies.")

        if dry_run and changes > 0:
            print("  Pass --execute to apply changes.")


def main() -> None:
    if not PLEX_URL or not PLEX_TOKEN:
        print("Error: PLEX_URL and PLEX_TOKEN environment variables required.", file=sys.stderr)
        print("Set them in .env or export them.", file=sys.stderr)
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Plex collection and genre manager")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("audit", help="Show genre/collection statistics")
    subparsers.add_parser("missing-genres", help="Find items missing expected genres/collections")

    apply_parser = subparsers.add_parser("apply-rules", help="Apply collection rules to library")
    apply_parser.add_argument("--execute", action="store_true", help="Actually apply changes (default is dry-run)")

    args = parser.parse_args()
    rules = load_rules()

    try:
        if args.command == "audit":
            cmd_audit()
        elif args.command == "missing-genres":
            cmd_missing_genres(rules)
        elif args.command == "apply-rules":
            cmd_apply_rules(rules, dry_run=not args.execute)
    except (URLError, OSError) as e:
        print(f"\nPlex API error: {e}", file=sys.stderr)
        print("Check PLEX_URL and PLEX_TOKEN are correct.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
