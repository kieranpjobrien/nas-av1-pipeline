"""Plex metadata manager — collections, genres, content ratings, and labels.

Comprehensive metadata management for Plex movie libraries:
1. Audit genres, collections, content ratings, and labels
2. Report metadata health (unrated, ungenred, uncollected)
3. Apply rules-based metadata (collections, genres, ratings, labels)
4. Fix genre aliases (e.g. "Children" → "Family")

Requires PLEX_URL and PLEX_TOKEN environment variables.

Usage:
    python -m tools.plex_metadata audit                  # Full metadata stats
    python -m tools.plex_metadata report                 # Metadata health report
    python -m tools.plex_metadata missing-genres         # Find items missing expected genres
    python -m tools.plex_metadata apply-rules            # Dry-run rule application
    python -m tools.plex_metadata apply-rules --execute  # Apply changes
"""

import argparse
import json
import sys
from pathlib import Path
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from paths import PLEX_URL, PLEX_TOKEN, STAGING_DIR

RULES_FILE = STAGING_DIR / "control" / "plex_rules.json"

DEFAULT_RULES = {
    "_comment": "Rules for managing Plex metadata. Edit to customise.",
    "studio_collections": {
        "Walt Disney Pictures": ["Disney", "Family"],
        "Walt Disney Animation Studios": ["Disney", "Family", "Animation"],
        "Pixar": ["Pixar", "Family", "Animation"],
        "DreamWorks Animation": ["DreamWorks", "Family", "Animation"],
        "Illumination Entertainment": ["Family", "Animation"],
        "Illumination": ["Family", "Animation"],
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
    "genre_additions": {
        "_comment": "If a movie has genre X, also add genre Y. Used to fill gaps.",
        "Animation": ["Children"],
        "Family": ["Children"],
    },
    "genre_removals": {
        "_comment": "Remove these genres entirely (junk categories).",
        "genres": ["Reality", "TV Movie"],
    },
    "genre_aliases": {
        "Kids": "Family",
        "Sci-Fi": "Science Fiction",
    },
    "content_rating_rules": {
        "_comment": "Set content ratings by studio or collection. Only applied to unrated movies unless force=true.",
        "by_studio": {
            "Walt Disney Animation Studios": "G",
            "Pixar": "G",
            "DreamWorks Animation": "PG",
            "Illumination Entertainment": "PG",
            "Illumination": "PG",
        },
        "by_collection": {},
        "flag_unrated": True,
    },
    "label_rules": {
        "_comment": "Apply labels based on content rating or genre. Labels are useful for Plex filters.",
        "by_content_rating": {
            "G": ["Kid Safe"],
            "TV-Y": ["Kid Safe"],
            "TV-Y7": ["Kid Safe"],
            "TV-G": ["Kid Safe"],
        },
        "by_genre": {
            "Horror": ["Not For Kids"],
        },
    },
}


# ---------------------------------------------------------------------------
# Plex API helpers
# ---------------------------------------------------------------------------

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
    with urlopen(req, timeout=30) as resp:
        pass


def _get_library_sections() -> list[dict]:
    """Get all library sections."""
    root = _plex_get("/library/sections")
    sections = []
    for directory in root.findall(".//Directory"):
        sections.append({
            "key": directory.get("key"),
            "title": directory.get("title"),
            "type": directory.get("type"),
        })
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
        labels = [l.get("tag", "") for l in video.findall("Label")]

        movies.append({
            "rating_key": video.get("ratingKey"),
            "title": video.get("title", ""),
            "year": video.get("year", ""),
            "studio": video.get("studio", ""),
            "genres": genres,
            "collections": collections,
            "labels": labels,
            "content_rating": video.get("contentRating", ""),
        })
    return movies


def _get_existing_collections(section_key: str) -> dict[str, str]:
    """Get existing collections in a section. Returns {name: ratingKey}."""
    root = _plex_get(f"/library/sections/{section_key}/collections")
    return {
        c.get("title", ""): c.get("ratingKey", "")
        for c in root.findall(".//Directory")
    }


# ---------------------------------------------------------------------------
# Plex API write operations
# ---------------------------------------------------------------------------

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


def _remove_genre(section_key: str, rating_key: str, genre_name: str) -> None:
    """Remove a genre tag from a movie."""
    endpoint = (
        f"/library/sections/{section_key}/all?"
        f"type=1&id={rating_key}"
        f"&genre%5B%5D.tag.tag-={quote(genre_name)}"
        f"&genre.locked=1"
    )
    _plex_put(endpoint)


def _set_content_rating(section_key: str, rating_key: str, rating: str) -> None:
    """Set the content rating on a movie."""
    endpoint = (
        f"/library/sections/{section_key}/all?"
        f"type=1&id={rating_key}"
        f"&contentRating.value={quote(rating)}"
        f"&contentRating.locked=1"
    )
    _plex_put(endpoint)


def _add_label(section_key: str, rating_key: str, label_name: str) -> None:
    """Add a label to a movie."""
    endpoint = (
        f"/library/sections/{section_key}/all?"
        f"type=1&id={rating_key}"
        f"&label%5B0%5D.tag.tag={quote(label_name)}"
        f"&label.locked=1"
    )
    _plex_put(endpoint)


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

def load_rules() -> dict:
    """Load rules from control file, or create defaults."""
    if RULES_FILE.exists():
        try:
            return json.loads(RULES_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Warning: Failed to load {RULES_FILE}: {e}", file=sys.stderr)

    RULES_FILE.parent.mkdir(parents=True, exist_ok=True)
    RULES_FILE.write_text(
        json.dumps(DEFAULT_RULES, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Created default rules: {RULES_FILE}")
    return DEFAULT_RULES


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_audit() -> None:
    """Audit all metadata across movie libraries."""
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

        # Content rating stats
        rating_counts: dict[str, int] = {}
        unrated = []
        for m in movies:
            r = m["content_rating"] or "(unrated)"
            rating_counts[r] = rating_counts.get(r, 0) + 1
            if not m["content_rating"]:
                unrated.append(m)

        print(f"\n  Content Ratings ({len(rating_counts)} unique, {len(unrated)} unrated):")
        for rating, count in sorted(rating_counts.items(), key=lambda x: -x[1]):
            marker = " <--" if rating == "(unrated)" else ""
            print(f"    {rating:.<30} {count}{marker}")

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

        # Label stats
        label_counts: dict[str, int] = {}
        for m in movies:
            for l in m["labels"]:
                label_counts[l] = label_counts.get(l, 0) + 1
        no_label = sum(1 for m in movies if not m["labels"])

        print(f"\n  Labels ({len(label_counts)} unique, {no_label} unlabelled):")
        if label_counts:
            for label, count in sorted(label_counts.items(), key=lambda x: -x[1]):
                print(f"    {label:.<30} {count}")
        else:
            print("    (none)")

        # Studio stats
        studio_counts: dict[str, int] = {}
        for m in movies:
            if m["studio"]:
                studio_counts[m["studio"]] = studio_counts.get(m["studio"], 0) + 1

        print(f"\n  Studios ({len(studio_counts)} unique):")
        for studio, count in sorted(studio_counts.items(), key=lambda x: -x[1])[:15]:
            print(f"    {studio:.<30} {count}")


def cmd_report(rules: dict) -> None:
    """Metadata health report — find gaps and issues."""
    sections = _get_movie_sections()
    aliases = rules.get("genre_aliases", {})

    for section in sections:
        print(f"\n{'=' * 60}")
        print(f"  Metadata Health: {section['title']}")
        print(f"{'=' * 60}")

        movies = _get_all_movies(section["key"])

        # Unrated movies
        unrated = [m for m in movies if not m["content_rating"]]
        print(f"\n  Unrated movies ({len(unrated)}):")
        if unrated:
            for m in sorted(unrated, key=lambda x: x["title"])[:30]:
                studio = f" [{m['studio']}]" if m["studio"] else ""
                print(f"    {m['title']} ({m['year']}){studio}")
            if len(unrated) > 30:
                print(f"    ... and {len(unrated) - 30} more")
        else:
            print("    All movies have content ratings.")

        # No genres
        no_genre = [m for m in movies if not m["genres"]]
        print(f"\n  No genres ({len(no_genre)}):")
        if no_genre:
            for m in sorted(no_genre, key=lambda x: x["title"])[:20]:
                print(f"    {m['title']} ({m['year']})")
            if len(no_genre) > 20:
                print(f"    ... and {len(no_genre) - 20} more")
        else:
            print("    All movies have genres.")

        # No collections
        no_coll = [m for m in movies if not m["collections"]]
        print(f"\n  Not in any collection ({len(no_coll)}):")
        if no_coll:
            for m in sorted(no_coll, key=lambda x: x["title"])[:20]:
                print(f"    {m['title']} ({m['year']})")
            if len(no_coll) > 20:
                print(f"    ... and {len(no_coll) - 20} more")
        else:
            print("    All movies are in collections.")

        # Genre alias issues
        alias_issues = []
        for m in movies:
            bad_genres = [g for g in m["genres"] if g in aliases]
            if bad_genres:
                alias_issues.append((m, bad_genres))

        print(f"\n  Genre alias issues ({len(alias_issues)}):")
        if alias_issues:
            for m, bad in sorted(alias_issues, key=lambda x: x[0]["title"])[:20]:
                fixes = ", ".join(f'"{g}" → "{aliases[g]}"' for g in bad)
                print(f"    {m['title']} ({m['year']}): {fixes}")
            if len(alias_issues) > 20:
                print(f"    ... and {len(alias_issues) - 20} more")
        else:
            print("    No alias issues found.")

        # Summary
        total = len(movies)
        print(f"\n  Summary:")
        print(f"    Total:       {total}")
        print(f"    Unrated:     {len(unrated)} ({len(unrated)/total*100:.1f}%)" if total else "")
        print(f"    No genres:   {len(no_genre)} ({len(no_genre)/total*100:.1f}%)" if total else "")
        print(f"    No collect.: {len(no_coll)} ({len(no_coll)/total*100:.1f}%)" if total else "")
        print(f"    Alias fixes: {len(alias_issues)}")


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
            for studio_pattern, colls in rules.get("studio_collections", {}).items():
                if studio_pattern.lower() in movie["studio"].lower():
                    for c in colls:
                        if c not in movie["collections"]:
                            expected_collections.add(c)
                            reasons.append(f"studio={movie['studio']}")

            # Genre-based rules
            for genre, colls in rules.get("genre_collections", {}).items():
                if genre in movie["genres"]:
                    for c in colls:
                        if c not in movie["collections"]:
                            expected_collections.add(c)
                            reasons.append(f"genre={genre}")

            # Title-based rules
            for pattern, colls in rules.get("title_patterns", {}).items():
                if pattern.lower() in movie["title"].lower():
                    for c in colls:
                        if c not in movie["collections"]:
                            expected_collections.add(c)
                            reasons.append(f"title match '{pattern}'")

            if expected_collections:
                issues.append({
                    "movie": movie,
                    "missing_collections": sorted(expected_collections),
                    "reasons": list(set(reasons)),
                })

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
    """Apply all metadata rules — collections, genres, ratings, labels."""
    sections = _get_movie_sections()
    action = "would" if dry_run else "will"

    for section in sections:
        print(f"\nLibrary: {section['title']}")
        movies = _get_all_movies(section["key"])
        total_changes = 0

        for movie in movies:
            changes = []

            # --- Collection rules ---
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

            for coll in sorted(collections_to_add):
                changes.append(("collection", f"+collection [{coll}]", lambda sk=section["key"], rk=movie["rating_key"], cn=coll: _add_to_collection(sk, rk, cn)))

            # --- Genre additions (if has X, also add Y) ---
            genres_to_add = set()
            for trigger_genre, add_genres in rules.get("genre_additions", {}).items():
                if isinstance(add_genres, list) and trigger_genre in movie["genres"]:
                    for g in add_genres:
                        if g not in movie["genres"]:
                            genres_to_add.add(g)
            for genre in sorted(genres_to_add):
                changes.append(("genre", f'+genre [{genre}]', lambda sk=section["key"], rk=movie["rating_key"], gn=genre: _add_genre(sk, rk, gn)))

            # --- Genre removals (junk categories) ---
            removal_list = rules.get("genre_removals", {}).get("genres", [])
            for bad_genre in removal_list:
                if bad_genre in movie["genres"]:
                    changes.append(("genre", f'-genre [{bad_genre}]', lambda sk=section["key"], rk=movie["rating_key"], bg=bad_genre: _remove_genre(sk, rk, bg)))

            # --- Genre alias fixes ---
            aliases = rules.get("genre_aliases", {})
            for old_genre in list(movie["genres"]):
                if old_genre in aliases:
                    new_genre = aliases[old_genre]
                    if new_genre not in movie["genres"]:
                        changes.append(("genre", f'genre "{old_genre}" -> "{new_genre}"', lambda sk=section["key"], rk=movie["rating_key"], ng=new_genre: _add_genre(sk, rk, ng)))

            # --- Content rating rules ---
            cr_rules = rules.get("content_rating_rules", {})
            if not movie["content_rating"]:
                # Try to infer rating from studio
                for studio_pattern, rating in cr_rules.get("by_studio", {}).items():
                    if studio_pattern.lower() in movie["studio"].lower():
                        changes.append(("rating", f"+contentRating [{rating}]", lambda sk=section["key"], rk=movie["rating_key"], r=rating: _set_content_rating(sk, rk, r)))
                        break

                # Try to infer from collection
                if not any(c[0] == "rating" for c in changes):
                    for coll_name, rating in cr_rules.get("by_collection", {}).items():
                        if coll_name in movie["collections"] or coll_name in collections_to_add:
                            changes.append(("rating", f"+contentRating [{rating}] (via collection {coll_name})", lambda sk=section["key"], rk=movie["rating_key"], r=rating: _set_content_rating(sk, rk, r)))
                            break

            # --- Label rules ---
            label_rules = rules.get("label_rules", {})
            labels_to_add = set()

            # Determine effective content rating (current or about-to-be-set)
            effective_rating = movie["content_rating"]
            if not effective_rating:
                for c in changes:
                    if c[0] == "rating":
                        # Extract rating from the description
                        effective_rating = c[1].split("[")[1].split("]")[0] if "[" in c[1] else ""
                        break

            for rating, label_list in label_rules.get("by_content_rating", {}).items():
                if effective_rating == rating:
                    for label in label_list:
                        if label not in movie["labels"]:
                            labels_to_add.add(label)

            for genre, label_list in label_rules.get("by_genre", {}).items():
                if genre in movie["genres"]:
                    for label in label_list:
                        if label not in movie["labels"]:
                            labels_to_add.add(label)

            # "Not For Kids" overrides "Kid Safe" — don't apply both
            if "Not For Kids" in labels_to_add:
                labels_to_add.discard("Kid Safe")

            for label in sorted(labels_to_add):
                changes.append(("label", f"+label [{label}]", lambda sk=section["key"], rk=movie["rating_key"], ln=label: _add_label(sk, rk, ln)))

            # --- Apply or report ---
            if changes:
                descs = ", ".join(c[1] for c in changes)
                print(f"  {action}: {movie['title']} ({movie['year']}) — {descs}")

                if not dry_run:
                    for _, desc, fn in changes:
                        try:
                            fn()
                        except (URLError, OSError) as e:
                            print(f"    ERROR {desc}: {e}", file=sys.stderr)

                total_changes += 1

        action_word = "Would update" if dry_run else "Updated"
        print(f"\n  {action_word} {total_changes} movies.")

        if dry_run and total_changes > 0:
            print("  Pass --execute to apply changes.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    # Ensure UTF-8 output on Windows (avoids cp1252 encoding errors with non-Latin titles)
    if sys.stdout.encoding != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    if not PLEX_URL or not PLEX_TOKEN:
        print("Error: PLEX_URL and PLEX_TOKEN environment variables required.", file=sys.stderr)
        print("Set them in .env or export them.", file=sys.stderr)
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Plex metadata manager — collections, genres, ratings, labels")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("audit", help="Show full metadata statistics")
    subparsers.add_parser("report", help="Metadata health report (gaps and issues)")
    subparsers.add_parser("missing-genres", help="Find items missing expected genres/collections")

    apply_parser = subparsers.add_parser("apply-rules", help="Apply all metadata rules")
    apply_parser.add_argument("--execute", action="store_true",
                              help="Actually apply changes (default is dry-run)")

    args = parser.parse_args()
    rules = load_rules()

    try:
        if args.command == "audit":
            cmd_audit()
        elif args.command == "report":
            cmd_report(rules)
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
