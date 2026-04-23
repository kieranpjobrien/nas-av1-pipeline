"""Atmos/TrueHD upgrade recommender.

Given a title in the local library, decide whether a better disc release
(TrueHD-Atmos, 4K HDR) is available on bluray.com so the user can re-source
before the AV1 pipeline encodes the current (lower-quality) version.

Phase 1 scope:
- `scrapers.bluray_com`  — polite scraper with 7-day SQLite cache.
- `matcher`              — difflib-based fuzzy title+year matcher.
- `scorer`               — ranked upgrade score from signal aggregation.
- `db`                   — SQLite persistence (schema below).
- `__main__`             — CLI entrypoint (`refresh`, `top`, `show`).

Database lives at ``F:\\AV1_Staging\\upgrades.sqlite``; bump schema on
change, keep migrations idempotent (``CREATE TABLE IF NOT EXISTS``).
"""

from __future__ import annotations

__all__ = ["db", "matcher", "scorer"]
