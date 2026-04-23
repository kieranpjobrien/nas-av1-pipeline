"""Fuzzy title matcher for library files vs. scraper results.

Phase 1 uses ``difflib.SequenceMatcher`` with a strict year gate (±1) and
punctuation-insensitive comparison. No ML model, no HuggingFace. Good
enough for ~90% of titles in the user's library; harder edge cases
(re-releases, remakes, foreign-language originals) can be revisited in
Phase 2.

If ``rapidfuzz`` happens to be importable we use it for a faster ratio,
but the module must remain pip-dependency-free by default.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any

try:
    from rapidfuzz import fuzz as _rapid_fuzz  # type: ignore[import-not-found]

    def _ratio(a: str, b: str) -> float:
        """Delegate to rapidfuzz when available (3-5× faster than difflib)."""
        return _rapid_fuzz.ratio(a, b) / 100.0
except ImportError:  # pragma: no cover — plain-stdlib path is the default
    def _ratio(a: str, b: str) -> float:
        """Fallback: stdlib difflib ratio (slightly slower, identical semantics)."""
        return SequenceMatcher(None, a, b).ratio()


logger = logging.getLogger(__name__)

# ±1 year accommodates the common NAS-vs-bluray.com year drift (theatrical
# vs. home-release year, or a rights-holder listing by calendar date).
YEAR_TOLERANCE = 1
AUTHORITATIVE_THRESHOLD = 0.85
FUZZY_THRESHOLD = 0.70


_PUNCT_RE = re.compile(r"[^\w\s]")
_WS_RE = re.compile(r"\s+")
_ARTICLE_RE = re.compile(r"^(?:the|a|an)\s+", re.IGNORECASE)


def _normalise(title: str) -> str:
    """Lowercase, strip punctuation, NFD-fold diacritics, collapse whitespace.

    We also strip a leading article ("The ", "A ", "An ") because
    bluray.com is inconsistent about listing titles as "A Quiet Place"
    vs "Quiet Place, A" — dropping the article from both sides avoids
    punishing a legitimate match.
    """
    if not title:
        return ""
    t = unicodedata.normalize("NFD", title)
    t = "".join(ch for ch in t if unicodedata.category(ch) != "Mn")
    t = t.casefold()
    t = _PUNCT_RE.sub(" ", t)
    t = _WS_RE.sub(" ", t).strip()
    t = _ARTICLE_RE.sub("", t)
    return t


def _year_ok(library_year: int | None, candidate_year: int | None) -> bool:
    """True when the years match within ±YEAR_TOLERANCE (or either is unknown).

    Missing years from bluray.com (they do occasionally ship a hit with
    no year in the title tag) fall through to the string-only ratio so
    we don't miss a legitimate match on that alone.
    """
    if library_year is None or candidate_year is None:
        return True
    return abs(library_year - candidate_year) <= YEAR_TOLERANCE


def confidence_for(ratio: float) -> str | None:
    """Return ``'authoritative' | 'fuzzy' | None`` for a normalised ratio."""
    if ratio >= AUTHORITATIVE_THRESHOLD:
        return "authoritative"
    if ratio >= FUZZY_THRESHOLD:
        return "fuzzy"
    return None


def best_match(
    library_title: str, year: int | None, candidates: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """Pick the highest-scoring candidate that passes year and ratio gates.

    Args:
        library_title: Title parsed from the library filename
            (e.g. ``"Dune"`` from ``"Dune (2021).mkv"``).
        year: Parsed release year, or None if unknown.
        candidates: List of scraper-result dicts; each must have ``title``
            and optionally ``year``. Extra keys are preserved.

    Returns:
        The winning candidate dict with a ``_match_ratio`` float and
        ``_match_confidence`` label attached. ``None`` if no candidate
        clears the fuzzy threshold or all candidates fail the year gate.
    """
    if not library_title or not candidates:
        return None

    lib_norm = _normalise(library_title)
    if not lib_norm:
        return None

    best: dict[str, Any] | None = None
    best_ratio = 0.0

    for cand in candidates:
        cand_title = cand.get("title") or ""
        cand_year = cand.get("year")
        if not _year_ok(year, cand_year):
            continue
        cand_norm = _normalise(cand_title)
        if not cand_norm:
            continue
        r = _ratio(lib_norm, cand_norm)
        # Small bonus for exact year match — settles close ratios toward
        # the truly correct release year when both candidates normalise
        # to the same string (e.g. a 2021 original vs. a 2019 re-release).
        if year is not None and cand_year == year:
            r += 0.02
        if r > best_ratio:
            best_ratio = r
            best = cand

    if best is None:
        logger.info("matcher: no candidates passed year gate for '%s' (%s)",
                    library_title, year)
        return None

    conf = confidence_for(best_ratio)
    if conf is None:
        logger.warning(
            "matcher: best ratio %.2f below threshold for '%s' (%s) -> '%s' (%s)",
            best_ratio, library_title, year, best.get("title"), best.get("year"),
        )
        return None

    result = dict(best)
    result["_match_ratio"] = round(best_ratio, 4)
    result["_match_confidence"] = conf
    return result
