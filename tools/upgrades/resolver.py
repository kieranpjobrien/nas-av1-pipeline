"""Resolve library titles to bluray.com product URLs via DuckDuckGo HTML search.

bluray.com's own ``/search.php`` endpoint is JavaScript-driven — the raw HTML
response carries no product anchors — so the Phase 1 in-site search never
matched anything. This module sidesteps that by asking DuckDuckGo's legacy
HTML frontend for the same query and regex-extracting the resulting
``bluray.com/movies/<slug>/<id>/`` links.

Design constraints:

* **Stdlib only.** ``urllib.request`` + ``re``; no third-party search clients.
* **1 req/s** rate limit shared with ``bluray_com``'s own lock pattern (a
  private lock here — DDG is a separate origin, so we don't want its
  backoff to block genuine bluray.com fetches).
* **7-day cache** in the existing ``scraper_cache`` SQLite table, keyed on
  the DDG URL. Cache miss -> HTTP -> parse -> best-match -> return.
* **CAPTCHA-aware.** If DDG returns an anomaly / CAPTCHA page the resolver
  logs a warning and returns ``None`` without retrying.
* **Year-gated.** Candidates must pass the matcher's ±1 year gate; titles
  with only a mismatching year are rejected.
"""

from __future__ import annotations

import html
import logging
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from tools.upgrades import db as updb
from tools.upgrades import matcher

logger = logging.getLogger(__name__)

USER_AGENT = "NASCleanup/1.0 (personal-use, fair-use search)"
DDG_URL = "https://html.duckduckgo.com/html/"

# Independent 1 rps lock for DDG — bluray.com fetches use their own lock.
_MIN_INTERVAL_S: float = 1.0
_rate_lock = threading.Lock()
_last_request_at: float = 0.0

# DDG rewrites outgoing URLs through ``//duckduckgo.com/l/?uddg=<encoded>``.
_UDDG_RE = re.compile(r"//duckduckgo\.com/l/\?(?:[^\"']*&(?:amp;)?)?uddg=([^\"'&]+)")
# Direct (unwrapped) product URL pattern — used as a fallback on pages where
# DDG omits the redirect wrapper, and for easy host-only matches.
_PRODUCT_URL_RE = re.compile(
    r"https?://(?:www\.)?blu-?ray\.com/movies/[A-Za-z0-9_\-%.]+/\d+/?",
    re.IGNORECASE,
)
# One full <div class="result ...">...</div> block per DDG hit.
_RESULT_BLOCK_RE = re.compile(
    r'<div class="result results_links[^"]*"[^>]*>'
    r'(?P<body>.*?)<div class="clear"></div>\s*</div>',
    re.DOTALL | re.IGNORECASE,
)
_RESULT_TITLE_RE = re.compile(
    r'class="result__a"[^>]*>(?P<title>[^<]+)</a>',
    re.IGNORECASE,
)
_RESULT_SNIPPET_RE = re.compile(
    r'class="result__snippet"[^>]*>(?P<snippet>.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)
_YEAR_RE = re.compile(r"\((\d{4})\)")
# "Dune Blu-ray (Target Exclusive)" -> "Dune"
_TITLE_SUFFIX_RE = re.compile(
    r"\s*(?:4K\s*)?(?:Ultra\s*HD\s*)?(?:Blu-?ray|UHD|DVD|Steelbook).*$",
    re.IGNORECASE,
)


def _rate_limit() -> None:
    """Block until at least ``_MIN_INTERVAL_S`` has passed since the last DDG call."""
    global _last_request_at
    with _rate_lock:
        now = time.monotonic()
        wait = _MIN_INTERVAL_S - (now - _last_request_at)
        if wait > 0:
            time.sleep(wait)
        _last_request_at = time.monotonic()


def _http_get(url: str, *, timeout: float = 20.0) -> str | None:
    """Fetch ``url`` as text. Returns None on network / HTTP failure."""
    _rate_limit()
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310 — public HTTPS
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as exc:
        logger.error("DDG HTTP error for %s: %s", url, exc)
        return None


def _build_query_url(title: str, year: int | None) -> str:
    """Construct the DDG HTML search URL for a title/year."""
    q_parts: list[str] = [f'"{title}"']
    if year is not None:
        q_parts.append(str(year))
    q_parts.append("site:bluray.com/movies")
    q = " ".join(q_parts)
    return DDG_URL + "?" + urllib.parse.urlencode({"q": q})


def _is_captcha(body: str) -> bool:
    """Return True if the DDG response looks like an anomaly / CAPTCHA page."""
    lower = body.lower()
    # DDG's CAPTCHA / rate-limit page contains "anomaly"; their standard no-
    # results page doesn't. Be conservative: require either the word
    # "anomaly" or an explicit challenge marker.
    return "anomaly" in lower or "captcha" in lower


def _decode_uddg(href: str) -> str | None:
    """Unwrap a DDG ``//duckduckgo.com/l/?uddg=<encoded>`` redirect to its target URL."""
    m = _UDDG_RE.search(href)
    if not m:
        return None
    encoded = m.group(1)
    # The outer DDG HTML has &amp; HTML-escaped entities; _UDDG_RE captures
    # up to the first & or quote so the encoded tail is clean.
    try:
        return urllib.parse.unquote(encoded)
    except (ValueError, UnicodeDecodeError):
        return None


def _clean_title(raw: str) -> str:
    """Strip trailing "Blu-ray" / "4K Ultra HD" / "Steelbook" etc. from a result title."""
    t = html.unescape(raw).strip()
    t = _TITLE_SUFFIX_RE.sub("", t)
    # Drop common edition suffixes in parentheses: "(Target Exclusive)", etc.
    t = re.sub(r"\s*\(.*?\)\s*$", "", t).strip()
    return t


def _extract_year(snippet_html: str) -> int | None:
    """Look for the first ``(YYYY)`` tuple in a DDG snippet fragment."""
    # Snippets may be full of <b>...</b> spans, including around the year.
    # Strip tags first, then decode entities, then regex.
    text = re.sub(r"<[^>]+>", "", snippet_html)
    text = html.unescape(text)
    m = _YEAR_RE.search(text)
    if m:
        return int(m.group(1))
    return None


def _parse_ddg_results(body: str) -> list[dict[str, object]]:
    """Extract ``{url, title, year}`` candidates from a DDG HTML results page.

    Only product-page URLs matching ``*blu-ray.com/movies/<slug>/<id>/`` pass
    the filter. Non-product hits (reviews, retailer listings, fan sites)
    are dropped.
    """
    candidates: list[dict[str, object]] = []
    seen: set[str] = set()
    for block_match in _RESULT_BLOCK_RE.finditer(body):
        block = block_match.group("body")
        title_m = _RESULT_TITLE_RE.search(block)
        if not title_m:
            continue
        raw_title = title_m.group("title")
        # Find the wrapped URL — prefer the result__a href, fall back to any uddg.
        url: str | None = None
        anchor_m = re.search(
            r'class="result__a"\s+href="([^"]+)"',
            block,
        )
        if anchor_m:
            url = _decode_uddg(anchor_m.group(1))
        if url is None:
            # Fallback: first uddg in the block, or a direct product URL.
            any_uddg = _UDDG_RE.search(block)
            if any_uddg:
                try:
                    url = urllib.parse.unquote(any_uddg.group(1))
                except (ValueError, UnicodeDecodeError):
                    url = None
        if url is None:
            direct = _PRODUCT_URL_RE.search(block)
            if direct:
                url = direct.group(0)
        if not url:
            continue
        product_m = _PRODUCT_URL_RE.search(url)
        if not product_m:
            continue
        product_url = product_m.group(0)
        # Normalise trailing slash.
        if not product_url.endswith("/"):
            product_url += "/"
        if product_url in seen:
            continue
        seen.add(product_url)

        snippet_m = _RESULT_SNIPPET_RE.search(block)
        snippet_html = snippet_m.group("snippet") if snippet_m else ""
        year = _extract_year(snippet_html)
        # Some titles ship "Dune (2021) Blu-ray" — look there too.
        if year is None:
            year = _extract_year(raw_title)

        candidates.append({
            "url": product_url,
            "title": _clean_title(raw_title),
            "year": year,
        })
    return candidates


def resolve_url(
    title: str,
    year: int | None,
    *,
    cache: sqlite3.Connection | None = None,
) -> str | None:
    """Return a bluray.com product URL for ``title``/``year``, or ``None``.

    Uses DuckDuckGo's HTML frontend and caches the response for 7 days in
    the shared ``scraper_cache`` table. Respects a 1 req/s rate limit.
    The returned URL is the best-ranked candidate that clears both the
    fuzzy-title and ±1-year gates of :mod:`tools.upgrades.matcher`.

    Args:
        title: The library title (e.g. ``"Dune"``).
        year: Release year (preferred); ``None`` skips the year gate.
        cache: Optional SQLite connection. When ``None``, a one-shot
            connection to the default upgrades DB is opened.

    Returns:
        A URL like ``https://www.blu-ray.com/movies/Dune-Blu-ray/305895/``,
        or ``None`` when no candidate passes the matcher.
    """
    if not title:
        return None

    own_conn = cache is None
    conn = cache if cache is not None else updb.connect()
    assert conn is not None
    try:
        url = _build_query_url(title, year)
        body = updb.cache_get(conn, url, ttl_days=7)
        if body is None:
            body = _http_get(url)
            if body is None:
                return None
            updb.cache_set(conn, url, body)

        if _is_captcha(body):
            logger.warning(
                "DDG returned a CAPTCHA / anomaly page for %r (%s) — skipping",
                title, year,
            )
            return None

        candidates = _parse_ddg_results(body)
        if not candidates:
            logger.info("DDG returned no bluray.com product candidates for %r (%s)",
                        title, year)
            return None

        match = matcher.best_match(title, year, candidates)
        if match is None:
            logger.info(
                "DDG candidates failed matcher for %r (%s): %d hits considered",
                title, year, len(candidates),
            )
            return None
        result_url = str(match.get("url") or "")
        return result_url or None
    finally:
        if own_conn:
            conn.close()
