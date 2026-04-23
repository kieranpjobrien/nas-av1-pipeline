"""Polite bluray.com scraper for disc-release audio/video capabilities.

bluray.com lists every commercial Blu-ray / UHD Blu-ray release of a title
along with its audio tracks (codec + channels) and video spec.

Title → product URL resolution lives in :mod:`tools.upgrades.resolver`
(a DuckDuckGo HTML search backend — bluray.com's in-site search is JS-
rendered and unusable from raw HTTP). This module owns the product-page
fetch and parse path only.

Design constraints (aligned with ``pipeline/metadata.py``):

* **Stdlib only.** ``urllib.request`` + regex; no ``requests``, no
  BeautifulSoup.
* **1 req/s** global rate limit via a module-level ``threading.Lock``.
* **7-day cache** keyed by URL in the shared ``scraper_cache`` SQLite
  table (see ``tools.upgrades.db``). Callers are expected to pass a live
  ``sqlite3.Connection``; the scraper does not open its own DB.
* **No login / no paid endpoints.** Public pages only.

The module is deliberately lenient: bluray.com occasionally ships markup
with small variations across product pages, so the parser extracts with
regex on specific well-known labels ("Audio", "Video", "Dolby Atmos")
and degrades to empty lists on parse failure rather than raising.
"""

from __future__ import annotations

import html
import logging
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from typing import Any

from tools.upgrades import db as updb

logger = logging.getLogger(__name__)

USER_AGENT = "NASCleanup/1.0 (personal-use)"
BASE_URL = "https://www.blu-ray.com"

# 1 request per second — plenty of headroom over bluray.com's tolerance
# for polite public scraping. Do NOT increase without pre-approval.
_MIN_INTERVAL_S: float = 1.0

_rate_lock = threading.Lock()
_last_request_at: float = 0.0


def _rate_limit() -> None:
    """Block until at least ``_MIN_INTERVAL_S`` has elapsed since the previous HTTP call.

    Thread-safe: uses a module-level lock and ``time.monotonic`` so the
    interval survives system-clock jumps. Mirrors
    ``pipeline.metadata._rate_limit`` but with a 1 rps ceiling.
    """
    global _last_request_at
    with _rate_lock:
        now = time.monotonic()
        wait = _MIN_INTERVAL_S - (now - _last_request_at)
        if wait > 0:
            time.sleep(wait)
        _last_request_at = time.monotonic()


def _http_get(url: str, *, timeout: float = 20.0) -> str | None:
    """Fetch a URL's body as text. Returns None on any HTTP / network error."""
    _rate_limit()
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310 — public HTTPS only
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as exc:
        logger.error("bluray.com HTTP error for %s: %s", url, exc)
        return None


def _fetch_cached(
    conn: sqlite3.Connection, url: str, *, ttl_days: int = 7
) -> str | None:
    """Return cached body if fresh, otherwise fetch + store + return."""
    cached = updb.cache_get(conn, url, ttl_days=ttl_days)
    if cached is not None:
        logger.debug("bluray.com cache hit: %s", url)
        return cached
    body = _http_get(url)
    if body is not None:
        updb.cache_set(conn, url, body)
    return body


# ---------- Search / URL resolution ----------


def search_title(
    title: str, year: int, conn: sqlite3.Connection | None = None
) -> list[dict[str, Any]]:
    """Resolve a bluray.com product page for ``title`` / ``year``.

    bluray.com's public ``/search.php`` page is JavaScript-rendered — the
    raw HTML response carries no product anchors — so we delegate to
    :func:`tools.upgrades.resolver.resolve_url`, which queries DuckDuckGo's
    HTML frontend for ``site:bluray.com/movies`` hits and applies the
    Phase 1 matcher for title+year verification.

    Args:
        title: Free-form title (e.g. ``"Dune"``).
        year:  Theatrical year. Used for the search query and the matcher
               year gate (±1).
        conn:  Optional SQLite connection for cache access. If None, a
               one-shot connection to the default DB is opened.

    Returns:
        A list with a single ``{url, title, year}`` dict on success, or
        an empty list when no product URL resolves. The shape matches the
        Phase 1 contract so ``matcher.best_match`` still works downstream.
    """
    # Local import to avoid a circular import at module load time — the
    # resolver depends on tools.upgrades.matcher, which is stdlib-only.
    from tools.upgrades import resolver

    url = resolver.resolve_url(title, year, cache=conn)
    if not url:
        return []
    return [{"url": url, "title": title, "year": year}]


# ---------- Product-page parsing ----------

# Patterns applied to the raw HTML (not via HTMLParser) because the
# audio-track block on bluray.com is rendered as a flat <li> / <p> list
# with very consistent prefixes we can regex directly.

_AUDIO_LINE_RE = re.compile(
    r"(?P<lang>[A-Za-z]+):\s*"
    r"(?P<codec>Dolby\s+TrueHD(?:\s+with\s+Dolby\s+Atmos)?"
    r"|Dolby\s+Atmos"
    r"|DTS[:\-][A-Z0-9\s]+"
    r"|DTS-HD\s+(?:MA|Master\s+Audio|High\s+Resolution)"
    r"|Dolby\s+Digital(?:\s+Plus|\s+EX)?"
    r"|LPCM"
    r"|PCM"
    r"|Dolby\s+Surround"
    r"|DTS)"
    r"\s*(?P<channels>\d+\.\d+|\d+\.\d)?",
    re.IGNORECASE,
)

_VIDEO_LINE_RE = re.compile(
    r"(?P<codec>HEVC|H\.?264|AV1|MPEG-?4\s*AVC|VC-1)"
    r"(?:[^,<]*?\b(?P<res>2160p|4K|1080p|720p)\b)?"
    r"(?:[^,<]*?\b(?P<hdr>HDR10\+?|Dolby\s+Vision|HDR))?",
    re.IGNORECASE,
)

_EDITION_HEADER_RE = re.compile(
    r"<(?:h\d|title)[^>]*>\s*(?P<name>[^<]{3,120})\s*</(?:h\d|title)>",
    re.IGNORECASE,
)


def _strip_tags(s: str) -> str:
    """Cheap HTML->text — kept for short fragments only."""
    return html.unescape(re.sub(r"<[^>]+>", " ", s))


def _parse_edition_block(block: str, url: str) -> dict[str, Any]:
    """Extract audio/video capabilities from one edition fragment.

    ``block`` should be the raw HTML for a single edition (a 4K UHD,
    standard Blu-ray, etc.). We don't know block boundaries with 100%
    certainty, so the parser is forgiving: it tests the entire blob
    against our line regexes and aggregates any hits.
    """
    text = _strip_tags(block)

    audio_tracks: list[dict[str, Any]] = []
    has_atmos = False
    has_truehd = False
    for m in _AUDIO_LINE_RE.finditer(text):
        codec = re.sub(r"\s+", " ", m.group("codec")).strip()
        codec_low = codec.lower()
        track = {
            "lang": m.group("lang"),
            "codec": codec,
            "channels": m.group("channels") or "",
        }
        audio_tracks.append(track)
        if "atmos" in codec_low:
            has_atmos = True
        if "truehd" in codec_low:
            has_truehd = True

    has_4k_hdr = False
    vid_match = _VIDEO_LINE_RE.search(text)
    video_codec = ""
    video_res = ""
    if vid_match:
        video_codec = vid_match.group("codec") or ""
        video_res = (vid_match.group("res") or "").lower()
        hdr = (vid_match.group("hdr") or "").strip()
        if video_res in {"2160p", "4k"} and hdr:
            has_4k_hdr = True
        # Fall back: if the edition header says UHD, count it as 4K
    if "4k" in text.lower() or "uhd" in text.lower() or "2160p" in text.lower():
        # Only treat as 4K HDR if there's evidence of HDR encoding
        if re.search(r"hdr|dolby\s+vision", text, re.IGNORECASE):
            has_4k_hdr = True

    header = ""
    m_hdr = _EDITION_HEADER_RE.search(block)
    if m_hdr:
        header = m_hdr.group("name").strip()

    return {
        "name": header,
        "audio_tracks": audio_tracks,
        "has_atmos": has_atmos,
        "has_truehd": has_truehd,
        "has_4k_hdr": has_4k_hdr,
        "video_codec": video_codec,
        "video_res": video_res,
        "url": url,
    }


def _split_editions(html_body: str) -> list[str]:
    """Split the product page into per-edition HTML blocks.

    bluray.com product pages render multiple editions (4K UHD, BD, 3D)
    as sibling blocks introduced by an ``<h3>`` or ``<h2>`` header. We
    use those as split points; if none exist, we treat the whole page
    as a single edition.
    """
    pieces = re.split(r"(?=<h[23][^>]*>)", html_body, flags=re.IGNORECASE)
    return [p for p in pieces if len(p) > 60]  # drop empty fragments


def fetch_product_page(
    url: str, conn: sqlite3.Connection | None = None
) -> dict[str, Any]:
    """Fetch + parse a bluray.com product page.

    Returns:
        ``{"title": str, "year": int|None, "editions": list[dict], "url": str}``.
        ``editions`` entries follow ``_parse_edition_block``'s shape.
        Never raises on parse failure — returns an "editions": [] result.
    """
    own_conn = conn is None
    if own_conn:
        conn = updb.connect()
    assert conn is not None
    try:
        body = _fetch_cached(conn, url)
        if not body:
            return {"title": "", "year": None, "editions": [], "url": url}

        # Title + year from <title>...Movie (2021) Blu-ray | Blu-ray.com</title>
        title = ""
        year: int | None = None
        m = re.search(r"<title>\s*([^<]+?)\s*</title>", body, re.IGNORECASE)
        if m:
            raw = m.group(1)
            ym = re.search(r"\((\d{4})\)", raw)
            if ym:
                year = int(ym.group(1))
                raw = raw[: ym.start()].strip()
            # Strip common suffixes
            raw = re.sub(r"\s*(?:Blu-ray|4K|UHD|DVD)\s*\|.*$", "", raw, flags=re.IGNORECASE)
            raw = re.sub(r"\s*\|\s*Blu-ray\.com.*$", "", raw, flags=re.IGNORECASE)
            title = raw.strip()

        editions = [_parse_edition_block(block, url) for block in _split_editions(body)]
        # Drop editions that yielded no useful audio/video signal
        editions = [
            e for e in editions
            if e["audio_tracks"] or e["has_4k_hdr"] or e["video_codec"]
        ]
        return {"title": title, "year": year, "editions": editions, "url": url}
    finally:
        if own_conn:
            conn.close()


def summarise_best(editions: list[dict[str, Any]]) -> dict[str, Any]:
    """Collapse edition list into a single "best available" summary.

    Priority: 4K HDR edition > Atmos+TrueHD > TrueHD > first entry.
    """
    if not editions:
        return {
            "best_available_label": "",
            "best_source_url": "",
            "has_atmos_available": False,
            "has_truehd_available": False,
            "has_4k_hdr_available": False,
        }

    has_atmos = any(e["has_atmos"] for e in editions)
    has_truehd = any(e["has_truehd"] for e in editions)
    has_4k = any(e["has_4k_hdr"] for e in editions)

    # Pick the "headline" edition: prefer 4K + Atmos, then Atmos, then first.
    pref = (
        next((e for e in editions if e["has_4k_hdr"] and e["has_atmos"]), None)
        or next((e for e in editions if e["has_atmos"]), None)
        or next((e for e in editions if e["has_truehd"]), None)
        or editions[0]
    )

    descriptor_parts: list[str] = []
    if pref["has_4k_hdr"]:
        descriptor_parts.append("4K UHD Blu-ray")
    elif pref["name"]:
        descriptor_parts.append(pref["name"])
    else:
        descriptor_parts.append("Blu-ray")

    codec_parts: list[str] = []
    if pref["has_atmos"]:
        codec_parts.append("TrueHD Atmos")
    elif pref["has_truehd"]:
        codec_parts.append("TrueHD")
    if codec_parts:
        descriptor_parts.append("(" + ", ".join(codec_parts) + ")")

    return {
        "best_available_label": " ".join(descriptor_parts).strip(),
        "best_source_url": pref.get("url", ""),
        "has_atmos_available": has_atmos,
        "has_truehd_available": has_truehd,
        "has_4k_hdr_available": has_4k,
    }
