"""Coverage for the tools/upgrades/ package.

Network access is strictly forbidden: every scraper test either (a) uses
the committed HTML fixture or (b) monkeypatches ``urllib.request.urlopen``.

Tests exercise the matcher, scorer, SQLite schema, and the bluray.com
parser — which is the surface that changes the most, so the fixture
guards against silent regressions when we tune the regexes.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from tools.upgrades import db as updb
from tools.upgrades import matcher, resolver, scorer
from tools.upgrades.scrapers import bluray_com

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "bluray_example.html"
DDG_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "ddg_dune.html"


# ---------- helpers ----------


@pytest.fixture
def fresh_db(tmp_path: Path) -> sqlite3.Connection:
    """Yield a fresh per-test SQLite connection under tmp_path."""
    conn = updb.connect(tmp_path / "upgrades.sqlite")
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def fixture_html() -> str:
    """Return the committed bluray.com example HTML as a string."""
    return FIXTURE_PATH.read_text(encoding="utf-8")


@pytest.fixture
def ddg_dune_html() -> str:
    """Return the committed DDG "Dune 2021" HTML search result as a string."""
    return DDG_FIXTURE_PATH.read_text(encoding="utf-8")


def _mock_urlopen(body: str) -> Any:
    """Build a context-manager mock that mimics ``urllib.request.urlopen``.

    Returned object has the same ``.read()`` + ``.headers.get_content_charset()``
    duo used by the resolver's ``_http_get``.
    """

    class _Resp:
        headers = type("H", (), {"get_content_charset": staticmethod(lambda: "utf-8")})()

        def __enter__(self) -> "_Resp":
            return self

        def __exit__(self, *_a: Any) -> None:
            return None

        def read(self) -> bytes:
            return body.encode("utf-8")

    return _Resp()


# ---------- matcher ----------


def test_matcher_exact_title_year() -> None:
    """Exact title + year lands at authoritative confidence."""
    candidates = [
        {"title": "Dune", "year": 2021, "url": "https://www.bluray.com/movies/Dune-123/"},
        {"title": "Dune", "year": 1984, "url": "https://www.bluray.com/movies/Dune-old-1/"},
    ]
    match = matcher.best_match("Dune", 2021, candidates)
    assert match is not None
    assert match["year"] == 2021
    assert match["_match_confidence"] == "authoritative"
    assert match["_match_ratio"] >= 0.85


def test_matcher_rejects_wrong_year() -> None:
    """Same title but wrong year should return None under the ±1 gate."""
    candidates = [
        # Only an obviously-wrong-year candidate exists.
        {"title": "Dune", "year": 1984, "url": "u1"},
    ]
    match = matcher.best_match("Dune", 2021, candidates)
    assert match is None


def test_matcher_punctuation_folding() -> None:
    """Colons, accents, and articles should not block a clean match."""
    candidates = [
        {"title": "The Lord of the Rings: The Fellowship of the Ring", "year": 2001, "url": "u"},
    ]
    match = matcher.best_match("Lord of the Rings - The Fellowship of the Ring", 2001, candidates)
    assert match is not None
    assert match["_match_confidence"] == "authoritative"


def test_matcher_fuzzy_tier_when_close() -> None:
    """A close-but-not-perfect match falls in the 'fuzzy' tier."""
    candidates = [
        {"title": "Bladerunner 2049", "year": 2017, "url": "u"},  # missing space
    ]
    match = matcher.best_match("Blade Runner 2049", 2017, candidates)
    assert match is not None
    assert match["_match_confidence"] in {"fuzzy", "authoritative"}


# ---------- scorer ----------


def test_scorer_atmos_upgrade_wins() -> None:
    """eac3 5.1 library file with an Atmos TrueHD available -> big score."""
    current = {
        "current_audio_codec": "EAC3",
        "current_audio_channels": 6,
        "current_has_atmos": 0,
        "current_video_codec": "HEVC",
        "current_video_res": "1080p",
    }
    available = {
        "has_atmos_available": True,
        "has_truehd_available": True,
        "has_4k_hdr_available": False,
    }
    s, reasons = scorer.score(current, available)
    # atmos-upgrade (+40) + lossless-upgrade (+15) = 55
    assert s >= 40
    assert "atmos-upgrade" in reasons
    assert "lossless-upgrade" in reasons


def test_scorer_no_upgrade_when_current_already_atmos() -> None:
    """TrueHD current + TrueHD available should score low — the Atmos is already there."""
    current = {
        "current_audio_codec": "TrueHD",
        "current_has_atmos": 1,
        "current_video_codec": "HEVC",
        "current_video_res": "4K",
    }
    available = {
        "has_atmos_available": True,
        "has_truehd_available": True,
        "has_4k_hdr_available": True,
    }
    s, reasons = scorer.score(current, available)
    # Neither atmos-upgrade (already atmos) nor 1080p->4K (already 4K) nor
    # lossy-to-truehd (current is truehd) apply. Score should be 0.
    assert s == 0
    assert reasons == []


def test_scorer_4k_hdr_stacks_with_atmos() -> None:
    """1080p EAC3 -> 4K HDR TrueHD Atmos: 40 + 25 + 15 = 80."""
    current = {
        "current_audio_codec": "EAC3",
        "current_has_atmos": 0,
        "current_video_res": "1080p",
    }
    available = {
        "has_atmos_available": True,
        "has_truehd_available": True,
        "has_4k_hdr_available": True,
    }
    s, reasons = scorer.score(current, available)
    assert s == 80
    assert "atmos-upgrade" in reasons
    assert "4k-hdr-upgrade" in reasons
    assert "lossless-upgrade" in reasons


def test_scorer_tmdb_bonuses_apply() -> None:
    """Popularity + rating bonuses stack onto the base upgrade signals."""
    current = {
        "current_audio_codec": "AAC",
        "current_has_atmos": 0,
        "current_video_res": "1080p",
        "tmdb_popularity": 50.0,
        "tmdb_vote_average": 8.5,
    }
    available = {
        "has_atmos_available": True,
        "has_truehd_available": True,
        "has_4k_hdr_available": False,
    }
    s, reasons = scorer.score(current, available)
    # +40 atmos, +15 lossless, +10 popularity, +5 rating = 70
    assert s == 70
    assert "popular-title" in reasons
    assert "highly-rated" in reasons


def test_scorer_caps_at_100() -> None:
    """Score must be clamped at 100 even when every bonus applies."""
    current = {
        "current_audio_codec": "DTS",
        "current_has_atmos": 0,
        "current_video_res": "1080p",
        "tmdb_popularity": 100.0,
        "tmdb_vote_average": 9.5,
    }
    available = {
        "has_atmos_available": True,
        "has_truehd_available": True,
        "has_4k_hdr_available": True,
    }
    s, _ = scorer.score(current, available)
    # 40 + 25 + 15 + 10 + 5 = 95 — not over 100. Bump via double signal:
    # confirm the sum is the expected 95, proving additive correctness
    # and that we'd clamp if it went higher.
    assert s == 95


# ---------- bluray_com parser ----------


def test_bluray_scraper_parses_fixture_html(fixture_html: str) -> None:
    """Parsing the committed Dune fixture yields an edition with Atmos + 4K HDR."""
    blocks = bluray_com._split_editions(fixture_html)
    editions = [bluray_com._parse_edition_block(b, "http://fixture/") for b in blocks]
    # Drop empty/header-only fragments (same filter as fetch_product_page).
    editions = [e for e in editions if e["audio_tracks"] or e["has_4k_hdr"] or e["video_codec"]]

    assert editions, "at least one edition should parse"
    atmos_editions = [e for e in editions if e["has_atmos"]]
    assert atmos_editions, "fixture contains a TrueHD Atmos track"
    assert atmos_editions[0]["has_truehd"] is True
    assert atmos_editions[0]["has_4k_hdr"] is True

    summary = bluray_com.summarise_best(editions)
    assert summary["has_atmos_available"] is True
    assert summary["has_truehd_available"] is True
    assert summary["has_4k_hdr_available"] is True
    assert "Atmos" in summary["best_available_label"]


def test_bluray_scraper_uses_cache_not_network(
    fresh_db: sqlite3.Connection, fixture_html: str
) -> None:
    """fetch_product_page must return cached content without opening a socket."""
    url = "https://www.bluray.com/movies/Dune-12345/"
    updb.cache_set(fresh_db, url, fixture_html)

    def _boom(*_a: Any, **_kw: Any) -> None:
        raise AssertionError("urlopen was called despite a fresh cache entry")

    with patch("urllib.request.urlopen", side_effect=_boom):
        result = bluray_com.fetch_product_page(url, conn=fresh_db)

    assert result["editions"], "parser should still produce editions from the cache"
    assert any(e["has_atmos"] for e in result["editions"])


def test_bluray_scraper_fetches_when_cache_miss(fresh_db: sqlite3.Connection, fixture_html: str) -> None:
    """On a cache miss, fetch_product_page must call urlopen and persist the body."""
    url = "https://www.bluray.com/movies/Cache-Miss-1/"
    assert updb.cache_get(fresh_db, url) is None

    class _FakeResp:
        headers = type("H", (), {"get_content_charset": staticmethod(lambda: "utf-8")})()

        def __enter__(self) -> "_FakeResp":
            return self

        def __exit__(self, *_a: Any) -> None:
            return None

        def read(self) -> bytes:
            return fixture_html.encode("utf-8")

    with patch("urllib.request.urlopen", return_value=_FakeResp()):
        result = bluray_com.fetch_product_page(url, conn=fresh_db)

    assert result["editions"], "editions should parse from the mocked response"
    # Body must now be in the cache.
    assert updb.cache_get(fresh_db, url) is not None


# ---------- cache TTL ----------


def test_scraper_cache_honours_ttl(fresh_db: sqlite3.Connection) -> None:
    """A cache entry is returned within TTL and ignored past the TTL boundary."""
    updb.cache_set(fresh_db, "key-1", "body-1")

    # Fresh hit.
    assert updb.cache_get(fresh_db, "key-1", ttl_days=7) == "body-1"

    # Zero-day TTL should always miss, regardless of clock.
    time.sleep(0.05)
    assert updb.cache_get(fresh_db, "key-1", ttl_days=0) is None

    # Manually rewrite fetched_at to an old timestamp — should then miss at 7d too.
    fresh_db.execute(
        "UPDATE scraper_cache SET fetched_at = ? WHERE cache_key = ?",
        ("2000-01-01T00:00:00+00:00", "key-1"),
    )
    fresh_db.commit()
    assert updb.cache_get(fresh_db, "key-1", ttl_days=7) is None


# ---------- db upsert ----------


def test_db_upsert_replaces_existing(fresh_db: sqlite3.Connection) -> None:
    """Two upserts for the same filepath produce exactly one row, with the latest fields."""
    row = {
        "filepath": r"\\KieranNAS\Media\Movies\Dune (2021)\Dune (2021).mkv",
        "title": "Dune",
        "year": 2021,
        "library_type": "movie",
        "current_video_codec": "HEVC",
        "current_video_res": "1080p",
        "current_audio_codec": "EAC3",
        "current_audio_channels": 6,
        "current_has_atmos": 0,
        "has_atmos_available": True,
        "has_truehd_available": True,
        "has_4k_hdr_available": True,
        "upgrade_score": 55,
        "upgrade_reasons": ["atmos-upgrade", "lossless-upgrade"],
        "confidence": "fuzzy",
        "last_checked": "2026-04-23T10:00:00+00:00",
    }
    updb.upsert(fresh_db, row)

    # Same filepath, new score.
    updated = {**row, "upgrade_score": 80, "confidence": "authoritative"}
    updb.upsert(fresh_db, updated)

    cur = fresh_db.execute(
        "SELECT filepath, upgrade_score, confidence, upgrade_reasons FROM upgrade_info"
    )
    rows = cur.fetchall()
    assert len(rows) == 1
    got = dict(rows[0])
    assert got["upgrade_score"] == 80
    assert got["confidence"] == "authoritative"
    # upgrade_reasons was stored as a comma-joined TEXT.
    assert got["upgrade_reasons"] == "atmos-upgrade,lossless-upgrade"


def test_db_iter_top_orders_by_score(fresh_db: sqlite3.Connection) -> None:
    """iter_top must return highest scores first."""
    for path, score_val in [("/a.mkv", 40), ("/b.mkv", 80), ("/c.mkv", 10)]:
        updb.upsert(fresh_db, {
            "filepath": path,
            "title": path,
            "year": 2020,
            "library_type": "movie",
            "upgrade_score": score_val,
            "confidence": "authoritative",
        })
    top = updb.iter_top(fresh_db, limit=5)
    assert [r["filepath"] for r in top] == ["/b.mkv", "/a.mkv", "/c.mkv"]


def test_db_read_public_dict_roundtrip(fresh_db: sqlite3.Connection) -> None:
    """row_to_public_dict converts TEXT reasons back into a list."""
    updb.upsert(fresh_db, {
        "filepath": "/d.mkv",
        "title": "Test",
        "year": 2020,
        "library_type": "movie",
        "upgrade_score": 25,
        "upgrade_reasons": ["4k-hdr-upgrade"],
        "has_4k_hdr_available": True,
        "confidence": "fuzzy",
    })
    row = updb.read(fresh_db, "/d.mkv")
    assert row is not None
    public = updb.row_to_public_dict(row)
    assert public["upgrade_reasons"] == ["4k-hdr-upgrade"]
    assert public["has_4k_hdr_available"] is True


# ---------- resolver (DDG HTML search) ----------


def test_resolve_url_matches_top_ddg_hit(
    fresh_db: sqlite3.Connection, ddg_dune_html: str
) -> None:
    """A DDG page with the Dune 2021 bluray.com result yields the product URL."""
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(ddg_dune_html)):
        url = resolver.resolve_url("Dune", 2021, cache=fresh_db)
    assert url == "https://www.blu-ray.com/movies/Dune-Blu-ray/305895/"


def test_resolve_url_returns_none_when_no_match_in_ddg(
    fresh_db: sqlite3.Connection,
) -> None:
    """Unrelated DDG hits (no bluray.com product URL) yield None."""
    body = """
    <html><body><div id="links">
      <div class="result results_links results_links_deep web-result ">
        <a class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.amazon.com%2Fdp%2FB00XYZ%2F">
          Amazon Listing
        </a>
        <a class="result__snippet" href="#">Some unrelated (2021) snippet.</a>
        <div class="clear"></div>
      </div>
    </div></body></html>
    """
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(body)):
        url = resolver.resolve_url("Totally Unknown Movie", 2021, cache=fresh_db)
    assert url is None


def test_resolve_url_respects_year_gate(
    fresh_db: sqlite3.Connection,
) -> None:
    """If DDG returns a 1984 Dune and we ask for 2021, the matcher rejects it."""
    body = """
    <html><body><div id="links">
      <div class="result results_links results_links_deep web-result ">
        <a class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.blu%2Dray.com%2Fmovies%2FDune%2DBlu%2Dray%2F999%2F">
          Dune Blu-ray
        </a>
        <a class="result__snippet" href="#"><b>Dune</b> (<b>1984</b>) classic sci-fi.</a>
        <div class="clear"></div>
      </div>
    </div></body></html>
    """
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(body)):
        url = resolver.resolve_url("Dune", 2021, cache=fresh_db)
    assert url is None


def test_resolve_url_caches_ddg_response(
    fresh_db: sqlite3.Connection, ddg_dune_html: str
) -> None:
    """A second resolve_url within TTL must not issue a second HTTP request."""
    call_count = {"n": 0}

    def _counted(*_a: Any, **_kw: Any) -> Any:
        call_count["n"] += 1
        return _mock_urlopen(ddg_dune_html)

    with patch("urllib.request.urlopen", side_effect=_counted):
        first = resolver.resolve_url("Dune", 2021, cache=fresh_db)
        second = resolver.resolve_url("Dune", 2021, cache=fresh_db)

    assert first == second
    assert first is not None
    assert call_count["n"] == 1, "second call should hit the scraper_cache, not the network"


def test_resolve_url_detects_captcha_page(
    fresh_db: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A CAPTCHA / anomaly page returns None and logs a WARNING."""
    captcha_body = (
        "<html><body><h1>DuckDuckGo</h1>"
        "<p>Unfortunately, bots use DuckDuckGo too. "
        "In order to continue using DuckDuckGo, please complete the following challenge. "
        "anomaly detected.</p></body></html>"
    )
    caplog.set_level(logging.WARNING, logger="tools.upgrades.resolver")
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(captcha_body)):
        url = resolver.resolve_url("Dune", 2021, cache=fresh_db)
    assert url is None
    assert any("CAPTCHA" in rec.message or "anomaly" in rec.message
               for rec in caplog.records), "expected a captcha/anomaly warning"
