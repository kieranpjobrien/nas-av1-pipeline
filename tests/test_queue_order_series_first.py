"""Queue ordering: series (smallest-first) before movies (largest-first).

Operator's call 2026-07-26, while the library is still being acquired: the
encoder should try to catch up with the downloads. Small episodes complete in
minutes so the backlog COUNT drops fastest, and movies are deferred because
they most often arrive already HEVC/AV1 — which video_is_finished treats as
done, so that queue is cheap anyway.
"""

from pipeline.__main__ import _sort_full_gamut


def _item(path, size, lib):
    return {"filepath": path, "file_size_bytes": size, "library_type": lib}


def _queue():
    return [
        _item("mov_small.mkv", 2_000_000_000, "movie"),
        _item("ep_big.mkv", 900_000_000, "series"),
        _item("mov_big.mkv", 40_000_000_000, "movie"),
        _item("ep_small.mkv", 300_000_000, "series"),
        _item("ep_mid.mkv", 600_000_000, "series"),
    ]


def test_all_series_before_any_movie():
    q = _queue()
    _sort_full_gamut(q, {"encode_queue_order": "series_first"}, set())
    kinds = [i["library_type"] for i in q]
    assert kinds == ["series", "series", "series", "movie", "movie"], "every series must be encoded before any movie"


def test_series_ascending_movies_descending():
    q = _queue()
    _sort_full_gamut(q, {"encode_queue_order": "series_first"}, set())
    assert [i["filepath"] for i in q] == [
        "ep_small.mkv",  # series: smallest first
        "ep_mid.mkv",
        "ep_big.mkv",
        "mov_big.mkv",  # movies: largest first
        "mov_small.mkv",
    ]


def test_priority_still_wins_and_stays_smallest_first():
    """A prioritised movie must still jump the whole series queue — the
    operator's explicit intent outranks the type ordering."""
    q = _queue()
    _sort_full_gamut(q, {"encode_queue_order": "series_first"}, {"mov_big.mkv"})
    assert q[0]["filepath"] == "mov_big.mkv"


def test_anime_counts_as_series():
    q = [
        _item("m.mkv", 10_000_000_000, "movie"),
        _item("a.mkv", 500_000_000, "anime"),
    ]
    _sort_full_gamut(q, {"encode_queue_order": "series_first"}, set())
    assert q[0]["filepath"] == "a.mkv"


def test_legacy_flat_orders_still_work():
    q = _queue()
    _sort_full_gamut(q, {"encode_queue_order": "largest_first"}, set())
    assert q[0]["filepath"] == "mov_big.mkv"
    assert q[-1]["filepath"] == "ep_small.mkv"

    q = _queue()
    _sort_full_gamut(q, {"encode_queue_order": "smallest_first"}, set())
    assert q[0]["filepath"] == "ep_small.mkv"
    assert q[-1]["filepath"] == "mov_big.mkv"
