"""Score-based ranking of upgrade candidates.

Combines current-media signals (codec, resolution, Atmos presence) with
best-available signals from the scraper and a small TMDb popularity
nudge, capped at 100.

Rules (applied additively, order-independent):

* Current audio NOT Atmos AND best has Atmos        -> +40, "atmos-upgrade"
* Current video 1080p AND best has 4K HDR           -> +25, "4k-hdr-upgrade"
* Current audio lossy/DTS AND best has TrueHD       -> +15, "lossless-upgrade"
* TMDb popularity > 20                              -> +10, "popular-title"
* TMDb vote_average > 8.0                           -> +5,  "highly-rated"

Everything else returns a score of 0 with no reasons, which the CLI
displays as "no upgrade available".
"""

from __future__ import annotations

from typing import Any

LOSSY_AUDIO_CODECS: frozenset[str] = frozenset({
    "eac3", "eac-3", "ac3", "ac-3", "dolby digital", "dolby digital plus",
    "aac", "mp3", "mp2", "vorbis",
})
DTS_FAMILY: frozenset[str] = frozenset({
    "dts", "dts-hd", "dts hd", "dts-hd ma", "dts hd ma",
    "dts-hd master audio", "dts:x", "dts x",
})


def _is_lossy_or_dts(codec: str | None) -> bool:
    """True if ``codec`` is a lossy codec OR any DTS variant (we prefer TrueHD)."""
    if not codec:
        return False
    c = codec.strip().lower()
    if c in LOSSY_AUDIO_CODECS:
        return True
    if c in DTS_FAMILY:
        return True
    # Partial match for "dolby digital plus 5.1" style strings.
    return any(c.startswith(x) for x in ("dts", "ac3", "eac3", "ac-3", "eac-3", "aac"))


def _is_1080p(res: str | None) -> bool:
    """True if the resolution label indicates 1080p (string form, as we store it)."""
    if not res:
        return False
    r = res.strip().lower()
    return r in {"1080p", "hd", "1080", "full hd"}


def score(current: dict[str, Any], available: dict[str, Any]) -> tuple[int, list[str]]:
    """Compute the upgrade score and reasons.

    Args:
        current: Dict with keys ``current_video_codec``, ``current_video_res``,
            ``current_audio_codec``, ``current_has_atmos``,
            ``tmdb_popularity`` (float), ``tmdb_vote_average`` (float).
            Any missing key is treated as a neutral signal.
        available: Dict with keys ``has_atmos_available``, ``has_truehd_available``,
            ``has_4k_hdr_available`` (all bools / 0-1).

    Returns:
        ``(score, reasons)`` — an int in ``[0, 100]`` and a list of
        human-readable reason codes. Both empty/zero when nothing applies.
    """
    points = 0
    reasons: list[str] = []

    cur_has_atmos = bool(current.get("current_has_atmos"))
    cur_audio = current.get("current_audio_codec") or ""
    cur_res = current.get("current_video_res") or ""

    avail_atmos = bool(available.get("has_atmos_available"))
    avail_truehd = bool(available.get("has_truehd_available"))
    avail_4k_hdr = bool(available.get("has_4k_hdr_available"))

    # Atmos upgrade: largest single win for the Sonos Arc owner.
    if not cur_has_atmos and avail_atmos:
        points += 40
        reasons.append("atmos-upgrade")

    # 4K HDR upgrade from 1080p master.
    if _is_1080p(cur_res) and avail_4k_hdr:
        points += 25
        reasons.append("4k-hdr-upgrade")

    # Lossless upgrade from lossy/DTS. Only fire when TrueHD is actually
    # available (an Atmos-only EAC3-JOC listing doesn't count as lossless).
    if _is_lossy_or_dts(cur_audio) and avail_truehd:
        points += 15
        reasons.append("lossless-upgrade")

    # TMDb popularity / rating nudges — small because they're proxies.
    pop = current.get("tmdb_popularity")
    if isinstance(pop, (int, float)) and pop > 20:
        points += 10
        reasons.append("popular-title")

    vote = current.get("tmdb_vote_average")
    if isinstance(vote, (int, float)) and vote > 8.0:
        points += 5
        reasons.append("highly-rated")

    return min(points, 100), reasons
