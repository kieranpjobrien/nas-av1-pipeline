"""Derive a per-file ``content_grade`` from TMDb metadata, and compute the
encode CQ that grade implies.

Background — 2026-05-03 conversation: the existing CQ matrix keys only on
``library_type × resolution × HDR``. That means every 1080p sitcom episode
gets the same CQ as a 1080p prestige drama, which wastes ~30% disk space
on Seinfeld talking-head shots (which compress to nothing visible at much
harsher CQ) while not pushing harder on 1990s tv-animation that could
absolutely take it.

The grade adds two axes on top of the existing matrix:
  * **Production type** (``sitcom``, ``tv_animation``, ``cinema_animation``,
    ``classic_film``, ``default``) — derived from TMDb genre + runtime +
    library_type.
  * **Age** (within ``sitcom`` and ``tv_animation`` only) — older content
    has lower-fidelity masters and can take more compression.

Final encode CQ = base_matrix_cq + grade_offset + age_offset, capped to
[0, 63] and clamped against an absolute floor of 0 (no negative offsets
in the current scheme — the user explicitly chose 'go harder everywhere'
on 2026-05-03 rather than preserving more bits on classic films).

This module is pure (no I/O). The encoder consumes its output via
``resolve_encode_params``; the audit tool consumes it to flag any AV1
file whose stamped CQ doesn't match the rule's target.
"""

from __future__ import annotations

# --- Grade constants -------------------------------------------------------

GRADE_SITCOM = "sitcom"
GRADE_TV_ANIMATION = "tv_animation"
GRADE_TV_ANIMATION_LONG = "tv_animation_long"
GRADE_CINEMA_ANIMATION = "cinema_animation"
GRADE_CLASSIC_FILM = "classic_film"
GRADE_BLOCKBUSTER = "blockbuster"
GRADE_DEFAULT = "default"

ALL_GRADES = (
    GRADE_SITCOM,
    GRADE_TV_ANIMATION,
    GRADE_TV_ANIMATION_LONG,
    GRADE_CINEMA_ANIMATION,
    GRADE_CLASSIC_FILM,
    GRADE_BLOCKBUSTER,
    GRADE_DEFAULT,
)

# Offset added to the base resolution-matrix CQ. Production-type signal.
_GRADE_BASE_OFFSET: dict[str, int] = {
    GRADE_SITCOM: 5,                    # talking-head, static, low motion → +5
    GRADE_TV_ANIMATION: 3,              # flat colours, simple motion → +3
    # 3D CGI animation series (Clone Wars, Bad Batch, ATLA, anime-action).
    # Compresses better than live-action drama but worse than flat-shaded
    # comedy animation: detail-rich character models + dark space gradients
    # need more bits than Bluey/Bob's Burgers but less than Breaking Bad.
    # User-validated 2026-05-06: Bad Batch at CQ 32 (4K SDR base 30 + 2)
    # is the sweet spot.
    GRADE_TV_ANIMATION_LONG: 2,
    GRADE_DEFAULT: 0,
    GRADE_CINEMA_ANIMATION: 0,          # detail-rich CGI for cinema → no offset
    GRADE_CLASSIC_FILM: 1,              # film grain doesn't need pristine bits at HD → +1
    # CGI-heavy spectacle compresses well — smooth gradients, predictable
    # motion vectors, masking from chaotic action scenes. The 4K HDR base
    # CQ of 22 is calibrated for cinematography-driven films (Dune, Blade
    # Runner 2049). Marvel / DC / comic-book blockbusters can take +3
    # without visible artefacts at typical viewing distances. Set 2026-05-03
    # at user's request — Avengers Endgame at default (CQ 22) felt overkill.
    GRADE_BLOCKBUSTER: 3,
}

# Keyword + genre tests for the blockbuster grade. Both must match — keyword
# alone false-positives on Birdman (Drama / superhero theme), genre alone
# false-positives on every action movie. The intersection nails Marvel /
# DC / Star Wars / Sin City without bleeding into prestige cinema.
_BLOCKBUSTER_KEYWORDS = frozenset(
    {
        "superhero",
        "marvel cinematic universe",
        "dc extended universe",
        "based on comic",
    }
)
_BLOCKBUSTER_GENRES = frozenset(
    {
        "action",
        "adventure",
        "science fiction",
        "fantasy",
    }
)

# Cap on total offset so a stacking of grade + age can't push CQ to a level
# where artefacts get visible even on simple content. AV1 NVENC at CQ ~38 is
# the practical ceiling for talking-head sitcom material.
_MAX_TOTAL_OFFSET = 8
_MIN_TOTAL_OFFSET = 0

# Absolute CQ guard rails — never let a config bug drive CQ outside these.
# AV1 supports 0-63 but we never want to go below 18 (visually lossless,
# huge files) or above 45 (visible blocking even on simple content).
_ABSOLUTE_MIN_CQ = 18
_ABSOLUTE_MAX_CQ = 45


def _normalise_genres(tmdb: dict | None) -> set[str]:
    """Return TMDb genres as a lowercased set. TMDb stores genres as a list
    of dicts with ``name`` keys, but some older callers store them as plain
    strings — accept both."""
    if not tmdb:
        return set()
    raw = tmdb.get("genres") or []
    out: set[str] = set()
    for g in raw:
        if isinstance(g, str):
            out.add(g.lower())
        elif isinstance(g, dict):
            name = g.get("name") or ""
            if name:
                out.add(name.lower())
    return out


def _normalise_keywords(tmdb: dict | None) -> set[str]:
    """Return TMDb keywords as a lowercased set. TMDb keywords come as a
    list of plain strings via the scanner's enrichment pipeline."""
    if not tmdb:
        return set()
    raw = tmdb.get("keywords") or []
    return {(k or "").lower() for k in raw if k}


def _entry_year(entry: dict) -> int | None:
    """Pull the most-relevant year for grading.

    For series we use ``first_air_date`` (the show started — captures the
    early-90s sitcom signal). For movies, ``release_date``. Returns
    ``None`` if neither is parseable, which means age offset can't apply.
    """
    tmdb = entry.get("tmdb") or {}
    library_type = (entry.get("library_type") or "").lower()
    is_series = library_type in ("series", "show", "tv", "anime")
    candidate = tmdb.get("first_air_date") if is_series else tmdb.get("release_date")
    if not candidate:
        # Fall back to the other field if the primary one is empty
        candidate = tmdb.get("release_date") if is_series else tmdb.get("first_air_date")
    if not candidate or len(candidate) < 4:
        return None
    head = candidate[:4]
    if not head.isdigit():
        return None
    return int(head)


def _entry_runtime_min(entry: dict) -> int | None:
    """Pull the canonical runtime in minutes — TMDb ``runtime`` for movies,
    ``episode_run_time`` (a list, take the first/typical) for series."""
    tmdb = entry.get("tmdb") or {}
    rt = tmdb.get("runtime")
    if rt:
        return int(rt)
    ert = tmdb.get("episode_run_time") or []
    if ert and isinstance(ert, list):
        return int(ert[0])
    # Fall back to the file's own duration if TMDb didn't say
    duration_secs = entry.get("duration_seconds")
    if duration_secs:
        return int(duration_secs / 60)
    return None


def derive_grade(entry: dict) -> str:
    """Classify a media_report entry into a content grade.

    The classifier is intentionally narrow — it only fires on signals
    strong enough to justify a CQ change. Everything not matched falls
    through to ``GRADE_DEFAULT`` (no offset).

    Decision tree (first match wins):
      1. Series + Animation + Action&Adventure → ``tv_animation_long`` (+2)
         Captures 3D CGI action animation (Bad Batch, Clone Wars, ATLA,
         anime-action). Detail-rich character models + dark space gradients
         need more bits than flat-shaded comedy animation, but still less
         than live-action drama.
      2. Series + Animation → ``tv_animation`` (+3)
         Captures flat-shaded comedy animation (Bluey, Bob's Burgers,
         Family Guy, Simpsons, etc.). Flat colours + simple motion
         compress aggressively.
      3. Series + Comedy + ≤30 min runtime → ``sitcom`` (+5)
      4. Movie + Animation → ``cinema_animation`` (+0)
      5. Movie + comic/superhero keyword + action-ish genre → ``blockbuster`` (+3)
      6. Movie + pre-1980 + (Drama OR Romance OR War) → ``classic_film`` (+1)
      7. Otherwise → ``default`` (+0)

    Why genre over runtime for the animation split: pre-2026-05-06 the
    rule was ``animation AND runtime < 25 → tv_animation``. Episodes of
    a single show (Bad Batch) drifted across the 25-min threshold
    naturally — S01E08 at 23 min landed tv_animation, S01E07 at 25 min
    landed default. Same show, two grades. Per-file ``duration_seconds``
    is unreliable when TMDb's ``episode_run_time`` is None. Genre is a
    show-level signal that doesn't drift across episodes.

    Animation grades come BEFORE sitcom. Bob's Burgers is animated AND
    comedy; we want it in tv_animation (+3) not sitcom (+5) because
    the animated frame structure dominates the compression budget.

    Blockbuster goes BEFORE classic_film so e.g. Superman (1978) lands
    in the VFX-heavy bucket rather than getting the gentler classic
    treatment. Animated superhero films (Spider-Verse, Big Hero 6)
    stay in cinema_animation because the animated frame structure
    dominates the compression budget.
    """
    library_type = (entry.get("library_type") or "").lower()
    is_series = library_type in ("series", "show", "tv", "anime")
    is_movie = library_type in ("movie", "film")

    genres = _normalise_genres(entry.get("tmdb"))
    runtime = _entry_runtime_min(entry)
    year = _entry_year(entry)

    # Action-tier animation marker. TMDb's TV genre tag is "Action &
    # Adventure" as a single concatenated string (different from movies,
    # which split them). Match on substring so "action & adventure" hits
    # without false-positiving on standalone "comedy" / "family" / etc.
    is_action_animation = bool(
        is_series
        and "animation" in genres
        and any("action" in g or "adventure" in g for g in genres)
    )

    # 1. TV Animation Long — 3D CGI action animation (Bad Batch, Clone Wars,
    #    ATLA, anime-action). Genre signal is more reliable than runtime
    #    because per-episode duration drift across the 25-min boundary used
    #    to flip individual episodes between tv_animation and default within
    #    the same show.
    if is_action_animation:
        return GRADE_TV_ANIMATION_LONG

    # 2. TV Animation — flat-shaded comedy animation (Bluey, Bob's Burgers,
    #    Family Guy). Sits BEFORE sitcom precedence — flat-shading dominates
    #    the compression budget over the talking-head sitcom signal, so
    #    Bob's Burgers gets +3 (tv_animation) not +5 (sitcom).
    if is_series and "animation" in genres:
        return GRADE_TV_ANIMATION

    # 2. Sitcom (live-action talking-head)
    if is_series and "comedy" in genres and runtime is not None and runtime <= 30:
        return GRADE_SITCOM

    # 3. Cinema Animation — animation movies (Pixar / Disney / Ghibli /
    #    Spider-Verse). Stays before blockbuster: an animated comic
    #    adaptation behaves like animation for compression purposes, so
    #    Spider-Verse gets +0 (cinema_animation) not +3 (blockbuster).
    if is_movie and "animation" in genres:
        return GRADE_CINEMA_ANIMATION

    # 4. Blockbuster — Marvel / DC / Star Wars / comic adaptations. CGI
    #    spectacle has smooth gradients and chaotic action that masks
    #    artefacts, so it can take +3 vs the cinematography baseline.
    #    Requires both a comic/superhero keyword AND an action-ish genre
    #    to avoid false-positives like Birdman (Drama with superhero theme).
    if is_movie:
        keywords = _normalise_keywords(entry.get("tmdb"))
        if keywords & _BLOCKBUSTER_KEYWORDS and genres & _BLOCKBUSTER_GENRES:
            return GRADE_BLOCKBUSTER

    # 5. Classic Film
    if is_movie and year is not None and year < 1980 and (
        "drama" in genres or "romance" in genres or "war" in genres
    ):
        return GRADE_CLASSIC_FILM

    return GRADE_DEFAULT


def age_offset(grade: str, year: int | None) -> int:
    """Year-based bonus on top of the grade offset, scoped to grades where
    older content meaningfully has lower-fidelity masters."""
    if year is None:
        return 0
    if grade == GRADE_SITCOM:
        if year < 1995:
            return 3
        if year < 2010:
            return 1
        return 0
    if grade == GRADE_TV_ANIMATION:
        if year < 2000:
            return 2
        return 0
    return 0


def cq_offset(grade: str, year: int | None) -> int:
    """Total CQ offset to apply on top of the base resolution-matrix CQ.
    Capped to the configured ceiling/floor."""
    base = _GRADE_BASE_OFFSET.get(grade, 0)
    age = age_offset(grade, year)
    total = base + age
    return max(_MIN_TOTAL_OFFSET, min(_MAX_TOTAL_OFFSET, total))


def target_cq(base_cq: int, entry: dict) -> tuple[int, str, int]:
    """Compute the grade-aware target CQ for an entry.

    Returns ``(final_cq, grade, offset_applied)``. The offset is what was
    actually added (after the cap), so callers can log a precise
    explanation: "CQ 30 + sitcom +5 + pre-1995 +3 capped at +8 → 38".
    """
    grade = derive_grade(entry)
    year = _entry_year(entry)
    offset = cq_offset(grade, year)
    final = max(_ABSOLUTE_MIN_CQ, min(_ABSOLUTE_MAX_CQ, base_cq + offset))
    return final, grade, offset
