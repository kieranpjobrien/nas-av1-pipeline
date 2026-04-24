"""LLM-backed film taste scorer — "is this film worth a home-cinema upgrade?"

Why a separate scorer from ``scorer.py``
---------------------------------------
``scorer.py`` is deterministic: given current state + bluray.com best
available, it computes an upgrade *gap* (4K/HDR/Atmos delta). That's
necessary but not sufficient — Fast 5 has a wide 4K/Atmos gap too, but
you don't actually want to spend time and bytes on it.

This scorer encodes *taste* (the subjective question "does this film
reward a high-fidelity presentation?"), so that the final ranking is

    final = gap_score × (taste_score / 10)

The taste component is produced by Claude against a user-curated seed
list (see ``taste_seeds.json``) — examples in, score out. The prompt is
constructed to be reusable across thousands of calls so prompt caching
shoulders 90%+ of the cost.

Cost model (Opus 4.7, 2026-04-24 pricing, input $5 / output $25 per 1M):
    * System prompt (instructions + ~27 seeds):     ~4500 tokens
    * Per-film user message:                        ~150 tokens
    * Per-film output:                              ~100 tokens

Uncached per call:   ~0.01 USD
Cached per call:     ~0.002 USD (system read at 0.1x)
7000 films cached:   ~$14 one-off, ~$0 to rescore existing (skip-if-fresh)

Cache invariant
---------------
The system prompt is built deterministically — sorted seed order, stable
JSON indentation, no timestamps or random IDs — so the prefix stays
byte-identical across calls. A ``cache_control`` marker sits on the
single system text block. The per-film query lives in the user message,
after the breakpoint, so it never invalidates the cache.

Minimum cacheable prefix on Opus 4.7 is 4096 tokens; the seed set is
sized so the system prompt clears that. If you reduce the seed list,
also check ``response.usage.cache_creation_input_tokens`` on the first
call — if it's 0, the prompt fell below threshold and the cache_control
is silently ignored.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import anthropic
    from pydantic import BaseModel, Field
except ImportError as exc:  # pragma: no cover — surfaced by CLI at runtime
    raise ImportError(
        "taste_scorer requires anthropic + pydantic. "
        "Run: uv sync (after pyproject.toml picks them up)."
    ) from exc

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

MODEL = "claude-opus-4-7"
SEEDS_PATH = Path(__file__).parent / "taste_seeds.json"

# --------------------------------------------------------------------------
# Structured output
# --------------------------------------------------------------------------


class TasteScore(BaseModel):
    """Validated shape of Claude's response.

    ``score`` is capped 0–10; ``rationale`` is bounded so the scorer can't
    blow out token budgets on a single call.
    """

    score: int = Field(..., ge=0, le=10, description="0-10 taste score — see scale in system prompt")
    rationale: str = Field(..., min_length=8, max_length=600, description="1-2 sentences citing specific craft elements")


@dataclass
class ScoreResult:
    """Per-call outcome, including token accounting so callers can audit cache hits."""

    score: int
    rationale: str
    input_tokens: int
    output_tokens: int
    cache_creation_tokens: int
    cache_read_tokens: int

    @property
    def cache_hit(self) -> bool:
        """True when the system-prompt prefix was served from cache."""
        return self.cache_read_tokens > 0


# --------------------------------------------------------------------------
# Seed loading + system prompt assembly
# --------------------------------------------------------------------------


def load_seeds(path: Path = SEEDS_PATH) -> dict[str, Any]:
    """Read the seed JSON. Raises FileNotFoundError with a helpful message."""
    if not path.exists():
        raise FileNotFoundError(
            f"seeds file missing: {path}. "
            "Copy tools/upgrades/taste_seeds.json from the repo or regenerate."
        )
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def seed_version(seeds: dict[str, Any]) -> int:
    """Integer version from the seed file — bumped on edits via the UI so stale
    scores can be detected and rescored automatically."""
    return int(seeds.get("version", 1))


def _format_seed(s: dict[str, Any]) -> str:
    """Render one seed as the system-prompt sees it.

    Order: SCORE x — Title (Year), Director\n    rationale. Matches the
    natural way a human would list calibration points, and keeps the key
    signal (score) at the start of the line so Claude can pattern-match.
    """
    return (
        f"SCORE {s['score']} — {s['title']} ({s['year']}), {s['director']}\n"
        f"    {s['rationale']}"
    )


def build_system_prompt(seeds: dict[str, Any]) -> str:
    """Assemble the full system prompt.

    IMPORTANT: every field that goes into this string must be deterministic.
    - Seeds are sorted (title, year) so JSON reordering can't invalidate cache.
    - No timestamps, UUIDs, or per-request identifiers embedded.
    - Section order is fixed (instructions → scale → weights → seeds → format).
    """
    # Sort for determinism. Title+year is the natural key.
    high = sorted(seeds.get("high", []), key=lambda x: (x["title"], x["year"]))
    low = sorted(seeds.get("low", []), key=lambda x: (x["title"], x["year"]))

    sections = [
        (
            "You are a film-quality evaluator. Your sole job: rate how much a "
            "given film rewards a high-fidelity home-cinema presentation (4K HDR, "
            "Dolby Atmos, large screen, calibrated speakers) relative to a "
            "standard 1080p stream. You are NOT rating the film's entertainment "
            "value, popularity, or star power. You are rating whether the craft "
            "beneath the image and the sound is substantive enough that fidelity "
            "investment genuinely changes what the viewer experiences."
        ),
        (
            "THE SCALE (0–10, integer)\n"
            "\n"
            "10 — Masterful craft where fidelity genuinely transforms the experience.\n"
            "    Rigorous cinematography with intentional composition and lighting.\n"
            "    Sound design that rewards object-based surround. A director who\n"
            "    built the work to survive critical examination. Cultural longevity\n"
            "    — people still reference it years later.\n"
            "\n"
            "8–9 — Strong craft with material fidelity gains. Substantive camera\n"
            "    work, carefully mixed audio, recognisable auteur direction.\n"
            "    Benefits meaningfully from the upgrade; not sacred but not wasted.\n"
            "\n"
            "5–7 — Competent, watchable, fidelity-neutral. A better encode is\n"
            "    nice-to-have but not transformative. Most solid genre films live\n"
            "    here: well-executed within their category but no visible ambition\n"
            "    beyond the category itself.\n"
            "\n"
            "2–4 — Mass-market spectacle with limited craft reward. Marketing-budget\n"
            "    action, committee-directed streaming originals, films where the\n"
            "    image is serviceable but anonymous. Watchable once; upgrade is\n"
            "    cosmetic at best.\n"
            "\n"
            "0–1 — Actively hostile to fidelity. Shaky-cam chaos, streaming-first\n"
            "    compression aesthetics, made-for-TV production values. No upgrade\n"
            "    will change the viewing experience because there was no craft to\n"
            "    reveal."
        ),
        (
            "WEIGHT THESE SIGNALS (in rough order of importance)\n"
            "\n"
            "1. Cinematography — is the image the work of a considered DP? Named\n"
            "   cinematographer with a body of recognised work, or anonymous\n"
            "   digital-capture-and-grade?\n"
            "2. Sound design — is the mix built to reward a surround/Atmos system?\n"
            "   Discrete object placement, intentional silences, subsonic extension\n"
            "   used for effect — or is it flat loud-is-good streaming mix?\n"
            "3. Direction — recognised auteur with consistent sensibility, or\n"
            "   committee/streaming-algorithm-directed?\n"
            "4. Source master — shot on film/IMAX/65mm, or streaming-native\n"
            "   digital at contract bitrates?\n"
            "5. Cultural longevity — still discussed, referenced, restored — or\n"
            "   trended and disappeared? A film that's still being remastered\n"
            "   20 years on has craft underneath; a film forgotten in 6 months\n"
            "   doesn't.\n"
            "6. Genre tell — action/sci-fi/horror/war often reward fidelity.\n"
            "   Straight dialogue comedy, talking-heads drama, sitcoms rarely do.\n"
            "\n"
            "DO NOT WEIGHT\n"
            "- Popularity, box-office, or IMDb rating. A forgettable $300M\n"
            "  blockbuster scores LOWER than a quiet $5M indie if the latter\n"
            "  shows directorial intent.\n"
            "- Franchise or star power. The Fast & Furious franchise is popular;\n"
            "  it's still a 2. A24 arthouse is niche; plenty of it is an 8+.\n"
            "- Whether YOU would watch it. This is about craft density, not taste."
        ),
        (
            "CALIBRATION POINTS — the user has flagged these explicitly. Your "
            "scores must be consistent with this set. If your score for a new "
            "film would imply a different tier than these references suggest, "
            "recheck your reasoning before answering.\n"
            "\n"
            "HIGH-TIER REFERENCES (the kind of film that's worth the upgrade):\n"
            "\n" + "\n\n".join(_format_seed(s) for s in high) +
            "\n\n"
            "LOW-TIER REFERENCES (the kind of film that isn't worth the upgrade):\n"
            "\n" + "\n\n".join(_format_seed(s) for s in low)
        ),
        (
            "EDGE CASES AND COMMON PITFALLS\n"
            "\n"
            "Animated films (Pixar, Ghibli, Illumination, DreamWorks)\n"
            "    Animation is a tricky category because the 'image' is fully\n"
            "    synthesised — there's no grain or lens character to reveal.\n"
            "    Score on (a) artistic direction and shot design, (b) sound\n"
            "    design ambition, (c) whether the studio targeted theatrical\n"
            "    presentation. Pixar at their peak (Wall-E, Inside Out) or\n"
            "    Ghibli (Princess Mononoke, Spirited Away) are 8-10 because the\n"
            "    craft discipline is pervasive. Generic kid-feature cash-grabs\n"
            "    (most Illumination late period, most DreamWorks sequels) are\n"
            "    4-6 — watchable but nothing exceptional to reveal.\n"
            "\n"
            "Horror\n"
            "    Sound design carries horror more than any other genre — the\n"
            "    best horror films (The Thing, The Shining, Hereditary, The\n"
            "    Witch) have sound mixes engineered to exploit silence and\n"
            "    subsonic unease. Score these in the 7-9 range. Generic slasher\n"
            "    or jump-scare-driven horror (most recent Conjuring entries,\n"
            "    Saw sequels) is 3-5 — loud noises and flat coverage don't\n"
            "    reward fidelity.\n"
            "\n"
            "Comedy and drama (dialogue-driven)\n"
            "    Most dialogue-led comedy or chamber drama genuinely is\n"
            "    fidelity-neutral (4-6). The rare exception is the auteur\n"
            "    drama with deliberate visual identity — Anderson, Coens,\n"
            "    Villeneuve, Lanthimos — which can reach 8+. Straight sitcom\n"
            "    features and streaming rom-coms are 1-3.\n"
            "\n"
            "Documentaries\n"
            "    Usually 3-6. Exception: natural-history docs shot in 8K HDR\n"
            "    (Planet Earth, David Attenborough series) are 9-10 — they\n"
            "    justify the investment as pure visual spectacle. Talking-head\n"
            "    Netflix-style docs are 2-3 regardless of subject.\n"
            "\n"
            "Foreign-language films\n"
            "    Do not down-weight for being subtitled. Apply the same\n"
            "    craft criteria. A Kurosawa, Tarkovsky, Kiarostami, or Bong\n"
            "    Joon-ho film deserves its score based on cinematography and\n"
            "    sound, not on Anglophone box-office.\n"
            "\n"
            "Older films (pre-1980)\n"
            "    Classic films with quality restorations (Criterion, studio\n"
            "    archive) can be 8-10 if the original photography was\n"
            "    considered (70mm, 65mm, VistaVision, Technicolor IB prints).\n"
            "    Don't penalise for being black-and-white — well-shot B&W\n"
            "    (Lawrence of Arabia B&W sequences, The Third Man, Citizen\n"
            "    Kane) often rewards fidelity more than mediocre colour.\n"
            "\n"
            "Streaming originals\n"
            "    The dominant signal for low scores in the modern library.\n"
            "    Films shot exclusively for Netflix/Amazon/Apple and released\n"
            "    day-one-to-streaming almost never have theatrical-grade craft.\n"
            "    Exceptions (The Irishman, Roma, Tár) earn their high scores\n"
            "    the normal way — recognised directors, name DPs, source-master\n"
            "    discipline. But the default for streaming-native content is\n"
            "    3 or lower unless there's specific evidence of ambition.\n"
            "\n"
            "'But it's my favourite' isn't a reason to score high\n"
            "    The user explicitly wants taste-driven craft ranking, NOT\n"
            "    personal-preference ranking. If the user's favourite film is\n"
            "    Dumb and Dumber, it should still score 3 — there's nothing\n"
            "    in the craft that rewards a home-cinema upgrade. The user\n"
            "    can watch it anyway; the upgrade filter is about allocating\n"
            "    finite encode/bandwidth budget to films where the effort\n"
            "    actually pays off.\n"
            "\n"
            "When you don't recognise the film\n"
            "    The TMDb synopsis + director + year + genres gives you more\n"
            "    than enough to form a reasonable estimate. Lean on director\n"
            "    filmography you DO know, release year patterns (pre-streaming\n"
            "    era vs post), and genre conventions. If you're genuinely\n"
            "    uncertain, score conservatively (5) and say so in the\n"
            "    rationale. A wrong confident 9 is worse than an honest 5."
        ),
        (
            "OUTPUT FORMAT\n"
            "\n"
            "Return STRICT JSON. No markdown, no preamble, no explanation text\n"
            "around the JSON. Shape:\n"
            "\n"
            "{\"score\": <0-10 integer>, \"rationale\": \"<1-2 sentences, cite "
            "specific craft elements: DP, director, sound design, source master>\"}\n"
            "\n"
            "The rationale must be concrete and film-specific — name the DP, the\n"
            "director's body of work, the sound design specifics. Do not write\n"
            "generic boilerplate like \"well-crafted film with strong\n"
            "cinematography\" — that tells the user nothing. If you don't know the\n"
            "specifics, acknowledge it in the rationale and score conservatively."
        ),
    ]
    return "\n\n---\n\n".join(sections)


# --------------------------------------------------------------------------
# User message assembly
# --------------------------------------------------------------------------


def _format_user_query(
    title: str,
    year: int | None,
    director: str | None,
    genres: list[str] | None,
    overview: str | None,
) -> str:
    """Render the per-film query.

    Kept compact so the volatile portion of the prompt is small — cache
    cost scales with cached-prefix tokens, not this section.
    """
    lines = [f"Film: {title}"]
    if year:
        lines[-1] += f" ({year})"
    if director:
        lines.append(f"Director: {director}")
    if genres:
        lines.append(f"Genres: {', '.join(genres)}")
    if overview:
        # Clip to a reasonable length; TMDb overviews are usually 1-3 paragraphs,
        # and we just need directorial context, not the full synopsis.
        clipped = overview.strip()
        if len(clipped) > 800:
            clipped = clipped[:800].rsplit(" ", 1)[0] + "…"
        lines.append(f"Synopsis: {clipped}")

    lines.append("")
    lines.append(
        "Rate this film's home-cinema upgrade worthiness on the 0-10 scale. "
        "Return JSON only."
    )
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Core scoring call
# --------------------------------------------------------------------------


def score_film(
    client: anthropic.Anthropic,
    title: str,
    year: int | None = None,
    director: str | None = None,
    genres: list[str] | None = None,
    overview: str | None = None,
    *,
    system_prompt: str | None = None,
    seeds: dict[str, Any] | None = None,
) -> ScoreResult:
    """Score one film. Returns ``ScoreResult`` with token accounting.

    Args:
        client: An ``anthropic.Anthropic()`` instance. Reuse across calls —
            each new client rebuilds HTTP connection pools.
        title, year, director, genres, overview: Film metadata.
        system_prompt: Pre-built prompt (reused across calls for cache hits).
            If omitted, the prompt is built from ``seeds`` or the default file.
        seeds: Parsed seed dict. Only used to build the prompt if the prompt
            is omitted.

    Returns:
        ScoreResult with score, rationale, and token usage (including cache
        hit/miss fields for cost auditing).

    Raises:
        anthropic.APIError: Network or API failure. Caller decides on retry.
        pydantic.ValidationError: Claude returned non-conforming JSON. Rare
            with structured outputs but possible on refusal.
    """
    if system_prompt is None:
        system_prompt = build_system_prompt(seeds or load_seeds())

    user_msg = _format_user_query(title, year, director, genres, overview)

    # Adaptive thinking is the only thinking mode on Opus 4.7. Film taste is
    # a genuine reasoning task (weighing seeds vs the query), so giving Claude
    # room to think produces better calibration than flat generation.
    response = client.messages.parse(
        model=MODEL,
        max_tokens=1024,
        thinking={"type": "adaptive"},
        system=[
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_msg}],
        output_format=TasteScore,
    )

    validated: TasteScore = response.parsed_output
    u = response.usage
    return ScoreResult(
        score=int(validated.score),
        rationale=validated.rationale,
        input_tokens=int(getattr(u, "input_tokens", 0) or 0),
        output_tokens=int(getattr(u, "output_tokens", 0) or 0),
        cache_creation_tokens=int(getattr(u, "cache_creation_input_tokens", 0) or 0),
        cache_read_tokens=int(getattr(u, "cache_read_input_tokens", 0) or 0),
    )


# --------------------------------------------------------------------------
# DB persistence
# --------------------------------------------------------------------------

# Schema lives alongside the existing upgrades tables. Added here (not in
# db.py) so the scorer module is self-contained and db.py stays focused on
# the bluray-gap flow. ``connect()`` in db.py runs _SCHEMA which now includes
# taste_scores too (we update that module below).


def persist_score(
    conn: sqlite3.Connection,
    *,
    title: str,
    year: int | None,
    result: ScoreResult,
    seed_ver: int,
) -> None:
    """Upsert one taste_scores row.

    Keyed by (title, year) not filepath — one film, one taste score, even if
    the library has multiple files for the same film.
    """
    conn.execute(
        """
        INSERT OR REPLACE INTO taste_scores
            (title, year, score, rationale, model, seed_version, scored_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            title,
            year,
            int(result.score),
            result.rationale,
            MODEL,
            int(seed_ver),
            time.time(),
        ),
    )
    conn.commit()


def fetch_score(
    conn: sqlite3.Connection, title: str, year: int | None, seed_ver: int
) -> dict[str, Any] | None:
    """Return a cached score if it's present AND matches the current seed version.

    A seed-version mismatch means the user edited the taste-seed list
    (adding/removing references via the UI); the cached score is stale
    relative to the new calibration, so we return None and the caller rescores.
    """
    cur = conn.execute(
        "SELECT * FROM taste_scores WHERE title = ? AND (year = ? OR (year IS NULL AND ? IS NULL))",
        (title, year, year),
    )
    row = cur.fetchone()
    if not row:
        return None
    if int(row["seed_version"]) != int(seed_ver):
        return None
    return dict(row)


def fetch_all_scores(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return every taste_scores row, newest first."""
    cur = conn.execute(
        "SELECT * FROM taste_scores ORDER BY scored_at DESC"
    )
    return [dict(r) for r in cur.fetchall()]
