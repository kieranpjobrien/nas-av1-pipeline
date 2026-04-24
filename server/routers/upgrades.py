"""Upgrades recommender API — ranked candidate list + seed CRUD + on-demand rescore.

Routes:
    GET    /api/upgrades/ranked                - combined (gap × taste) ranked list
    GET    /api/upgrades/seeds                 - current taste-seed list
    POST   /api/upgrades/seeds                 - replace seed list (bumps version)
    POST   /api/upgrades/seeds/add             - append one seed
    DELETE /api/upgrades/seeds/{tier}/{title}  - remove one seed
    POST   /api/upgrades/rescore               - score a single film on demand

The ranking UI reads /api/upgrades/ranked. The seed editor reads /api/upgrades/seeds
and sends back full replacement. The on-demand "rescore" button per row calls
/api/upgrades/rescore.

Design note: all heavy work (LLM calls) runs synchronously within the request.
One call takes ~5-15s with adaptive thinking; that's an acceptable latency for a
manual "rescore this title" click. Batch rescoring still goes through the CLI
(``uv run python -m tools.upgrades taste-rescore``) — the API doesn't expose a
batch endpoint because the UI doesn't need one (seed edits are meant to be
infrequent; batch rescore is a deliberate, long-running operation).
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, HTTPException
from pydantic import BaseModel, Field

from tools.upgrades import db as updb

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/upgrades", tags=["upgrades"])


# --------------------------------------------------------------------------
# Ranked list
# --------------------------------------------------------------------------


@router.get("/ranked")
def get_ranked(limit: int = 100) -> dict[str, Any]:
    """Return candidates ordered by ``upgrade_score × (taste_score / 10)``.

    Films without a taste_score yet are included with an assumed neutral 5/10
    so the UI can surface them for the user to rescore rather than hiding
    them entirely. The ``taste_score`` field is explicitly ``null`` in that
    case so the frontend can render "rescore me" instead of a misleading 5.
    """
    limit = max(1, min(int(limit), 500))
    conn = updb.connect()
    try:
        cur = conn.execute(
            """
            SELECT
                u.filepath,
                u.title,
                u.year,
                u.library_type,
                u.current_video_res,
                u.current_audio_codec,
                u.current_has_atmos,
                u.has_atmos_available,
                u.has_4k_hdr_available,
                u.has_truehd_available,
                u.upgrade_score,
                u.upgrade_reasons,
                u.confidence,
                u.best_available_label,
                u.best_source_url,
                t.score AS taste_score,
                t.rationale AS taste_rationale,
                t.scored_at AS taste_scored_at,
                t.seed_version AS taste_seed_version,
                CAST(COALESCE(u.upgrade_score, 0) AS REAL)
                    * (CAST(COALESCE(t.score, 5) AS REAL) / 10.0) AS combined_score
            FROM upgrade_info u
            LEFT JOIN taste_scores t
                ON t.title = u.title
                AND (t.year = u.year OR (t.year IS NULL AND u.year IS NULL))
            WHERE u.upgrade_score IS NOT NULL
            ORDER BY combined_score DESC, u.title ASC
            LIMIT ?
            """,
            (limit,),
        )
        rows: list[dict[str, Any]] = []
        for r in cur.fetchall():
            row = dict(r)
            # Reasons is CSV in DB; render as list to the UI.
            reasons_csv = row.get("upgrade_reasons") or ""
            row["upgrade_reasons"] = [x for x in reasons_csv.split(",") if x]
            # Bool normalisation for the UI
            for col in (
                "current_has_atmos",
                "has_atmos_available",
                "has_4k_hdr_available",
                "has_truehd_available",
            ):
                if row.get(col) is not None:
                    row[col] = bool(row[col])
            rows.append(row)
    finally:
        conn.close()

    return {"count": len(rows), "limit": limit, "candidates": rows}


# --------------------------------------------------------------------------
# Seeds CRUD
# --------------------------------------------------------------------------


class Seed(BaseModel):
    """One seed entry — a user-curated calibration point for the taste scorer."""

    title: str = Field(..., min_length=1, max_length=200)
    year: int = Field(..., ge=1880, le=2100)
    score: int = Field(..., ge=0, le=10)
    director: str = Field("", max_length=200)
    rationale: str = Field(..., min_length=8, max_length=2000)


class SeedBundle(BaseModel):
    """Full seeds-file replacement payload."""

    version: int = Field(..., ge=1)
    high: list[Seed]
    low: list[Seed]


def _seeds_path() -> Path:
    """Locate taste_seeds.json relative to the tools.upgrades package."""
    from tools.upgrades import taste_scorer  # local import — keeps FastAPI cold-start light
    return taste_scorer.SEEDS_PATH


def _read_seeds() -> dict[str, Any]:
    """Read the seeds file or return an empty scaffold if missing."""
    p = _seeds_path()
    if not p.exists():
        return {"version": 1, "high": [], "low": []}
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def _write_seeds(seeds: dict[str, Any]) -> None:
    """Write the seeds file atomically — temp + rename so a mid-write crash
    can't leave us with a half-JSON file (which would then fail loading on
    the next rescore and break the whole flow)."""
    from datetime import date

    p = _seeds_path()
    seeds.setdefault("description", "User-editable taste calibration for tools.upgrades.taste_scorer")
    seeds["updated_at"] = date.today().isoformat()
    tmp = p.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(seeds, f, indent=2, ensure_ascii=False)
    os.replace(str(tmp), str(p))


@router.get("/seeds")
def get_seeds() -> dict[str, Any]:
    """Return the current seed list as stored."""
    return _read_seeds()


@router.post("/seeds")
def put_seeds(bundle: SeedBundle) -> dict[str, Any]:
    """Replace the seed list entirely. Bumps the version so the scorer
    rescores stale rows on next pass.

    The UI's "save seeds" button calls this with the full list. For
    single-entry additions, use ``/api/upgrades/seeds/add`` which handles
    the version bump + array append.
    """
    current = _read_seeds()
    new_version = max(int(bundle.version), int(current.get("version", 1)) + 1)
    out = {
        "version": new_version,
        "high": [s.model_dump() for s in bundle.high],
        "low": [s.model_dump() for s in bundle.low],
    }
    _write_seeds(out)
    return {"ok": True, "version": new_version,
            "high_count": len(out["high"]), "low_count": len(out["low"])}


class SeedAddRequest(BaseModel):
    tier: str = Field(..., pattern="^(high|low)$")
    seed: Seed


@router.post("/seeds/add")
def add_seed(req: SeedAddRequest) -> dict[str, Any]:
    """Append a single seed to high/low and bump the version."""
    seeds = _read_seeds()
    tier_list: list[dict[str, Any]] = list(seeds.get(req.tier, []))
    # Replace-if-exists by (title, year)
    key = (req.seed.title.lower(), req.seed.year)
    tier_list = [
        s for s in tier_list
        if ((s.get("title") or "").lower(), s.get("year")) != key
    ]
    tier_list.append(req.seed.model_dump())
    seeds[req.tier] = tier_list
    seeds["version"] = int(seeds.get("version", 1)) + 1
    _write_seeds(seeds)
    return {"ok": True, "version": seeds["version"], "tier": req.tier,
            "tier_count": len(tier_list)}


@router.delete("/seeds/{tier}")
def delete_seed(tier: str, title: str, year: int) -> dict[str, Any]:
    """Remove one seed from the given tier. Used by the UI's per-row delete
    button. Bumps the version so subsequent rescores reflect the change."""
    if tier not in ("high", "low"):
        raise HTTPException(400, detail="tier must be 'high' or 'low'")
    seeds = _read_seeds()
    tier_list: list[dict[str, Any]] = list(seeds.get(tier, []))
    key = (title.lower(), year)
    kept = [
        s for s in tier_list
        if ((s.get("title") or "").lower(), s.get("year")) != key
    ]
    if len(kept) == len(tier_list):
        raise HTTPException(404, detail=f"{tier} seed not found: {title} ({year})")
    seeds[tier] = kept
    seeds["version"] = int(seeds.get("version", 1)) + 1
    _write_seeds(seeds)
    return {"ok": True, "version": seeds["version"], "tier": tier,
            "removed": {"title": title, "year": year}}


# --------------------------------------------------------------------------
# Single-film rescore
# --------------------------------------------------------------------------


class RescoreRequest(BaseModel):
    title: str = Field(..., min_length=1)
    year: int | None = None
    director: str | None = None
    genres: list[str] = Field(default_factory=list)
    overview: str = ""


@router.post("/rescore")
def rescore(req: RescoreRequest) -> dict[str, Any]:
    """Score one film on demand and persist the result.

    This synchronously calls Claude (~5-15s with adaptive thinking). Returns
    the new score + rationale + cache-hit stats so the UI can show them inline.
    """
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise HTTPException(500, detail="ANTHROPIC_API_KEY not set on server")

    try:
        import anthropic
        from tools.upgrades import taste_scorer
    except ImportError as exc:
        raise HTTPException(500, detail=f"taste_scorer deps missing: {exc}")

    client = anthropic.Anthropic()
    conn = updb.connect()
    try:
        seeds = taste_scorer.load_seeds()
        seed_ver = taste_scorer.seed_version(seeds)
        system_prompt = taste_scorer.build_system_prompt(seeds)
        try:
            result = taste_scorer.score_film(
                client,
                title=req.title,
                year=req.year,
                director=req.director,
                genres=req.genres,
                overview=req.overview,
                system_prompt=system_prompt,
            )
        except anthropic.APIError as exc:
            raise HTTPException(502, detail=f"Claude API error: {exc}")

        taste_scorer.persist_score(
            conn, title=req.title, year=req.year,
            result=result, seed_ver=seed_ver,
        )
        return {
            "ok": True,
            "title": req.title,
            "year": req.year,
            "score": result.score,
            "rationale": result.rationale,
            "cache_hit": result.cache_hit,
            "tokens": {
                "input": result.input_tokens,
                "output": result.output_tokens,
                "cache_read": result.cache_read_tokens,
                "cache_creation": result.cache_creation_tokens,
            },
        }
    finally:
        conn.close()
