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
def get_ranked(limit: int = 100, library_type: str = "all") -> dict[str, Any]:
    """Return candidates ordered by ``upgrade_score × (taste_score / 10)``.

    Three sources get merged:
      * Movies in ``upgrade_info`` (have a bluray.com gap) — full ranking.
      * Series in ``taste_scores`` only (no gap detection for series yet) —
        ranked by raw taste score. The UI still shows them so you can spin
        "Masters of the Air / Band of Brothers are 10/10 → grab the best
        available version" even without a scraper-sourced gap.

    ``library_type`` query filters to ``movie``, ``series``, or ``all`` (default).
    """
    limit = max(1, min(int(limit), 500))
    conn = updb.connect()
    try:
        rows: list[dict[str, Any]] = []

        # Single source-of-truth for library_type: taste_scores.library_type.
        # We LEFT JOIN to upgrade_info for the per-movie bluray.com gap so
        # films with no gap-data still appear (just without the Available
        # column populated). Combined score:
        #   * Movies with a gap  : gap × (taste/10)     → [0, 100]
        #   * Movies without gap : taste × 5            → [0, 50]
        #   * Series             : taste × 10           → [0, 100]
        # The asymmetric weighting keeps high-taste titles visible even
        # without scraper data, while still rewarding real gap evidence
        # when it exists.
        want_types = ("movie", "series") if library_type == "all" else (library_type,)
        placeholders = ",".join("?" * len(want_types))
        cur = conn.execute(
            f"""
            SELECT
                t.title,
                t.year,
                t.library_type,
                t.score                AS taste_score,
                t.rationale            AS taste_rationale,
                t.scored_at            AS taste_scored_at,
                t.seed_version         AS taste_seed_version,
                u.filepath,
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
                CASE
                    WHEN t.library_type = 'series' THEN
                        CAST(t.score AS REAL) * 10.0
                    WHEN u.upgrade_score IS NOT NULL THEN
                        CAST(u.upgrade_score AS REAL) * (CAST(t.score AS REAL) / 10.0)
                    ELSE
                        CAST(t.score AS REAL) * 5.0
                END AS combined_score
            FROM taste_scores t
            LEFT JOIN upgrade_info u
                ON u.title = t.title
                AND (u.year = t.year OR (u.year IS NULL AND t.year IS NULL))
            WHERE t.library_type IN ({placeholders})
            ORDER BY combined_score DESC, t.title ASC
            LIMIT ?
            """,
            (*want_types, limit),
        )
        for r in cur.fetchall():
            row = dict(r)
            reasons_csv = row.get("upgrade_reasons") or ""
            row["upgrade_reasons"] = [x for x in reasons_csv.split(",") if x]
            for col in ("current_has_atmos", "has_atmos_available",
                        "has_4k_hdr_available", "has_truehd_available"):
                if row.get(col) is not None:
                    row[col] = bool(row[col])
            rows.append(row)
    finally:
        conn.close()

    return {"count": len(rows), "limit": limit, "library_type": library_type, "candidates": rows}


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


# --------------------------------------------------------------------------
# Radarr integration — flip profile + trigger search
# --------------------------------------------------------------------------


class RadarrUpgradeRequest(BaseModel):
    """One-click 'ask Radarr to grab a better source' payload from the UI."""

    filepath: str | None = None
    title: str = Field(..., min_length=1)
    year: int | None = None
    quality_profile_id: int = Field(..., ge=1)
    quality_profile_name: str = ""


@router.get("/radarr/profiles")
def get_radarr_profiles() -> dict[str, Any]:
    """Return Radarr's quality profiles, or a ``disabled`` sentinel if creds
    aren't set — the UI uses this to render "Radarr (off)" gracefully."""
    from tools import radarr

    if not radarr.is_configured():
        return {
            "disabled": True,
            "reason": "RADARR_URL and/or RADARR_API_KEY not set",
        }
    try:
        profiles = radarr.list_quality_profiles()
    except radarr.RadarrError as exc:
        raise HTTPException(502, detail=f"Radarr API error: {exc}")
    return {
        "disabled": False,
        "profiles": [
            {"id": p.get("id"), "name": p.get("name")}
            for p in profiles
            if p.get("id") is not None
        ],
    }


@router.post("/radarr/upgrade")
def radarr_upgrade(req: RadarrUpgradeRequest) -> dict[str, Any]:
    """Change a movie's quality profile in Radarr and trigger a search.

    Radarr finds a matching release per its indexers + custom-format scoring
    and hands the grab off to your download client. Our pipeline will notice
    the new file via Bazarr/scanner on the next scan. This endpoint does NOT
    delete the current file — that's the delete button's job, and it should
    be a separate deliberate action.
    """
    from tools import radarr

    if not radarr.is_configured():
        raise HTTPException(400, detail="Radarr not configured (RADARR_URL/RADARR_API_KEY)")
    try:
        result = radarr.upgrade_via_radarr(
            filepath=req.filepath,
            title=req.title,
            year=req.year,
            quality_profile_id=req.quality_profile_id,
        )
    except radarr.RadarrNotConfigured as exc:
        raise HTTPException(400, detail=str(exc))
    except radarr.RadarrError as exc:
        raise HTTPException(502, detail=f"Radarr: {exc}")
    result["profile_name"] = req.quality_profile_name
    return result


# --------------------------------------------------------------------------
# Sonarr integration (mirror of Radarr)
# --------------------------------------------------------------------------


class SonarrUpgradeRequest(BaseModel):
    """Payload for 'ask Sonarr to upgrade this series'."""

    filepath: str | None = None
    title: str = Field(..., min_length=1)
    year: int | None = None
    quality_profile_id: int = Field(..., ge=1)
    quality_profile_name: str = ""


@router.get("/sonarr/profiles")
def get_sonarr_profiles() -> dict[str, Any]:
    """Return Sonarr's quality profiles, or a ``disabled`` sentinel if unconfigured."""
    from tools import sonarr

    if not sonarr.is_configured():
        return {
            "disabled": True,
            "reason": "SONARR_URL and/or SONARR_API_KEY not set",
        }
    try:
        profiles = sonarr.list_quality_profiles()
    except sonarr.SonarrError as exc:
        raise HTTPException(502, detail=f"Sonarr API error: {exc}")
    return {
        "disabled": False,
        "profiles": [
            {"id": p.get("id"), "name": p.get("name")}
            for p in profiles
            if p.get("id") is not None
        ],
    }


@router.post("/sonarr/upgrade")
def sonarr_upgrade(req: SonarrUpgradeRequest) -> dict[str, Any]:
    """Change a series' quality profile in Sonarr and trigger a full-series search."""
    from tools import sonarr

    if not sonarr.is_configured():
        raise HTTPException(400, detail="Sonarr not configured (SONARR_URL/SONARR_API_KEY)")
    try:
        result = sonarr.upgrade_via_sonarr(
            filepath=req.filepath,
            title=req.title,
            year=req.year,
            quality_profile_id=req.quality_profile_id,
        )
    except sonarr.SonarrNotConfigured as exc:
        raise HTTPException(400, detail=str(exc))
    except sonarr.SonarrError as exc:
        raise HTTPException(502, detail=f"Sonarr: {exc}")
    result["profile_name"] = req.quality_profile_name
    return result
