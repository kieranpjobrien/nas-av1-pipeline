"""Best-effort auto-configure a 'Quality+' profile in Radarr and Sonarr.

What it sets up (if missing, gracefully degrading if blocked):

1. Custom formats that describe things we care about:
   * Atmos, TrueHD, DTS-HD MA  (positive — Atmos is the whole point)
   * Dolby Vision, HDR10, 4K/UHD  (positive — fidelity signals)
   * BluRay Remux, BluRay Encode  (positive — source tier)
   * 3D, SBS, MVC  (negative — user explicitly wants these excluded)
   * Cam/TS/Telesync  (negative — garbage releases)

2. A quality profile called "Quality+" that:
   * Allows up through UHD BluRay Remux on Radarr, WEB-DL/BluRay-2160p on Sonarr
   * Scores the custom formats above per our taste
   * upgrade_allowed = true (Radarr will chase better sources over time)

Design notes
------------
- Everything is fail-soft. If a format already exists, we skip it. If creation
  errors (schema changed, permission denied, etc.), we log and continue.
- The profile clones the widest existing profile (defaults to "Any") and adds
  our format scores on top, rather than constructing one from scratch. This
  inherits Radarr/Sonarr's quality-id conventions without us having to know
  them.
- Safe to re-run. Second invocation is a no-op (idempotent).

Usage:
    uv run python -m tools.radarr_sonarr_setup
    uv run python -m tools.radarr_sonarr_setup --radarr-only
    uv run python -m tools.radarr_sonarr_setup --sonarr-only
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Custom format definitions
# --------------------------------------------------------------------------
# Radarr and Sonarr share the same custom-format schema so these are reusable.
# Each format has `specifications` — a list of predicate objects with `required`
# flags. We use release-title regex specs exclusively (the most portable signal
# across indexers). The `implementation` value is the Radarr/Sonarr class name;
# `ReleaseTitleSpecification` has been stable across v3 of both apps for years.

def _title_spec(name: str, pattern: str, negate: bool = False) -> dict[str, Any]:
    """Release-title regex specification. Matches the indexer's announcement line."""
    return {
        "name": name,
        "implementation": "ReleaseTitleSpecification",
        "negate": bool(negate),
        "required": True,
        "fields": [{"name": "value", "value": pattern}],
    }


# Positive formats — crank scores high so they win against anything else
POSITIVE_FORMATS: list[dict[str, Any]] = [
    {
        "name": "nc-Atmos",
        "includeCustomFormatWhenRenaming": False,
        "specifications": [_title_spec("Atmos", r"\bAtmos\b")],
    },
    {
        "name": "nc-TrueHD",
        "includeCustomFormatWhenRenaming": False,
        "specifications": [_title_spec("TrueHD", r"\b(TRUEHD|TrueHD|True-HD)\b")],
    },
    {
        "name": "nc-DTS-HD-MA",
        "includeCustomFormatWhenRenaming": False,
        "specifications": [_title_spec("DTS-HD MA", r"\bDTS[-. ]?HD[-. ]?MA\b")],
    },
    {
        "name": "nc-Dolby-Vision",
        "includeCustomFormatWhenRenaming": False,
        "specifications": [_title_spec("Dolby Vision", r"\b(DV|Dolby[-. ]?Vision|DoVi)\b")],
    },
    {
        "name": "nc-HDR10",
        "includeCustomFormatWhenRenaming": False,
        "specifications": [_title_spec("HDR10", r"\bHDR(10\+?)?\b")],
    },
    {
        "name": "nc-BluRay-Remux",
        "includeCustomFormatWhenRenaming": False,
        "specifications": [_title_spec("Remux", r"\b(Remux|BluRay[-. ]?REMUX)\b")],
    },
]

# Negative formats — massive negative scores so they're rejected
NEGATIVE_FORMATS: list[dict[str, Any]] = [
    {
        "name": "nc-Reject-3D",
        "includeCustomFormatWhenRenaming": False,
        "specifications": [_title_spec("3D", r"\b(3D|SBS|OU|TAB|MVC|Half[-. ]?SBS|Half[-. ]?OU|Full[-. ]?SBS|Full[-. ]?OU)\b")],
    },
    {
        "name": "nc-Reject-CAM",
        "includeCustomFormatWhenRenaming": False,
        "specifications": [_title_spec("CAM/TS", r"\b(CAM|HDCAM|HDTS|TS|TELESYNC|TELECINE|TC)\b")],
    },
]

# Name -> score to apply on the Quality+ profile
FORMAT_SCORES: dict[str, int] = {
    "nc-Atmos": 5000,
    "nc-TrueHD": 3000,
    "nc-DTS-HD-MA": 1500,
    "nc-Dolby-Vision": 2500,
    "nc-HDR10": 2000,
    "nc-BluRay-Remux": 3500,
    "nc-Reject-3D": -10000,
    "nc-Reject-CAM": -20000,
}

PROFILE_NAME = "Quality+"


# --------------------------------------------------------------------------
# Generic HTTP helpers (stdlib only — consistent with the rest of this tree)
# --------------------------------------------------------------------------


class AppError(RuntimeError):
    """Raised when an API call fails in a way we can't handle locally."""


def _request(
    base_url: str,
    api_key: str,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    body: dict | list | None = None,
    timeout: float = 20.0,
) -> Any:
    """Thin wrapper around urllib. Raises AppError on non-2xx, returns parsed JSON on success."""
    url = base_url.rstrip("/") + path
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url,
        method=method,
        data=data,
        headers={
            "X-Api-Key": api_key,
            "Accept": "application/json",
            **({"Content-Type": "application/json"} if data else {}),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            if not raw:
                return None
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        try:
            body_txt = e.read().decode("utf-8", errors="replace")[:400]
        except Exception:
            body_txt = ""
        raise AppError(f"{e.code} {e.reason} — {method} {path} — {body_txt}") from e
    except urllib.error.URLError as e:
        raise AppError(f"connection failed: {method} {url} — {e.reason}") from e


# --------------------------------------------------------------------------
# Per-app orchestration
# --------------------------------------------------------------------------


def _ensure_custom_formats(
    base_url: str, api_key: str, definitions: list[dict[str, Any]], app_label: str
) -> dict[str, int]:
    """Create any of ``definitions`` that don't already exist. Return name→id map.

    Silent-skips formats that already exist (by name match). Logs per creation.
    Fail-soft: if creating one errors, we log it and continue with the rest.
    """
    try:
        existing = _request(base_url, api_key, "GET", "/api/v3/customformat") or []
    except AppError as e:
        logger.warning("%s: couldn't list custom formats (%s); skipping all creation", app_label, e)
        return {}

    by_name = {f.get("name"): f.get("id") for f in existing if f.get("name")}
    logger.info("%s: %d existing custom formats", app_label, len(by_name))

    for spec in definitions:
        name = spec["name"]
        if name in by_name:
            logger.debug("%s: already present: %s", app_label, name)
            continue
        try:
            created = _request(base_url, api_key, "POST", "/api/v3/customformat", body=spec)
            if isinstance(created, dict) and created.get("id"):
                by_name[name] = created["id"]
                logger.info("%s: created custom format '%s' (id=%s)", app_label, name, created["id"])
            else:
                logger.warning("%s: creation returned no id for %s", app_label, name)
        except AppError as e:
            logger.warning("%s: couldn't create '%s' (%s) — skipping", app_label, name, e)

    return by_name


def _get_template_profile(
    base_url: str, api_key: str, app_label: str
) -> dict[str, Any] | None:
    """Return the widest existing profile to clone from.

    Preference order: 'Any' > 'HD-1080p' > first one. We clone its `items` array
    (which encodes the quality allow-list) rather than build it by hand, because
    the quality IDs / names differ between Radarr and Sonarr versions and
    reconstructing them from scratch is fragile.
    """
    try:
        profiles = _request(base_url, api_key, "GET", "/api/v3/qualityprofile") or []
    except AppError as e:
        logger.warning("%s: couldn't list profiles (%s)", app_label, e)
        return None

    for name in ("Any", "HD - 1080p", "HD-1080p", "Standard"):
        for p in profiles:
            if p.get("name") == name:
                logger.info("%s: cloning template profile '%s'", app_label, name)
                return p
    if profiles:
        logger.info("%s: no named template — cloning first profile '%s'",
                    app_label, profiles[0].get("name"))
        return profiles[0]
    return None


def _build_quality_plus(
    template: dict[str, Any], format_ids: dict[str, int]
) -> dict[str, Any]:
    """Transform a template profile into 'Quality+' with format scoring applied.

    Mutates a deep copy — caller should not reuse the input. Preserves the
    quality `items` array as-is (upgrade-allow semantics) and layers the
    format scores on top. Cutoff is left at the template's value; upgrade
    chases the format scores, not quality tier alone.
    """
    out = json.loads(json.dumps(template))  # deep copy
    out.pop("id", None)
    out["name"] = PROFILE_NAME
    out["upgradeAllowed"] = True

    # Build format-items list using only formats that actually got created.
    # Anything unscored gets score 0 (no-op) so profile validation doesn't
    # complain about unknown ids.
    format_items = []
    for fname, fid in format_ids.items():
        format_items.append({
            "format": fid,
            "name": fname,
            "score": FORMAT_SCORES.get(fname, 0),
        })
    # Radarr/Sonarr REQUIRE entries for EVERY existing custom format. Merge
    # the template's existing formatItems (which has all ids) with our scores.
    template_fmt_items = {fi.get("format"): fi for fi in (template.get("formatItems") or [])}
    our_fmt_items = {fi["format"]: fi for fi in format_items}
    merged = []
    for fid, existing in template_fmt_items.items():
        if fid in our_fmt_items:
            merged.append(our_fmt_items[fid])
        else:
            # Keep the template's score for untouched formats (usually 0)
            merged.append(existing)
    # Add any of our new formats the template didn't know about
    for fid, fi in our_fmt_items.items():
        if fid not in template_fmt_items:
            merged.append(fi)
    out["formatItems"] = merged

    # Minimum score to accept = 0. Negative-scored releases (3D, CAM) get rejected.
    out["minFormatScore"] = 0
    # Encourage upgrades when a better-scored release appears
    out["cutoffFormatScore"] = 5000

    return out


def _ensure_profile(
    base_url: str, api_key: str, profile: dict[str, Any], app_label: str
) -> int | None:
    """Create the Quality+ profile if absent; return its id (or None on failure)."""
    try:
        profiles = _request(base_url, api_key, "GET", "/api/v3/qualityprofile") or []
    except AppError as e:
        logger.warning("%s: couldn't re-list profiles (%s)", app_label, e)
        return None

    for p in profiles:
        if p.get("name") == PROFILE_NAME:
            logger.info("%s: '%s' profile already exists (id=%s) — leaving as-is",
                        app_label, PROFILE_NAME, p.get("id"))
            return p.get("id")

    try:
        created = _request(base_url, api_key, "POST", "/api/v3/qualityprofile", body=profile)
        if isinstance(created, dict) and created.get("id"):
            logger.info("%s: created '%s' profile (id=%s)", app_label, PROFILE_NAME, created["id"])
            return created["id"]
        logger.warning("%s: profile creation returned no id", app_label)
        return None
    except AppError as e:
        logger.warning("%s: couldn't create '%s' profile (%s)", app_label, PROFILE_NAME, e)
        return None


def configure(base_url: str, api_key: str, app_label: str) -> dict[str, Any]:
    """Run the full setup for one app. Returns summary dict."""
    if not base_url or not api_key:
        logger.info("%s: not configured (missing URL or API key) — skipping", app_label)
        return {"configured": False}

    logger.info("%s: %s", app_label, base_url)
    # 1. Custom formats
    all_formats = POSITIVE_FORMATS + NEGATIVE_FORMATS
    format_ids = _ensure_custom_formats(base_url, api_key, all_formats, app_label)

    # 2. Quality profile
    template = _get_template_profile(base_url, api_key, app_label)
    if template is None:
        logger.warning("%s: no template profile available — can't create Quality+", app_label)
        return {"configured": True, "formats": len(format_ids), "profile_id": None}

    profile = _build_quality_plus(template, format_ids)
    profile_id = _ensure_profile(base_url, api_key, profile, app_label)

    return {
        "configured": True,
        "formats": len(format_ids),
        "profile_id": profile_id,
        "profile_name": PROFILE_NAME,
    }


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Auto-configure a Quality+ profile in Radarr and Sonarr.",
    )
    p.add_argument("--radarr-only", action="store_true")
    p.add_argument("--sonarr-only", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    do_radarr = not args.sonarr_only
    do_sonarr = not args.radarr_only
    summary: dict[str, Any] = {}

    if do_radarr:
        summary["radarr"] = configure(
            os.environ.get("RADARR_URL", ""),
            os.environ.get("RADARR_API_KEY", ""),
            "radarr",
        )
    if do_sonarr:
        summary["sonarr"] = configure(
            os.environ.get("SONARR_URL", ""),
            os.environ.get("SONARR_API_KEY", ""),
            "sonarr",
        )

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
