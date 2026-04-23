"""Library analysis endpoints for media report, completion tracking, and quick wins.

Routes:
    GET  /api/media-report        - full media report
    GET  /api/library-completion  - library completion statistics
    GET  /api/completion-missing  - files missing a specific completion category
"""

import time

from fastapi import APIRouter, HTTPException

from paths import MEDIA_REPORT
from pipeline.config import ENG_LANGS, KEEP_LANGS
from pipeline.streams import is_hi_external, is_hi_internal, tmdb_keeper_langs
from server.helpers import read_json_safe

router = APIRouter()

# Simple time-based cache for library completion (polled frequently)
_completion_cache: dict | None = None
_completion_cache_time: float = 0.0
_COMPLETION_CACHE_TTL: float = 5.0


def _norm_lang(raw: str | None) -> str:
    """Return a lowercase, stripped language code (empty string for None)."""
    return (raw or "").lower().strip()


def _stream_lang(stream: dict) -> str:
    """Return the effective language of a stream (language -> detected_language -> 'und')."""
    return _norm_lang(stream.get("language") or stream.get("detected_language") or "und")


def _eng(lang: str | None) -> bool:
    """Return True if the language code is any English variant in ENG_LANGS."""
    return _norm_lang(lang) in ENG_LANGS


def _compliance_for_entry(entry: dict, keep_langs: set[str] | None = None) -> dict:
    """Return per-entry compliance flags for a media_report file entry.

    Args:
        entry: A single file dict from media_report.json.
        keep_langs: Language set considered acceptable for SUBTITLES. Defaults to
            :data:`pipeline.config.KEEP_LANGS` (eng/en/english/und/"").

    Returns a dict with:
        is_av1: bool - video codec is AV1
        audio_ok: bool - audio codec AND language both compliant (False if zero streams)
        audio_codec_ok: bool - every audio stream is EAC-3
        audio_lang_ok: bool - every audio stream language is in keeper set
        subs_ok: bool - exactly one regular English sub, no HI, no foreign
        has_tmdb: bool - entry has a TMDb record
        filename_matches: bool - filename title matches parent folder
        no_foreign_subs: bool - no non-keeper subtitle streams (internal or external)
        violations: list[str] - human-readable reasons for non-compliance

    Key invariant: empty ``audio_streams`` -> ``audio_ok=False`` (zero-audio files are
    damage, not compliance - the 1,787-file incident was caused by
    ``if audio_streams else True``).
    """
    if keep_langs is None:
        keep_langs = KEEP_LANGS

    violations: list[str] = []

    # Video
    is_av1 = entry.get("video", {}).get("codec_raw") == "av1"
    if not is_av1:
        violations.append("video_not_av1")

    # Audio
    audio_streams = entry.get("audio_streams") or []
    if not audio_streams:
        # ZERO-AUDIO is NOT compliant (see header - the 1,787-file incident rule).
        audio_codec_ok = False
        audio_lang_ok = False
        violations.append("audio_zero_streams")
    else:
        audio_codec_ok = all(
            (a.get("codec_raw") or a.get("codec", "")).lower() in ("eac3", "e-ac-3") for a in audio_streams
        )
        if not audio_codec_ok:
            violations.append("audio_codec_not_eac3")

        # Language policy: ORIGINAL LANGUAGE only, not English-only. A Japanese
        # film keeps Japanese audio, not dubbed English. TMDb's original_language
        # is the authoritative source; und/unknown is always acceptable.
        tmdb = entry.get("tmdb") or {}
        orig_lang = _norm_lang(tmdb.get("original_language"))
        audio_keepers = tmdb_keeper_langs(orig_lang)
        if audio_keepers is None:
            # No TMDb original_language -> can't judge; be permissive.
            audio_lang_ok = True
        else:
            audio_lang_ok = all(_stream_lang(a) in audio_keepers for a in audio_streams)
            if not audio_lang_ok:
                violations.append("audio_foreign_language")

    audio_ok = audio_codec_ok and audio_lang_ok

    # Subtitles (internal + external)
    sub_streams = entry.get("subtitle_streams") or []
    ext_subs = entry.get("external_subtitles") or []

    regular_eng_internal = sum(
        1 for s in sub_streams if _eng(s.get("language") or s.get("detected_language")) and not is_hi_internal(s)
    )
    regular_eng_external = sum(
        1 for s in ext_subs if _eng(s.get("language")) and not is_hi_external(s.get("filename") or "")
    )
    regular_eng_count = regular_eng_internal + regular_eng_external

    hi_sub_count = sum(1 for s in sub_streams if is_hi_internal(s)) + sum(
        1 for s in ext_subs if is_hi_external(s.get("filename") or "")
    )

    non_keep_internal = sum(1 for s in sub_streams if _stream_lang(s) not in keep_langs)
    non_keep_external = sum(1 for s in ext_subs if _norm_lang(s.get("language") or "und") not in keep_langs)
    no_foreign_subs = (non_keep_internal + non_keep_external) == 0

    has_english_sub = regular_eng_count == 1
    subs_ok = has_english_sub and no_foreign_subs and hi_sub_count == 0

    if not has_english_sub:
        violations.append("subs_english_count_wrong")
    if not no_foreign_subs:
        violations.append("subs_foreign_present")
    if hi_sub_count > 0:
        violations.append("subs_hi_present")

    has_tmdb = bool(entry.get("tmdb"))
    if not has_tmdb:
        violations.append("no_tmdb")

    filename_matches = bool(entry.get("filename_matches_folder", True))
    if not filename_matches:
        violations.append("filename_mismatch")

    return {
        "is_av1": is_av1,
        "audio_ok": audio_ok,
        "audio_codec_ok": audio_codec_ok,
        "audio_lang_ok": audio_lang_ok,
        "subs_ok": subs_ok,
        "has_tmdb": has_tmdb,
        "filename_matches": filename_matches,
        "no_foreign_subs": no_foreign_subs,
        "violations": violations,
    }


@router.get("/api/media-report")
def get_media_report() -> dict:
    """Return the full media report."""
    data = read_json_safe(MEDIA_REPORT)
    if data is None:
        raise HTTPException(404, "media_report.json not found")
    return data


@router.get("/api/library-completion")
def get_library_completion() -> dict:
    """True library completion: AV1 video + EAC-3 audio + English-only subs."""
    global _completion_cache, _completion_cache_time

    now = time.monotonic()
    if _completion_cache is not None and (now - _completion_cache_time) < _COMPLETION_CACHE_TTL:
        return _completion_cache

    data = read_json_safe(MEDIA_REPORT)
    if data is None:
        raise HTTPException(404, "media_report.json not found")

    files = data.get("files", [])
    total = len(files)

    counts: dict = {
        "total": total,
        "av1": 0,
        "eac3_done": 0,
        "subs_done": 0,
        "no_foreign_subs": 0,
        "fully_done": 0,
        "needs_video": 0,
        "needs_audio": 0,
        "needs_subs": 0,
        "quick_wins_audio": [],
        "quick_wins_subs": [],
    }

    for f in files:
        fp = f.get("filepath", "")
        c = _compliance_for_entry(f)
        is_av1 = c["is_av1"]
        audio_ok = c["audio_ok"]
        subs_ok = c["subs_ok"]
        no_foreign_subs = c["no_foreign_subs"]

        if is_av1:
            counts["av1"] += 1
        else:
            counts["needs_video"] += 1

        if audio_ok:
            counts["eac3_done"] += 1
        elif is_av1:
            counts["needs_audio"] += 1
            if subs_ok:
                counts["quick_wins_audio"].append(fp)

        if subs_ok:
            counts["subs_done"] += 1
        if no_foreign_subs:
            counts["no_foreign_subs"] += 1
        if not subs_ok and is_av1:
            counts["needs_subs"] += 1
            if audio_ok:
                counts["quick_wins_subs"].append(fp)

        if is_av1 and audio_ok and subs_ok:
            counts["fully_done"] += 1

    counts["pct_video"] = round(100 * counts["av1"] / total, 1) if total else 0
    counts["pct_audio"] = round(100 * counts["eac3_done"] / total, 1) if total else 0
    counts["pct_subs"] = round(100 * counts["subs_done"] / total, 1) if total else 0
    counts["pct_no_foreign_subs"] = round(100 * counts["no_foreign_subs"] / total, 1) if total else 0
    counts["pct_done"] = round(100 * counts["fully_done"] / total, 1) if total else 0
    counts["quick_wins_audio_count"] = len(counts["quick_wins_audio"])
    counts["quick_wins_subs_count"] = len(counts["quick_wins_subs"])
    del counts["quick_wins_audio"]
    del counts["quick_wins_subs"]

    # Detailed completion stats for hero display
    has_tmdb = sum(1 for f in files if f.get("tmdb"))
    has_clean_filename = 0
    has_english_filename = 0
    # Union count: files with ANY und stream (audio or sub). Avoids the double-
    # subtract bug where files with both und audio AND und subs counted twice
    # and could drive pct_langs_known below zero.
    files_with_und = 0
    und_langs = {"und", "unk", ""}

    try:
        from pipeline.filename import clean_filename as _cf
    except ImportError:
        _cf = None
    _cf_failed = 0

    for f in files:
        if _cf:
            try:
                clean = _cf(f.get("filepath", ""), f.get("library_type", ""))
                if not clean or clean == f.get("filename"):
                    has_clean_filename += 1
            except Exception:
                _cf_failed += 1
        else:
            # Filename cleaner unavailable - do NOT silently count as clean.
            # Leave unchanged so the metric reflects uncertainty.
            _cf_failed += 1

        # English filename check: filename title portion must ascii_key-match
        # its parent folder. Parent folders are canonically English-titled even
        # for foreign-origin films (e.g. Howl's Moving Castle folder with
        # Japanese audio inside). So NO TMDb-foreign bypass here.
        if f.get("filename_matches_folder", True):
            has_english_filename += 1

        # Union check: is this file "und" (audio or subs)?
        # Use detected_language as a fallback, and treat a detected_language of "und"
        # itself as still-unknown (previously it was counted as "known" because the
        # `not a.get("detected_language")` test was truthy-only).
        def _is_und(stream: dict) -> bool:
            lang = _norm_lang(stream.get("language") or "und")
            if lang not in und_langs:
                return False
            det = _norm_lang(stream.get("detected_language"))
            if det and det not in und_langs:
                return False
            return True

        file_has_und = any(_is_und(a) for a in f.get("audio_streams", []) or []) or any(
            _is_und(s) for s in f.get("subtitle_streams", []) or []
        )
        if file_has_und:
            files_with_und += 1

    counts["has_tmdb"] = has_tmdb
    counts["pct_tmdb"] = round(100 * has_tmdb / total, 1) if total else 0
    counts["has_clean_filename"] = has_clean_filename
    counts["pct_filename"] = round(100 * has_clean_filename / total, 1) if total else 0
    counts["has_english_filename"] = has_english_filename
    counts["pct_english_filename"] = round(100 * has_english_filename / total, 1) if total else 0
    counts["files_with_und"] = files_with_und
    # langs_known = files with NO und streams (audio or subs). Use union, not sum.
    counts["pct_langs_known"] = round(100 * (total - files_with_und) / total, 1) if total else 0
    counts["filename_check_failed"] = _cf_failed
    # legacy field names kept for old frontend callers
    counts["und_audio_files"] = files_with_und
    counts["und_sub_files"] = files_with_und

    # Real gap fill count
    try:
        from pipeline.config import build_config as _bc
        from pipeline.gap_filler import analyse_gaps

        _gap_config = _bc({})
        gap_count = 0
        for f in files:
            if f.get("video", {}).get("codec_raw") == "av1":
                gaps = analyse_gaps(f, _gap_config)
                if gaps.needs_anything:
                    gap_count += 1
        counts["gap_fill_count"] = gap_count
    except Exception:
        counts["gap_fill_count"] = 0

    # Tier breakdown from media report
    tiers: dict[str, dict] = {}
    for f in files:
        codec = f.get("video", {}).get("codec_raw", "?")
        codec_name = f.get("video", {}).get("codec", codec)
        res = f.get("video", {}).get("resolution_class", "?")
        is_av1 = codec == "av1"

        a_streams = f.get("audio_streams", [])
        # Zero audio = NOT ok (see header comment - 1,787 files got misclassified
        # as "Done" in this exact code path for weeks).
        if not a_streams:
            a_ok = False
            a_clean = False
        else:
            a_ok = all((a.get("codec_raw") or a.get("codec", "")).lower() in ("eac3", "e-ac-3") for a in a_streams)
            a_clean = all(i == 0 or _stream_lang(a) in KEEP_LANGS for i, a in enumerate(a_streams))
        s_ok = all(_stream_lang(s) in KEEP_LANGS for s in f.get("subtitle_streams", []))

        if is_av1 and a_ok and a_clean and s_ok:
            tier = "Done"
        elif not is_av1:
            tier = f"{codec_name} {res}"
        elif not a_ok:
            tier = "Audio remux (AV1)"
        else:
            tier = "Cleanup remux (AV1)"

        if tier not in tiers:
            tiers[tier] = {"total": 0, "done": 0}
        tiers[tier]["total"] += 1
        if tier == "Done":
            tiers[tier]["done"] += 1

    counts["tiers"] = [
        {"name": name, "total": t["total"], "done": t["done"]}
        for name, t in sorted(tiers.items(), key=lambda x: -x[1]["total"])
    ]

    _completion_cache = counts
    _completion_cache_time = now
    return counts


@router.get("/api/completion-missing")
def get_completion_missing(category: str) -> dict:
    """Return files missing a specific completion category.

    Categories: video, audio, subs, tmdb, langs, filename
    """
    data = read_json_safe(MEDIA_REPORT)
    if data is None:
        raise HTTPException(404, "media_report.json not found")
    files = data.get("files", [])
    und_langs = {"und", "unk", ""}
    missing: list[dict] = []
    try:
        from pipeline.filename import clean_filename as _cf
    except ImportError:
        _cf = None

    for f in files:
        fp = f.get("filepath", "")
        fn = f.get("filename", "")
        cr = f.get("video", {}).get("codec_raw", "")
        hit = False
        if category == "video":
            hit = cr != "av1"
        elif category == "audio":
            if cr == "av1":
                # Reuse the canonical compliance helper so drill-down matches the
                # completion totals exactly (no drift between code paths).
                c = _compliance_for_entry(f)
                hit = not c["audio_ok"]
        elif category == "subs":
            hit = not all(_stream_lang(s) in KEEP_LANGS for s in f.get("subtitle_streams", []))
        elif category == "tmdb":
            hit = not f.get("tmdb")
        elif category == "langs":
            hit = any(
                _norm_lang(a.get("language") or "und") in und_langs and not a.get("detected_language")
                for a in f.get("audio_streams", [])
            )
            if not hit:
                hit = any(
                    _norm_lang(s.get("language") or "und") in und_langs and not s.get("detected_language")
                    for s in f.get("subtitle_streams", [])
                )
        elif category == "filename":
            if _cf:
                try:
                    clean = _cf(fp, f.get("library_type", ""))
                    hit = clean is not None and clean != fn
                except Exception:
                    pass
        if hit:
            entry: dict = {"filepath": fp, "filename": fn, "library_type": f.get("library_type", "")}
            audio_tracks = []
            for i, a in enumerate(f.get("audio_streams", [])):
                lang = a.get("language") or a.get("detected_language") or "und"
                audio_tracks.append(
                    {
                        "index": i,
                        "codec": a.get("codec", "?"),
                        "language": lang,
                        "channels": a.get("channels", 0),
                        "title": a.get("title", ""),
                    }
                )
            entry["audio_tracks"] = audio_tracks

            sub_tracks = []
            for i, s in enumerate(f.get("subtitle_streams", [])):
                lang = s.get("language") or s.get("detected_language") or "und"
                sub_tracks.append(
                    {
                        "index": i,
                        "codec": s.get("codec", "?"),
                        "language": lang,
                        "title": s.get("title", ""),
                    }
                )
            entry["sub_tracks"] = sub_tracks

            has_english_audio = any(
                _eng(a.get("language") or a.get("detected_language")) for a in f.get("audio_streams", [])
            )
            entry["has_english_audio"] = has_english_audio
            entry["video_codec"] = f.get("video", {}).get("codec", "?")

            if category == "filename" and _cf:
                try:
                    entry["suggested_name"] = _cf(fp, f.get("library_type", ""))
                except Exception:
                    pass

            if f.get("tmdb"):
                entry["tmdb_title"] = f["tmdb"].get("director") or ", ".join(f["tmdb"].get("created_by", []))

            missing.append(entry)

    return {"category": category, "count": len(missing), "files": missing[:500]}
