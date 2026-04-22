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
from server.helpers import read_json_safe

router = APIRouter()

# Simple time-based cache for library completion (polled frequently)
_completion_cache: dict | None = None
_completion_cache_time: float = 0.0
_COMPLETION_CACHE_TTL: float = 5.0


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
        is_av1 = f.get("video", {}).get("codec_raw") == "av1"

        audio_streams = f.get("audio_streams", [])
        audio_codec_ok = (
            all((a.get("codec_raw") or a.get("codec", "")).lower() in ("eac3", "e-ac-3") for a in audio_streams)
            if audio_streams
            else True
        )
        # Language policy: ORIGINAL LANGUAGE only, not English-only.
        # A Japanese film keeps Japanese audio, not dubbed English.
        # `tmdb_original_language` is the authoritative source of what counts as original.
        # und/unknown always acceptable (default), plus the TMDb-original language.
        tmdb = f.get("tmdb") or {}
        orig_lang = (tmdb.get("original_language") or "").lower().strip()
        # Map ISO 639-1 (TMDb) → ISO 639-2 (ffprobe) for common languages
        _iso1_to_iso2 = {
            "en": "eng", "ja": "jpn", "ko": "kor", "zh": "chi", "fr": "fre",
            "de": "ger", "es": "spa", "it": "ita", "pt": "por", "ru": "rus",
            "sv": "swe", "no": "nor", "da": "dan", "fi": "fin", "nl": "dut",
            "pl": "pol", "cs": "cze", "hu": "hun", "tr": "tur", "ar": "ara",
            "hi": "hin", "th": "tha", "he": "heb", "el": "gre",
        }
        keeper_langs = {"und", ""}
        if orig_lang:
            keeper_langs.add(orig_lang)  # TMDb iso1 form
            iso2 = _iso1_to_iso2.get(orig_lang)
            if iso2:
                keeper_langs.add(iso2)
        else:
            # No TMDb original_language known → fall back to permissive (anything acceptable)
            keeper_langs = None

        if keeper_langs is not None and audio_streams:
            audio_clean = all(
                (a.get("language") or a.get("detected_language") or "und").lower().strip() in keeper_langs
                for a in audio_streams
            )
        else:
            audio_clean = True
        audio_ok = audio_codec_ok and audio_clean

        sub_streams = f.get("subtitle_streams", [])
        ext_subs = f.get("external_subtitles") or []

        def _is_hi_internal(s: dict) -> bool:
            """Flag hearing-impaired / SDH / closed-caption internal sub streams."""
            disp = s.get("disposition") or {}
            if disp.get("hearing_impaired") or disp.get("captions"):
                return True
            title = (s.get("title") or "").lower()
            if any(tok in title for tok in ("hi", "sdh", "hearing", "caption")):
                # be strict — "hi" alone risks matching "history"; require word boundary
                import re as _re
                if _re.search(r"\b(hi|sdh|hearing|cc|closed.caption)\b", title):
                    return True
            return False

        def _is_hi_external(s: dict) -> bool:
            fn = (s.get("filename") or "").lower()
            parts = fn.split(".")
            return any(p in ("hi", "sdh", "cc") for p in parts[1:-1])

        def _eng(lang: str) -> bool:
            return (lang or "").lower().strip() in ENG_LANGS

        # Regular (non-HI) English subs — internal or external
        regular_eng_internal = sum(
            1 for s in sub_streams
            if _eng(s.get("language") or s.get("detected_language")) and not _is_hi_internal(s)
        )
        regular_eng_external = sum(
            1 for s in ext_subs
            if _eng(s.get("language")) and not _is_hi_external(s)
        )
        regular_eng_count = regular_eng_internal + regular_eng_external

        # HI variants (want zero)
        hi_sub_count = sum(1 for s in sub_streams if _is_hi_internal(s)) + \
                       sum(1 for s in ext_subs if _is_hi_external(s))

        # Foreign (non-keeper) langs
        non_keep_internal = sum(
            1 for s in sub_streams
            if (s.get("language") or s.get("detected_language") or "und").lower().strip() not in KEEP_LANGS
        )
        non_keep_external = sum(
            1 for s in ext_subs
            if (s.get("language") or "und").lower().strip() not in KEEP_LANGS
        )
        no_foreign_subs = (non_keep_internal + non_keep_external) == 0

        # Library policy: exactly ONE regular English sub, no HI, no foreign.
        has_english_sub = regular_eng_count == 1
        subs_ok = has_english_sub and no_foreign_subs and hi_sub_count == 0

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
            # Filename cleaner unavailable — do NOT silently count as clean.
            # Leave unchanged so the metric reflects uncertainty.
            _cf_failed += 1

        # English filename check: matches_folder flag (populated by scanner).
        # A file whose title portion ascii_key-matches its parent folder is
        # treated as having an English (or library-canonical) filename.
        # Files whose TMDb original_language is a non-English language are also
        # OK — the filename being in that language is a legit keeper.
        if f.get("filename_matches_folder", True):
            has_english_filename += 1
        else:
            orig_lang = (((f.get("tmdb") or {}).get("original_language") or "")).lower()
            if orig_lang and orig_lang != "en":
                has_english_filename += 1

        # Union check: is this file "und" (audio or subs)?
        # Use detected_language as a fallback, and treat a detected_language of "und"
        # itself as still-unknown (previously it was counted as "known" because the
        # `not a.get("detected_language")` test was truthy-only).
        def _is_und(stream: dict) -> bool:
            lang = (stream.get("language") or "und").lower().strip()
            if lang not in und_langs:
                return False
            det = (stream.get("detected_language") or "").lower().strip()
            if det and det not in und_langs:
                return False
            return True

        file_has_und = any(_is_und(a) for a in f.get("audio_streams", []) or []) \
                    or any(_is_und(s) for s in f.get("subtitle_streams", []) or [])
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
        a_ok = (
            all((a.get("codec_raw") or a.get("codec", "")).lower() in ("eac3", "e-ac-3") for a in a_streams)
            if a_streams
            else True
        )
        a_clean = (
            all(
                i == 0 or (a.get("language") or a.get("detected_language") or "und").lower().strip() in KEEP_LANGS
                for i, a in enumerate(a_streams)
            )
            if a_streams
            else True
        )
        s_ok = all(
            (s.get("language") or s.get("detected_language") or "und").lower().strip() in KEEP_LANGS
            for s in f.get("subtitle_streams", [])
        )

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
                a_ok = (
                    all(
                        (a.get("codec_raw") or a.get("codec", "")).lower() in ("eac3", "e-ac-3")
                        for a in f.get("audio_streams", [])
                    )
                    if f.get("audio_streams")
                    else True
                )
                a_clean = (
                    all(
                        i == 0
                        or (a.get("language") or a.get("detected_language") or "und").lower().strip() in KEEP_LANGS
                        for i, a in enumerate(f.get("audio_streams", []))
                    )
                    if f.get("audio_streams")
                    else True
                )
                hit = not (a_ok and a_clean)
        elif category == "subs":
            hit = not all(
                (s.get("language") or s.get("detected_language") or "und").lower().strip() in KEEP_LANGS
                for s in f.get("subtitle_streams", [])
            )
        elif category == "tmdb":
            hit = not f.get("tmdb")
        elif category == "langs":
            hit = any(
                (a.get("language") or "und").lower().strip() in und_langs and not a.get("detected_language")
                for a in f.get("audio_streams", [])
            )
            if not hit:
                hit = any(
                    (s.get("language") or "und").lower().strip() in und_langs and not s.get("detected_language")
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
                (a.get("language") or a.get("detected_language") or "").lower().strip() in ENG_LANGS
                for a in f.get("audio_streams", [])
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
