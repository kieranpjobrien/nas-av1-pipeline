"""Library analysis endpoints for media report, completion tracking, and quick wins.

Routes:
    GET  /api/media-report        - full media report
    GET  /api/library-completion  - library completion statistics
    GET  /api/completion-missing  - files missing a specific completion category
"""

from fastapi import APIRouter, HTTPException

from paths import MEDIA_REPORT
from server.helpers import read_json_safe

router = APIRouter()


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
    data = read_json_safe(MEDIA_REPORT)
    if data is None:
        raise HTTPException(404, "media_report.json not found")

    files = data.get("files", [])
    total = len(files)
    keep_langs = {"eng", "en", "english", "und", ""}

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
        audio_clean = (
            all(
                i == 0 or (a.get("language") or a.get("detected_language") or "und").lower().strip() in keep_langs
                for i, a in enumerate(audio_streams)
            )
            if audio_streams
            else True
        )
        audio_ok = audio_codec_ok and audio_clean

        eng_sub_langs = {"eng", "en", "english"}
        sub_streams = f.get("subtitle_streams", [])
        eng_sub_count = sum(
            1
            for s in sub_streams
            if (s.get("language") or s.get("detected_language") or "").lower().strip() in eng_sub_langs
        )
        has_one_eng_sub = eng_sub_count == 1

        non_eng_subs = sum(
            1
            for s in sub_streams
            if (s.get("language") or s.get("detected_language") or "und").lower().strip() not in keep_langs
        )
        no_foreign_subs = non_eng_subs == 0
        subs_ok = has_one_eng_sub and no_foreign_subs

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
    has_und_audio = 0
    has_und_subs = 0
    und_langs = {"und", "unk", ""}

    try:
        from pipeline.filename import clean_filename as _cf
    except ImportError:
        _cf = None

    for f in files:
        if _cf:
            try:
                clean = _cf(f.get("filepath", ""), f.get("library_type", ""))
                if not clean or clean == f.get("filename"):
                    has_clean_filename += 1
            except Exception:
                pass
        else:
            has_clean_filename += 1

        for a in f.get("audio_streams", []):
            lang = (a.get("language") or "und").lower().strip()
            if lang in und_langs and not a.get("detected_language"):
                has_und_audio += 1
                break
        for s in f.get("subtitle_streams", []):
            lang = (s.get("language") or "und").lower().strip()
            if lang in und_langs and not s.get("detected_language"):
                has_und_subs += 1
                break

    counts["has_tmdb"] = has_tmdb
    counts["pct_tmdb"] = round(100 * has_tmdb / total, 1) if total else 0
    counts["has_clean_filename"] = has_clean_filename
    counts["pct_filename"] = round(100 * has_clean_filename / total, 1) if total else 0
    counts["und_audio_files"] = has_und_audio
    counts["und_sub_files"] = has_und_subs
    counts["pct_langs_known"] = round(100 * (total - has_und_audio - has_und_subs) / total, 1) if total else 0

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
                i == 0 or (a.get("language") or a.get("detected_language") or "und").lower().strip() in keep_langs
                for i, a in enumerate(a_streams)
            )
            if a_streams
            else True
        )
        s_ok = all(
            (s.get("language") or s.get("detected_language") or "und").lower().strip() in keep_langs
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
    keep_langs = {"eng", "en", "english", "und", ""}
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
                        or (a.get("language") or a.get("detected_language") or "und").lower().strip() in keep_langs
                        for i, a in enumerate(f.get("audio_streams", []))
                    )
                    if f.get("audio_streams")
                    else True
                )
                hit = not (a_ok and a_clean)
        elif category == "subs":
            hit = not all(
                (s.get("language") or s.get("detected_language") or "und").lower().strip() in keep_langs
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

            eng_audio = {"eng", "en", "english"}
            has_english_audio = any(
                (a.get("language") or a.get("detected_language") or "").lower().strip() in eng_audio
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
