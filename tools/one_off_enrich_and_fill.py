"""One-off: enrich missing TMDb + fill residual UND from TMDb fallback.

Run after a rescan picks up new files. Idempotent — re-runs do nothing
if the gaps are already closed.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from paths import MEDIA_REPORT, TMDB_API_KEY  # noqa: E402
from pipeline.metadata import _clean_show_name, parse_movie_filename  # noqa: E402

TMDB = "https://api.themoviedb.org/3"
ISO2_TO_3 = {
    "en": "eng", "es": "spa", "fr": "fra", "de": "deu", "it": "ita",
    "pt": "por", "ru": "rus", "ja": "jpn", "ko": "kor", "zh": "zho",
    "ar": "ara", "tr": "tur", "pl": "pol", "nl": "nld", "sv": "swe",
    "da": "dan", "no": "nor", "fi": "fin", "cs": "ces", "el": "ell",
    "he": "heb", "hi": "hin", "id": "ind", "th": "tha", "vi": "vie",
    "hu": "hun", "ro": "ron", "uk": "ukr",
}


def _search(kind: str, query: str, year: int | None = None) -> dict | None:
    params = {"api_key": TMDB_API_KEY, "query": query}
    if year and kind == "movie":
        params["year"] = year
    url = f"{TMDB}/search/{kind}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=30) as r:
        results = json.loads(r.read().decode("utf-8")).get("results") or []
    return results[0] if results else None


def _details(kind: str, id_: int) -> dict:
    url = f"{TMDB}/{kind}/{id_}?api_key={TMDB_API_KEY}&append_to_response=credits,keywords"
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def _is_und(stream: dict) -> bool:
    lang = (stream.get("language") or "und").lower().strip()
    if lang not in {"und", "unk", ""}:
        return False
    detected = (stream.get("detected_language") or "").lower().strip()
    if detected and detected not in {"und", "unk", ""}:
        return False
    return True


def main() -> int:
    rep_path = Path(MEDIA_REPORT)
    rep = json.loads(rep_path.read_text(encoding="utf-8"))
    files = rep.get("files") or []

    # === Enrich missing TMDb ===
    missing = [f for f in files if not (f.get("tmdb") or {}).get("tmdb_id")]
    print(f"TMDb missing: {len(missing)}")
    enriched = 0
    for entry in missing:
        fp = entry["filepath"]
        lib = entry.get("library_type", "")
        try:
            if lib == "movie":
                title, year = parse_movie_filename(entry["filename"])
                if not title:
                    continue
                m = _search("movie", title, year)
                if not m:
                    continue
                d = _details("movie", m["id"])
                director = next(
                    (c["name"] for c in d.get("credits", {}).get("crew", []) if c.get("job") == "Director"),
                    None,
                )
                entry["tmdb"] = {
                    "tmdb_id": d["id"],
                    "imdb_id": d.get("imdb_id"),
                    "original_language": d.get("original_language"),
                    "genres": [g["name"] for g in d.get("genres", [])],
                    "release_year": year,
                    "release_date": d.get("release_date", ""),
                    "runtime": d.get("runtime"),
                    "director": director,
                    "cast": [c["name"] for c in d.get("credits", {}).get("cast", [])[:10]],
                    "keywords": [k["name"] for k in d.get("keywords", {}).get("keywords", [])][:15],
                    "vote_average": d.get("vote_average"),
                }
                enriched += 1
            else:
                show = os.path.basename(os.path.dirname(os.path.dirname(fp)))
                cleaned = _clean_show_name(show)
                m = _search("tv", cleaned)
                if not m:
                    continue
                d = _details("tv", m["id"])
                entry["tmdb"] = {
                    "tmdb_id": d["id"],
                    "original_language": d.get("original_language"),
                    "genres": [g["name"] for g in d.get("genres", [])],
                    "first_air_date": d.get("first_air_date", ""),
                    "episode_run_time": d.get("episode_run_time") or [],
                    "cast": [c["name"] for c in d.get("credits", {}).get("cast", [])[:10]],
                    "keywords": [k["name"] for k in d.get("keywords", {}).get("results", [])][:15],
                    "created_by": [c["name"] for c in d.get("created_by", [])],
                    "networks": [n["name"] for n in d.get("networks", [])],
                }
                enriched += 1
            time.sleep(0.2)
        except Exception:
            pass
    print(f"  enriched: {enriched}")

    # === Fill residual UND from TMDb fallback ===
    mkvprop = (
        shutil.which("mkvpropedit")
        or r"C:\Program Files\MKVToolNix\mkvpropedit.exe"
    )
    mkvm = shutil.which("mkvmerge") or r"C:\Program Files\MKVToolNix\mkvmerge.exe"

    und_files = [
        f
        for f in files
        if any(_is_und(a) for a in (f.get("audio_streams") or []))
        or any(_is_und(s) for s in (f.get("subtitle_streams") or []))
    ]
    print(f"\nUND residual: {len(und_files)}")
    filled = 0
    for entry in und_files:
        fp = entry["filepath"]
        target = ISO2_TO_3.get(((entry.get("tmdb") or {}).get("original_language") or "").lower())
        if not target:
            print(f"  no_tmdb_lang: {os.path.basename(fp)[:60]}")
            continue
        r = subprocess.run(
            [mkvm, "--identification-format", "json", "--identify", fp],
            capture_output=True, text=True, timeout=30, encoding="utf-8", errors="replace",
        )
        if r.returncode != 0:
            print(f"  identify_fail: {os.path.basename(fp)[:60]}")
            continue
        info = json.loads(r.stdout)
        args = [mkvprop, fp]
        n = 0
        for tr in info.get("tracks", []):
            if tr.get("type") not in ("audio", "subtitles"):
                continue
            cur = ((tr.get("properties") or {}).get("language") or "und").lower()
            if cur in ("und", "unk", ""):
                args.extend(["--edit", f"track:{tr.get('id', 0) + 1}", "--set", f"language={target}"])
                n += 1
        if n == 0:
            continue
        pr = subprocess.run(args, capture_output=True, text=True, timeout=60, encoding="utf-8", errors="replace")
        if pr.returncode >= 2:
            print(f"  write_fail: {os.path.basename(fp)[:60]}")
            continue
        for stream in (entry.get("audio_streams") or []) + (entry.get("subtitle_streams") or []):
            if _is_und(stream):
                stream["language"] = target
        filled += 1
    print(f"  filled: {filled}")

    # Persist
    tmp = rep_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(rep, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, rep_path)

    # Final tally
    from pipeline.filename import clean_filename
    total = len(files)
    miss_t = sum(1 for f in files if not (f.get("tmdb") or {}).get("tmdb_id"))
    miss_c = 0
    for f in files:
        try:
            c = clean_filename(f.get("filepath", ""), f.get("library_type", ""))
            if c and c != f.get("filename"):
                miss_c += 1
        except Exception:
            pass
    miss_l = sum(
        1
        for f in files
        if any(_is_und(a) for a in (f.get("audio_streams") or []))
        or any(_is_und(s) for s in (f.get("subtitle_streams") or []))
    )
    print()
    print(f"TMDb Metadata:  {100 * (total - miss_t) / total:.2f}%  ({miss_t} to go)")
    print(f"Clean Filename: {100 * (total - miss_c) / total:.2f}%  ({miss_c} to go)")
    print(f"Langs Known:    {100 * (total - miss_l) / total:.2f}%  ({miss_l} to go)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
