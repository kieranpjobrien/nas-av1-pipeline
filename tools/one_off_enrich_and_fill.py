"""Enrich missing TMDb + fill residual UND from TMDb fallback.

Run after a rescan picks up new files. Idempotent - re-runs do nothing if the
gaps are already closed.

2026-08-17: this ran twice against a 1,568-entry gap and closed NONE of it,
reporting success both times. Two defects, each of which alone was enough:

1. It read the whole report, held it in memory for the ~40 minutes the TMDb
   lookups take, then wrote it back wholesale with os.replace(). Every other
   writer (pipeline/language.py, pipeline/metadata.py, full_gamut) goes
   through report_lock.patch_report. So the pipeline's writes during those 40
   minutes were clobbered, and the pipeline's own in-flight copy clobbered the
   enrichment right back. That is rule 13, and the exact shape of the
   2026-04-29 report wipe.

   Now: lookups happen OUTSIDE the lock (they are slow and need no lock), and
   results are merged in small batches INSIDE patch_report, keyed by filepath.

2. ``except Exception: pass`` around each lookup. A failing API key, a rate
   limit, a parse error - all silently counted as "nothing to do". Failures
   are now counted and the reasons reported.
"""

from __future__ import annotations

import collections
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

from paths import TMDB_API_KEY  # noqa: E402
from pipeline.metadata import _clean_show_name, parse_movie_filename  # noqa: E402
from tools.report_lock import patch_report, read_report  # noqa: E402

TMDB = "https://api.themoviedb.org/3"
ISO2_TO_3 = {
    "en": "eng",
    "es": "spa",
    "fr": "fra",
    "de": "deu",
    "it": "ita",
    "pt": "por",
    "ru": "rus",
    "ja": "jpn",
    "ko": "kor",
    "zh": "zho",
    "ar": "ara",
    "tr": "tur",
    "pl": "pol",
    "nl": "nld",
    "sv": "swe",
    "da": "dan",
    "no": "nor",
    "fi": "fin",
    "cs": "ces",
    "el": "ell",
    "he": "heb",
    "hi": "hin",
    "id": "ind",
    "th": "tha",
    "vi": "vie",
    "hu": "hun",
    "ro": "ron",
    "uk": "ukr",
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


def _lookup(entry: dict) -> dict | None:
    """TMDb blob for one entry, or None. Raises on failure - caller counts it."""
    fp = entry["filepath"]
    lib = entry.get("library_type", "")
    if lib == "movie":
        title, year = parse_movie_filename(entry["filename"])
        if not title:
            return None
        m = _search("movie", title, year)
        if not m:
            return None
        d = _details("movie", m["id"])
        director = next(
            (c["name"] for c in d.get("credits", {}).get("crew", []) if c.get("job") == "Director"),
            None,
        )
        return {
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
    show = os.path.basename(os.path.dirname(os.path.dirname(fp)))
    m = _search("tv", _clean_show_name(show))
    if not m:
        return None
    d = _details("tv", m["id"])
    return {
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


# Results are merged back this many at a time. Small enough that a crash loses
# little, large enough not to thrash the lock the pipeline also needs.
MERGE_BATCH = 25


def _merge(results: dict[str, dict]) -> None:
    """Write ``filepath -> tmdb blob`` into the report under the shared lock."""
    if not results:
        return

    def apply(report: dict) -> None:
        for f in report.get("files") or []:
            blob = results.get(f.get("filepath"))
            if blob and not (f.get("tmdb") or {}).get("tmdb_id"):
                f["tmdb"] = blob

    patch_report(apply)


def main() -> int:
    rep = read_report()
    files = rep.get("files") or []

    # === Enrich missing TMDb ===
    missing = [f for f in files if not (f.get("tmdb") or {}).get("tmdb_id")]
    print(f"TMDb missing: {len(missing)}", flush=True)
    enriched = 0
    no_match = 0
    failures: collections.Counter = collections.Counter()
    pending: dict[str, dict] = {}
    for i, entry in enumerate(missing, 1):
        try:
            blob = _lookup(entry)
            if blob is None:
                no_match += 1
            else:
                pending[entry["filepath"]] = blob
                enriched += 1
        except Exception as exc:  # noqa: BLE001 - counted and reported, never silent
            failures[type(exc).__name__] += 1
        if len(pending) >= MERGE_BATCH:
            _merge(pending)
            pending = {}
            print(
                f"  {i}/{len(missing)} processed, {enriched} enriched, {no_match} no-match, {sum(failures.values())} errors",
                flush=True,
            )
        time.sleep(0.2)
    _merge(pending)
    print(f"  enriched: {enriched}  no_match: {no_match}  errors: {sum(failures.values())}")
    if failures:
        print(f"  error breakdown: {dict(failures)}")

    # Re-read so the UND pass below sees the merged state, not our stale copy.
    rep = read_report()
    files = rep.get("files") or []
    # === Fill residual UND from TMDb fallback ===
    mkvprop = shutil.which("mkvpropedit") or r"C:\Program Files\MKVToolNix\mkvpropedit.exe"
    mkvm = shutil.which("mkvmerge") or r"C:\Program Files\MKVToolNix\mkvmerge.exe"

    und_files = [
        f
        for f in files
        if any(_is_und(a) for a in (f.get("audio_streams") or []))
        or any(_is_und(s) for s in (f.get("subtitle_streams") or []))
    ]
    print(f"\nUND residual: {len(und_files)}")
    filled = 0
    lang_fixes: dict[str, str] = {}
    for entry in und_files:
        fp = entry["filepath"]
        target = ISO2_TO_3.get(((entry.get("tmdb") or {}).get("original_language") or "").lower())
        if not target:
            print(f"  no_tmdb_lang: {os.path.basename(fp)[:60]}")
            continue
        r = subprocess.run(
            [mkvm, "--identification-format", "json", "--identify", fp],
            capture_output=True,
            text=True,
            timeout=30,
            encoding="utf-8",
            errors="replace",
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
        lang_fixes[fp] = target
        filled += 1
    print(f"  filled: {filled}")

    # Persist the UND language fixes through the same lock. Keyed by filepath
    # so we only touch the streams we actually changed.
    if lang_fixes:

        def apply_langs(report: dict) -> None:
            for f in report.get("files") or []:
                tgt = lang_fixes.get(f.get("filepath"))
                if not tgt:
                    continue
                for stream in (f.get("audio_streams") or []) + (f.get("subtitle_streams") or []):
                    if _is_und(stream):
                        stream["language"] = tgt

        patch_report(apply_langs)

    # Final tally - re-read so the numbers reflect what is actually on disk.
    files = read_report().get("files") or []
    from pipeline.filename import clean_filename

    total = len(files)
    miss_t = sum(1 for f in files if not (f.get("tmdb") or {}).get("tmdb_id"))
    miss_c = 0
    unparsable = 0
    for f in files:
        try:
            c = clean_filename(f.get("filepath", ""), f.get("library_type", ""))
            if c and c != f.get("filename"):
                miss_c += 1
        except Exception:  # noqa: BLE001 - counted, not swallowed
            unparsable += 1
    miss_l = sum(
        1
        for f in files
        if any(_is_und(a) for a in (f.get("audio_streams") or []))
        or any(_is_und(s) for s in (f.get("subtitle_streams") or []))
    )
    print()
    print(f"TMDb Metadata:  {100 * (total - miss_t) / total:.2f}%  ({miss_t} to go)")
    print(
        f"Clean Filename: {100 * (total - miss_c) / total:.2f}%  ({miss_c} to go{f', {unparsable} unparsable' if unparsable else ''})"
    )
    print(f"Langs Known:    {100 * (total - miss_l) / total:.2f}%  ({miss_l} to go)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
