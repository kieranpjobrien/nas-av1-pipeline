"""Fix audio tracks whose language TAG lies, using whisper to hear the truth.

2026-08-22: every Bluey episode played Taiwanese. The releases ship 24 audio
tracks with *every one* tagged ``eng``, so the pipeline's language-based strip
had nothing foreign to remove and kept a Chinese track as stream 0 - which
players then pick by default.

    track 0: zh (0.99)  太不理 放我收拾出好的雜草
    track 1: en (0.97)  "I'd like to pick up these grass-clippings..."
    track 2: zh (0.99)  喂,保養,我要你把雜草放進家手推車

The strip logic was never wrong. It reads ``detected_language`` (whisper) in
preference to the tag, exactly as it should - but ``faster_whisper`` was not
installed in the venv, so detection returned nothing every time and the code
fell back to the mislabelled tag. Same reason the language-detect passes kept
reporting ``phase1=0 phase2=0 phase3=0``.

This does the minimum needed to let the normal machinery work:
  * listen to a sample of each audio track and detect the real language,
  * write that language back with mkvpropedit,
  * set the first track matching the title's original language as default.

Deliberately NON-DESTRUCTIVE: mkvpropedit edits metadata in place. No remux,
no re-encode, no track removal. If a detection is wrong the cost is one bad
tag, not a lost track. Removing the foreign tracks is left to the pipeline's
existing strip, which becomes correct once the tags are honest.

Run:  uv run python -m tools.retag_audio_languages --match Bluey
      uv run python -m tools.retag_audio_languages --match Bluey --execute
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DASHBOARD = "http://127.0.0.1:8000/api/media-report"

# Sample position and length per track. Far enough in to clear titles/silence,
# short enough that a 24-track file does not take all night.
SAMPLE_OFFSET = 200
SAMPLE_SECS = 20
# Below this, treat the detection as unusable and leave the tag alone rather
# than replacing a wrong tag with a differently wrong one.
MIN_CONFIDENCE = 0.70

ISO2_TO_3 = {
    "en": "eng",
    "zh": "zho",
    "es": "spa",
    "fr": "fra",
    "de": "deu",
    "it": "ita",
    "pt": "por",
    "ru": "rus",
    "ja": "jpn",
    "ko": "kor",
    "nl": "nld",
    "sv": "swe",
    "da": "dan",
    "no": "nor",
    "fi": "fin",
    "cs": "ces",
    "el": "ell",
    "pl": "pol",
    "tr": "tur",
    "hu": "hun",
    "ro": "ron",
    "sk": "slk",
    "ar": "ara",
    "he": "heb",
    "hi": "hin",
    "id": "ind",
    "th": "tha",
    "vi": "vie",
    "uk": "ukr",
}


def mkvmerge() -> str:
    return shutil.which("mkvmerge") or r"C:\Program Files\MKVToolNix\mkvmerge.exe"


def mkvpropedit() -> str:
    return shutil.which("mkvpropedit") or r"C:\Program Files\MKVToolNix\mkvpropedit.exe"


def audio_tracks(path: str) -> list[dict]:
    """Audio tracks as mkvmerge sees them, in file order."""
    out = subprocess.run(
        [mkvmerge(), "--identification-format", "json", "--identify", path],
        capture_output=True,
        text=True,
        timeout=180,
        encoding="utf-8",
        errors="replace",
    )
    if out.returncode != 0:
        return []
    return [t for t in json.loads(out.stdout).get("tracks", []) if t.get("type") == "audio"]


def detect_track(path: str, audio_index: int, model) -> tuple[str | None, float]:
    """(iso639-1, confidence) for one audio track, by listening to it."""
    with tempfile.TemporaryDirectory() as tmp:
        wav = os.path.join(tmp, "s.wav")
        r = subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                str(SAMPLE_OFFSET),
                "-t",
                str(SAMPLE_SECS),
                "-i",
                path,
                "-map",
                f"0:a:{audio_index}",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-c:a",
                "pcm_s16le",
                wav,
                "-y",
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if r.returncode != 0 or not os.path.exists(wav):
            return None, 0.0
        _segments, info = model.transcribe(wav, language=None, beam_size=1)
        return info.language, float(info.language_probability or 0.0)


def plan_for(path: str, original_language: str, model) -> dict:
    """What we would change for one file. No side effects."""
    tracks = audio_tracks(path)
    detections = []
    for i, t in enumerate(tracks):
        lang, conf = detect_track(path, i, model)
        tagged = ((t.get("properties") or {}).get("language") or "und").lower()
        detections.append(
            {
                "audio_index": i,
                "track_id": t.get("id"),
                "tagged": tagged,
                "detected": lang,
                "confidence": conf,
                "name": (t.get("properties") or {}).get("track_name") or "",
            }
        )
    # The default should be the first track actually in the original language.
    want = (original_language or "en").lower()[:2]
    default_idx = next(
        (d["audio_index"] for d in detections if d["detected"] == want and d["confidence"] >= MIN_CONFIDENCE),
        None,
    )
    return {"path": path, "tracks": detections, "default_audio_index": default_idx}


def apply_plan(plan: dict) -> tuple[int, int]:
    """Write corrected tags + default flag. Returns (tags_written, default_set)."""
    args = [mkvpropedit(), plan["path"]]
    tags = 0
    for d in plan["tracks"]:
        iso3 = ISO2_TO_3.get(d["detected"] or "")
        if not iso3 or d["confidence"] < MIN_CONFIDENCE:
            continue
        if d["tagged"] == iso3:
            continue  # already correct
        args += ["--edit", f"track:a{d['audio_index'] + 1}", "--set", f"language={iso3}"]
        tags += 1
    dflt = 0
    if plan["default_audio_index"] is not None:
        for d in plan["tracks"]:
            flag = "1" if d["audio_index"] == plan["default_audio_index"] else "0"
            args += ["--edit", f"track:a{d['audio_index'] + 1}", "--set", f"flag-default={flag}"]
        dflt = 1
    if tags == 0 and dflt == 0:
        return 0, 0
    r = subprocess.run(args, capture_output=True, text=True, timeout=300, encoding="utf-8", errors="replace")
    if r.returncode >= 2:
        print(f"    mkvpropedit failed: {(r.stdout or r.stderr)[:120]}")
        return 0, 0
    return tags, dflt


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--match", required=True, help="substring of the filepath to process")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--model", default="tiny")
    args = ap.parse_args()

    rep = json.load(urllib.request.urlopen(DASHBOARD, timeout=180))
    files = [f for f in (rep.get("files") or []) if args.match.lower() in f.get("filepath", "").lower()]
    files = [f for f in files if len(f.get("audio_streams") or []) > 1]
    if args.limit:
        files = files[: args.limit]
    print(f"{len(files)} multi-track files matching {args.match!r}")
    if not files:
        return

    os.environ.setdefault("WHISPER_FORCE_CPU", "1")  # never take a CUDA context beside NVENC
    from faster_whisper import WhisperModel

    model = WhisperModel(args.model, device="cpu", compute_type="int8")

    retagged = defaulted = skipped = 0
    for n, f in enumerate(files, 1):
        orig = (f.get("tmdb") or {}).get("original_language") or "en"
        plan = plan_for(f["filepath"], orig, model)
        if not plan["tracks"]:
            skipped += 1
            continue
        summary = " ".join(
            f"{d['audio_index']}:{d['detected'] or '?'}{'*' if d['audio_index'] == plan['default_audio_index'] else ''}"
            for d in plan["tracks"]
        )
        print(f"  [{n}/{len(files)}] {os.path.basename(f['filepath'])[:46]}  {summary}")
        if plan["default_audio_index"] is None:
            print(f"      no track detected as {orig!r} - leaving alone")
            skipped += 1
            continue
        if args.execute:
            t, d = apply_plan(plan)
            retagged += t
            defaulted += d
    print(f"\ntags corrected: {retagged}   defaults set: {defaulted}   skipped: {skipped}")
    if not args.execute:
        print("(dry run - pass --execute to write)")


if __name__ == "__main__":
    main()
