"""Strip dub tracks whose language tag lies, and promote the real one to default.

The Bluey releases are the worst case seen so far: every audio track ships
tagged ``eng``, two of the three are Mandarin, and the Mandarin one carries the
default flag - so players pick it and the episode plays in Chinese.

    S03E15 Explorers
      a1  tag=eng  name=Taiwan  default=TRUE   ->  heard zh (1.00)
      a2  tag=eng  name=-       default=False  ->  heard en (0.35)
      a3  tag=eng  name=-       default=False  ->  heard zh (0.81)

``retag_audio_languages`` fixes the tags but leaves the extra tracks in place
and could not act here anyway: a single 20-second sample through the ``tiny``
model gave the real English track only 0.35 confidence, under its 0.70 floor.

This decides by LISTENING, at three points in the runtime through the ``small``
model, and takes the majority verdict. That lifts the same file to a clear
answer. Then one mkvmerge pass drops the dubs and one mkvpropedit pass fixes
the surviving tags and the default flag.

Safety, in priority order:

  * **Never strip a track we are unsure about.** Below ``MIN_CONFIDENCE`` the
    track is KEPT, whatever the tag claims. Stated inviolate by the operator
    2026-04-29.
  * **Never leave a file with no audio.** If no keeper survives, the file is
    skipped whole and reported.
  * **Keep the original language AND English for animation** - the standing
    dual-audio policy. For an English-original show like Bluey that is just
    English; for Ghibli it keeps Japanese and English both.

Run:
    uv run python -m tools.fix_mislabelled_audio --match Bluey
    uv run python -m tools.fix_mislabelled_audio --match Bluey --execute
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Three samples beat one: a single 20s window can land on music, a silent beat
# or an ad break. Taken at these fractions of runtime.
SAMPLE_POINTS = (0.25, 0.50, 0.72)
SAMPLE_SECS = 30
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


def identify(path: str) -> dict:
    r = subprocess.run(
        [mkvmerge(), "--identification-format", "json", "--identify", path],
        capture_output=True,
        text=True,
        timeout=300,
        encoding="utf-8",
        errors="replace",
    )
    return json.loads(r.stdout) if r.returncode == 0 else {}


def duration_secs(path: str) -> float:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", path],
        capture_output=True,
        text=True,
        timeout=120,
    )
    try:
        return float((r.stdout or "0").strip())
    except ValueError:
        return 0.0


def detect(path: str, audio_index: int, model, dur: float) -> tuple[str | None, float]:
    """Majority language across several samples. (iso639-1, mean confidence)."""
    votes: list[tuple[str, float]] = []
    for frac in SAMPLE_POINTS:
        offset = max(0.0, dur * frac)
        if dur and offset + SAMPLE_SECS > dur:
            offset = max(0.0, dur - SAMPLE_SECS - 1)
        with tempfile.TemporaryDirectory() as tmp:
            wav = os.path.join(tmp, "s.wav")
            r = subprocess.run(
                [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-ss",
                    f"{offset:.2f}",
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
                timeout=600,
            )
            if r.returncode != 0 or not os.path.exists(wav):
                continue
            _seg, info = model.transcribe(wav, language=None, beam_size=1)
            if info.language:
                votes.append((info.language, float(info.language_probability or 0.0)))
    if not votes:
        return None, 0.0
    winner = collections.Counter(lang for lang, _ in votes).most_common(1)[0][0]
    confs = [c for lang, c in votes if lang == winner]
    # Majority share matters as much as the model's own confidence: 3/3 samples
    # agreeing at 0.6 is stronger evidence than 1/3 at 0.9.
    share = len(confs) / len(votes)
    return winner, (sum(confs) / len(confs)) * share


def plan(path: str, original_language: str, model) -> dict:
    tracks = [t for t in identify(path).get("tracks", []) if t.get("type") == "audio"]
    dur = duration_secs(path)
    allowed = {"en", (original_language or "en").lower()[:2]}

    detections = []
    for i, t in enumerate(tracks):
        lang, conf = detect(path, i, model, dur)
        pr = t.get("properties") or {}
        detections.append(
            {
                "audio_index": i,
                "tagged": (pr.get("language") or "und").lower(),
                "name": pr.get("track_name") or "",
                "was_default": bool(pr.get("default_track")),
                "heard": lang,
                "conf": conf,
            }
        )

    keep, drop = [], []
    for d in detections:
        confident_foreign = d["heard"] is not None and d["heard"] not in allowed and d["conf"] >= MIN_CONFIDENCE
        (drop if confident_foreign else keep).append(d)

    skip = ""
    if not keep:
        skip = "every track reads as foreign - refusing to leave the file with no audio"

    # Default goes to the best-evidenced track in the original language, else
    # the best-evidenced English one.
    #
    # NEVER fall back to "the first keeper". Bluey S03E18 Rain detects as
    # br(0.27)/br(0.32)/br(0.35) - the samples must land on music - and the
    # first keeper there is the Taiwanese track. Promoting it would have
    # recreated, by hand, the exact bug this tool exists to fix. If nothing
    # clears the confidence floor we leave the file's flags untouched and say
    # so.
    def best(lang_codes):
        cands = [d for d in keep if d["heard"] in lang_codes and d["conf"] >= MIN_CONFIDENCE]
        return max(cands, key=lambda d: d["conf"]) if cands else None

    chosen = best({(original_language or "en").lower()[:2]}) or best({"en"})
    if chosen is None and not drop:
        skip = skip or "no track identified with confidence - leaving flags alone"

    return {
        "path": path,
        "tracks": detections,
        "keep": [d["audio_index"] for d in keep],
        "drop": [d["audio_index"] for d in drop],
        "default_index": chosen["audio_index"] if chosen else None,
        "skip": skip,
    }


def apply(p: dict) -> bool:
    """mkvmerge to drop the dubs, then mkvpropedit for tags + default flag."""
    path = p["path"]
    if p["drop"]:
        from pipeline.gap_filler import GapAnalysis, _strip_tracks_locally

        info = identify(path)
        sub_ids = [t["id"] for t in info.get("tracks", []) if t.get("type") == "subtitles"]
        gaps = GapAnalysis(
            needs_track_removal=True,
            audio_keep_indices=p["keep"],
            sub_keep_indices=list(range(len(sub_ids))),
            source_sub_count=len(sub_ids),
        )
        from pipeline.config import build_config

        gaps._config = build_config()  # type: ignore[attr-defined]
        if not _strip_tracks_locally(path, gaps):
            return False

    # Indices have shifted if we dropped tracks: the keepers are now 0..n-1
    # in their original relative order.
    remaining = [d for d in p["tracks"] if d["audio_index"] in p["keep"]]
    args = [mkvpropedit(), path]
    for new_i, d in enumerate(remaining):
        iso3 = ISO2_TO_3.get(d["heard"] or "")
        if iso3 and d["conf"] >= MIN_CONFIDENCE and d["tagged"] != iso3:
            args += ["--edit", f"track:a{new_i + 1}", "--set", f"language={iso3}"]
        if p["default_index"] is not None:
            flag = "1" if d["audio_index"] == p["default_index"] else "0"
            args += ["--edit", f"track:a{new_i + 1}", "--set", f"flag-default={flag}"]
    if len(args) == 2:
        return True  # nothing left to change after the strip
    r = subprocess.run(args, capture_output=True, text=True, timeout=600, encoding="utf-8", errors="replace")
    if r.returncode >= 2:
        print(f"      mkvpropedit failed: {(r.stdout or r.stderr)[:150]}")
        return False
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--match", required=True, help="substring of the filepath")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--model", default="small")
    ap.add_argument("--original-language", default="en")
    ap.add_argument("--root", default=r"\\KieranNAS\Media")
    args = ap.parse_args()

    paths = []
    for dirpath, _d, names in os.walk(args.root):
        for nm in names:
            if nm.lower().endswith(".mkv") and args.match.lower() in os.path.join(dirpath, nm).lower():
                paths.append(os.path.join(dirpath, nm))
    paths.sort()
    print(f"{len(paths)} file(s) matching {args.match!r}")
    if not paths:
        return 0

    os.environ.setdefault("WHISPER_FORCE_CPU", "1")  # rule 9c - never a CUDA context beside NVENC
    from faster_whisper import WhisperModel

    model = WhisperModel(args.model, device="cpu", compute_type="int8")

    todo = []
    for n, path in enumerate(paths, 1):
        info = identify(path)
        aud = [t for t in info.get("tracks", []) if t.get("type") == "audio"]
        if len(aud) < 2 and all((t.get("properties") or {}).get("default_track") for t in aud):
            continue  # single correctly-flagged track: nothing to decide
        p = plan(path, args.original_language, model)
        desc = " ".join(
            f"a{d['audio_index'] + 1}:{d['heard'] or '?'}({d['conf']:.2f})"
            f"{'*' if d['audio_index'] == p['default_index'] else ''}"
            for d in p["tracks"]
        )
        print(f"  [{n}/{len(paths)}] {os.path.basename(path)[:44]:44} {desc}")
        if p["skip"]:
            print(f"      SKIP: {p['skip']}")
            continue
        if p["drop"]:
            print(f"      drop a{[i + 1 for i in p['drop']]}, default -> a{p['default_index'] + 1}")
        todo.append(p)
        if args.limit and len(todo) >= args.limit:
            break

    print(f"\n{len(todo)} file(s) to fix")
    if not args.execute:
        print("(dry run - pass --execute to write)")
        return 0

    ok = bad = 0
    for n, p in enumerate(todo, 1):
        print(f"  applying [{n}/{len(todo)}] {os.path.basename(p['path'])[:50]}")
        try:
            if apply(p):
                ok += 1
            else:
                bad += 1
                print("      FAILED - file left untouched")
        except Exception as exc:  # noqa: BLE001 - reported, never silent
            bad += 1
            print(f"      ERROR {type(exc).__name__}: {exc}")
    print(f"\nfixed={ok}  failed={bad}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
