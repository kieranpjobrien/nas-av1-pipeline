"""Strip foreign audio/subtitle tracks from files the pipeline will never revisit.

The encoder strips foreign tracks as part of every conversion, so anything
still queued for AV1 fixes itself. Two groups never get that:

  * files already AV1/HEVC - the pipeline is done with them;
  * files parked ``flagged_undersized`` - the source is below the quality floor
    and we have decided not to re-encode them (the better release does not
    exist), so they sit at their current track layout forever.

Between them that is ~280 files carrying foreign audio and ~930 carrying
foreign subs. This closes those out with a pure remux: mkvmerge copies every
stream we keep bit-for-bit, so there is no re-encode and no quality cost.

Reuses the gap filler's own mux + verify path (``_strip_tracks_locally`` ->
``_post_mux_verify_and_replace``), which ffprobes the output and refuses the
atomic replace if video or audio came out short. That guard is what caught the
2026-04-22 zero-audio bug; there is no reason to write a second, less careful
one here.

Safety rules, in order of importance:

  * **Never drop a track whose language is unknown.** und/unk tracks are KEPT,
    always. Stated inviolate by the operator 2026-04-29.
  * **Never leave a file with no audio.** If every audio track would be
    dropped, the file is skipped whole and reported - that means the tags are
    lying (the Bluey case) and it wants ``retag_audio_languages`` first.
  * **Never judge audio without knowing the original language.** No TMDb
    original_language means we cannot tell a dub from the real thing, so the
    audio side is skipped for that file.

Subtitles need no TMDb: policy is English-only (KEEP_LANGS), same rule the
compliance checker applies.

Run:
    uv run python -m tools.strip_foreign_tracks --match Bluey
    uv run python -m tools.strip_foreign_tracks --execute --limit 50
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.config import KEEP_LANGS  # noqa: E402
from pipeline.gap_filler import GapAnalysis  # noqa: E402
from pipeline.qualify import equivalence_bucket  # noqa: E402

DASHBOARD = "http://127.0.0.1:8000/api/media-report"

# Languages that mean "we cannot judge this track", so it is never dropped:
#   und/unk/""  - no language recorded
#   mul         - MULTIPLE languages. On a multilingual film this is the real
#                 mix, not a dub. Joyeux Noel (2005) is French/German/English
#                 in the same scenes and TMDb calls it 'fr', so a naive rule
#                 reads its 'mul' track as foreign and drops the actual film.
#   mis/qaa     - "uncoded" / reserved-for-local-use; same problem.
UNJUDGEABLE = {"und", "unk", "", "mul", "mis", "qaa"}
UNKNOWN = UNJUDGEABLE  # back-compat alias


def stream_lang(s: dict) -> str:
    """The language we actually believe, preferring whisper over the tag."""
    det = (s.get("detected_language") or "").lower().strip()
    if det and det not in UNKNOWN:
        return det
    return (s.get("language") or "").lower().strip()


def allowed_audio_langs(entry: dict) -> set[str] | None:
    """Languages an audio track may be in, or None if we cannot tell."""
    orig = ((entry.get("tmdb") or {}).get("original_language") or "").lower().strip()
    if not orig:
        return None
    return set(KEEP_LANGS) | equivalence_bucket(orig)


def plan_file(entry: dict) -> dict:
    """Which track indices to keep. Pure - no side effects, no I/O."""
    audio = entry.get("audio_streams") or []
    subs = entry.get("subtitle_streams") or []

    allowed = allowed_audio_langs(entry)
    audio_keep: list[int] = []
    audio_drop: list[int] = []
    for i, a in enumerate(audio):
        lang = stream_lang(a)
        if allowed is None or lang in UNKNOWN or lang in allowed:
            audio_keep.append(i)  # unknown language is never dropped
        else:
            audio_drop.append(i)

    sub_keep: list[int] = []
    sub_drop: list[int] = []
    for i, s in enumerate(subs):
        lang = stream_lang(s)
        if lang in UNJUDGEABLE or lang in KEEP_LANGS:
            sub_keep.append(i)
        else:
            sub_drop.append(i)

    # Never strip the last comprehensible subtitle off a foreign-language film.
    # Amour is French with French subs and no English ones; "English subs only"
    # applied literally would leave a French film with no subtitles at all,
    # which is worse than an untidy track list. The post-mux verify cannot
    # catch this - it only checks the output matches the plan, so a plan of
    # "keep zero subs" passes it happily.
    note = ""
    orig = ((entry.get("tmdb") or {}).get("original_language") or "").lower().strip()
    foreign_film = bool(orig) and orig not in equivalence_bucket("en")
    english_sub_survives = any(stream_lang(subs[i]) in KEEP_LANGS - UNJUDGEABLE for i in sub_keep)
    if foreign_film and sub_drop and not english_sub_survives:
        sub_keep = list(range(len(subs)))
        sub_drop = []
        note = f"kept foreign subs - {orig} film with no English sub to fall back on"

    skip = ""
    if audio_drop and not audio_keep:
        skip = "would leave zero audio - tags are probably wrong, retag first"
    elif not audio_drop and not sub_drop:
        skip = "nothing foreign"

    return {
        "filepath": entry.get("filepath", ""),
        "audio_keep": audio_keep,
        "audio_drop": audio_drop,
        "sub_keep": sub_keep,
        "sub_drop": sub_drop,
        "source_sub_count": len(subs),
        "audio_langs_dropped": [stream_lang(audio[i]) for i in audio_drop],
        "sub_langs_dropped": [stream_lang(subs[i]) for i in sub_drop],
        "skip": skip,
        "note": note,
    }


def apply_plan(plan: dict, config: dict) -> bool:
    """Hand the plan to the gap filler's mux + verify + atomic replace."""
    from pipeline.gap_filler import _strip_tracks_locally

    gaps = GapAnalysis(
        needs_track_removal=True,
        audio_keep_indices=plan["audio_keep"],
        sub_keep_indices=plan["sub_keep"],
        source_sub_count=plan["source_sub_count"],
    )
    gaps._config = config  # type: ignore[attr-defined]
    return _strip_tracks_locally(plan["filepath"], gaps)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="actually rewrite files")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--match", default="", help="substring of the filepath")
    ap.add_argument("--subs-only", action="store_true")
    ap.add_argument("--audio-only", action="store_true")
    ap.add_argument(
        "--include-queued",
        action="store_true",
        help="also touch files the pipeline still has queued (DANGEROUS - it may be "
        "mid-encode on them; by default those are skipped because the encode strips "
        "them anyway)",
    )
    args = ap.parse_args()

    # Files the pipeline still owns must not be remuxed underneath it. A
    # concurrent mkvmerge on a file being fetched/encoded/uploaded is a
    # corruption risk, and pointless besides: the encode strips foreign tracks
    # itself. Only terminal rows - and rows absent from the DB - are ours.
    import sqlite3

    PIPELINE_OWNS = {"pending", "qualifying", "fetching", "processing", "uploading"}
    owned: set[str] = set()
    if not args.include_queued:
        try:
            conn = sqlite3.connect("F:/AV1_Staging/pipeline_state.db")
            owned = {
                fp for fp, st in conn.execute("SELECT filepath, status FROM pipeline_files") if st in PIPELINE_OWNS
            }
            conn.close()
        except Exception as exc:  # noqa: BLE001 - never silently strip what we could not check
            print(f"FATAL: cannot read pipeline state DB ({exc}); refusing to run blind")
            return 1

    with urllib.request.urlopen(DASHBOARD, timeout=300) as fh:
        report = json.load(fh)
    files = report.get("files") or []
    if args.match:
        files = [f for f in files if args.match.lower() in (f.get("filepath") or "").lower()]

    from pipeline.config import build_config

    config = build_config()

    plans = []
    zero_audio: list[str] = []
    no_tmdb = 0
    queued_skipped = 0
    for entry in files:
        if (entry.get("filepath") or "") in owned:
            queued_skipped += 1
            continue
        p = plan_file(entry)
        if args.subs_only:
            p["audio_drop"], p["audio_keep"] = [], list(range(len(entry.get("audio_streams") or [])))
        if args.audio_only:
            p["sub_drop"], p["sub_keep"] = [], list(range(len(entry.get("subtitle_streams") or [])))
        if p["skip"].startswith("would leave zero audio"):
            zero_audio.append(p["filepath"])
            continue
        if not p["audio_drop"] and not p["sub_drop"]:
            continue
        if p["audio_drop"] and allowed_audio_langs(entry) is None:
            no_tmdb += 1
            continue
        if not os.path.exists(p["filepath"]):
            continue
        plans.append(p)

    if args.limit:
        plans = plans[: args.limit]

    print(f"{len(plans)} file(s) with foreign tracks to strip")
    if queued_skipped:
        print(f"{queued_skipped} skipped - pipeline still owns them (it strips during encode)")
    if zero_audio:
        print(f"{len(zero_audio)} skipped - stripping would leave zero audio (run retag_audio_languages):")
        for fp in zero_audio[:10]:
            print(f"    {os.path.basename(fp)[:66]}")
    if no_tmdb:
        print(f"{no_tmdb} skipped - no TMDb original_language, cannot judge audio")

    done = failed = 0
    for n, p in enumerate(plans, 1):
        desc = []
        if p["audio_drop"]:
            desc.append(f"audio-{p['audio_langs_dropped']}")
        if p["sub_drop"]:
            desc.append(f"subs-{p['sub_langs_dropped']}")
        print(f"  [{n}/{len(plans)}] {os.path.basename(p['filepath'])[:52]}  drop {' '.join(desc)}")
        if not args.execute:
            continue
        try:
            if apply_plan(p, config):
                done += 1
            else:
                failed += 1
                print("      FAILED - original left untouched")
        except Exception as exc:  # noqa: BLE001 - reported, never silent
            failed += 1
            print(f"      ERROR {type(exc).__name__}: {exc}")

    print(f"\nstripped={done}  failed={failed}  zero_audio_skipped={len(zero_audio)}  no_tmdb_skipped={no_tmdb}")
    if not args.execute:
        print("(dry run - pass --execute to write)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
