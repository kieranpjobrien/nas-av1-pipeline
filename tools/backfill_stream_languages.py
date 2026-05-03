"""Backfill per-stream language tags on AV1 files encoded by the
pre-2026-05-04 pipeline.

Background
----------
``pipeline.ffmpeg.build_ffmpeg_cmd`` used to call ``-map_metadata -1``
without re-stamping per-stream language tags. Combined with the E-AC-3
encoder's not-carrying-tags behaviour, every encoded file ended up with
UND across all internal audio and subtitle tracks even though the
language detection had stored the correct values in
``pipeline_state.db`` extras (``detected_audio`` / ``detected_subs``).

Sample of 5 latest done encodes pre-fix: 100% UND on all tracks.

The encoder fix landed in commit XXXX so future encodes are correct.
This tool patches the existing files: for each ``status='done'`` row,
read the source-of-truth languages from extras, build a Matroska XML
patch via mkvpropedit, and write per-track language attributes. No
re-encode needed — mkvpropedit only touches headers, ~50ms per file.

Usage
-----
::

    # Dry-run, default — show what would change for the first 50 files:
    uv run python -m tools.backfill_stream_languages --limit 50

    # Apply, scoped to a path glob:
    uv run python -m tools.backfill_stream_languages --apply \\
        --path-contains "Seven Samurai"

    # Apply across the entire library (slow, do this once):
    uv run python -m tools.backfill_stream_languages --apply --workers 4

The tool refuses to write 'und' tags — if state extras lacks a real
language for a track, the track stays untagged (better than committing
UND to disk).

Discipline rule alignment
-------------------------
This is the post-incident response to the inviolate rule from
``feedback_languages_known_before_strip.md``: never strip a track
without knowing its language. We knew the languages — we just weren't
WRITING them. This tool closes the loop on the historical files.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")


@dataclass
class TrackPatch:
    """One per-track language patch for mkvpropedit's --edit/track flag."""

    track_position: int  # 1-based position within the file's tracks
    language: str  # ISO 639-2 code (eng/jpn/und/etc.)
    title: str | None = None  # optional track title


def _find_mkvpropedit() -> str | None:
    exe = shutil.which("mkvpropedit")
    if exe:
        return exe
    for candidate in (
        r"C:\Program Files\MKVToolNix\mkvpropedit.exe",
        r"C:\Program Files (x86)\MKVToolNix\mkvpropedit.exe",
    ):
        if os.path.isfile(candidate):
            return candidate
    return None


def _find_mkvmerge() -> str | None:
    exe = shutil.which("mkvmerge")
    if exe:
        return exe
    for candidate in (
        r"C:\Program Files\MKVToolNix\mkvmerge.exe",
        r"C:\Program Files (x86)\MKVToolNix\mkvmerge.exe",
    ):
        if os.path.isfile(candidate):
            return candidate
    return None


def _read_track_layout(filepath: str, mkvmerge: str) -> list[dict] | None:
    """Return [{number, type, codec, language}] for each track in the MKV.

    track number is 1-based and matches what mkvpropedit's --edit
    track:N expects. Returns None on identify failure.
    """
    try:
        result = subprocess.run(
            [mkvmerge, "--identification-format", "json", "--identify", filepath],
            capture_output=True,
            text=True,
            timeout=60,
            encoding="utf-8",
            errors="replace",
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    try:
        info = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    out: list[dict] = []
    for tr in info.get("tracks", []):
        props = tr.get("properties") or {}
        out.append(
            {
                "number": tr.get("id", 0) + 1,  # mkvmerge ids are 0-based; mkvpropedit uses 1-based
                "type": tr.get("type"),  # video / audio / subtitles
                "codec": tr.get("codec", ""),
                "language": (props.get("language") or "und").lower(),
                "title": props.get("track_name") or "",
            }
        )
    return out


def _build_patches(
    on_disk_tracks: list[dict],
    detected_audio: list[dict],
    detected_subs: list[dict],
) -> list[TrackPatch]:
    """Map state-DB language data onto the on-disk track layout.

    Walks audio + subtitle tracks in order (first audio on disk = first
    detected_audio entry, etc.). Skips tracks where the detected
    language is missing or 'und' — won't write UND to disk.
    """
    patches: list[TrackPatch] = []
    audio_iter = iter(detected_audio)
    sub_iter = iter(detected_subs)
    for tr in on_disk_tracks:
        ttype = tr.get("type")
        if ttype == "audio":
            det = next(audio_iter, None)
            if det is None:
                continue
        elif ttype == "subtitles":
            det = next(sub_iter, None)
            if det is None:
                continue
        else:
            continue
        new_lang = (det.get("language") or "").strip().lower()
        new_title = (det.get("title") or "").strip()
        if not new_lang or new_lang in ("und", "unk"):
            # Don't overwrite with UND — leave whatever's there. Real
            # detection-failure cases are rare, and the inviolate rule
            # says never claim a language we don't know.
            if not new_title:
                continue
            new_lang = ""  # signal: title-only update
        old_lang = (tr.get("language") or "und").lower()
        old_title = tr.get("title") or ""
        if (new_lang and new_lang != old_lang) or (new_title and new_title != old_title):
            patches.append(
                TrackPatch(
                    track_position=tr["number"],
                    language=new_lang,
                    title=new_title or None,
                )
            )
    return patches


def _apply_patches(
    filepath: str,
    patches: list[TrackPatch],
    mkvpropedit: str,
) -> tuple[bool, str]:
    """Run mkvpropedit with the per-track edits. Returns (ok, message)."""
    if not patches:
        return True, "no changes"
    args: list[str] = [mkvpropedit, filepath]
    for p in patches:
        args.extend(["--edit", f"track:{p.track_position}"])
        if p.language:
            args.extend(["--set", f"language={p.language}"])
        if p.title:
            args.extend(["--set", f"name={p.title}"])
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=60,
            encoding="utf-8",
            errors="replace",
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        return False, f"subprocess error: {e}"
    if result.returncode >= 2:
        # mkvpropedit puts errors on stdout (analysis-line then Error: line)
        for line in (result.stdout or "").splitlines():
            stripped = line.strip()
            if stripped.startswith("Error:"):
                return False, stripped[len("Error:"):].strip()
        return False, f"rc={result.returncode}"
    return True, f"patched {len(patches)} track(s)"


def _process_one(
    filepath: str,
    extras: dict,
    *,
    mkvmerge: str,
    mkvpropedit: str,
    apply: bool,
) -> dict:
    """Process a single file. Returns a result dict for reporting."""
    detected_audio = extras.get("detected_audio") or []
    detected_subs = extras.get("detected_subs") or []
    if not detected_audio and not detected_subs:
        return {"filepath": filepath, "skipped": "no detected_audio/subs in state extras"}
    if not os.path.exists(filepath):
        return {"filepath": filepath, "skipped": "file no longer exists on disk"}

    layout = _read_track_layout(filepath, mkvmerge)
    if layout is None:
        return {"filepath": filepath, "skipped": "mkvmerge could not identify file"}

    patches = _build_patches(layout, detected_audio, detected_subs)
    if not patches:
        return {"filepath": filepath, "skipped": "already in sync"}

    if not apply:
        # Dry-run: describe what would change
        diffs = []
        for p in patches:
            track = next(t for t in layout if t["number"] == p.track_position)
            old = track.get("language", "und")
            diffs.append(f"track {p.track_position} ({track.get('type')}): {old} -> {p.language or '(unchanged)'}")
        return {"filepath": filepath, "would_patch": diffs}

    ok, msg = _apply_patches(filepath, patches, mkvpropedit)
    return {"filepath": filepath, "patched": ok, "message": msg, "count": len(patches)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--state-db", default=None, help="pipeline_state.db (default paths.PIPELINE_STATE_DB)")
    parser.add_argument("--apply", action="store_true", help="Actually patch files (default: dry-run)")
    parser.add_argument("--limit", type=int, default=0, help="Stop after N files (0 = all)")
    parser.add_argument("--path-contains", default=None, help="Substring filter on filepath")
    parser.add_argument("--workers", type=int, default=4, help="Parallel mkvpropedit calls")
    args = parser.parse_args()

    state_db = args.state_db
    if state_db is None:
        from paths import PIPELINE_STATE_DB
        state_db = str(PIPELINE_STATE_DB)

    mkvmerge = _find_mkvmerge()
    mkvpropedit = _find_mkvpropedit()
    if not mkvmerge or not mkvpropedit:
        print("MKVToolNix not found on PATH or in standard locations", file=sys.stderr)
        return 2

    con = sqlite3.connect(state_db)
    cur = con.cursor()
    cur.execute("SELECT filepath, extras FROM pipeline_files WHERE status='done' AND extras IS NOT NULL")
    rows = cur.fetchall()
    con.close()

    # Filter and limit
    filtered = []
    for fp, ex in rows:
        if args.path_contains and args.path_contains.lower() not in fp.lower():
            continue
        try:
            extras = json.loads(ex)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not extras.get("detected_audio") and not extras.get("detected_subs"):
            continue
        filtered.append((fp, extras))
        if args.limit and len(filtered) >= args.limit:
            break

    print(f"Candidates: {len(filtered)} (apply={args.apply}, workers={args.workers})")

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(_process_one, fp, ex, mkvmerge=mkvmerge, mkvpropedit=mkvpropedit, apply=args.apply)
            for fp, ex in filtered
        ]
        for i, fut in enumerate(as_completed(futures), 1):
            r = fut.result()
            results.append(r)
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(filtered)}")

    # Summary
    skipped = [r for r in results if r.get("skipped")]
    would_patch = [r for r in results if r.get("would_patch")]
    patched = [r for r in results if r.get("patched") is True]
    failed = [r for r in results if r.get("patched") is False]

    print()
    print("=== Summary ===")
    print(f"  Skipped:        {len(skipped)}")
    print(f"  Would patch:    {len(would_patch)}")
    print(f"  Patched:        {len(patched)}")
    print(f"  Failed:         {len(failed)}")

    if would_patch and not args.apply:
        print("\n--- Sample of would-patch (first 10) ---")
        for r in would_patch[:10]:
            print(f"  {os.path.basename(r['filepath'])}")
            for d in r["would_patch"]:
                print(f"    {d}")

    if failed:
        print("\n--- Failed samples (first 5) ---")
        for r in failed[:5]:
            print(f"  {os.path.basename(r['filepath'])}: {r.get('message')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
