"""Tag dialogue-free audio tracks with ISO 639-2 ``zxx`` (no linguistic content).

Use case: orchestral shorts (Paperman, The Lost Thing, Inner Workings,
Feast) and music videos / silent films. Whisper correctly fails to detect
a language on these because there's no speech. Without an explicit tag,
the tracks sit in the und queue forever and the dashboard "Langs Known"
metric undercounts.

The ``zxx`` code is the official ISO 639-2 marker for "no linguistic
content / not applicable" — exactly this case.

Usage:

    # Single file
    uv run python -m tools.tag_no_dialogue --file "/path/to/movie.mkv"

    # All audio tracks of a file → zxx (default; usually correct)
    uv run python -m tools.tag_no_dialogue --file "/path/to/movie.mkv" --tracks all

    # Just one specific audio track
    uv run python -m tools.tag_no_dialogue --file "/path/to/movie.mkv" --tracks 0

    # From a JSON list of titles
    uv run python -m tools.tag_no_dialogue --titles tools/no_dialogue.json

    # Dry-run shows what would happen
    uv run python -m tools.tag_no_dialogue --file ... --dry-run

The companion ``KEEP_LANGS`` update treats ``zxx`` as non-foreign so
compliance and track-strip both leave these tracks alone afterwards.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")


_MKVPROPEDIT_SEARCH = (
    r"C:\Program Files\MKVToolNix\mkvpropedit.exe",
    r"C:\Program Files (x86)\MKVToolNix\mkvpropedit.exe",
)


def _find_mkvpropedit() -> str | None:
    found = shutil.which("mkvpropedit")
    if found:
        return found
    for c in _MKVPROPEDIT_SEARCH:
        if os.path.isfile(c):
            return c
    return None


def _identify_audio_track_indices(filepath: str) -> list[int]:
    """Return mkvmerge audio track IDs (absolute, NOT per-type) for a file."""
    try:
        from pipeline import local_mux
    except ImportError:
        # Fallback: shell out to mkvmerge --identify
        local_mux = None

    if local_mux:
        info = local_mux.local_identify(filepath, timeout=60)
        if not info:
            return []
        return [
            t["id"]
            for t in info.get("tracks", [])
            if t.get("type") == "audio"
        ]

    # Manual fallback
    exe = shutil.which("mkvmerge") or r"C:\Program Files\MKVToolNix\mkvmerge.exe"
    result = subprocess.run(
        [exe, "--identify", "--identification-format", "json", filepath],
        capture_output=True, text=True, timeout=60, encoding="utf-8", errors="replace",
    )
    if result.returncode > 1 or not result.stdout:
        return []
    try:
        info = json.loads(result.stdout)
    except json.JSONDecodeError:
        return []
    return [t["id"] for t in info.get("tracks", []) if t.get("type") == "audio"]


def _apply_zxx(filepath: str, track_ids: list[int], dry_run: bool = False) -> tuple[bool, str]:
    """Apply ``language=zxx`` to the named audio track IDs via mkvpropedit.

    mkvpropedit's track selectors are 1-indexed by track type, NOT the
    absolute mkvmerge IDs. So we convert: audio_track_index_in_type =
    position of this track in the audio_tracks list. Caller passes
    absolute IDs; we look up ordinal.
    """
    exe = _find_mkvpropedit()
    if not exe:
        return False, "mkvpropedit not found — install MKVToolNix"

    # Build list of (audio_track_ordinal, abs_id) pairs.
    abs_ids = _identify_audio_track_indices(filepath)
    if not abs_ids:
        return False, f"no audio tracks identified in {filepath}"

    if track_ids == ["all"]:
        ordinals = list(range(1, len(abs_ids) + 1))
    else:
        ordinals = []
        for tid in track_ids:
            try:
                pos = abs_ids.index(int(tid)) + 1  # 1-indexed
            except ValueError:
                return False, f"track id {tid} not found in {filepath} (have {abs_ids})"
            ordinals.append(pos)

    args = [exe, filepath]
    for ordinal in ordinals:
        args.extend(["--edit", f"track:a{ordinal}", "--set", "language=zxx"])

    if dry_run:
        return True, f"[DRY] would run: {' '.join(args)}"

    result = subprocess.run(
        args, capture_output=True, text=True, timeout=120, encoding="utf-8", errors="replace"
    )
    if result.returncode >= 2:
        return False, f"mkvpropedit rc={result.returncode}: {result.stderr or result.stdout}"
    return True, f"applied zxx to {len(ordinals)} audio track(s) (ordinals {ordinals})"


def _patch_report(filepath: str, track_ids_or_all) -> int:
    """Mirror the on-disk tag in ``media_report.json`` so the dashboard
    metric updates immediately without waiting for a scanner re-probe."""
    try:
        from tools.report_lock import patch_report
    except ImportError:
        return 0

    updated = [0]

    def _patch(report: dict) -> None:
        for entry in report.get("files", []) or []:
            if entry.get("filepath") != filepath:
                continue
            audio = entry.get("audio_streams") or []
            for i, a in enumerate(audio):
                if track_ids_or_all == ["all"] or i in track_ids_or_all:
                    a["language"] = "zxx"
                    a["detected_language"] = "zxx"
                    a["detection_method"] = "manual_no_dialogue"
                    a["detection_confidence"] = 1.0
                    updated[0] += 1
            return

    patch_report(_patch)
    return updated[0]


def _process_one(filepath: str, tracks_arg: str, dry_run: bool) -> int:
    """Tag one file. Returns 0 on success, non-zero on failure."""
    if not os.path.exists(filepath):
        logging.error(f"  NOT FOUND: {filepath}")
        return 1

    if tracks_arg == "all":
        track_ids: list = ["all"]
    else:
        try:
            track_ids = [int(t) for t in tracks_arg.split(",") if t.strip()]
        except ValueError:
            logging.error(f"  bad --tracks value: {tracks_arg!r}")
            return 1

    ok, msg = _apply_zxx(filepath, track_ids, dry_run=dry_run)
    if not ok:
        logging.error(f"  FAIL: {os.path.basename(filepath)}: {msg}")
        return 1
    logging.info(f"  OK: {os.path.basename(filepath)}: {msg}")
    if not dry_run:
        n = _patch_report(filepath, track_ids if track_ids != ["all"] else ["all"])
        if n:
            logging.info(f"    media_report patched: {n} stream(s) → language=zxx")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Tag no-dialogue audio tracks with ISO 639-2 'zxx'")
    parser.add_argument("--file", help="Single file to tag")
    parser.add_argument(
        "--titles",
        help=(
            "JSON file with shape {'files': [{'filepath': '...', 'tracks': 'all' or '0,1'}, ...]}. "
            "Per-entry tracks defaults to 'all' if omitted."
        ),
    )
    parser.add_argument(
        "--tracks", default="all",
        help="Track selector: 'all' or comma-sep audio track IDs (mkvmerge absolute IDs)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen, change nothing")
    args = parser.parse_args()

    if not args.file and not args.titles:
        parser.error("provide --file FILEPATH or --titles JSON")

    failures = 0

    if args.file:
        failures += _process_one(args.file, args.tracks, args.dry_run)

    if args.titles:
        try:
            payload = json.loads(Path(args.titles).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            logging.error(f"could not read --titles file: {e}")
            return 2
        for entry in payload.get("files", []) or []:
            fp = entry.get("filepath")
            if not fp:
                continue
            tracks_arg = entry.get("tracks") or "all"
            failures += _process_one(fp, str(tracks_arg), args.dry_run)

    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
