"""
Mux External Subtitles
======================
Finds external subtitle files (.srt, .ass, .ssa, .sub) next to MKV files on the
NAS, muxes them into the MKV using mkvmerge, and deletes the external files.

Usage:
    python -m tools.mux_external_subs              # dry run
    python -m tools.mux_external_subs --execute     # actually mux
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from paths import NAS_MOVIES, NAS_SERIES, MEDIA_REPORT

SUB_EXTENSIONS = {".srt", ".ass", ".ssa", ".sub"}

# Language tag normalisation: filename tag -> ISO 639-2/B
_LANG_MAP = {
    "en": "eng",
    "eng": "eng",
    "english": "eng",
    "fr": "fre",
    "fre": "fre",
    "french": "fre",
    "de": "ger",
    "ger": "ger",
    "german": "ger",
    "es": "spa",
    "spa": "spa",
    "spanish": "spa",
    "it": "ita",
    "ita": "ita",
    "italian": "ita",
    "pt": "por",
    "por": "por",
    "portuguese": "por",
    "ja": "jpn",
    "jpn": "jpn",
    "japanese": "jpn",
    "zh": "chi",
    "chi": "chi",
    "chinese": "chi",
    "ko": "kor",
    "kor": "kor",
    "korean": "kor",
    "ru": "rus",
    "rus": "rus",
    "russian": "rus",
    "ar": "ara",
    "ara": "ara",
    "arabic": "ara",
    "nl": "dut",
    "dut": "dut",
    "dutch": "dut",
    "sv": "swe",
    "swe": "swe",
    "swedish": "swe",
    "da": "dan",
    "dan": "dan",
    "danish": "dan",
    "no": "nor",
    "nor": "nor",
    "norwegian": "nor",
    "fi": "fin",
    "fin": "fin",
    "finnish": "fin",
    "pl": "pol",
    "pol": "pol",
    "polish": "pol",
    "cs": "cze",
    "cze": "cze",
    "czech": "cze",
    "hu": "hun",
    "hun": "hun",
    "hungarian": "hun",
    "el": "gre",
    "gre": "gre",
    "greek": "gre",
    "tr": "tur",
    "tur": "tur",
    "turkish": "tur",
    "he": "heb",
    "heb": "heb",
    "hebrew": "heb",
    "th": "tha",
    "tha": "tha",
    "thai": "tha",
    "vi": "vie",
    "vie": "vie",
    "vietnamese": "vie",
    "hi": "hin",
    "hin": "hin",
    "hindi": "hin",
}

# MKVToolNix common install locations
_MKVMERGE_SEARCH = [
    r"C:\Program Files\MKVToolNix\mkvmerge.exe",
    r"C:\Program Files (x86)\MKVToolNix\mkvmerge.exe",
]


def _find_mkvmerge() -> str | None:
    found = shutil.which("mkvmerge")
    if found:
        return found
    for path in _MKVMERGE_SEARCH:
        if os.path.isfile(path):
            return path
    return None


def _parse_language(subtitle_path: Path) -> str:
    """Extract language code from subtitle filename.

    Expects patterns like:
        Movie (2020).eng.srt  -> eng
        Movie (2020).en.srt   -> eng
        Movie (2020).english.srt -> eng
        Movie (2020).srt      -> eng (default)
    """
    # Remove the subtitle extension, then check if there's a language tag
    stem = subtitle_path.stem  # e.g. "Movie (2020).eng"
    parts = stem.rsplit(".", 1)
    if len(parts) == 2:
        tag = parts[1].lower()
        if tag in _LANG_MAP:
            return _LANG_MAP[tag]
    # No recognised language tag — default to English
    return "eng"


def _find_parent_mkv(subtitle_path: Path) -> Path | None:
    """Find the MKV file that this subtitle belongs to.

    Strips the language tag (if present) and subtitle extension to find a
    matching .mkv in the same directory.
    """
    stem = subtitle_path.stem  # e.g. "Movie (2020).eng"
    parts = stem.rsplit(".", 1)
    if len(parts) == 2 and parts[1].lower() in _LANG_MAP:
        mkv_stem = parts[0]
    else:
        mkv_stem = stem

    mkv_path = subtitle_path.parent / f"{mkv_stem}.mkv"
    if mkv_path.exists():
        return mkv_path
    return None


def _scan_for_external_subs(directories: list[Path]) -> list[dict]:
    """Walk NAS directories and find external subtitle files next to MKVs."""
    results = []
    for root_dir in directories:
        if not root_dir.exists():
            print(f"WARNING: Directory not accessible: {root_dir}")
            continue
        for dirpath, _, filenames in os.walk(root_dir):
            for fname in filenames:
                sub_path = Path(dirpath) / fname
                if sub_path.suffix.lower() not in SUB_EXTENSIONS:
                    continue
                mkv_path = _find_parent_mkv(sub_path)
                if mkv_path is None:
                    continue
                lang = _parse_language(sub_path)
                results.append({
                    "sub_path": str(sub_path),
                    "mkv_path": str(mkv_path),
                    "language": lang,
                    "sub_filename": fname,
                    "mkv_filename": mkv_path.name,
                })
    return results


def _mux_subtitle(mkv_path: str, sub_path: str, language: str,
                  mkvmerge: str) -> bool:
    """Mux a single subtitle file into an MKV using mkvmerge."""
    tmp_path = mkv_path + ".muxsubs_tmp.mkv"

    try:
        src_size = os.path.getsize(mkv_path)

        cmd = [
            mkvmerge, "-o", tmp_path,
            mkv_path,
            "--language", f"0:{language}",
            sub_path,
        ]

        timeout = 60 + int(src_size / (10 * 1024 * 1024))
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                                encoding="utf-8", errors="replace")
        if result.returncode >= 2:
            print(f"  ERROR: mkvmerge failed")
            print(f"  stderr: {result.stderr[:500]}")
            return False

        if not os.path.exists(tmp_path):
            print(f"  ERROR: mkvmerge produced no output")
            return False

        dst_size = os.path.getsize(tmp_path)
        if dst_size < src_size * 0.5:
            print(f"  ERROR: Output too small ({dst_size} vs {src_size}), keeping original")
            os.remove(tmp_path)
            return False

        os.replace(tmp_path, mkv_path)
        return True

    except subprocess.TimeoutExpired:
        print(f"  ERROR: Timeout")
        return False
    except Exception as e:
        print(f"  ERROR: {e}")
        return False
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def main() -> None:
    sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(description="Mux external subtitle files into MKVs")
    parser.add_argument("--execute", action="store_true",
                        help="Actually mux files (default: dry run)")
    args = parser.parse_args()

    print("Scanning NAS for external subtitle files...")
    subs = _scan_for_external_subs([NAS_MOVIES, NAS_SERIES])

    if not subs:
        print("No external subtitle files found next to MKV files.")
        return

    print(f"Found {len(subs)} external subtitle file(s) to mux\n")

    if not args.execute:
        print("DRY RUN — showing files:")
        for s in subs[:50]:
            print(f"  {s['sub_filename']} ({s['language']}) -> {s['mkv_filename']}")
        if len(subs) > 50:
            print(f"  ... and {len(subs) - 50} more")
        print(f"\nRun with --execute to mux subtitles into MKV files.")
        return

    mkvmerge = _find_mkvmerge()
    if not mkvmerge:
        print("ERROR: mkvmerge not found. Install MKVToolNix or add to PATH.", file=sys.stderr)
        sys.exit(1)

    print(f"Using mkvmerge: {mkvmerge}")
    print(f"Muxing {len(subs)} subtitle file(s)...\n")

    completed = 0
    success = 0
    start_time = time.monotonic()

    for s in subs:
        completed += 1
        elapsed = time.monotonic() - start_time
        if completed > 1 and elapsed > 0:
            rate = elapsed / (completed - 1)
            remaining = (len(subs) - completed) * rate
            eta = f"ETA: ~{remaining / 60:.0f}m" if remaining >= 60 else f"ETA: ~{remaining:.0f}s"
        else:
            eta = ""

        print(f"  [{completed}/{len(subs)}] {eta} — {s['sub_filename']} ({s['language']}) -> {s['mkv_filename']}",
              flush=True)

        if not os.path.exists(s["mkv_path"]):
            print(f"  SKIP: MKV not found")
            continue
        if not os.path.exists(s["sub_path"]):
            print(f"  SKIP: Subtitle file gone")
            continue

        ok = _mux_subtitle(s["mkv_path"], s["sub_path"], s["language"], mkvmerge)
        if ok:
            success += 1
            # Delete the external subtitle file
            try:
                os.remove(s["sub_path"])
                print(f"  Deleted {s['sub_filename']}")
            except OSError as e:
                print(f"  WARNING: Could not delete subtitle file: {e}")

            # Update media report
            try:
                from tools.scanner import update_report_entry
                library_type = "movie" if "Movies" in s["mkv_path"] else "series"
                update_report_entry(s["mkv_path"], str(MEDIA_REPORT), library_type)
            except Exception as e:
                print(f"  Report update failed (non-fatal): {e}")

    elapsed = time.monotonic() - start_time
    elapsed_str = f"{elapsed / 60:.1f}m" if elapsed >= 60 else f"{elapsed:.0f}s"
    print(f"\nDone: {success}/{len(subs)} files muxed in {elapsed_str}")


if __name__ == "__main__":
    main()
