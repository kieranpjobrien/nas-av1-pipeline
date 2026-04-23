"""NAS walker + ffprobe + ``files`` row writer.

This module owns the only write path into the :mod:`naslib.inventory.files`
table. It walks the NAS movie and series roots, runs ``ffprobe`` on each
media file, normalises the output into typed stream descriptors, and calls
:func:`naslib.inventory.upsert_file` once per scanned file.

The scanner also sets the ``damage_flag`` column. If a file used to have
audio on a previous scan and now has none, we mark it ``"audio_lost"`` so
later tooling can surface it without destroying any further data. This is a
purely passive signal — we never remediate damage here, only report it.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import SCHEMA_VERSION
from .inventory import (
    INVENTORY_DB,
    AudioStream,
    ExternalSub,
    FileRow,
    LibraryType,
    SubStream,
    connect,
    delete_file,
    iter_files,
    read_file,
    stamp_last_scan,
    transaction,
    upsert_file,
)

# ``paths`` lives at the repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from paths import NAS_MOVIES, NAS_SERIES  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: File extensions considered "media" by the scanner.
VIDEO_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".mkv",
        ".mp4",
        ".avi",
        ".m4v",
        ".wmv",
        ".flv",
        ".mov",
        ".ts",
        ".webm",
        ".mpg",
        ".mpeg",
        ".m2ts",
    }
)

#: Extensions we recognise as subtitle sidecars sitting next to a media file.
SIDECAR_EXTENSIONS: frozenset[str] = frozenset({".srt", ".ass", ".ssa", ".sub", ".vtt", ".idx"})

#: Default concurrency for ffprobe. Raising this past ~4 gives diminishing
#: returns over SMB and starts to hammer the NAS's CPU.
DEFAULT_PROBE_WORKERS: int = 4


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ScanStats:
    """Counters reported at the end of a scan run."""

    scanned: int = 0
    probed: int = 0
    unchanged: int = 0
    probe_failed: int = 0
    deleted: int = 0
    damaged: int = 0


def scan_library(
    *,
    incremental: bool = False,
    workers: int = DEFAULT_PROBE_WORKERS,
    damage_check: bool = False,
    db_path: Path | None = None,
) -> ScanStats:
    """Walk the NAS, ffprobe files as needed, and update the inventory.

    Args:
        incremental: If ``True``, skip any file whose ``(size, mtime)`` matches
            the existing row. The default (``False``) still reads the existing
            row so we can detect damage but will re-probe every file.
        workers: Number of concurrent ffprobe subprocesses.
        damage_check: If ``True``, do not re-probe; only walk the NAS, compare
            sizes against the existing inventory, and flag damage. This is the
            cheap mode used as a smoke test and post-deploy sanity check.
        db_path: Optional override for the SQLite path (tests).

    Returns:
        A populated :class:`ScanStats` dataclass.
    """
    stats = ScanStats()
    targets = _discover_targets()
    stats.scanned = len(targets)

    with connect(db_path or INVENTORY_DB) as conn:
        existing: dict[str, FileRow] = {row.filepath: row for row in iter_files(conn)}

        if damage_check:
            _run_damage_check(conn, targets, existing, stats)
            stamp_last_scan(conn)
            return stats

        to_probe: list[tuple[str, LibraryType]] = []
        for filepath, lib_type in targets:
            try:
                st = os.stat(filepath)
            except OSError:
                continue
            prev = existing.get(filepath)
            if (
                incremental
                and prev is not None
                and int(prev.size_bytes) == int(st.st_size)
                and abs(float(prev.mtime) - float(st.st_mtime)) < 1.0
                and prev.scan_version == SCHEMA_VERSION
            ):
                stats.unchanged += 1
                continue
            to_probe.append((filepath, lib_type))

        # Probe outside the transaction so we're not holding a write lock over
        # minutes of I/O. Gather all results into memory then do one bulk
        # transaction at the end.
        probed_rows: list[FileRow] = []
        if to_probe:
            probed_rows = _probe_many(to_probe, workers)
            stats.probed = len(probed_rows)
            stats.probe_failed = len(to_probe) - len(probed_rows)

        # Identify files that existed in the DB but are no longer on the NAS.
        seen_paths = {fp for fp, _ in targets}
        missing = [fp for fp in existing if fp not in seen_paths]

        with transaction(conn):
            for row in probed_rows:
                prev = existing.get(row.filepath)
                row.damage_flag = _derive_damage_flag(prev, row)
                if row.damage_flag is not None:
                    stats.damaged += 1
                upsert_file(conn, row)
            for gone in missing:
                delete_file(conn, gone)
                stats.deleted += 1
            stamp_last_scan(conn)

    return stats


# ---------------------------------------------------------------------------
# Walk
# ---------------------------------------------------------------------------


def _discover_targets() -> list[tuple[str, LibraryType]]:
    """Walk both NAS roots and return ``(absolute_path, library_type)`` tuples."""
    targets: list[tuple[str, LibraryType]] = []
    roots: tuple[tuple[Path, LibraryType], ...] = (
        (NAS_MOVIES, "movie"),
        (NAS_SERIES, "series"),
    )
    for root, lib_type in roots:
        if not root.exists():
            continue
        for dirpath, _, filenames in os.walk(root):
            for name in filenames:
                if Path(name).suffix.lower() in VIDEO_EXTENSIONS:
                    targets.append((os.path.join(dirpath, name), lib_type))
    return targets


# ---------------------------------------------------------------------------
# Probe + normalise
# ---------------------------------------------------------------------------


def _probe_many(targets: list[tuple[str, LibraryType]], workers: int) -> list[FileRow]:
    """Fan out ffprobe across ``workers`` threads and return successful results."""
    results: list[FileRow] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(_probe_one, fp, lt): fp for fp, lt in targets}
        for future in as_completed(futures):
            row = future.result()
            if row is not None:
                results.append(row)
    return results


def _probe_one(filepath: str, library_type: LibraryType) -> FileRow | None:
    """Run ffprobe on one file and return a :class:`FileRow`, or ``None`` on failure."""
    probe = _ffprobe(filepath)
    if probe is None:
        return None
    try:
        st = os.stat(filepath)
    except OSError:
        return None
    return _build_row(filepath, library_type, probe, st.st_size, st.st_mtime)


def _ffprobe(filepath: str) -> dict[str, Any] | None:
    """Invoke ``ffprobe -show_format -show_streams`` and parse the JSON output."""
    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        filepath,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            encoding="utf-8",
            errors="replace",
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None
    if result.returncode != 0:
        return None
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _build_row(
    filepath: str,
    library_type: LibraryType,
    probe: dict[str, Any],
    size_bytes: int,
    mtime: float,
) -> FileRow:
    """Assemble a :class:`FileRow` from raw ffprobe output."""
    streams = probe.get("streams") or []
    fmt = probe.get("format") or {}
    duration = float(fmt.get("duration") or 0.0)

    video = _first_video(streams)
    audio = _collect_audio(streams)
    subs = _collect_subs(streams)
    external = _discover_external_subs(filepath)

    v_codec, v_width, v_height, v_hdr, v_bitdepth, v_bitrate = _video_fields(video)

    return FileRow(
        filepath=filepath,
        library_type=library_type,
        size_bytes=size_bytes,
        mtime=mtime,
        duration_secs=duration,
        video_codec=v_codec,
        video_width=v_width,
        video_height=v_height,
        video_hdr=v_hdr,
        video_bit_depth=v_bitdepth,
        video_bitrate_kbps=v_bitrate,
        audio_streams=audio,
        sub_streams=subs,
        external_subs=external,
        tmdb=None,  # planner/tagger writes this separately
        filename_matches_folder=True,  # reserved for future compliance checks
    )


def _first_video(streams: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the first video stream (ignoring cover art / thumbnail streams)."""
    for s in streams:
        if s.get("codec_type") == "video":
            # Exclude attached pictures — they're embedded JPEGs, not movies.
            disposition = s.get("disposition") or {}
            if disposition.get("attached_pic"):
                continue
            return s
    return None


def _video_fields(
    video: dict[str, Any] | None,
) -> tuple[str | None, int | None, int | None, bool, int | None, int | None]:
    """Extract the normalised fields we store in the ``files`` table."""
    if video is None:
        return None, None, None, False, None, None
    codec = str(video.get("codec_name") or "").lower() or None
    width = int(video.get("width") or 0) or None
    height = int(video.get("height") or 0) or None
    transfer = video.get("color_transfer") or ""
    primaries = video.get("color_primaries") or ""
    is_hdr = transfer in ("smpte2084", "arib-std-b67") or primaries == "bt2020"
    bit_depth = video.get("bits_per_raw_sample")
    if bit_depth is not None:
        bit_depth = int(bit_depth)
    else:
        pix_fmt = str(video.get("pix_fmt") or "")
        if "10" in pix_fmt:
            bit_depth = 10
        elif "12" in pix_fmt:
            bit_depth = 12
        else:
            bit_depth = 8
    bitrate_kbps = video.get("bit_rate")
    bitrate_int = int(int(bitrate_kbps) / 1000) if bitrate_kbps else None
    return codec, width, height, is_hdr, bit_depth, bitrate_int


def _collect_audio(streams: list[dict[str, Any]]) -> list[AudioStream]:
    """Extract audio streams in original order, preserving the absolute index."""
    result: list[AudioStream] = []
    for s in streams:
        if s.get("codec_type") != "audio":
            continue
        codec = str(s.get("codec_name") or "").lower()
        profile = str(s.get("profile") or "").lower()
        lossless_codecs = {
            "truehd",
            "flac",
            "alac",
            "pcm_s16le",
            "pcm_s24le",
            "pcm_s32le",
            "pcm_f32le",
            "pcm_s16be",
            "pcm_s24be",
            "pcm_s32be",
            "pcm_f32be",
        }
        is_lossless = (
            codec in lossless_codecs
            or "hd ma" in profile
            or "hd-ma" in profile
            or codec == "dts"
            and ("ma" in profile or "hd" in profile)
        )
        tags = s.get("tags") or {}
        bitrate = s.get("bit_rate")
        bitrate_kbps = int(int(bitrate) / 1000) if bitrate else None
        result.append(
            AudioStream(
                index=int(s.get("index") or 0),
                codec=codec,
                language=str(tags.get("language") or "und"),
                channels=int(s.get("channels") or 0),
                bitrate_kbps=bitrate_kbps,
                lossless=is_lossless,
            )
        )
    return result


def _collect_subs(streams: list[dict[str, Any]]) -> list[SubStream]:
    """Extract subtitle streams with forced/HI dispositions."""
    result: list[SubStream] = []
    for s in streams:
        if s.get("codec_type") != "subtitle":
            continue
        disposition = s.get("disposition") or {}
        tags = s.get("tags") or {}
        result.append(
            SubStream(
                index=int(s.get("index") or 0),
                codec=str(s.get("codec_name") or ""),
                language=str(tags.get("language") or "und"),
                title=str(tags.get("title") or ""),
                forced=bool(disposition.get("forced") or 0),
                hi=bool(disposition.get("hearing_impaired") or 0),
            )
        )
    return result


def _discover_external_subs(filepath: str) -> list[ExternalSub]:
    """Find sidecar subtitle files sitting next to a given media file.

    Sidecar naming examples we handle (lowered, then split on ``.``):

    * ``foo.srt`` → stem match, lang ``und``
    * ``foo.en.srt`` → lang ``en``
    * ``foo.en.hi.srt`` → lang ``en`` + HI flag
    * ``foo.forced.en.srt`` → lang ``en`` + forced flag
    """
    result: list[ExternalSub] = []
    parent = os.path.dirname(filepath)
    stem = Path(filepath).stem.lower()
    try:
        names = os.listdir(parent)
    except OSError:
        return result
    for name in names:
        ext = Path(name).suffix.lower()
        if ext not in SIDECAR_EXTENSIONS:
            continue
        sib_stem = Path(name).stem.lower()
        if sib_stem != stem and not sib_stem.startswith(stem + "."):
            continue
        suffix = sib_stem[len(stem) :].lstrip(".")
        parts = suffix.split(".") if suffix else []
        language = "und"
        forced = False
        hi = False
        for part in parts:
            if part in ("forced",):
                forced = True
            elif part in ("hi", "sdh"):
                hi = True
            elif len(part) in (2, 3) and part.isalpha() and language == "und":
                language = part
        result.append(ExternalSub(filename=name, language=language, forced=forced, hi=hi))
    return result


# ---------------------------------------------------------------------------
# Damage detection
# ---------------------------------------------------------------------------


def _derive_damage_flag(prev: FileRow | None, fresh: FileRow) -> str | None:
    """Decide whether a fresh probe indicates data loss vs the prior row.

    Returns one of:

    * ``"audio_lost"`` — prior row had audio, current probe reports none.
    * ``"size_collapse"`` — file shrank to <10% of the prior size (encode is
      normally 30-60% of source; 10% is always a truncation).
    * ``None`` — file looks healthy, or there's no prior row to compare against.
    """
    if fresh.audio_count == 0:
        if prev is not None and prev.audio_count > 0:
            return "audio_lost"
        # Unknown prior state + zero audio is still suspicious; flag it so the
        # user sees it in ``--damage-check`` output even on first scan.
        return "audio_lost"
    if prev is not None and prev.size_bytes > 0:
        shrink_ratio = fresh.size_bytes / prev.size_bytes
        if shrink_ratio < 0.10:
            return "size_collapse"
    return None


def _run_damage_check(
    conn: Any,
    targets: list[tuple[str, LibraryType]],
    existing: dict[str, FileRow],
    stats: ScanStats,
) -> None:
    """Cheap walk that reports zero-audio files from the existing inventory.

    We deliberately do *not* re-probe here; the goal is to print an alarm with
    what we already know. If a file is new (no prior probe) we cannot tell
    whether it's damaged without an expensive probe, so we skip it.
    """
    seen = {fp for fp, _ in targets}
    damaged_now: list[FileRow] = []
    for fp in sorted(seen & existing.keys()):
        row = existing[fp]
        if row.damage_flag is not None or row.audio_count == 0:
            damaged_now.append(row)
    stats.damaged = len(damaged_now)
    if damaged_now:
        print("damage check — files flagged for zero-audio or size collapse:")
        for row in damaged_now[:50]:
            flag = row.damage_flag or "audio_lost"
            print(f"  [{flag}] a={row.audio_count} v={row.video_codec or '?'} {row.filepath}")
        if len(damaged_now) > 50:
            print(f"  ... and {len(damaged_now) - 50} more")
    else:
        print("damage check — no flagged files.")
    # ``conn`` unused but kept in the signature to document single-writer
    # discipline: only this module reads/writes file rows.
    _ = (conn, read_file)
