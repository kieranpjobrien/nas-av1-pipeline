"""CLI entry point — run via `python -m pipeline` or `uv run python -m pipeline`."""

import argparse
import copy
import faulthandler
import json
import logging
import os
import sys
from datetime import datetime

# Pipeline mode forces whisper to run on CPU. The GPU is owned by NVENC for
# the live encode workers, and running whisper on the same chip caused a
# BSOD on 2026-04-21 (rule 9a). CPU + faster-whisper int8 is fast enough
# (~5-15s per file at tiny model) that it can run inline without bottlenecking
# fetch or encode workers. Set BEFORE pipeline.language imports faster_whisper
# so the flag is picked up at first model load.
os.environ.setdefault("WHISPER_FORCE_CPU", "1")

# Crash diagnostics — Python segfaulted at 0xc0000005 in python314.dll on
# 2026-04-27 22:47 with no log line, no traceback, no clue. faulthandler
# catches SIGSEGV / fatal errors and dumps a Python-level stack trace of
# EVERY thread to its registered file before the interpreter dies.
# That's how we'll know whether the segfault was in faster-whisper /
# ctranslate2, an ffmpeg subprocess interaction, our own threading, or
# CPython itself. The handler must be installed BEFORE any C extension
# loads native code, hence wired in at the top of the entry module.
_FAULT_LOG_PATH = os.path.join(
    os.environ.get("AV1_STAGING") or r"F:\AV1_Staging",
    "pipeline_faulthandler.log",
)
try:
    os.makedirs(os.path.dirname(_FAULT_LOG_PATH), exist_ok=True)
    _fault_log = open(_FAULT_LOG_PATH, "a", encoding="utf-8", buffering=1)
    _fault_log.write(
        f"\n--- pipeline start {datetime.now().isoformat(timespec='seconds')} (pid={os.getpid()}) ---\n"
    )
    _fault_log.flush()
    faulthandler.enable(file=_fault_log, all_threads=True)
except OSError:
    # Disk full / permissions / weird drive layout — fall back to stderr.
    faulthandler.enable(all_threads=True)

from paths import MEDIA_REPORT, STAGING_DIR  # noqa: E402
from pipeline.config import build_config  # noqa: E402
from pipeline.control import PipelineControl  # noqa: E402
from pipeline.state import FileStatus, PipelineState, is_terminal  # noqa: E402


def setup_logging(staging_dir: str):
    log_file = os.path.join(staging_dir, "pipeline.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    sys.stdout.reconfigure(line_buffering=True)


def _build_full_gamut_item(entry: dict) -> dict:
    """Project a media_report entry into the queue-item shape the GPU/fetch
    workers consume. Pulled out of :func:`categorise_entry` so the AV1
    force-reencode path can reuse it without duplicating field plucking.

    Carries the full ``tmdb`` blob through so downstream consumers
    (``derive_grade`` for content-grade CQ, ``finalize_upload``'s
    standards-compliance verify for foreign-language audio, etc.) have
    the genres / keywords / original_language data they need.

    Pre-2026-05-08 the item dropped tmdb, which silently downgraded
    every encode's grade to ``default`` — Avengers IW (blockbuster
    target cq=25) re-encoded at the default cq=22, getting a 5%
    shrink instead of the ~25% it should have. Same root cause
    blocked verify on foreign-language films (Crouching Tiger 'chi',
    Seven Samurai 'jpn') because verify's KEEP_LANGS check needs
    original_language to know which non-English audio is legitimate.
    """
    video = entry.get("video", {}) or {}
    codec_raw = video.get("codec_raw", "")
    return {
        "filepath": entry.get("filepath", ""),
        "filename": entry.get("filename", ""),
        "file_size_bytes": entry.get("file_size_bytes", 0),
        "file_size_gb": entry.get("file_size_gb", 0),
        "duration_seconds": entry.get("duration_seconds", 0),
        "video_codec": video.get("codec", codec_raw),
        "resolution": video.get("resolution_class", ""),
        "bitrate_kbps": entry.get("overall_bitrate_kbps", 0) or 0,
        "hdr": video.get("hdr", False),
        "bit_depth": video.get("bit_depth", 8),
        "audio_streams": entry.get("audio_streams", []),
        "subtitle_streams": entry.get("subtitle_streams", []),
        "subtitle_count": entry.get("subtitle_count", 0),
        "library_type": entry.get("library_type", ""),
        "tmdb": entry.get("tmdb") or {},
    }


def categorise_entry(
    entry: dict,
    config: dict,
    state: PipelineState,
    control: PipelineControl,
) -> tuple[str, dict | None]:
    """Decide which queue (if any) a media-report entry belongs to.

    Returns ``(category, queue_item)``:
      * ``("full_gamut", item_dict)`` — needs full re-encode
      * ``("gap_filler", entry)`` — already AV1 but needs cleanup work
      * ``("skip", None)`` — terminal state, control-skipped, or empty entry

    Side effects: marks unprobeable entries (codec_raw missing) as
    ``FLAGGED_CORRUPT`` in the state DB so they surface in the Flagged
    pane instead of silently rotting in PENDING.

    Used at startup by :func:`build_queues` AND mid-session by the
    orchestrator's refresh worker so a Sonarr/Radarr drop-in becomes
    next-up automatically without waiting for a pipeline restart.

    Force-reencode of already-AV1 files: when the user clicks "Queue for
    re-encode" on the dashboard, the requeue endpoint sets
    ``force_reencode=true`` in the row's ``extras`` JSON. Without that
    flag the AV1 branch below would route to gap_filler/skip and the
    user's queue action would be a silent no-op (the only thing it'd
    achieve is flipping status to ``pending``, which is invisible to
    the queue builder for AV1 files). The flag is cleared in
    ``full_gamut`` on a successful DONE transition; if the encode
    fails the flag stays set so the next queue-build pass picks it up
    again automatically.
    """
    from pipeline.gap_filler import analyse_gaps

    filepath = entry.get("filepath", "")
    video = entry.get("video", {})
    codec_raw = video.get("codec_raw", "")

    if not filepath:
        return ("skip", None)

    if control.should_skip(filepath):
        return ("skip", None)

    # Already terminal? Skip. DONE means encoded successfully; FLAGGED_*
    # means qualify/audit deliberately parked the file. Earlier versions
    # only skipped "done", so flagged rows landed back in the queue and
    # got re-encoded with the wrong audio.
    existing = state.get_file(filepath)
    if existing and is_terminal(existing["status"]):
        return ("skip", None)

    # Unprobeable: ffprobe couldn't determine the video codec. Earlier
    # versions silently skipped these files at queue-build time, so
    # corrupt / truncated files sat in PENDING forever, never visible to
    # the user. Flag them so the Flagged pane surfaces them.
    if not codec_raw:
        state.set_file(
            filepath,
            FileStatus.FLAGGED_CORRUPT,
            stage="scan",
            reason="ffprobe could not determine video codec",
        )
        return ("skip", None)

    if codec_raw == "av1":
        # User-initiated force re-encode wins over the codec check.
        # Without this an already-AV1 file at the wrong CQ can never be
        # re-encoded — the queue builder would route it to gap_filler
        # (audio strip / sub stamp only) or skip outright.
        if existing and existing.get("force_reencode"):
            return ("full_gamut", _build_full_gamut_item(entry))
        gaps = analyse_gaps(entry, config)
        # gap_filler explicitly does NOT do audio transcodes (see
        # pipeline/gap_filler.py:389-394 — fetch+ffmpeg+upload is heavy
        # and excluded). Pre-2026-05-12 we still routed AV1 files with
        # AC-3/DTS/etc. audio to gap_filler, which ran its other ops
        # (track strip, tags) and then marked DONE — leaving the wrong
        # audio in place. That's a Rule-1 violation (DONE-on-a-lie).
        # LotR Return of the King shipped with AC-3 5.1 + commentary
        # sub because of this. Route audio-transcode files to full_gamut
        # so the encoder actually does the transcode.
        if gaps.needs_audio_transcode:
            return ("full_gamut", _build_full_gamut_item(entry))
        if gaps.needs_anything:
            return ("gap_filler", entry)
        return ("skip", None)

    # Non-AV1 → full re-encode
    return ("full_gamut", _build_full_gamut_item(entry))


def _read_priority_paths(staging_dir: str | None = None) -> set[str]:
    """Return the set of paths in ``control/priority.json -> paths``.

    Items whose filepath is in this set get bumped to the front of
    ``full_gamut_queue`` regardless of size. Used for one-off test
    runs (e.g. "encode these 30 specific files first overnight to
    verify the fix actually shrinks them"). Empty list / missing
    file = no bump (queue stays in its normal size order).

    The 2026-05-08 incident review noted the old force-stack
    mechanism was removed without a replacement. This is a lighter
    replacement: read-once-at-build-time, no run-time IPC.
    """
    if staging_dir is None:
        staging_dir = str(STAGING_DIR)
    prio_path = os.path.join(staging_dir, "control", "priority.json")
    if not os.path.exists(prio_path):
        return set()
    try:
        with open(prio_path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return set()
    return set(data.get("paths") or [])


def _sort_full_gamut(queue: list, config: dict, priority_paths: set[str]) -> None:
    """In-place sort of the full_gamut queue.

    Order:
      1. Priority paths (per ``control/priority.json -> paths``), among
         themselves sorted by size in the configured direction.
      2. Everything else, sorted by size in the configured direction.

    Largest-first is the default (the user's 2026-05-02 ask) so big
    files burn down the ETA first; smallest_first config override
    is preserved for the legacy burn-through-quick-wins use case.
    """
    order = (config.get("encode_queue_order") or "largest_first").lower()
    largest_first = order == "largest_first"

    def _key(item: dict) -> tuple:
        is_priority = 0 if item.get("filepath") in priority_paths else 1
        size = item.get("file_size_bytes", 0)
        # Sort by (priority_rank ASC, size in configured direction).
        # Negate size for largest-first so the natural ascending tuple
        # sort puts bigger files first within each priority class.
        return (is_priority, -size if largest_first else size)

    queue.sort(key=_key)


def build_queues(report_path: str, config: dict, state: PipelineState, control: PipelineControl):
    """Build separate queues for full_gamut and gap_filler from the media report."""
    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)

    from pipeline.gap_filler import analyse_gaps

    full_gamut_queue = []
    gap_filler_queue = []

    for entry in report.get("files", []):
        category, item = categorise_entry(entry, config, state, control)
        if category == "full_gamut":
            full_gamut_queue.append(item)
        elif category == "gap_filler":
            gap_filler_queue.append(item)

    priority_paths = _read_priority_paths()
    _sort_full_gamut(full_gamut_queue, config, priority_paths)
    if priority_paths:
        n_prio = sum(1 for it in full_gamut_queue if it.get("filepath") in priority_paths)
        logging.info(
            f"Priority bump active: {n_prio} of {len(full_gamut_queue)} full_gamut "
            f"items lifted to the front of the queue"
        )

    # Gap filler: NAS-only work first (no fetch), then by size
    # needs_fetch is True only for audio transcode — everything else runs on NAS
    def _gap_sort_key(entry):
        gaps = analyse_gaps(entry, config)
        return (1 if gaps.needs_fetch else 0, entry.get("file_size_bytes", 0))

    gap_filler_queue.sort(key=_gap_sort_key)

    return full_gamut_queue, gap_filler_queue


def main():
    parser = argparse.ArgumentParser(description="AV1 Media Pipeline — one pass, everything done")
    parser.add_argument("--report", type=str, default=str(MEDIA_REPORT))
    parser.add_argument("--staging", type=str, default=str(STAGING_DIR))
    parser.add_argument("--state-file", type=str, default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-replace", action="store_true")
    parser.add_argument(
        "--no-gap-filler", action="store_true", help="Disable gap filler (GPU encodes only, no cleanup)"
    )
    parser.add_argument("--gap-filler-only", action="store_true", help="Run gap filler only (no GPU encodes)")
    parser.add_argument("--max-staging-gb", type=int, default=None)
    parser.add_argument("--max-fetch-gb", type=int, default=None)
    args = parser.parse_args()

    # Config
    overrides_path = os.path.join(args.staging, "control", "config_overrides.json")
    file_overrides = {}
    if os.path.exists(overrides_path):
        try:
            with open(overrides_path, encoding="utf-8") as f:
                file_overrides = json.load(f)
        except Exception:
            pass
    config = build_config(file_overrides)

    if args.no_replace:
        config["replace_original"] = False
    if args.max_staging_gb is not None:
        config["max_staging_bytes"] = args.max_staging_gb * 1024**3
    if args.max_fetch_gb is not None:
        config["max_fetch_buffer_bytes"] = args.max_fetch_gb * 1024**3

    # Staging
    os.makedirs(args.staging, exist_ok=True)
    setup_logging(args.staging)

    # ProcessRegistry reconcile at session start. Before we launch any worker
    # threads, drop entries whose PIDs are dead or recycled — ghost entries from
    # previous sessions that crashed without cleaning up would otherwise block
    # our own registration for the same role.
    from pathlib import Path

    from pipeline.process_registry import ProcessRegistry

    registry_path = Path(args.staging) / "control" / "agents.registry.json"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry = ProcessRegistry(registry_path)
    dead = registry.reconcile()
    logging.info(f"Reaped {len(dead)} dead registry entries: {dead}")

    # State
    db_path = args.state_file or os.path.join(args.staging, "pipeline_state.db")
    state = PipelineState(db_path)
    serializable_config = copy.deepcopy(config)
    if isinstance(serializable_config.get("lossless_audio_codecs"), set):
        serializable_config["lossless_audio_codecs"] = sorted(serializable_config["lossless_audio_codecs"])
    state.set_meta("config", serializable_config)
    state.save()

    # Control
    control = PipelineControl(args.staging)

    # Build queues
    if not os.path.exists(args.report):
        logging.error(f"Report not found: {args.report}")
        sys.exit(1)

    full_gamut_queue, gap_filler_queue = build_queues(args.report, config, state, control)

    logging.info(f"Full gamut: {len(full_gamut_queue)} files to encode")
    logging.info(f"Gap filler: {len(gap_filler_queue)} files to clean up")

    if args.dry_run:
        logging.info("\nDRY RUN -- full gamut queue:")
        for item in full_gamut_queue[:20]:
            codec = item.get("video_codec", "?")
            res = item.get("resolution", "?")
            logging.info(f"  {codec} {res:6s} {item['filename']}")
        if len(full_gamut_queue) > 20:
            logging.info(f"  ... and {len(full_gamut_queue) - 20} more")
        logging.info("\nDRY RUN -- gap filler queue:")
        from pipeline.gap_filler import analyse_gaps

        for entry in gap_filler_queue[:20]:
            gaps = analyse_gaps(entry, config)
            logging.info(f"  {gaps.describe():30s} {entry['filename']}")
        if len(gap_filler_queue) > 20:
            logging.info(f"  ... and {len(gap_filler_queue) - 20} more")
        return

    if not full_gamut_queue and not gap_filler_queue:
        logging.info("Nothing to process!")
        return

    # Run orchestrator under the process registry so a crashed session's
    # entry is still reaped next time (reconcile above) and live entries
    # are visible to tools like `tools/invariants.py`.
    from pipeline.orchestrator import Orchestrator

    orchestrator = Orchestrator(config, state, args.staging, control)

    with registry.register("pipeline", sys.argv):
        if args.gap_filler_only:
            orchestrator.run([], gap_filler_queue, enable_gap_filler=True)
        else:
            orchestrator.run(full_gamut_queue, gap_filler_queue, enable_gap_filler=not args.no_gap_filler)


if __name__ == "__main__":
    main()
