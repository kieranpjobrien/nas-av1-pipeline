"""CLI entry point — run via `python -m pipeline` or `uv run python -m pipeline`."""

import argparse
import copy
import json
import logging
import os
import sys

from paths import MEDIA_REPORT, STAGING_DIR
from pipeline.config import build_config
from pipeline.control import PipelineControl
from pipeline.state import PipelineState, is_terminal


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


def build_queues(report_path: str, config: dict, state: PipelineState, control: PipelineControl):
    """Build separate queues for full_gamut and gap_filler from the media report."""
    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)

    from pipeline.gap_filler import analyse_gaps

    full_gamut_queue = []
    gap_filler_queue = []

    for entry in report.get("files", []):
        filepath = entry.get("filepath", "")
        video = entry.get("video", {})
        codec_raw = video.get("codec_raw", "")

        if not filepath or not codec_raw:
            continue

        # Skip if control says so
        if control.should_skip(filepath):
            continue

        # Already in a terminal state? Skip. DONE means encoded successfully;
        # FLAGGED_* means the audit / qualify step deliberately parked the file
        # for the user to action via the UI. Earlier versions only skipped
        # "done", so flagged rows landed back in the queue and got re-encoded
        # with the wrong audio. To force re-processing, delete the state DB
        # row for the file.
        existing = state.get_file(filepath)
        if existing and is_terminal(existing["status"]):
            continue

        if codec_raw == "av1":
            # Already AV1 — check if gap filling needed
            gaps = analyse_gaps(entry, config)
            if gaps.needs_anything:
                gap_filler_queue.append(entry)
        else:
            # Needs full encode
            res = video.get("resolution_class", "")
            codec = video.get("codec", codec_raw)
            bitrate = entry.get("overall_bitrate_kbps", 0) or 0

            full_gamut_queue.append(
                {
                    "filepath": filepath,
                    "filename": entry["filename"],
                    "file_size_bytes": entry.get("file_size_bytes", 0),
                    "file_size_gb": entry.get("file_size_gb", 0),
                    "duration_seconds": entry.get("duration_seconds", 0),
                    "video_codec": codec,
                    "resolution": res,
                    "bitrate_kbps": bitrate,
                    "hdr": video.get("hdr", False),
                    "bit_depth": video.get("bit_depth", 8),
                    "audio_streams": entry.get("audio_streams", []),
                    "subtitle_streams": entry.get("subtitle_streams", []),
                    "subtitle_count": entry.get("subtitle_count", 0),
                    "library_type": entry.get("library_type", ""),
                }
            )

    # Smallest-first: easiest quick wins run before the big remuxes.
    full_gamut_queue.sort(key=lambda x: x["file_size_bytes"])

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
