"""CLI entry point — run via `python -m pipeline` or `uv run python -m pipeline`."""

import argparse
import copy
import json
import logging
import os
import sys

from paths import STAGING_DIR, MEDIA_REPORT
from pipeline.config import DEFAULT_CONFIG, build_config
from pipeline.control import PipelineControl
from pipeline.state import PipelineState, migrate_from_json


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
    from pipeline.ffmpeg import has_bulky_audio

    keep_langs = {"eng", "en", "english", "und", ""}
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

        # Already done in pipeline?
        existing = state.get_file(filepath)
        if existing and existing["status"] == "done":
            continue

        if codec_raw == "av1":
            # Already AV1 — check if gap filling needed
            gaps = analyse_gaps(entry, config)
            if gaps.needs_anything:
                gap_filler_queue.append(entry)
        else:
            # Needs full encode
            # Assign priority tier
            from pipeline.config import get_res_key
            res = video.get("resolution_class", "")
            codec = video.get("codec", codec_raw)
            bitrate = entry.get("overall_bitrate_kbps", 0) or 0

            tier_idx = 99
            tier_name = f"{codec} {res}"
            for idx, tier in enumerate(config.get("priority_tiers", [])):
                tier_codec = tier.get("codec")
                tier_res = tier.get("resolution")
                min_br = tier.get("min_bitrate_kbps", 0)
                max_br = tier.get("max_bitrate_kbps", float("inf"))
                if (tier_codec is None or tier_codec.lower() in codec_raw.lower() or tier_codec.lower() in codec.lower()):
                    if tier_res is None or tier_res.lower() == res.lower():
                        if min_br <= bitrate <= max_br:
                            tier_idx = idx
                            tier_name = tier.get("name", tier_name)
                            break

            full_gamut_queue.append({
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
                "priority_tier": tier_idx,
                "tier_name": tier_name,
            })

    # Sort: full_gamut by priority tier then size desc
    full_gamut_queue.sort(key=lambda x: (x["priority_tier"], -x["file_size_bytes"]))

    # Gap filler: smallest first (fastest wins)
    gap_filler_queue.sort(key=lambda x: x.get("file_size_bytes", 0))

    return full_gamut_queue, gap_filler_queue


def main():
    parser = argparse.ArgumentParser(description="AV1 Media Pipeline — one pass, everything done")
    parser.add_argument("--report", type=str, default=str(MEDIA_REPORT))
    parser.add_argument("--staging", type=str, default=str(STAGING_DIR))
    parser.add_argument("--state-file", type=str, default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-replace", action="store_true")
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

    # State
    json_state = os.path.join(args.staging, "pipeline_state.json")
    db_path = args.state_file or os.path.join(args.staging, "pipeline_state.db")
    migrate_from_json(json_state, db_path)
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
        logging.info("\nDRY RUN — full gamut queue:")
        for item in full_gamut_queue[:20]:
            logging.info(f"  {item['tier_name']:25s} {item['filename']}")
        if len(full_gamut_queue) > 20:
            logging.info(f"  ... and {len(full_gamut_queue) - 20} more")
        logging.info(f"\nDRY RUN — gap filler queue:")
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

    # Run orchestrator
    from pipeline.orchestrator import Orchestrator
    orchestrator = Orchestrator(config, state, args.staging, control)
    orchestrator.run(full_gamut_queue, gap_filler_queue)


if __name__ == "__main__":
    main()
