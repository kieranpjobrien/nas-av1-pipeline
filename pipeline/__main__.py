"""CLI entry point — run via `python -m pipeline`."""

import argparse
import copy
import logging
import os
import sys

from paths import STAGING_DIR, MEDIA_REPORT
from pipeline.config import DEFAULT_CONFIG
from pipeline.queue import build_priority_queue
from pipeline.runner import Pipeline
from pipeline.state import PipelineState


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


def main():
    parser = argparse.ArgumentParser(
        description="AV1 Media Conversion Pipeline (NVENC)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Control system:
  Copy template JSONs from control_templates/ into <staging>/control/ to activate.
  Delete the file to deactivate. Pipeline checks every file boundary.

  Pause (drop one to pause, delete to resume):
    pause_all.json       - pause everything
    pause_fetch.json     - pause fetching (current encode continues)
    pause_encode.json    - pause encoding (fetches continue)
    pause.json           - generic (reads "type" from JSON)
    PAUSE                - empty file in staging dir (no JSON needed)

  Queue control:
    skip.json            - {"paths": ["Z:\\path\\file.mkv", ...]}
    priority.json        - {"paths": ["Z:\\path\\file.mkv", ...]}

  Encode overrides:
    gentle.json          - per-file or pattern CQ/preset overrides
                           {"paths": {...}, "patterns": {...}, "default_offset": 0}

Examples:
  python -m pipeline --report media_report.json --dry-run
  python -m pipeline --report media_report.json --tier "H.264 1080p" --no-replace
  python -m pipeline --resume
""",
    )
    parser.add_argument("--report", type=str, default=str(MEDIA_REPORT),
                        help="Path to media_report.json from scanner")
    parser.add_argument("--staging", type=str, default=str(STAGING_DIR),
                        help="Local staging directory (default: E:\\AV1_Staging)")
    parser.add_argument("--state-file", type=str, default=None,
                        help="State file path (default: <staging>/pipeline_state.json)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from existing state file")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without doing it")
    parser.add_argument("--no-replace", action="store_true",
                        help="Don't replace originals on NAS (keep .av1.mkv alongside original)")
    parser.add_argument("--audio", choices=["copy", "smart"], default=None,
                        help="Audio mode: copy (passthrough) or smart (lossless->EAC3, lossy->copy)")
    parser.add_argument("--max-staging-gb", type=int, default=None,
                        help="Max staging space in GB (default: 1500)")
    parser.add_argument("--max-fetch-gb", type=int, default=None,
                        help="Max fetch buffer in GB (default: 300)")
    parser.add_argument("--tier", type=str, default=None,
                        help="Only process specific tier (e.g. 'H.264 1080p')")
    args = parser.parse_args()

    # Config — deep copy nested dicts
    config = copy.deepcopy(DEFAULT_CONFIG)

    if args.no_replace:
        config["replace_original"] = False
    if args.audio is not None:
        config["audio_mode"] = args.audio
    if args.max_staging_gb is not None:
        config["max_staging_bytes"] = args.max_staging_gb * 1024**3
    if args.max_fetch_gb is not None:
        config["max_fetch_buffer_bytes"] = args.max_fetch_gb * 1024**3

    # Staging
    os.makedirs(args.staging, exist_ok=True)
    setup_logging(args.staging)

    # State
    state_file = args.state_file or os.path.join(args.staging, "pipeline_state.json")
    state = PipelineState(state_file)
    # Store config (convert sets to lists for JSON serialization)
    serializable_config = copy.deepcopy(config)
    if isinstance(serializable_config.get("lossless_audio_codecs"), set):
        serializable_config["lossless_audio_codecs"] = sorted(serializable_config["lossless_audio_codecs"])
    state.data["config"] = serializable_config
    state.save()

    # Build queue
    if not os.path.exists(args.report):
        logging.error(f"Report not found: {args.report}")
        sys.exit(1)

    queue = build_priority_queue(args.report, config, state)

    # Filter by tier if requested
    if args.tier:
        queue = [item for item in queue if item["tier_name"].lower() == args.tier.lower()]
        logging.info(f"Filtered to tier '{args.tier}': {len(queue)} files")

    if not queue:
        logging.info("Nothing to process!")
        return

    # Run pipeline
    pipeline = Pipeline(config, state, args.staging, report_path=args.report)
    pipeline.run(queue, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
