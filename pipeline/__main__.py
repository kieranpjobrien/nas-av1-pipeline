"""CLI entry point — run via `python -m pipeline`."""

import argparse
import copy
import logging
import os
import sys

from paths import STAGING_DIR, MEDIA_REPORT
from pipeline.config import DEFAULT_CONFIG
from pipeline.control import PipelineControl
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
    profiles.json        - assign content to quality profiles
                           {"paths": {...}, "patterns": {...}, "default": "baseline"}
                           Profiles: protected (high quality), baseline, lossy (aggressive)
    reencode.json        - re-encode already-AV1 files with different CQ
                           {"files": {...}, "patterns": {...}}

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

    # Config — load defaults + config_overrides.json + CLI args
    from pipeline.config import build_config
    overrides_path = os.path.join(args.staging, "control", "config_overrides.json")
    file_overrides = {}
    if os.path.exists(overrides_path):
        try:
            import json as _json
            with open(overrides_path, encoding="utf-8") as _f:
                file_overrides = _json.load(_f)
        except Exception:
            pass
    config = build_config(file_overrides)

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

    # State — SQLite backed. Migrate from JSON on first run.
    from pipeline.state import migrate_from_json
    json_state = os.path.join(args.staging, "pipeline_state.json")
    db_path = args.state_file or os.path.join(args.staging, "pipeline_state.db")
    migrate_from_json(json_state, db_path)
    state = PipelineState(db_path)
    # Store config snapshot
    serializable_config = copy.deepcopy(config)
    if isinstance(serializable_config.get("lossless_audio_codecs"), set):
        serializable_config["lossless_audio_codecs"] = sorted(serializable_config["lossless_audio_codecs"])
    state.set_meta("config", serializable_config)
    state.save()

    # Build queue
    if not os.path.exists(args.report):
        logging.error(f"Report not found: {args.report}")
        sys.exit(1)

    control = PipelineControl(args.staging)

    def is_reencode(filepath: str) -> bool:
        return control.get_reencode_override(filepath) is not None

    reencode_data = control._read_control_file("reencode.json") or {}
    has_reencode = bool(reencode_data.get("files") or reencode_data.get("patterns"))
    queue = build_priority_queue(args.report, config, state,
                                 is_reencode=is_reencode if has_reencode else None)

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
