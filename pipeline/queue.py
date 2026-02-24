"""Priority queue builder — loads media report and builds encoding order."""

import json
import logging

from pipeline.state import FileStatus, PipelineState


def build_priority_queue(report_path: str, config: dict, state: PipelineState) -> list[dict]:
    """Load report, filter already-AV1 files, sort by priority tier then file size."""
    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)

    files = report.get("files", [])
    queue = []

    for f in files:
        filepath = f["filepath"]
        video = f.get("video", {})
        codec = video.get("codec", "")
        codec_raw = video.get("codec_raw", "")
        resolution = video.get("resolution_class", "")
        bitrate = f.get("overall_bitrate_kbps", 0) or 0

        # Skip already AV1
        if codec_raw in ("av1",):
            existing = state.get_file(filepath)
            if not existing:
                state.set_file(filepath, FileStatus.SKIPPED, reason="already AV1")
            continue

        # Skip unknown codec
        if codec == "unknown":
            existing = state.get_file(filepath)
            if not existing:
                state.set_file(filepath, FileStatus.SKIPPED, reason="unknown codec")
            continue

        # Check if already completed
        existing = state.get_file(filepath)
        if existing and existing["status"] in (FileStatus.VERIFIED.value, FileStatus.SKIPPED.value):
            continue

        # Assign priority tier
        tier_idx = len(config["priority_tiers"])  # default: lowest
        for idx, tier in enumerate(config["priority_tiers"]):
            codec_match = tier["codec"] is None or tier["codec"] == codec
            res_match = tier["resolution"] is None or tier["resolution"] == resolution
            min_br = tier.get("min_bitrate_kbps", 0)
            max_br = tier.get("max_bitrate_kbps", float("inf"))
            bitrate_match = bitrate >= min_br and bitrate <= max_br

            if codec_match and res_match and bitrate_match:
                tier_idx = idx
                break

        queue.append({
            "filepath": filepath,
            "filename": f["filename"],
            "file_size_bytes": f["file_size_bytes"],
            "file_size_gb": f["file_size_gb"],
            "duration_seconds": f.get("duration_seconds", 0),
            "video_codec": codec,
            "resolution": resolution,
            "bitrate_kbps": bitrate,
            "hdr": video.get("hdr", False),
            "bit_depth": video.get("bit_depth", 8),
            "audio_streams": f.get("audio_streams", []),
            "subtitle_count": f.get("subtitle_count", 0),
            "library_type": f.get("library_type", ""),
            "priority_tier": tier_idx,
            "tier_name": config["priority_tiers"][tier_idx]["name"] if tier_idx < len(config["priority_tiers"]) else "other",
        })

    # Sort: priority tier ASC, then file size DESC (big files first within tier)
    queue.sort(key=lambda x: (x["priority_tier"], -x["file_size_bytes"]))

    logging.info(f"Queue built: {len(queue)} files to process")

    # Log tier breakdown
    tier_counts = {}
    for item in queue:
        tn = item["tier_name"]
        if tn not in tier_counts:
            tier_counts[tn] = {"count": 0, "size_gb": 0}
        tier_counts[tn]["count"] += 1
        tier_counts[tn]["size_gb"] += item["file_size_gb"]
    for tn, info in tier_counts.items():
        logging.info(f"  Tier '{tn}': {info['count']} files, {info['size_gb']:.1f} GB")

    return queue
