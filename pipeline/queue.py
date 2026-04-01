"""Priority queue builder — loads media report and builds encoding order."""

import json
import logging
import os
from typing import Callable, Optional

from pipeline.encoding import has_bulky_audio
from pipeline.state import FileStatus, PipelineState

_KEEP_LANGS = {"eng", "en", "english", "und", ""}


def _has_foreign_subs(file_entry: dict, config: dict) -> bool:
    """Check if a file has non-English subtitle streams that should be stripped."""
    if not config.get("strip_non_english_subs", True):
        return False
    for sub in file_entry.get("subtitle_streams", []):
        lang = (sub.get("language") or sub.get("detected_language") or "und").lower().strip()
        title = (sub.get("title") or "").lower()
        is_forced = "forced" in title or "foreign" in title
        if lang not in _KEEP_LANGS and not is_forced:
            return True
    return False


def _has_foreign_audio(file_entry: dict, config: dict) -> bool:
    """Check if a file has non-English/non-original audio tracks that should be stripped.

    Keeps track 0 (original language) and all English/und tracks.
    Returns True if there are extra foreign tracks to remove.
    """
    if not config.get("strip_non_english_audio", True):
        return False
    audio = file_entry.get("audio_streams", [])
    if len(audio) <= 1:
        return False
    for i, a in enumerate(audio):
        if i == 0:
            continue  # always keep original
        lang = (a.get("language") or a.get("detected_language") or "und").lower().strip()
        if lang not in _KEEP_LANGS:
            return True
    return False


def build_priority_queue(report_path: str, config: dict, state: PipelineState,
                         is_reencode: Optional[Callable] = None) -> list[dict]:
    """Load report, filter already-AV1 files, sort by priority tier then file size.

    AV1 files with bulky audio (lossless, DTS, high-bitrate AC-3) are queued
    for audio-only remux instead of being skipped.
    """
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

        # Already AV1: check if audio or subs need work, otherwise skip
        if codec_raw in ("av1",):
            if is_reencode and is_reencode(filepath):
                pass  # flagged for full re-encode, fall through
            elif has_bulky_audio(f, config):
                # Queue for audio-only remux (also strips subs in same pass)
                existing = state.get_file(filepath)
                if existing and existing["status"] in (FileStatus.VERIFIED.value, FileStatus.SKIPPED.value):
                    if existing.get("audio_only"):
                        continue
                    if existing.get("reason") == "already AV1":
                        state.set_file(filepath, FileStatus.PENDING, reason="audio remux needed")

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
                    "subtitle_streams": f.get("subtitle_streams", []),
                    "subtitle_count": f.get("subtitle_count", 0),
                    "library_type": f.get("library_type", ""),
                    "priority_tier": 999,
                    "tier_name": "Audio remux (AV1)",
                    "audio_only": True,
                })
                continue
            elif _has_foreign_subs(f, config) or _has_foreign_audio(f, config):
                # AV1 + EAC-3 but has foreign subs/audio — cleanup remux
                needs_subs = _has_foreign_subs(f, config)
                needs_audio = _has_foreign_audio(f, config)
                reason = "cleanup: " + " + ".join(
                    [x for x in ["foreign subs" if needs_subs else "", "foreign audio" if needs_audio else ""] if x]
                )
                existing = state.get_file(filepath)
                if existing and existing["status"] in (FileStatus.VERIFIED.value, FileStatus.SKIPPED.value):
                    if existing.get("cleanup_strip"):
                        continue
                    state.set_file(filepath, FileStatus.PENDING, reason=reason)

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
                    "subtitle_streams": f.get("subtitle_streams", []),
                    "subtitle_count": f.get("subtitle_count", 0),
                    "library_type": f.get("library_type", ""),
                    "priority_tier": 1000,
                    "tier_name": "Cleanup remux (AV1)",
                    "audio_only": True,
                    "cleanup_strip": True,
                })
                continue
            else:
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
            # Re-encode files get their state reset so they re-enter the queue
            if is_reencode and is_reencode(filepath):
                state.set_file(filepath, FileStatus.PENDING, reason="flagged for re-encode")
            else:
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
