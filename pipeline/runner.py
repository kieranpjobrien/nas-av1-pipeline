"""Main pipeline orchestration — Pipeline class, prefetch thread, signal handling."""

import copy
import json
import logging
import os
import queue as queue_mod
import signal
import sys
import threading
import time

from pipeline.config import get_res_key, resolve_encode_params
from pipeline.control import PipelineControl
from pipeline.encoding import format_bytes, format_duration, has_bulky_audio, stage_audio_remux, stage_encode
from pipeline.stages import get_free_space, get_staging_usage, stage_fetch, stage_replace, stage_upload, stage_verify
from pipeline.state import FileStatus, PipelineState


def format_eta(remaining_files: int, avg_secs_per_file: float) -> str:
    if avg_secs_per_file <= 0 or remaining_files <= 0:
        return "unknown"
    total_secs = remaining_files * avg_secs_per_file
    return format_duration(total_secs)


def format_eta_tier_aware(queue: list[dict], state: PipelineState) -> str:
    """Compute ETA using per-tier average encode times where available.

    For tiers with data, uses the tier-specific average. Falls back to the
    overall average for tiers without enough data.
    """
    tier_stats = state.stats.get("tier_stats", {})
    overall_completed = state.stats.get("completed", 0)
    overall_time = state.stats.get("total_encode_time_secs", 0)
    overall_avg = overall_time / overall_completed if overall_completed > 0 else 0

    if overall_avg <= 0:
        return "unknown"

    total_secs = 0.0
    remaining = 0
    for item in queue:
        filepath = item["filepath"]
        existing = state.get_file(filepath)
        status = existing["status"] if existing else None
        done_statuses = {FileStatus.VERIFIED.value, FileStatus.REPLACED.value,
                         FileStatus.SKIPPED.value, FileStatus.ERROR.value}
        if status in done_statuses:
            continue

        remaining += 1
        res_key = get_res_key(item)
        tier = tier_stats.get(res_key, {})
        tier_completed = tier.get("completed", 0)
        tier_time = tier.get("total_encode_time_secs", 0)
        if tier_completed >= 2 and tier_time > 0:
            total_secs += tier_time / tier_completed
        else:
            total_secs += overall_avg

    if remaining <= 0:
        return "done"
    return format_duration(total_secs)


class Pipeline:
    def __init__(self, config: dict, state: PipelineState, staging_dir: str,
                 report_path: str = ""):
        self.config = config
        self.state = state
        self.staging_dir = staging_dir
        self.report_path = report_path
        self.control = PipelineControl(staging_dir)
        self._shutdown = False
        self._report_cache = None  # lazy-loaded media report index

        # Upload worker queue: items waiting for upload → verify → replace
        self._upload_queue: queue_mod.Queue[tuple[dict, dict]] = queue_mod.Queue()
        # Registry of item metadata by filepath (for upload worker lookups)
        self._item_configs: dict[str, dict] = {}

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        if self._shutdown:
            logging.warning("Force quit — exiting immediately")
            sys.exit(1)
        logging.info("\nShutdown requested — finishing current file then stopping...")
        logging.info("(Press Ctrl+C again to force quit)")
        self._shutdown = True

    def _wait_for_staging_space(self):
        """Block until there's space in staging, or we're shut down."""
        while not self._shutdown:
            usage = get_staging_usage(self.staging_dir)
            free = get_free_space(self.staging_dir)
            # Also check fetch buffer — stage_fetch has its own limit
            fetch_dir = os.path.join(self.staging_dir, "fetch")
            fetch_usage = 0
            if os.path.exists(fetch_dir):
                for f in os.listdir(fetch_dir):
                    try:
                        fetch_usage += os.path.getsize(os.path.join(fetch_dir, f))
                    except OSError:
                        pass
            if (usage < self.config["max_staging_bytes"] and
                    free > self.config["min_free_space_bytes"] and
                    fetch_usage < self.config["max_fetch_buffer_bytes"]):
                return True
            logging.info(f"Waiting for staging space... "
                         f"(used: {format_bytes(usage)}, fetch: {format_bytes(fetch_usage)}, free: {format_bytes(free)})")
            time.sleep(30)
        return False

    def _prefetch_worker(self, queue: list[dict]):
        """Background thread: pre-fetch files from NAS while encoder is busy.

        Uses a byte-based buffer limit (from config) with a file count cap to
        keep the GPU fed without over-fetching. Large 4K files get fewer
        prefetches; small 720p files get more.
        """
        MAX_PREFETCH_FILES = 20  # hard cap on file count
        max_prefetch_bytes = self.config.get("max_fetch_buffer_bytes", 100 * 1024**3)

        logging.info("Prefetch thread started (buffer: %s, max %d files)",
                     format_bytes(max_prefetch_bytes), MAX_PREFETCH_FILES)
        while not self._shutdown:
            fetched_any = False

            # Count pending fetched files and estimate their total size
            fetched_paths = self.state.get_files_by_status(FileStatus.FETCHED)
            pending_fetched = len(fetched_paths)
            pending_bytes = 0
            for fp in fetched_paths:
                fi = self.state.get_file(fp)
                if fi:
                    pending_bytes += fi.get("input_size_bytes", 0) or 0
            if pending_fetched >= MAX_PREFETCH_FILES or pending_bytes >= max_prefetch_bytes:
                # Enough files queued for encoding, wait before fetching more
                for _ in range(6):
                    if self._shutdown:
                        break
                    time.sleep(5)
                continue

            # Build priority set — fetch these first
            priority_set = {
                os.path.normpath(p).lower()
                for p in self.control.get_priority_bumps()
            }

            # Sort queue: priority items first, then original order
            fetch_order = sorted(
                queue,
                key=lambda item: (
                    0 if os.path.normpath(item["filepath"]).lower() in priority_set else 1,
                ),
            )

            for item in fetch_order:
                if self._shutdown:
                    break

                filepath = item["filepath"]

                # Skip completed/error/already-fetched files
                existing = self.state.get_file(filepath)
                if existing and existing["status"] not in (
                    FileStatus.PENDING.value, None
                ):
                    continue

                # Skip if control system says so
                if self.control.should_skip(filepath):
                    continue

                # Re-check lookahead limit before each fetch
                fetched_paths = self.state.get_files_by_status(FileStatus.FETCHED)
                pending_fetched = len(fetched_paths)
                if pending_fetched >= MAX_PREFETCH_FILES:
                    break

                # Respect fetch pause
                while self.control.is_fetch_paused() and not self._shutdown:
                    time.sleep(5)
                if self._shutdown:
                    break

                # Try to fetch — returns None if buffer full
                result = stage_fetch(item, self.staging_dir, self.config, self.state)
                if result is not None:
                    fetched_any = True
                # If buffer full, skip to next item (will retry on next loop)

            if not fetched_any:
                # Nothing was fetchable this pass — wait before retrying
                for _ in range(6):  # 30s total, checking shutdown every 5s
                    if self._shutdown:
                        break
                    time.sleep(5)

        logging.info("Prefetch thread finished")

    def _upload_worker(self):
        """Background thread: upload encoded files to NAS while GPU encodes the next file.

        Processes upload → verify → replace for each encoded file, allowing the
        main loop to immediately start the next encode without waiting for network I/O.
        """
        logging.info("Upload worker started — uploads will overlap with encoding")
        while True:
            try:
                item, effective_config = self._upload_queue.get(timeout=5)
            except queue_mod.Empty:
                if self._shutdown and self._upload_queue.empty():
                    break
                continue

            filepath = item["filepath"]

            try:
                # Upload
                existing = self.state.get_file(filepath)
                current_status = existing["status"] if existing else None

                if current_status == FileStatus.ENCODED.value:
                    success = stage_upload(filepath, item, self.staging_dir, effective_config, self.state)
                    if not success:
                        self.state.stats["errors"] += 1
                        self.state.save()
                        self._upload_queue.task_done()
                        continue

                # Verify
                existing = self.state.get_file(filepath)
                current_status = existing["status"] if existing else None

                if current_status == FileStatus.UPLOADED.value:
                    success = stage_verify(filepath, item, effective_config, self.state)
                    if not success:
                        self.state.stats["errors"] += 1
                        self.state.save()
                        self._upload_queue.task_done()
                        continue

                # Replace
                existing = self.state.get_file(filepath)
                current_status = existing["status"] if existing else None

                if current_status == FileStatus.VERIFIED.value and effective_config.get("replace_original", True):
                    success = stage_replace(filepath, item, effective_config, self.state)
                    if success:
                        self.control.remove_reencode(filepath)
                    else:
                        self.state.stats["errors"] += 1
                        self.state.save()

                elif current_status == FileStatus.REPLACING.value:
                    success = stage_replace(filepath, item, effective_config, self.state)
                    if success:
                        self.control.remove_reencode(filepath)
                    else:
                        self.state.stats["errors"] += 1
                        self.state.save()

            except Exception as e:
                logging.error(f"Upload worker error for {item['filename']}: {e}")
                self.state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="upload_worker")
                self.state.stats["errors"] += 1
                self.state.save()

            self._upload_queue.task_done()

        logging.info("Upload worker finished")

    def _audio_remux_async(self, item: dict, effective_config: dict):
        """Run audio-only remux in a background thread (no GPU needed).

        After remux completes, enqueues the item for the upload worker.
        """
        filepath = item["filepath"]
        try:
            # Fetch if needed
            existing = self.state.get_file(filepath)
            current_status = existing["status"] if existing else None

            if current_status in (None, FileStatus.PENDING.value):
                local_path = stage_fetch(item, self.staging_dir, effective_config, self.state)
                if local_path is None:
                    logging.warning(f"Audio remux fetch failed: {item['filename']}")
                    self.state.set_file(filepath, FileStatus.ERROR,
                                        error="fetch failed for audio remux", stage="fetch")
                    self.state.stats["errors"] += 1
                    self.state.save()
                    return

            # Wait if still being fetched by prefetch thread
            existing = self.state.get_file(filepath)
            current_status = existing["status"] if existing else None
            if current_status == FileStatus.FETCHING.value:
                for _ in range(120):
                    if self._shutdown:
                        return
                    time.sleep(5)
                    existing = self.state.get_file(filepath)
                    current_status = existing["status"] if existing else None
                    if current_status != FileStatus.FETCHING.value:
                        break

            existing = self.state.get_file(filepath)
            current_status = existing["status"] if existing else None

            if current_status == FileStatus.FETCHED.value:
                output_path = stage_audio_remux(filepath, item, self.staging_dir, effective_config, self.state)
                if output_path is None:
                    self.state.stats["errors"] += 1
                    self.state.save()
                    return

            # Hand off to upload worker
            self._upload_queue.put((item, effective_config))

        except Exception as e:
            logging.error(f"Audio remux thread error for {item['filename']}: {e}")
            self.state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="audio_remux")
            self.state.stats["errors"] += 1
            self.state.save()

    def _resolve_profile(self, filepath: str) -> str:
        """Get the quality profile for a file from profiles.json."""
        return self.control.get_quality_profile(filepath)

    def _apply_gentle_overrides(self, item: dict) -> dict:
        """Apply quality profile, per-file CQ/preset overrides, and reencode overrides.

        Priority order (later wins): profile → gentle.json → reencode.json
        """
        filepath = item["filepath"]
        profile = self._resolve_profile(filepath)
        overrides = self.control.get_gentle_override(filepath)
        reencode_entry = self.control.get_reencode_override(filepath)

        if profile == "baseline" and not overrides and not reencode_entry:
            return self.config

        config = copy.deepcopy(self.config)
        # Resolve with profile applied
        params = resolve_encode_params(config, item, profile_name=profile)
        content_type = params["content_type"]
        res_key = params["res_key"]

        if profile != "baseline":
            # Bake profile-adjusted values into the config copy
            config["cq"][content_type][res_key] = params["cq"]
            config["nvenc_preset"][content_type][res_key] = params["preset"]
            config["nvenc_multipass"][content_type][res_key] = params["multipass"]
            config["nvenc_lookahead"][content_type][res_key] = params["lookahead"]
            logging.info(f"  Quality profile: {profile} (CQ {params['cq']}, {params['preset']})")

        # Gentle overrides stack on top of profile
        if overrides:
            if "cq_offset" in overrides:
                current_cq = config["cq"][content_type][res_key]
                config["cq"][content_type][res_key] = max(1, current_cq + overrides["cq_offset"])
                logging.info(f"  Gentle override: CQ {current_cq} -> {config['cq'][content_type][res_key]}")

            if "cq" in overrides:
                config["cq"][content_type][res_key] = overrides["cq"]
                logging.info(f"  Gentle override: CQ -> {overrides['cq']}")

            if "preset" in overrides:
                config["nvenc_preset"][content_type][res_key] = overrides["preset"]
                logging.info(f"  Gentle override: Preset -> {overrides['preset']}")

        # Reencode CQ takes final priority (absolute value, not offset)
        if reencode_entry and "cq" in reencode_entry:
            config["cq"][content_type][res_key] = reencode_entry["cq"]
            logging.info(f"  Reencode override: CQ -> {reencode_entry['cq']}")

        return config

    def _get_report_index(self) -> dict:
        """Lazy-load and cache the media report as a filepath→entry dict."""
        if self._report_cache is not None:
            return self._report_cache
        if not self.report_path or not os.path.exists(self.report_path):
            return {}
        try:
            with open(self.report_path, "r", encoding="utf-8") as f:
                report = json.load(f)
            self._report_cache = {
                os.path.normpath(entry["filepath"]).lower(): entry
                for entry in report.get("files", [])
            }
            logging.debug(f"Report index loaded: {len(self._report_cache)} entries")
        except Exception as e:
            logging.warning(f"Failed to load report for priority injection: {e}")
            self._report_cache = {}
        return self._report_cache

    def _build_queue_item(self, report_entry: dict) -> dict:
        """Build a queue item dict from a media report entry."""
        video = report_entry.get("video", {})
        codec = video.get("codec", "")
        resolution = video.get("resolution_class", "")
        bitrate = report_entry.get("overall_bitrate_kbps", 0) or 0

        tier_idx = len(self.config["priority_tiers"])
        for idx, tier in enumerate(self.config["priority_tiers"]):
            codec_match = tier["codec"] is None or tier["codec"] == codec
            res_match = tier["resolution"] is None or tier["resolution"] == resolution
            min_br = tier.get("min_bitrate_kbps", 0)
            max_br = tier.get("max_bitrate_kbps", float("inf"))
            if codec_match and res_match and bitrate >= min_br and bitrate <= max_br:
                tier_idx = idx
                break

        codec_raw = video.get("codec_raw", "").lower()
        is_audio_only = codec_raw == "av1" and has_bulky_audio(report_entry, self.config)

        return {
            "filepath": report_entry["filepath"],
            "filename": report_entry["filename"],
            "file_size_bytes": report_entry["file_size_bytes"],
            "file_size_gb": report_entry["file_size_gb"],
            "duration_seconds": report_entry.get("duration_seconds", 0),
            "video_codec": codec,
            "resolution": resolution,
            "bitrate_kbps": bitrate,
            "hdr": video.get("hdr", False),
            "bit_depth": video.get("bit_depth", 8),
            "audio_streams": report_entry.get("audio_streams", []),
            "subtitle_count": report_entry.get("subtitle_count", 0),
            "library_type": report_entry.get("library_type", ""),
            "priority_tier": 999 if is_audio_only else tier_idx,
            "tier_name": ("Audio remux (AV1)" if is_audio_only else
                          (self.config["priority_tiers"][tier_idx]["name"]
                           if tier_idx < len(self.config["priority_tiers"]) else "other")),
            **({"audio_only": True} if is_audio_only else {}),
        }

    def _inject_new_priority_items(self, queue: list[dict]) -> list[dict]:
        """Check priority.json for paths not in queue and inject them at the front."""
        bumps = self.control.get_priority_bumps()
        if not bumps:
            return queue

        queue_paths = {os.path.normpath(item["filepath"]).lower() for item in queue}
        report_index = self._get_report_index()
        new_items = []

        for path in bumps:
            norm = os.path.normpath(path).lower()
            if norm in queue_paths:
                continue

            # Skip if already completed in state
            existing = self.state.get_file(path)
            if existing and existing["status"] in (
                FileStatus.VERIFIED.value, FileStatus.REPLACED.value,
                FileStatus.SKIPPED.value,
            ):
                continue

            # Look up in media report
            entry = report_index.get(norm)
            if not entry:
                continue

            # Skip already-AV1 (unless in reencode list/patterns)
            if (entry.get("video", {}).get("codec_raw") or "") == "av1":
                if self.control.get_reencode_override(path) is None:
                    continue

            item = self._build_queue_item(entry)
            new_items.append(item)

        if new_items:
            logging.info(f"Injected {len(new_items)} new priority items into queue")
            queue = new_items + queue

        return queue

    def process_item(self, item: dict) -> bool:
        """Run one file through fetch (if needed) → encode. Upload is handled by upload worker."""
        filepath = item["filepath"]

        # Check current state for resume
        existing = self.state.get_file(filepath)
        current_status = existing["status"] if existing else None

        # Apply gentle overrides for this file
        effective_config = self._apply_gentle_overrides(item)

        # Recover zombie states from crashed runs: if stuck in ENCODING/UPLOADING
        # and the local file is gone, reset to PENDING so we re-fetch.
        # Note: FETCHING recovery is done at startup only (run() method) to avoid
        # racing with the active prefetch thread during normal operation.
        if current_status in (FileStatus.ENCODING.value, FileStatus.UPLOADING.value):
            local_path = (existing or {}).get("local_path", "")
            if not local_path or not os.path.exists(local_path):
                logging.info(f"Recovering stale {current_status} state (file gone): {item['filename']}")
                self.state.set_file(filepath, FileStatus.PENDING)
                current_status = FileStatus.PENDING.value

        # If FETCHING, the prefetch thread is actively copying — wait for it
        if current_status == FileStatus.FETCHING.value:
            logging.debug(f"Waiting for prefetch to complete: {item['filename']}")
            for _ in range(120):  # wait up to 10 minutes
                if self._shutdown:
                    return False
                time.sleep(5)
                existing = self.state.get_file(filepath)
                current_status = existing["status"] if existing else None
                if current_status != FileStatus.FETCHING.value:
                    break
            if current_status == FileStatus.FETCHING.value:
                # Still fetching after 10 min — likely stale, reset
                logging.warning(f"Fetch timed out, resetting: {item['filename']}")
                self.state.set_file(filepath, FileStatus.PENDING)
                current_status = FileStatus.PENDING.value

        # Fetch inline if this item hasn't been fetched yet (priority items,
        # resume mode, or when prefetch hasn't reached this file)
        if current_status in (None, FileStatus.PENDING.value):
            local_path = stage_fetch(item, self.staging_dir, effective_config, self.state)
            if local_path is None:
                if not self._wait_for_staging_space():
                    return False
                local_path = stage_fetch(item, self.staging_dir, effective_config, self.state)
                if local_path is None:
                    return False
            existing = self.state.get_file(filepath)
            current_status = existing["status"] if existing else None

        if self._shutdown:
            return False

        existing = self.state.get_file(filepath)
        current_status = existing["status"] if existing else None

        if current_status == FileStatus.FETCHED.value:
            if item.get("audio_only"):
                output_path = stage_audio_remux(filepath, item, self.staging_dir, effective_config, self.state)
            else:
                output_path = stage_encode(filepath, item, self.staging_dir, effective_config, self.state)
            if output_path is None:
                return False

        if self._shutdown:
            return False

        # Hand off to upload worker for upload → verify → replace
        existing = self.state.get_file(filepath)
        current_status = existing["status"] if existing else None

        if current_status == FileStatus.ENCODED.value:
            self._upload_queue.put((item, effective_config))
            return True

        # Handle resume cases where file is already past encode
        if current_status in (FileStatus.UPLOADED.value, FileStatus.VERIFIED.value,
                              FileStatus.REPLACING.value, FileStatus.UPLOADING.value):
            self._upload_queue.put((item, effective_config))
            return True

        return current_status in (FileStatus.VERIFIED.value, FileStatus.REPLACED.value)

    def print_progress(self, queue: list[dict], current_idx: int):
        """Print a progress summary."""
        stats = self.state.stats
        completed = stats["completed"]
        total = len(queue)
        saved = stats["bytes_saved"]
        encode_time = stats["total_encode_time_secs"]

        avg_time = encode_time / completed if completed > 0 else 0
        eta = format_eta_tier_aware(queue, self.state)

        replaced = len(self.state.get_files_by_status(FileStatus.REPLACED))
        uploading = self._upload_queue.qsize()

        print(f"\n{'=' * 70}")
        print(f"  Progress: {completed}/{total} files "
              f"({100 * completed / total:.1f}%)" if total > 0 else "")
        print(f"  Replaced: {replaced} originals")
        print(f"  Saved:    {format_bytes(saved)}")
        print(f"  Errors:   {stats['errors']}")
        if uploading > 0:
            print(f"  Upload queue: {uploading} files pending")
        print(f"  Avg encode time: {format_duration(avg_time)}")
        print(f"  ETA:      {eta}")

        # Per-tier breakdown
        tier_stats = stats.get("tier_stats", {})
        if tier_stats:
            print(f"  Per-tier stats:")
            for res_key, tier in sorted(tier_stats.items()):
                t_completed = tier.get("completed", 0)
                t_saved = tier.get("bytes_saved", 0)
                t_time = tier.get("total_encode_time_secs", 0)
                t_input = tier.get("total_input_bytes", 0)
                t_avg = t_time / t_completed if t_completed > 0 else 0
                t_speed = t_input / t_time / (1024**2) if t_time > 0 else 0
                print(f"    {res_key:>8}: {t_completed} done, "
                      f"{format_bytes(t_saved)} saved, "
                      f"avg {format_duration(t_avg)}/file, "
                      f"{t_speed:.1f} MB/s")

        print(f"  Staging:  {format_bytes(get_staging_usage(self.staging_dir))}")
        print(f"{'=' * 70}\n")

    def run(self, queue: list[dict], dry_run: bool = False):
        """Run the pipeline on the full queue."""
        logging.info(f"Pipeline starting: {len(queue)} files to process")
        logging.info(f"Staging: {self.staging_dir}")
        logging.info(f"Staging limit: {format_bytes(self.config['max_staging_bytes'])}")
        logging.info(f"Replace originals: {self.config.get('replace_original', True)}")
        logging.info(f"Control dir: {self.control.control_dir}")

        if dry_run:
            logging.info("DRY RUN — no files will be modified")
            total_size = sum(item["file_size_bytes"] for item in queue)
            logging.info(f"Would process {len(queue)} files ({format_bytes(total_size)})")
            for i, item in enumerate(queue[:30]):
                enc = resolve_encode_params(self.config, item)
                logging.info(f"  {i+1:4d}. [{item['tier_name']}] {item['filename']} "
                             f"({format_bytes(item['file_size_bytes'])}, "
                             f"{item['video_codec']}, {item['resolution']}, "
                             f"CQ:{enc['cq']}, {enc['preset']})")
            if len(queue) > 30:
                logging.info(f"  ... and {len(queue) - 30} more files")
            return

        # Create staging subdirs
        os.makedirs(os.path.join(self.staging_dir, "fetch"), exist_ok=True)
        os.makedirs(os.path.join(self.staging_dir, "encoded"), exist_ok=True)

        # Compact state — remove terminal entries to keep state file manageable
        self.state.compact()

        # Recover zombie states from any previous crashed run before processing.
        # This runs once at startup so stale FETCHING/ENCODING/UPLOADING entries
        # don't block the main loop or fill staging forever.
        zombie_count = 0
        for item in queue:
            filepath = item["filepath"]
            existing = self.state.get_file(filepath)
            if not existing:
                continue
            status = existing["status"]
            local_path = existing.get("local_path", "")
            output_path = existing.get("output_path", "")
            dest_path = existing.get("dest_path", "")

            if status == FileStatus.FETCHING.value:
                if local_path and os.path.exists(local_path):
                    self.state.set_file(filepath, FileStatus.FETCHED, local_path=local_path)
                else:
                    self.state.set_file(filepath, FileStatus.PENDING)
                zombie_count += 1
            elif status == FileStatus.ENCODING.value:
                if output_path and os.path.exists(output_path):
                    self.state.set_file(filepath, FileStatus.ENCODED)
                elif local_path and os.path.exists(local_path):
                    self.state.set_file(filepath, FileStatus.FETCHED)
                else:
                    self.state.set_file(filepath, FileStatus.PENDING)
                zombie_count += 1
            elif status == FileStatus.UPLOADING.value:
                if dest_path and os.path.exists(dest_path):
                    self.state.set_file(filepath, FileStatus.UPLOADED)
                elif output_path and os.path.exists(output_path):
                    self.state.set_file(filepath, FileStatus.ENCODED)
                else:
                    self.state.set_file(filepath, FileStatus.PENDING)
                zombie_count += 1

        if zombie_count:
            logging.info(f"Recovered {zombie_count} zombie states from previous crash")

        # Clean up orphaned fetch files — files in state that aren't in the queue
        # (e.g. after strip_tags renamed files on the NAS and a rescan updated the report)
        queue_fps = {item["filepath"] for item in queue}
        orphan_count = 0
        orphan_bytes = 0
        for filepath, info in list(self.state.data["files"].items()):
            if filepath in queue_fps:
                continue
            if info["status"] not in (FileStatus.FETCHED.value, FileStatus.FETCHING.value):
                continue
            local_path = info.get("local_path", "")
            if local_path and os.path.exists(local_path):
                try:
                    orphan_bytes += os.path.getsize(local_path)
                    os.remove(local_path)
                except OSError:
                    pass
            info["status"] = FileStatus.SKIPPED.value
            info["reason"] = "orphaned after rename"
            orphan_count += 1

        if orphan_count:
            self.state.save()
            logging.info(f"Cleaned up {orphan_count} orphaned fetch files "
                         f"({format_bytes(orphan_bytes)} freed)")

        # Apply control overrides to queue (skip, priority bumps)
        queue = self.control.apply_queue_overrides(queue)

        # Start prefetch thread — it fetches ahead while the main loop encodes
        prefetch_thread = threading.Thread(
            target=self._prefetch_worker, args=(queue,), daemon=True, name="prefetch"
        )
        prefetch_thread.start()
        logging.info("Concurrent prefetch enabled — GPU and network will overlap")

        # Start upload worker thread — uploads encoded files while GPU encodes next
        upload_thread = threading.Thread(
            target=self._upload_worker, daemon=True, name="upload"
        )
        upload_thread.start()
        logging.info("Parallel upload enabled — uploads overlap with encoding")

        # Track active audio remux threads
        audio_threads: list[threading.Thread] = []
        MAX_AUDIO_THREADS = 2  # limit concurrent CPU remux jobs

        processed = 0
        last_progress_at = 0
        # Main loop: find FETCHED items and process them (encode → hand off to upload worker)
        # Keep looping until all queue items are terminal or shutdown
        while not self._shutdown:
            # Check control system for pause
            self.control.check_pause(lambda: self._shutdown)
            if self._shutdown:
                break

            # Inject new priority items (cheap — mtime-cached read)
            queue = self._inject_new_priority_items(queue)

            # Re-check control overrides periodically
            if processed > 0 and processed % 5 == 0 and processed != last_progress_at:
                last_progress_at = processed
                self.print_progress(queue, processed)
                queue = self.control.apply_queue_overrides(queue)

            # Clean up finished audio threads
            audio_threads = [t for t in audio_threads if t.is_alive()]

            # Build priority set for fast lookup
            priority_set = {
                os.path.normpath(p).lower()
                for p in self.control.get_priority_bumps()
            }

            # Find next item to process.
            # Priority: 1) fetched priority items, 2) any fetched item (don't
            # let the GPU idle), 3) inline-fetch a priority item if nothing
            # else is ready.
            READY_STATUSES = (FileStatus.FETCHING.value, FileStatus.FETCHED.value,
                              FileStatus.ENCODING.value, FileStatus.ENCODED.value,
                              FileStatus.UPLOADING.value, FileStatus.UPLOADED.value,
                              FileStatus.REPLACING.value)

            ready_priority = None
            ready_any = None
            first_pending = None
            first_priority_pending = None
            all_done = True
            for item in queue:
                filepath = item["filepath"]
                existing = self.state.get_file(filepath)
                status = existing["status"] if existing else None
                is_priority = os.path.normpath(filepath).lower() in priority_set

                # Skip terminal states
                if status in (FileStatus.VERIFIED.value, FileStatus.REPLACED.value,
                               FileStatus.SKIPPED.value, FileStatus.ERROR.value):
                    continue

                all_done = False

                if status in READY_STATUSES:
                    if is_priority and ready_priority is None:
                        ready_priority = item
                    elif ready_any is None:
                        ready_any = item

                if status in (None, FileStatus.PENDING.value):
                    if first_pending is None:
                        first_pending = item
                    if is_priority and first_priority_pending is None:
                        first_priority_pending = item

            # Also check upload queue — items there aren't done yet
            if all_done and self._upload_queue.empty():
                break
            elif all_done:
                # All items dispatched but upload worker still busy
                time.sleep(5)
                continue

            # Pick best item: priority fetched > any fetched > priority pending > any pending
            ready_item = ready_priority or ready_any or first_priority_pending or first_pending
            if ready_item is None:
                if not prefetch_thread.is_alive() and self._upload_queue.empty():
                    break  # Everything is either done or errored
                time.sleep(5)
                continue

            filepath = ready_item["filepath"]

            # Skip if control system says so
            if self.control.should_skip(filepath):
                logging.info(f"Skipped (control): {ready_item['filename']}")
                self.state.set_file(filepath, FileStatus.SKIPPED, reason="control skip")
                continue

            processed += 1
            logging.info(f"\n[{processed}/{len(queue)}] {ready_item['tier_name']} | "
                         f"{ready_item['filename']} ({format_bytes(ready_item['file_size_bytes'])})")

            # Store tier and res_key on file state for dashboard display
            existing = self.state.get_file(filepath) or {}
            if not existing.get("tier"):
                self.state.set_file(
                    filepath,
                    FileStatus(existing.get("status", FileStatus.PENDING.value)),
                    tier=ready_item.get("tier_name", "Unknown"),
                    res_key=get_res_key(ready_item),
                )

            # Audio-only items: dispatch to background thread (no GPU needed)
            if ready_item.get("audio_only") and len(audio_threads) < MAX_AUDIO_THREADS:
                effective_config = self._apply_gentle_overrides(ready_item)
                t = threading.Thread(
                    target=self._audio_remux_async,
                    args=(ready_item, effective_config),
                    daemon=True,
                    name=f"audio-remux-{ready_item['filename'][:30]}",
                )
                t.start()
                audio_threads.append(t)
                logging.info(f"  Dispatched audio remux to background thread")
                continue

            success = self.process_item(ready_item)
            if not success:
                # If item is still PENDING after process_item failed, the fetch
                # buffer is likely full — wait before retrying to avoid tight loop
                existing = self.state.get_file(filepath)
                if existing and existing["status"] in (None, FileStatus.PENDING.value):
                    time.sleep(30)
            if not success and not self._shutdown:
                self.state.stats["errors"] += 1
                self.state.save()

        # Signal threads to stop and wait for them
        was_interrupted = self._shutdown
        self._shutdown = True  # Ensures prefetch thread exits
        prefetch_thread.join(timeout=10)

        # Wait for audio remux threads to finish
        for t in audio_threads:
            t.join(timeout=30)

        # Wait for upload worker to drain remaining items
        if not self._upload_queue.empty():
            logging.info(f"Waiting for upload worker to finish {self._upload_queue.qsize()} remaining items...")
        self._upload_queue.join()
        upload_thread.join(timeout=10)

        # Final summary
        self.print_progress(queue, processed)
        logging.info("Pipeline finished." if not was_interrupted else
                     "Pipeline paused — run again to resume.")
