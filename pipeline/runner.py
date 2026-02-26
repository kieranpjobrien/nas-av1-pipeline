"""Main pipeline orchestration — Pipeline class, prefetch thread, signal handling."""

import copy
import json
import logging
import os
import signal
import sys
import threading
import time

from pipeline.config import get_res_key, resolve_encode_params
from pipeline.control import PipelineControl
from pipeline.encoding import format_bytes, format_duration, stage_encode
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
            if (usage < self.config["max_staging_bytes"] and
                    free > self.config["min_free_space_bytes"]):
                return True
            logging.info(f"Waiting for staging space... "
                         f"(used: {format_bytes(usage)}, free: {format_bytes(free)})")
            time.sleep(30)
        return False

    def _prefetch_worker(self, queue: list[dict]):
        """Background thread: pre-fetch files from NAS while encoder is busy.

        Loops through the queue repeatedly until all items are fetched or
        the pipeline shuts down. This ensures priority items (which may be
        added after the initial queue is built) eventually get fetched.
        """
        logging.info("Prefetch thread started")
        while not self._shutdown:
            fetched_any = False
            for item in queue:
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

    def _apply_gentle_overrides(self, item: dict) -> dict:
        """Apply per-file CQ/preset overrides from gentle.json. Returns modified config."""
        overrides = self.control.get_gentle_override(item["filepath"])
        if not overrides:
            return self.config

        config = copy.deepcopy(self.config)
        params = resolve_encode_params(config, item)
        content_type = params["content_type"]
        res_key = params["res_key"]

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
            "priority_tier": tier_idx,
            "tier_name": (self.config["priority_tiers"][tier_idx]["name"]
                          if tier_idx < len(self.config["priority_tiers"]) else "other"),
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

            # Skip already-AV1
            if (entry.get("video", {}).get("codec_raw") or "") == "av1":
                continue

            item = self._build_queue_item(entry)
            new_items.append(item)

        if new_items:
            logging.info(f"Injected {len(new_items)} new priority items into queue")
            queue = new_items + queue

        return queue

    def process_item(self, item: dict) -> bool:
        """Run one file through: fetch (if needed) → encode → upload → verify → replace."""
        filepath = item["filepath"]

        # Check current state for resume
        existing = self.state.get_file(filepath)
        current_status = existing["status"] if existing else None

        # Apply gentle overrides for this file
        effective_config = self._apply_gentle_overrides(item)

        # Recover zombie states from crashed runs: if stuck in FETCHING/ENCODING/UPLOADING
        # and the local file is gone, reset to PENDING so we re-fetch.
        if current_status in (FileStatus.FETCHING.value, FileStatus.ENCODING.value,
                               FileStatus.UPLOADING.value):
            local_path = (existing or {}).get("local_path", "")
            if not local_path or not os.path.exists(local_path):
                logging.info(f"Recovering stale {current_status} state: {item['filename']}")
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
            output_path = stage_encode(filepath, item, self.staging_dir, effective_config, self.state)
            if output_path is None:
                return False

        if self._shutdown:
            return False

        existing = self.state.get_file(filepath)
        current_status = existing["status"] if existing else None

        if current_status == FileStatus.ENCODED.value:
            success = stage_upload(filepath, item, self.staging_dir, effective_config, self.state)
            if not success:
                return False

        if self._shutdown:
            return False

        existing = self.state.get_file(filepath)
        current_status = existing["status"] if existing else None

        if current_status == FileStatus.UPLOADED.value:
            success = stage_verify(filepath, item, effective_config, self.state)
            if not success:
                return False

        if self._shutdown:
            return False

        existing = self.state.get_file(filepath)
        current_status = existing["status"] if existing else None

        # Replace original on NAS (unless --no-replace)
        if current_status == FileStatus.VERIFIED.value and effective_config.get("replace_original", True):
            success = stage_replace(filepath, item, effective_config, self.state)
            return success

        # If REPLACING was interrupted, finish it
        if current_status == FileStatus.REPLACING.value:
            success = stage_replace(filepath, item, effective_config, self.state)
            return success

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

        print(f"\n{'=' * 70}")
        print(f"  Progress: {completed}/{total} files "
              f"({100 * completed / total:.1f}%)" if total > 0 else "")
        print(f"  Replaced: {replaced} originals")
        print(f"  Saved:    {format_bytes(saved)}")
        print(f"  Errors:   {stats['errors']}")
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

        # Apply control overrides to queue (skip, priority bumps)
        queue = self.control.apply_queue_overrides(queue)

        # Start prefetch thread — it fetches ahead while the main loop encodes
        prefetch_thread = threading.Thread(
            target=self._prefetch_worker, args=(queue,), daemon=True, name="prefetch"
        )
        prefetch_thread.start()
        logging.info("Concurrent prefetch enabled — GPU and network will overlap")

        processed = 0
        last_progress_at = 0
        # Main loop: find FETCHED items and process them (encode → upload → verify → replace)
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

            # Build priority set for fast lookup
            priority_set = {
                os.path.normpath(p).lower()
                for p in self.control.get_priority_bumps()
            }

            # Find next item to process. Priority items that need fetching
            # jump ahead of already-fetched non-priority items.
            ready_item = None
            first_pending = None
            all_done = True
            for item in queue:
                filepath = item["filepath"]
                existing = self.state.get_file(filepath)
                status = existing["status"] if existing else None

                # Skip terminal states
                if status in (FileStatus.VERIFIED.value, FileStatus.REPLACED.value,
                               FileStatus.SKIPPED.value, FileStatus.ERROR.value):
                    continue

                all_done = False

                # Ready to process: fetched, or mid-pipeline resume states.
                if status in (FileStatus.FETCHED.value,
                               FileStatus.ENCODING.value, FileStatus.ENCODED.value,
                               FileStatus.UPLOADING.value, FileStatus.UPLOADED.value,
                               FileStatus.REPLACING.value):
                    ready_item = item
                    break

                # Track first pending item for inline-fetch fallback
                if first_pending is None and status in (None, FileStatus.PENDING.value):
                    # If this pending item is priority, immediately prefer it
                    # over any fetched non-priority items deeper in the queue
                    if os.path.normpath(filepath).lower() in priority_set:
                        ready_item = item
                        break
                    first_pending = item

            if all_done:
                break

            if ready_item is None:
                # Nothing fetched or priority-pending — inline-fetch first pending
                ready_item = first_pending
                if ready_item is None:
                    if not prefetch_thread.is_alive():
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

            success = self.process_item(ready_item)
            if not success and not self._shutdown:
                self.state.stats["errors"] += 1
                self.state.save()

        # Signal prefetch thread to stop and wait for it
        was_interrupted = self._shutdown
        self._shutdown = True  # Ensures prefetch thread exits
        prefetch_thread.join(timeout=10)

        # Final summary
        self.print_progress(queue, processed)
        logging.info("Pipeline finished." if not was_interrupted else
                     "Pipeline paused — run again to resume.")
