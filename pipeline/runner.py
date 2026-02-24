"""Main pipeline orchestration — Pipeline class, prefetch thread, signal handling."""

import copy
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
    def __init__(self, config: dict, state: PipelineState, staging_dir: str):
        self.config = config
        self.state = state
        self.staging_dir = staging_dir
        self.control = PipelineControl(staging_dir)
        self._shutdown = False

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
        """Background thread: pre-fetch files from NAS while encoder is busy."""
        logging.info("Prefetch thread started")
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

            # Try to fetch — stage_fetch returns None if buffer full
            result = stage_fetch(item, self.staging_dir, self.config, self.state)
            if result is None:
                # Buffer full or space issue — wait and retry
                retries = 0
                while result is None and not self._shutdown and retries < 60:
                    time.sleep(10)
                    if self.control.is_fetch_paused():
                        continue
                    result = stage_fetch(item, self.staging_dir, self.config, self.state)
                    retries += 1
                if result is None and not self._shutdown:
                    logging.warning(f"Prefetch gave up after retries: {item['filename']}")

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
            logging.info(f"  Gentle override: CQ {current_cq} → {config['cq'][content_type][res_key]}")

        if "cq" in overrides:
            config["cq"][content_type][res_key] = overrides["cq"]
            logging.info(f"  Gentle override: CQ → {overrides['cq']}")

        if "preset" in overrides:
            config["nvenc_preset"][content_type][res_key] = overrides["preset"]
            logging.info(f"  Gentle override: Preset → {overrides['preset']}")

        return config

    def process_item(self, item: dict, prefetch_active: bool = False) -> bool:
        """Run one file through: encode → upload → verify → replace.

        When prefetch_active=True, expects the file to already be FETCHED by the
        prefetch thread. When False (resume/fallback), fetches inline.
        """
        filepath = item["filepath"]

        # Check current state for resume
        existing = self.state.get_file(filepath)
        current_status = existing["status"] if existing else None

        # Apply gentle overrides for this file
        effective_config = self._apply_gentle_overrides(item)

        # Fetch inline only when prefetch is not running (resume mode / fallback)
        if not prefetch_active and current_status in (None, FileStatus.PENDING.value):
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

        if current_status in (FileStatus.FETCHED.value, FileStatus.FETCHING.value):
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

            # Re-check control overrides periodically
            if processed > 0 and processed % 5 == 0 and processed != last_progress_at:
                last_progress_at = processed
                self.print_progress(queue, processed)
                queue = self.control.apply_queue_overrides(queue)

            # Find next item that's ready (FETCHED) or needs resume (mid-pipeline states)
            ready_item = None
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

                # Ready to process: fetched, or mid-pipeline resume states
                if status in (FileStatus.FETCHED.value, FileStatus.ENCODED.value,
                               FileStatus.UPLOADED.value, FileStatus.REPLACING.value):
                    ready_item = item
                    break

            if all_done:
                break

            if ready_item is None:
                # Nothing fetched yet — wait for prefetch thread
                if not prefetch_thread.is_alive():
                    # Prefetch finished but items remain unfetched — fall back to inline fetch
                    for item in queue:
                        filepath = item["filepath"]
                        existing = self.state.get_file(filepath)
                        status = existing["status"] if existing else None
                        if status in (None, FileStatus.PENDING.value):
                            ready_item = item
                            break
                    if ready_item is None:
                        break  # Everything is either done or errored
                else:
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

            prefetch_active = prefetch_thread.is_alive()
            success = self.process_item(ready_item, prefetch_active=prefetch_active)
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
