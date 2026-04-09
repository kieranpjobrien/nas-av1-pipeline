"""Pipeline orchestrator: 4 threads, clear responsibilities.

Thread 1 (GPU): full_gamut encodes — one file at a time, fully utilises NVENC
Thread 2 (Network): pre-fetches next files + uploads completed encodes
Thread 3 (Gap Filler): CPU-only cleanup directly on NAS
Thread 4 (Force Monitor): watches force stack, immediate fetch + route

Each file is owned by ONE thread from start to finish. No handoffs.
"""

import fnmatch
import json
import logging
import os
import queue as queue_mod
import signal
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from pipeline.config import get_res_key, resolve_encode_params
from pipeline.ffmpeg import format_bytes, format_duration
from pipeline.full_gamut import full_gamut
from pipeline.gap_filler import gap_fill, analyse_gaps
from pipeline.state import FileStatus, PipelineState
from pipeline.transfer import fetch_file, get_free_space, get_staging_usage


class Orchestrator:
    """4-thread pipeline coordinator."""

    def __init__(
        self,
        config: dict,
        state: PipelineState,
        staging_dir: str,
        control,  # PipelineControl
    ):
        self.config = config
        self.state = state
        self.staging_dir = staging_dir
        self.control = control
        self._shutdown = threading.Event()
        self._force_queue = queue_mod.Queue()  # LIFO-ish: items for force processing

        # Signal handling
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logging.info(f"Received signal {signum}, shutting down gracefully...")
        self._shutdown.set()

    def run(self, full_gamut_queue: list[dict], gap_filler_queue: list[dict]):
        """Main entry point. Starts all threads and waits for completion."""
        logging.info(f"Orchestrator starting:")
        logging.info(f"  Full gamut queue: {len(full_gamut_queue)} files")
        logging.info(f"  Gap filler queue: {len(gap_filler_queue)} files")
        logging.info(f"  Staging: {self.staging_dir}")
        logging.info(f"  Buffer: {format_bytes(self.config.get('max_fetch_buffer_bytes', 0))}")

        # Create staging subdirs
        for subdir in ("fetch", "encoded", "force", "whisper_tmp", "ocr_tmp"):
            os.makedirs(os.path.join(self.staging_dir, subdir), exist_ok=True)

        # Compact state
        self.state.compact()

        # Apply control overrides
        full_gamut_queue = self.control.apply_queue_overrides(full_gamut_queue)
        gap_filler_queue = self.control.apply_queue_overrides(gap_filler_queue)

        # Start threads
        threads = {}

        threads["gpu"] = threading.Thread(
            target=self._gpu_worker, args=(full_gamut_queue,),
            daemon=True, name="gpu-encode"
        )
        threads["network"] = threading.Thread(
            target=self._network_worker, args=(full_gamut_queue,),
            daemon=True, name="network-transfer"
        )
        threads["gap_filler"] = threading.Thread(
            target=self._gap_filler_worker, args=(gap_filler_queue,),
            daemon=True, name="gap-filler"
        )
        threads["force_monitor"] = threading.Thread(
            target=self._force_monitor,
            daemon=True, name="force-monitor"
        )

        for name, t in threads.items():
            t.start()
            logging.info(f"  Started {name} thread")

        # Wait for completion or shutdown
        try:
            while not self._shutdown.is_set():
                all_done = all(not t.is_alive() for t in threads.values())
                if all_done:
                    break
                self._shutdown.wait(timeout=5)
        except KeyboardInterrupt:
            self._shutdown.set()

        logging.info("Orchestrator shutting down...")
        self._shutdown.set()
        for t in threads.values():
            t.join(timeout=30)

        self.state.save()
        logging.info("Orchestrator finished")

    # === GPU Worker (Thread 1) ===

    def _gpu_worker(self, queue: list[dict]):
        """Main GPU encode thread. One file at a time, fully utilises NVENC."""
        logging.info("GPU worker started")
        dispatched: set[str] = set()
        processed = 0

        while not self._shutdown.is_set():
            # Check for force items needing GPU
            force_item = self._get_force_gpu_item()
            if force_item:
                item = force_item
            else:
                # Find next item from queue
                item = self._pick_next(queue, dispatched, gpu_only=True)

            if item is None:
                if self._all_done(queue, dispatched):
                    break
                self._shutdown.wait(timeout=5)
                continue

            filepath = item["filepath"]
            dispatched.add(filepath)
            processed += 1

            # Check pause
            while self.control.is_encode_paused() and not self._shutdown.is_set():
                self._shutdown.wait(timeout=5)

            logging.info(f"\n[GPU {processed}/{len(queue)}] {item.get('tier_name', '?')} | "
                         f"{item['filename']} ({format_bytes(item['file_size_bytes'])})")

            # Apply quality profile
            effective_config = self._apply_overrides(item)

            # Full gamut: fetch → encode → upload → replace → metadata → done
            success = full_gamut(filepath, item, effective_config, self.state, self.staging_dir)

            if not success:
                self.state.stats["errors"] = self.state.stats.get("errors", 0) + 1
                self.state.save()

        logging.info("GPU worker finished")

    # === Network Worker (Thread 2) ===

    def _network_worker(self, queue: list[dict]):
        """Pre-fetch files and handle uploads. Saturates NAS link."""
        logging.info("Network worker started")
        max_buffer = self.config.get("max_fetch_buffer_bytes", 2000 * 1024**3)

        while not self._shutdown.is_set():
            # Check fetch pause
            if self.control.is_fetch_paused():
                self._shutdown.wait(timeout=5)
                continue

            # Check buffer
            buffer_used = self._get_fetch_buffer_used()
            if buffer_used >= max_buffer:
                self._shutdown.wait(timeout=10)
                continue

            # Find next unfetched item
            fetched_any = False
            for item in queue:
                if self._shutdown.is_set():
                    break
                filepath = item["filepath"]
                existing = self.state.get_file(filepath)
                status = existing["status"] if existing else None

                if status and status != FileStatus.PENDING.value:
                    continue
                if self.control.should_skip(filepath):
                    continue

                # Fetch it
                result = fetch_file(filepath, item, self.staging_dir, self.config, self.state)
                if result is not None:
                    fetched_any = True
                    break  # one fetch per loop, then re-check buffer

            if not fetched_any:
                self._shutdown.wait(timeout=10)

        logging.info("Network worker finished")

    # === Gap Filler Worker (Thread 3) ===

    def _gap_filler_worker(self, queue: list[dict]):
        """CPU-only cleanup. Works directly on NAS for most operations."""
        logging.info(f"Gap filler started: {len(queue)} items")
        dispatched: set[str] = set()
        processed = 0

        while not self._shutdown.is_set():
            # Check for force items needing CPU
            force_item = self._get_force_cpu_item()
            if force_item:
                item = force_item
            else:
                item = self._pick_next_gap(queue, dispatched)

            if item is None:
                if self._all_done(queue, dispatched):
                    break
                self._shutdown.wait(timeout=5)
                continue

            filepath = item["filepath"]
            dispatched.add(filepath)
            processed += 1

            # Analyse what needs doing
            gaps = analyse_gaps(item, self.config)
            if not gaps.needs_anything:
                self.state.set_file(filepath, FileStatus.DONE, mode="gap_filler", reason="clean")
                continue

            logging.info(f"\n[GAP {processed}/{len(queue)}] {gaps.describe()} | {item['filename']}")

            success = gap_fill(filepath, item, gaps, self.config, self.state)
            if not success:
                self.state.stats["errors"] = self.state.stats.get("errors", 0) + 1
                self.state.save()

        logging.info("Gap filler finished")

    # === Force Monitor (Thread 4) ===

    def _force_monitor(self):
        """Watches the force stack. Immediate fetch + route to GPU or gap filler."""
        logging.info("Force monitor started")
        force_buffer_max = 200 * 1024**3  # 200 GB
        force_dir = os.path.join(self.staging_dir, "force")
        os.makedirs(force_dir, exist_ok=True)

        while not self._shutdown.is_set():
            # Check for new force items (mtime-cached by control)
            force_items = self.control.get_force_items()
            if not force_items:
                self._shutdown.wait(timeout=2)
                continue

            # Pop the top item (LIFO: most recent first)
            filepath = force_items[0]

            # Check force buffer
            force_used = sum(
                os.path.getsize(os.path.join(force_dir, f))
                for f in os.listdir(force_dir)
                if os.path.isfile(os.path.join(force_dir, f))
            ) if os.path.exists(force_dir) else 0

            if force_used >= force_buffer_max:
                logging.warning(f"Force buffer full ({format_bytes(force_used)}). Cannot accept new force items.")
                self._shutdown.wait(timeout=10)
                continue

            # Remove from force list
            self.control.remove_force_item(filepath)

            # Check if file exists
            if not os.path.exists(filepath):
                logging.warning(f"Force item not found: {os.path.basename(filepath)}")
                continue

            logging.info(f"\n[FORCE] {os.path.basename(filepath)}")

            # Determine what's needed
            # Quick check: is it AV1?
            from tools.report_lock import read_report
            try:
                report = read_report()
                file_entry = None
                for f in report.get("files", []):
                    if f.get("filepath") == filepath:
                        file_entry = f
                        break
            except Exception:
                file_entry = None

            if file_entry and file_entry.get("video", {}).get("codec_raw") == "av1":
                # Gap fill — mostly CPU/NAS work
                gaps = analyse_gaps(file_entry, self.config)
                if gaps.needs_anything:
                    gap_fill(filepath, file_entry, gaps, self.config, self.state)
                else:
                    logging.info(f"  Force item is already clean")
            else:
                # Needs full encode — put in the force queue for GPU worker
                self._force_queue.put((filepath, file_entry))
                logging.info(f"  Queued for GPU encode")

            self._shutdown.wait(timeout=1)

        logging.info("Force monitor finished")

    # === Helpers ===

    def _pick_next(self, queue: list[dict], dispatched: set[str], gpu_only: bool = False) -> dict | None:
        """Pick next item from queue that's ready to process."""
        force_set = {os.path.normpath(p).lower() for p in self.control.get_force_items()}
        priority_set = {os.path.normpath(p).lower() for p in self.control.get_priority_bumps()}

        best_force = None
        best_priority = None
        best_any = None

        for item in queue:
            fp = item["filepath"]
            if fp in dispatched:
                continue
            if self.control.should_skip(fp):
                continue

            existing = self.state.get_file(fp)
            status = existing["status"] if existing else None

            # Skip terminal states
            if status in (FileStatus.DONE.value, FileStatus.ERROR.value):
                continue

            # For GPU worker: prefer PROCESSING (already fetched) items
            if status == FileStatus.PROCESSING.value:
                # Already being processed elsewhere
                continue

            norm = os.path.normpath(fp).lower()
            is_force = norm in force_set
            is_priority = is_force or norm in priority_set

            if is_force and best_force is None:
                best_force = item
            elif is_priority and best_priority is None:
                best_priority = item
            elif best_any is None:
                best_any = item

            if best_force:
                break

        return best_force or best_priority or best_any

    def _pick_next_gap(self, queue: list[dict], dispatched: set[str]) -> dict | None:
        """Pick next gap filler item."""
        for item in queue:
            fp = item["filepath"]
            if fp in dispatched:
                continue

            existing = self.state.get_file(fp)
            status = existing["status"] if existing else None
            if status in (FileStatus.DONE.value, FileStatus.ERROR.value):
                continue
            if status == FileStatus.PROCESSING.value:
                continue

            return item
        return None

    def _get_force_gpu_item(self) -> dict | None:
        """Check if there's a force item needing GPU encode."""
        try:
            filepath, file_entry = self._force_queue.get_nowait()
            if file_entry:
                return file_entry
            # Build a minimal item from the filepath
            return {
                "filepath": filepath,
                "filename": os.path.basename(filepath),
                "file_size_bytes": os.path.getsize(filepath) if os.path.exists(filepath) else 0,
                "file_size_gb": 0,
                "duration_seconds": 0,
                "library_type": "movie" if "Movies" in filepath else "series",
            }
        except queue_mod.Empty:
            return None

    def _get_force_cpu_item(self) -> dict | None:
        """Force CPU items are handled directly by the force monitor, not here."""
        return None

    def _all_done(self, queue: list[dict], dispatched: set[str]) -> bool:
        """Check if all queue items are in terminal state."""
        for item in queue:
            fp = item["filepath"]
            existing = self.state.get_file(fp)
            status = existing["status"] if existing else None
            if status not in (FileStatus.DONE.value, FileStatus.ERROR.value, None):
                return False
            if fp not in dispatched and status is None:
                return False
        return True

    def _get_fetch_buffer_used(self) -> int:
        """Get bytes used in the fetch buffer."""
        fetch_dir = os.path.join(self.staging_dir, "fetch")
        if not os.path.exists(fetch_dir):
            return 0
        total = 0
        for f in os.listdir(fetch_dir):
            try:
                total += os.path.getsize(os.path.join(fetch_dir, f))
            except OSError:
                pass
        return total

    def _apply_overrides(self, item: dict) -> dict:
        """Apply quality profile and gentle overrides to config for this file."""
        import copy
        effective = copy.deepcopy(self.config)
        filepath = item["filepath"]

        # Quality profile
        profile = self.control.get_quality_profile(filepath)
        if profile:
            effective["_profile"] = profile

        # Gentle overrides
        override = self.control.get_gentle_override(filepath)
        if override:
            for k, v in override.items():
                effective[k] = v

        # Reencode overrides
        reencode = self.control.get_reencode_override(filepath)
        if reencode:
            for k, v in reencode.items():
                effective[k] = v

        return effective

    def print_progress(self):
        """Log a progress summary."""
        stats = self.state.stats
        completed = stats.get("completed", 0)
        errors = stats.get("errors", 0)
        saved = stats.get("bytes_saved", 0)

        logging.info(f"\n{'='*60}")
        logging.info(f"  Completed: {completed}")
        logging.info(f"  Saved: {format_bytes(saved)}")
        logging.info(f"  Errors: {errors}")
        logging.info(f"  Staging: {format_bytes(get_staging_usage(self.staging_dir))}")
        logging.info(f"{'='*60}")
