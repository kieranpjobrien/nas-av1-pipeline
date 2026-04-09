"""Pipeline orchestrator: 4 threads with GPU coordination.

GPU is a shared resource with priority: Force Next > Gap Filler > Full Gamut

Thread 1 (GPU/Encode): full_gamut encodes — holds GPU lock during NVENC encode
Thread 2 (Network): pre-fetches next files, saturates NAS link
Thread 3 (Gap Filler): CPU work while GPU encodes, grabs GPU between encodes for whisper
Thread 4 (Force Monitor): watches force stack, preempts GPU for immediate processing

Each file is owned by ONE thread from start to finish. No handoffs.
"""

import json
import logging
import os
import queue as queue_mod
import signal
import threading
import time

from pipeline.config import get_res_key
from pipeline.ffmpeg import format_bytes, format_duration
from pipeline.full_gamut import full_gamut
from pipeline.gap_filler import gap_fill, analyse_gaps
from pipeline.state import FileStatus, PipelineState
from pipeline.transfer import fetch_file, get_free_space, get_staging_usage


class Orchestrator:
    """4-thread pipeline coordinator with GPU sharing."""

    def __init__(self, config: dict, state: PipelineState, staging_dir: str, control):
        self.config = config
        self.state = state
        self.staging_dir = staging_dir
        self.control = control

        self._shutdown = threading.Event()
        self._gpu_lock = threading.Lock()          # only one GPU user at a time
        self._gpu_preempt = threading.Event()       # set by Force Monitor to interrupt GPU holder
        self._gpu_available = threading.Event()     # set when GPU is free
        self._gpu_available.set()                   # starts available
        self._force_gpu_queue = queue_mod.Queue()   # force items needing GPU

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logging.info(f"Received signal {signum}, shutting down...")
        self._shutdown.set()

    def run(self, full_gamut_queue: list[dict], gap_filler_queue: list[dict],
            enable_gap_filler: bool = True):
        """Main entry point."""
        logging.info(f"Orchestrator starting:")
        logging.info(f"  Full gamut: {len(full_gamut_queue)} files")
        logging.info(f"  Gap filler: {len(gap_filler_queue)} files ({'enabled' if enable_gap_filler else 'disabled'})")

        for subdir in ("fetch", "encoded", "force", "whisper_tmp", "ocr_tmp"):
            os.makedirs(os.path.join(self.staging_dir, subdir), exist_ok=True)

        self.state.compact()
        full_gamut_queue = self.control.apply_queue_overrides(full_gamut_queue)
        gap_filler_queue = self.control.apply_queue_overrides(gap_filler_queue)

        threads = {}
        threads["gpu"] = threading.Thread(
            target=self._gpu_worker, args=(full_gamut_queue,), daemon=True, name="gpu-encode")
        threads["network"] = threading.Thread(
            target=self._network_worker, args=(full_gamut_queue,), daemon=True, name="network")
        if enable_gap_filler:
            threads["gap_filler"] = threading.Thread(
                target=self._gap_filler_worker, args=(gap_filler_queue,), daemon=True, name="gap-filler")
        threads["force_monitor"] = threading.Thread(
            target=self._force_monitor, daemon=True, name="force-monitor")

        for name, t in threads.items():
            t.start()
            logging.info(f"  Started {name}")

        try:
            while not self._shutdown.is_set():
                if all(not t.is_alive() for t in threads.values()):
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

    # =========================================================================
    # GPU Worker — holds GPU lock during NVENC encode
    # =========================================================================

    def _gpu_worker(self, queue: list[dict]):
        """Full gamut encodes. Releases GPU lock between files so gap filler can use whisper."""
        logging.info("GPU worker started")
        dispatched: set[str] = set()
        processed = 0

        while not self._shutdown.is_set():
            # Check for force items needing GPU (highest priority)
            try:
                force_item = self._force_gpu_queue.get_nowait()
                item = force_item
                logging.info(f"[FORCE GPU] {item.get('filename', '?')}")
            except queue_mod.Empty:
                item = self._pick_next(queue, dispatched)

            if item is None:
                if self._all_done(queue, dispatched):
                    break
                self._shutdown.wait(timeout=5)
                continue

            filepath = item["filepath"]
            dispatched.add(filepath)
            processed += 1

            while self.control.is_encode_paused() and not self._shutdown.is_set():
                self._shutdown.wait(timeout=5)

            logging.info(f"\n[GPU {processed}/{len(queue)}] {item.get('tier_name', '?')} | "
                         f"{item['filename']} ({format_bytes(item.get('file_size_bytes', 0))})")

            # Acquire GPU lock — gap filler will release it if holding
            self._gpu_available.clear()
            with self._gpu_lock:
                effective_config = self._apply_overrides(item)
                success = full_gamut(filepath, item, effective_config, self.state, self.staging_dir)

            # Release GPU — gap filler can now grab it for whisper
            self._gpu_available.set()

            if not success:
                self.state.stats["errors"] = self.state.stats.get("errors", 0) + 1
                self.state.save()

        self._gpu_available.set()  # ensure gap filler isn't stuck waiting
        logging.info("GPU worker finished")

    # =========================================================================
    # Network Worker — pre-fetch files to keep GPU fed
    # =========================================================================

    def _network_worker(self, queue: list[dict]):
        """Continuous pre-fetch. Saturates NAS link."""
        logging.info("Network worker started")
        max_buffer = self.config.get("max_fetch_buffer_bytes", 2000 * 1024**3)

        while not self._shutdown.is_set():
            if self.control.is_fetch_paused():
                self._shutdown.wait(timeout=5)
                continue

            if self._get_fetch_buffer_used() >= max_buffer:
                self._shutdown.wait(timeout=10)
                continue

            fetched_any = False
            for item in queue:
                if self._shutdown.is_set():
                    break
                fp = item["filepath"]
                existing = self.state.get_file(fp)
                status = existing["status"] if existing else None
                if status and status != FileStatus.PENDING.value:
                    continue
                if self.control.should_skip(fp):
                    continue
                result = fetch_file(item, self.staging_dir, self.config, self.state)
                if result is not None:
                    fetched_any = True
                    break

            if not fetched_any:
                self._shutdown.wait(timeout=10)

        logging.info("Network worker finished")

    # =========================================================================
    # Gap Filler Worker — CPU work always, GPU (whisper) between encodes
    # =========================================================================

    def _gap_filler_worker(self, queue: list[dict]):
        """CPU-only cleanup. Grabs GPU between encodes for whisper language detection."""
        logging.info(f"Gap filler started: {len(queue)} items")
        dispatched: set[str] = set()
        processed = 0

        while not self._shutdown.is_set():
            item = self._pick_next_gap(queue, dispatched)
            if item is None:
                if self._all_done(queue, dispatched):
                    break
                self._shutdown.wait(timeout=5)
                continue

            filepath = item["filepath"]
            dispatched.add(filepath)
            processed += 1

            gaps = analyse_gaps(item, self.config)
            if not gaps.needs_anything:
                self.state.set_file(filepath, FileStatus.DONE, mode="gap_filler", reason="clean")
                continue

            logging.info(f"\n[GAP {processed}/{len(queue)}] {gaps.describe()} | {item['filename']}")

            # If this item needs whisper (language detection), we need the GPU.
            # Wait for GPU to be available (between encodes), then grab it.
            if gaps.needs_language_detect:
                # Do CPU work first while waiting
                cpu_gaps = _cpu_only_gaps(gaps)
                if cpu_gaps.needs_anything:
                    logging.info(f"  CPU work while waiting for GPU...")
                    gap_fill(filepath, item, cpu_gaps, self.config, self.state)

                # Now wait for and acquire GPU for whisper
                logging.info(f"  Waiting for GPU (whisper)...")
                while not self._gpu_available.wait(timeout=2):
                    if self._shutdown.is_set():
                        break
                    if self._gpu_preempt.is_set():
                        break  # force next preempted us

                if self._shutdown.is_set():
                    break

                if not self._gpu_preempt.is_set():
                    # We got the GPU — do whisper
                    self._gpu_available.clear()
                    with self._gpu_lock:
                        logging.info(f"  GPU acquired for whisper")
                        # Re-analyse with whisper enabled
                        from pipeline.language import detect_all_languages
                        try:
                            enriched = detect_all_languages(item, use_whisper=True)
                            if enriched:
                                item.update(enriched)
                                gaps = analyse_gaps(item, self.config)
                        except Exception as e:
                            logging.warning(f"  Whisper failed: {e}")
                    self._gpu_available.set()

                    # Now do remaining gap fill with detected languages
                    if gaps.needs_anything:
                        gap_fill(filepath, item, gaps, self.config, self.state)
                    else:
                        self.state.set_file(filepath, FileStatus.DONE, mode="gap_filler")
                else:
                    # Preempted by force next — defer this item
                    self._gpu_preempt.clear()
                    dispatched.discard(filepath)
                    logging.info(f"  Preempted by Force Next, deferring")
                    continue
            else:
                # No GPU needed — just do it
                gap_fill(filepath, item, gaps, self.config, self.state)

            if not self.state.get_file(filepath) or self.state.get_file(filepath).get("status") == "error":
                self.state.stats["errors"] = self.state.stats.get("errors", 0) + 1
                self.state.save()

        logging.info("Gap filler finished")

    # =========================================================================
    # Force Monitor — immediate fetch + GPU preemption
    # =========================================================================

    def _force_monitor(self):
        """Watches force stack. Preempts GPU for immediate processing."""
        logging.info("Force monitor started")
        force_buffer_max = 200 * 1024**3
        force_dir = os.path.join(self.staging_dir, "force")
        os.makedirs(force_dir, exist_ok=True)

        while not self._shutdown.is_set():
            force_items = self.control.get_force_items()
            if not force_items:
                self._shutdown.wait(timeout=2)
                continue

            filepath = force_items[0]
            self.control.remove_force_item(filepath)

            if not os.path.exists(filepath):
                logging.warning(f"Force item not found: {os.path.basename(filepath)}")
                continue

            logging.info(f"\n[FORCE] {os.path.basename(filepath)}")

            # Look up in media report
            file_entry = self._lookup_file(filepath)

            if file_entry and file_entry.get("video", {}).get("codec_raw") == "av1":
                # Gap fill — check if GPU needed
                gaps = analyse_gaps(file_entry, self.config)
                if gaps.needs_language_detect:
                    # Preempt GPU
                    logging.info(f"  Preempting GPU for force item...")
                    self._gpu_preempt.set()

                    # Wait for GPU to be released
                    while not self._gpu_available.wait(timeout=2):
                        if self._shutdown.is_set():
                            break

                    if not self._shutdown.is_set():
                        self._gpu_available.clear()
                        with self._gpu_lock:
                            self._gpu_preempt.clear()
                            gap_fill(filepath, file_entry, gaps, self.config, self.state)
                        self._gpu_available.set()
                elif gaps.needs_anything:
                    gap_fill(filepath, file_entry, gaps, self.config, self.state)
                else:
                    logging.info(f"  Force item already clean")
            else:
                # Needs full encode — preempt GPU
                logging.info(f"  Force item needs full encode — preempting GPU")
                self._gpu_preempt.set()

                while not self._gpu_available.wait(timeout=2):
                    if self._shutdown.is_set():
                        break

                if not self._shutdown.is_set():
                    self._gpu_available.clear()
                    with self._gpu_lock:
                        self._gpu_preempt.clear()
                        if file_entry:
                            effective_config = self._apply_overrides(file_entry)
                            full_gamut(filepath, file_entry, effective_config, self.state, self.staging_dir)
                        else:
                            # Minimal item — build from filepath
                            item = {
                                "filepath": filepath,
                                "filename": os.path.basename(filepath),
                                "file_size_bytes": os.path.getsize(filepath) if os.path.exists(filepath) else 0,
                                "file_size_gb": 0,
                                "duration_seconds": 0,
                                "library_type": "movie" if "Movies" in filepath else "series",
                            }
                            full_gamut(filepath, item, self.config, self.state, self.staging_dir)
                    self._gpu_available.set()

            self._shutdown.wait(timeout=1)

        logging.info("Force monitor finished")

    # =========================================================================
    # Helpers
    # =========================================================================

    def _pick_next(self, queue: list[dict], dispatched: set[str]) -> dict | None:
        """Pick next item from full gamut queue."""
        for item in queue:
            fp = item["filepath"]
            if fp in dispatched:
                continue
            if self.control.should_skip(fp):
                continue
            existing = self.state.get_file(fp)
            status = existing["status"] if existing else None
            if status in (FileStatus.DONE.value, FileStatus.ERROR.value):
                continue
            if status == FileStatus.PROCESSING.value:
                continue
            return item
        return None

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
            return item
        return None

    def _all_done(self, queue: list[dict], dispatched: set[str]) -> bool:
        for item in queue:
            fp = item["filepath"]
            existing = self.state.get_file(fp)
            status = existing["status"] if existing else None
            if status not in (FileStatus.DONE.value, FileStatus.ERROR.value):
                if fp not in dispatched or status is None:
                    return False
        return True

    def _get_fetch_buffer_used(self) -> int:
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
        import copy
        effective = copy.deepcopy(self.config)
        filepath = item.get("filepath", "")
        profile = self.control.get_quality_profile(filepath)
        if profile:
            effective["_profile"] = profile
        override = self.control.get_gentle_override(filepath)
        if override:
            for k, v in override.items():
                effective[k] = v
        reencode = self.control.get_reencode_override(filepath)
        if reencode:
            for k, v in reencode.items():
                effective[k] = v
        return effective

    def _lookup_file(self, filepath: str) -> dict | None:
        """Look up a file in the media report."""
        try:
            from tools.report_lock import read_report
            report = read_report()
            for f in report.get("files", []):
                if f.get("filepath") == filepath:
                    return f
        except Exception:
            pass
        return None


def _cpu_only_gaps(gaps):
    """Return a copy of gaps with GPU-requiring work stripped out."""
    from pipeline.gap_filler import GapAnalysis
    cpu = GapAnalysis(
        needs_track_removal=gaps.needs_track_removal,
        needs_audio_transcode=False,  # audio transcode is CPU but let's bundle it with whisper
        needs_metadata=gaps.needs_metadata,
        needs_filename_clean=gaps.needs_filename_clean,
        needs_language_detect=False,  # whisper needs GPU
        audio_keep_indices=gaps.audio_keep_indices[:],
        sub_keep_indices=gaps.sub_keep_indices[:],
        audio_transcode_indices=[],
        clean_name=gaps.clean_name,
    )
    return cpu
