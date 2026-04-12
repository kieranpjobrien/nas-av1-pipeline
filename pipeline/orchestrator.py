"""Pipeline orchestrator: 3 threads with clean separation.

Thread 1 (GPU): encodes only — no network I/O, blocks until file is fetched
Thread 2 (Network): bidirectional — uploads first (frees space), then fetches.
         One operation at a time, full NAS bandwidth in one direction.
Thread 3 (Gap Filler): CPU work on AV1 files, grabs GPU for whisper between encodes
"""

import json
import logging
import os
import queue as queue_mod
import signal
import threading
import time

from typing import Optional

from pipeline.config import get_res_key
from pipeline.ffmpeg import format_bytes, format_duration
from pipeline.full_gamut import full_gamut, finalize_upload
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
        self._gpu_wants: Optional[str] = None       # filepath the GPU is waiting on — network fetches this first

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

        # Reset any non-terminal states from previous crashed runs
        reset_count = self.state._conn.execute(
            "UPDATE pipeline_files SET status = ?, stage = NULL, error = NULL "
            "WHERE status NOT IN (?, ?)",
            ("pending", "done", "pending")
        ).rowcount
        if reset_count:
            logging.info(f"  Reset {reset_count} stale entries from previous run")

        # Remove ghost 'done' entries where the source file no longer exists (renamed/deleted)
        done_rows = self.state._conn.execute(
            "SELECT filepath FROM pipeline_files WHERE status = 'done'"
        ).fetchall()
        if done_rows:
            from concurrent.futures import ThreadPoolExecutor
            paths = [fp for (fp,) in done_rows]
            with ThreadPoolExecutor(max_workers=16) as pool:
                existence = list(pool.map(os.path.exists, paths))
            ghosts = [p for p, exists in zip(paths, existence) if not exists]
            for fp in ghosts:
                self.state._conn.execute("DELETE FROM pipeline_files WHERE filepath = ?", (fp,))
            if ghosts:
                logging.info(f"  Removed {len(ghosts)} ghost entries (files renamed/deleted)")

        self.state._conn.commit()

        # Clean orphaned fetch/encoded files from previous runs
        for subdir in ("fetch", "encoded"):
            d = os.path.join(self.staging_dir, subdir)
            if os.path.isdir(d):
                cleaned = 0
                for f in os.listdir(d):
                    try:
                        os.remove(os.path.join(d, f))
                        cleaned += 1
                    except OSError:
                        pass
                if cleaned:
                    logging.info(f"  Cleaned {cleaned} orphaned files from {subdir}/")

        self.state.compact()
        full_gamut_queue = self.control.apply_queue_overrides(full_gamut_queue)
        gap_filler_queue = self.control.apply_queue_overrides(gap_filler_queue)

        threads = {}
        threads["gpu"] = threading.Thread(
            target=self._gpu_worker, args=(full_gamut_queue, gap_filler_queue,), daemon=True, name="gpu-encode")
        threads["network"] = threading.Thread(
            target=self._network_worker, args=(full_gamut_queue,), daemon=True, name="network")
        if enable_gap_filler:
            threads["gap_filler"] = threading.Thread(
                target=self._gap_filler_worker, args=(gap_filler_queue,), daemon=True, name="gap-filler")

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

    def _gpu_worker(self, queue: list[dict], gap_queue: list[dict]):
        """Full gamut encodes + force items. Releases GPU lock between files."""
        logging.info("GPU worker started")
        dispatched: set[str] = set()
        processed = 0

        while not self._shutdown.is_set():
            # Priority 1: Force items from the force stack
            item = self._pop_force_item(gap_queue)
            is_force = item is not None

            # Priority 2: Regular queue
            if not item:
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

            # Tell the network worker what we need fetched
            self._gpu_wants = filepath

            tag = "FORCE" if is_force else f"GPU {processed}/{len(queue)}"
            logging.info(f"\n[{tag}] {item.get('tier_name', '?')} | "
                         f"{item['filename']} ({format_bytes(item.get('file_size_bytes', 0))})")

            # Acquire GPU lock
            self._gpu_available.clear()
            with self._gpu_lock:
                # Force items that are already AV1 → gap fill instead of full gamut
                is_av1 = item.get("video", {}).get("codec_raw") == "av1" if isinstance(item.get("video"), dict) else False
                if is_force and is_av1:
                    gaps = analyse_gaps(item, self.config)
                    if gaps.needs_anything:
                        gap_fill(filepath, item, gaps, self.config, self.state)
                else:
                    effective_config = self._apply_overrides(item)
                    success = full_gamut(filepath, item, effective_config, self.state, self.staging_dir)
                    if not success:
                        self.state.stats["errors"] = self.state.stats.get("errors", 0) + 1
                        self.state.save()

            self._gpu_wants = None
            self._gpu_available.set()

        self._gpu_wants = None
        self._gpu_available.set()
        logging.info("GPU worker finished")

    def _pop_force_item(self, gap_queue: list[dict]) -> dict | None:
        """Pop the top force item and return it as a queue-compatible dict."""
        force_items = self.control.get_force_items()
        if not force_items:
            return None

        filepath = force_items[0]
        self.control.remove_force_item(filepath)

        if not os.path.exists(filepath):
            logging.warning(f"Force item not found: {os.path.basename(filepath)}")
            return None

        # Look up in media report for full metadata
        entry = self._lookup_file(filepath)
        if entry:
            return entry

        # Check gap_queue
        for item in gap_queue:
            if item.get("filepath") == filepath:
                return item

        # Minimal fallback
        return {
            "filepath": filepath,
            "filename": os.path.basename(filepath),
            "file_size_bytes": os.path.getsize(filepath) if os.path.exists(filepath) else 0,
            "library_type": "movie" if "Movies" in filepath else "series",
        }

    # =========================================================================
    # Network Worker — pre-fetch files to keep GPU fed
    # =========================================================================

    def _network_worker(self, queue: list[dict]):
        """Bidirectional network worker. One operation at a time, full bandwidth.

        Priority order:
        1. Upload encoded files (frees local space, completes pipeline for that file)
        2. Fetch what the GPU is waiting on (never starve the GPU)
        3. Fetch force items (user priority)
        4. Fetch next from queue (pre-fetch ahead)
        """
        logging.info("Network worker started")
        max_buffer = self.config.get("max_fetch_buffer_bytes", 2000 * 1024**3)

        while not self._shutdown.is_set():
            did_work = False

            # === Priority 1: Upload any encoded files waiting ===
            upload_entry = self._find_pending_upload()
            if upload_entry:
                fp = upload_entry["filepath"]
                logging.info(f"[NET] Upload: {os.path.basename(fp)}")
                finalize_upload(fp, self.state, self.config)
                did_work = True
                continue  # check for more uploads before fetching

            if self.control.is_fetch_paused():
                self._shutdown.wait(timeout=5)
                continue

            if self._get_fetch_buffer_used() >= max_buffer:
                self._shutdown.wait(timeout=5)
                continue

            # === Priority 2: Fetch what the GPU is blocked on ===
            gpu_wants = self._gpu_wants
            if gpu_wants:
                existing = self.state.get_file(gpu_wants)
                status = existing["status"] if existing else None
                if status in (None, FileStatus.PENDING.value):
                    entry = self._lookup_file(gpu_wants)
                    if not entry:
                        entry = {"filepath": gpu_wants, "filename": os.path.basename(gpu_wants),
                                 "file_size_bytes": 0, "library_type": "movie" if "Movies" in gpu_wants else "series"}
                    result = fetch_file(entry, self.staging_dir, self.config, self.state)
                    if result is not None:
                        did_work = True
                        continue

            # === Priority 3: Fetch force items ===
            force_items = self.control.get_force_items()
            for fp in force_items:
                if self._shutdown.is_set():
                    break
                existing = self.state.get_file(fp)
                status = existing["status"] if existing else None
                if status and status != FileStatus.PENDING.value:
                    continue
                entry = self._lookup_file(fp)
                if not entry:
                    entry = {"filepath": fp, "filename": os.path.basename(fp),
                             "file_size_bytes": 0, "library_type": "movie" if "Movies" in fp else "series"}
                result = fetch_file(entry, self.staging_dir, self.config, self.state)
                if result is not None:
                    did_work = True
                    break

            # === Priority 3: Fetch next from queue ===
            if not did_work:
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
                        did_work = True
                        break

            if not did_work:
                self._shutdown.wait(timeout=5)

        logging.info("Network worker finished")

    def _find_pending_upload(self) -> dict | None:
        """Find a file with status=UPLOADING and stage=pending_upload."""
        rows = self.state._conn.execute(
            "SELECT filepath FROM pipeline_files WHERE status = ? AND stage = ?",
            (FileStatus.UPLOADING.value, "pending_upload")
        ).fetchall()
        if rows:
            return {"filepath": rows[0][0]}
        return None

    # =========================================================================
    # Gap Filler Worker — CPU work always, GPU (whisper) between encodes
    # =========================================================================

    def _gap_filler_worker(self, queue: list[dict]):
        """Parallel gap filler: operations split by type, not by file.

        3 MKV workers: track stripping + sub muxing (heavy NAS I/O via mkvmerge)
        2 QUICK workers: rename, metadata, foreign sub delete (instant, ALL files)

        Quick workers scan ALL gap filler files for instant ops independently.
        MKV workers handle the heavy remux ops. No conflicts — mkvmerge writes
        to a tmp file, mkvpropedit/rename operate on the original.
        """
        import queue as queue_mod
        from pipeline.gap_filler import (
            analyse_gaps, gap_fill, _scan_external_subs,
            _rename_file, GapAnalysis,
        )

        heavy_queue: queue_mod.Queue = queue_mod.Queue()
        quick_queue: queue_mod.Queue = queue_mod.Queue()
        stats_lock = threading.Lock()
        heavy_count = [0]
        quick_count = [0]

        # Build separate operation queues from all gap filler items
        for item in queue:
            filepath = item["filepath"]
            existing = self.state.get_file(filepath)
            status = existing["status"] if existing else None
            if status in (FileStatus.DONE.value, FileStatus.ERROR.value):
                continue

            gaps = analyse_gaps(item, self.config)
            gaps.needs_language_detect = False  # whisper handled separately

            # Quick ops: rename, metadata, foreign sub delete — queue independently
            if gaps.needs_filename_clean or gaps.needs_metadata or gaps.needs_foreign_sub_cleanup:
                quick_queue.put((item, gaps))

            # Heavy ops: track strip, sub mux, audio transcode
            if gaps.needs_track_removal or gaps.needs_sub_mux or gaps.needs_audio_transcode:
                heavy_queue.put((item, gaps))

        heavy_total = heavy_queue.qsize()
        quick_total = quick_queue.qsize()
        logging.info(f"Gap filler: {heavy_total} heavy (mkvmerge) + {quick_total} quick (rename/meta/delete)")

        def heavy_worker(name: str):
            while not self._shutdown.is_set():
                try:
                    item, gaps = heavy_queue.get(timeout=2)
                except queue_mod.Empty:
                    break

                filepath = item["filepath"]
                with stats_lock:
                    heavy_count[0] += 1
                    p = heavy_count[0]

                # Re-scan for external subs (deferred from queue building)
                _scan_external_subs(filepath, gaps)

                if not (gaps.needs_track_removal or gaps.needs_sub_mux or gaps.needs_audio_transcode):
                    heavy_queue.task_done()
                    continue

                logging.info(f"[{name} {p}/{heavy_total}] {gaps.describe()} | {item['filename']}")
                gap_fill(filepath, item, gaps, self.config, self.state)

                if self.state.get_file(filepath) and self.state.get_file(filepath).get("status") == "error":
                    self.state.stats["errors"] = self.state.stats.get("errors", 0) + 1
                    self.state.save()

                heavy_queue.task_done()

        def quick_worker(name: str):
            while not self._shutdown.is_set():
                try:
                    item, gaps = quick_queue.get(timeout=2)
                except queue_mod.Empty:
                    break

                filepath = item["filepath"]
                filename = item["filename"]
                library_type = item.get("library_type", "")

                if not os.path.exists(filepath):
                    # Check clean name
                    if gaps.clean_name:
                        alt = os.path.join(os.path.dirname(filepath), gaps.clean_name)
                        if os.path.exists(alt):
                            filepath = alt
                            filename = gaps.clean_name
                            gaps.needs_filename_clean = False
                        else:
                            quick_queue.task_done()
                            continue
                    else:
                        quick_queue.task_done()
                        continue

                with stats_lock:
                    quick_count[0] += 1
                    p = quick_count[0]

                parts = []
                if gaps.needs_filename_clean:
                    parts.append("rename")
                if gaps.needs_metadata:
                    parts.append("metadata")
                if gaps.needs_foreign_sub_cleanup:
                    parts.append(f"delete {len(gaps.foreign_external_subs)} foreign subs")

                logging.info(f"[{name} {p}/{quick_total}] {' + '.join(parts)} | {filename}")

                # Rename
                if gaps.needs_filename_clean and gaps.clean_name:
                    new_path = _rename_file(filepath, gaps.clean_name)
                    if new_path:
                        filepath = new_path
                        filename = gaps.clean_name

                # TMDb metadata
                if gaps.needs_metadata:
                    try:
                        from pipeline.metadata import enrich_and_tag
                        enrich_and_tag(filepath, filename, library_type)
                    except Exception:
                        pass

                # Delete foreign external subs
                if gaps.needs_foreign_sub_cleanup:
                    for sub_path in gaps.foreign_external_subs:
                        try:
                            os.remove(sub_path)
                        except OSError:
                            pass

                # Update media report so hero bars reflect the change
                try:
                    from pipeline.report import update_entry
                    update_entry(filepath, library_type)
                except Exception:
                    pass

                quick_queue.task_done()

        threads = []
        for i in range(3):
            threads.append(threading.Thread(target=heavy_worker, args=(f"MKV-{i}",), daemon=True))
        for i in range(2):
            threads.append(threading.Thread(target=quick_worker, args=(f"QUICK-{i}",), daemon=True))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        logging.info("Gap filler finished")

    # =========================================================================
    # Force Monitor — immediate fetch + GPU preemption
    # =========================================================================
    # Helpers
    # =========================================================================

    def _pick_next(self, queue: list[dict], dispatched: set[str]) -> dict | None:
        """Pick next item from full gamut queue. Prefers already-fetched files."""
        # First pass: find a file that's already fetched (status=PROCESSING, local file exists)
        for item in queue:
            fp = item["filepath"]
            if fp in dispatched:
                continue
            existing = self.state.get_file(fp)
            status = existing["status"] if existing else None
            if status == FileStatus.PROCESSING.value:
                local = existing.get("local_path")
                if local and os.path.exists(local):
                    return item

        # Second pass: pick next pending file (will need fetching)
        for item in queue:
            fp = item["filepath"]
            if fp in dispatched:
                continue
            if self.control.should_skip(fp):
                continue
            existing = self.state.get_file(fp)
            status = existing["status"] if existing else None
            if status in (FileStatus.DONE.value, FileStatus.ERROR.value, FileStatus.PROCESSING.value):
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


