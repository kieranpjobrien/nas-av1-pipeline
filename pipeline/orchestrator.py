"""Pipeline orchestrator: 3 worker types with clean separation.

GPU worker:        encodes AND uploads inline after each encode (one pass per file).
Fetch worker:      pre-fetches files to keep the GPU encoders fed.
Gap filler worker: one SSH-heavy worker (remote mkvmerge) + one local-quick worker.
"""

import logging
import os
import signal
import threading
from typing import Optional

from pipeline.circuit_breaker import CircuitBreaker, CircuitBreakerOpen
from pipeline.ffmpeg import format_bytes
from pipeline.full_gamut import finalize_upload, full_gamut
from pipeline.state import FileStatus, PipelineState
from pipeline.transfer import fetch_file

# Filename suffixes written by the pipeline's tmp-mux / staging steps.
# Shared with tools.scanner so both sides agree on what to exclude from
# the library view. Adding a new tmp-mux step? Add its suffix here.
#   .gapfill_tmp.mkv   — gap_filler track strip + sub mux intermediate
#   .submux_tmp.mkv    — tools.mux_external_subs staging output
#   .audiotrans_tmp.mkv — gap_filler audio transcode staging (future)
#   .av1.tmp           — full_gamut NVENC staging output
#   .naslib.tmp / .mkv — legacy naslib staging (module removed but defensive)
_PIPELINE_TMP_SUFFIXES = (
    ".gapfill_tmp.mkv",
    ".submux_tmp.mkv",
    ".audiotrans_tmp.mkv",
    ".av1.tmp",
    ".naslib.tmp",
    ".naslib.tmp.mkv",
)


class Orchestrator:
    """3-thread pipeline coordinator: GPU encode + network I/O + gap filler."""

    def __init__(self, config: dict, state: PipelineState, staging_dir: str, control):
        self.config = config
        self.state = state
        self.staging_dir = staging_dir
        self.control = control

        # Concurrent NVENC sessions. RTX 40-series has 2 NVENC chips → 2 concurrent encodes
        # are natively supported by hardware with no perf penalty. Settable via config.
        # Safety: each encode uses ~600 MB VRAM + 500 MB RAM + ~10% CPU — stays well within
        # 16 GB VRAM / 96 GB RAM / 24-thread budget on RTX 4080 workstation.
        self._gpu_concurrency = max(1, int(config.get("gpu_concurrency", 2)))

        self._shutdown = threading.Event()
        # Semaphore replaces the old 1-slot lock so N workers can hold a GPU slot at once.
        self._gpu_semaphore = threading.Semaphore(self._gpu_concurrency)
        # Set of filepaths each worker wants fetched next. Fetch worker reads this set to know
        # which files to prioritise — any worker waiting is enough to bump priority.
        self._gpu_wants_set: set[str] = set()
        self._gpu_wants_lock = threading.Lock()
        # Shared dispatched set across all GPU workers — prevents two workers picking the same file.
        self._dispatched: set[str] = set()
        self._dispatched_lock = threading.Lock()

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _set_gpu_wants(self, filepath: Optional[str], previous: Optional[str] = None) -> None:
        with self._gpu_wants_lock:
            if previous:
                self._gpu_wants_set.discard(previous)
            if filepath:
                self._gpu_wants_set.add(filepath)

    def _get_gpu_wants(self) -> set[str]:
        with self._gpu_wants_lock:
            return set(self._gpu_wants_set)

    def _handle_signal(self, signum, frame):
        logging.info(f"Received signal {signum}, shutting down...")
        self._shutdown.set()

    def _write_session_env(self) -> None:
        """Capture run-once environment (GPU, ffmpeg, git commit, config) to
        pipeline_env.json so each encode_history entry can reference a small
        session_id instead of carrying MBs of duplicate env info.
        """
        import json as _json
        import platform
        import subprocess as _sp
        import uuid
        from datetime import datetime, timezone

        session_id = uuid.uuid4().hex[:12]

        def _run(cmd, timeout=10):
            try:
                r = _sp.run(cmd, capture_output=True, text=True, timeout=timeout, encoding="utf-8", errors="replace")
                return r.stdout.strip() if r.returncode == 0 else None
            except Exception:
                return None

        # ffmpeg version — first line only
        ffmpeg_v = _run(["ffmpeg", "-version"])
        if ffmpeg_v:
            ffmpeg_v = ffmpeg_v.splitlines()[0]

        # GPU info via nvidia-smi
        gpu_line = _run(["nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader"])
        gpu_name, driver = (None, None)
        if gpu_line:
            parts = [p.strip() for p in gpu_line.split(",")]
            gpu_name = parts[0] if parts else None
            driver = parts[1] if len(parts) > 1 else None

        # Project git commit (for bisection)
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        git_commit = _run(["git", "-C", project_root, "rev-parse", "HEAD"])
        git_branch = _run(["git", "-C", project_root, "rev-parse", "--abbrev-ref", "HEAD"])
        git_dirty = _run(["git", "-C", project_root, "status", "--porcelain"])

        env = {
            "session_id": session_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "pid": os.getpid(),
            "host": platform.node(),
            "os": f"{platform.system()} {platform.release()}",
            "python": platform.python_version(),
            "ffmpeg_version": ffmpeg_v,
            "gpu_name": gpu_name,
            "gpu_driver": driver,
            "git": {
                "commit": git_commit,
                "branch": git_branch,
                "dirty": bool(git_dirty),
            },
            # A trimmed view of the effective config — full dump would be too noisy,
            # but the encode-shaping fields are the ones we'd bisect against.
            "config_subset": {
                "gpu_concurrency": self.config.get("gpu_concurrency"),
                "fetch_concurrency": self.config.get("fetch_concurrency"),
                "video_codec": self.config.get("video_codec"),
                "audio_mode": self.config.get("audio_mode"),
                "audio_eac3_surround_bitrate": self.config.get("audio_eac3_surround_bitrate"),
                "audio_eac3_stereo_bitrate": self.config.get("audio_eac3_stereo_bitrate"),
                "strip_non_english_audio": self.config.get("strip_non_english_audio"),
                "strip_non_english_subs": self.config.get("strip_non_english_subs"),
                "verify_duration_tolerance_secs": self.config.get("verify_duration_tolerance_secs"),
                "verify_duration_tolerance_pct": self.config.get("verify_duration_tolerance_pct"),
                "history_source_hash": self.config.get("history_source_hash"),
                "history_vmaf": self.config.get("history_vmaf"),
            },
        }
        try:
            env_path = os.path.join(self.staging_dir, "pipeline_env.json")
            tmp = env_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                _json.dump(env, f, indent=2, ensure_ascii=False)
            os.replace(tmp, env_path)
            logging.info(
                f"  Session {session_id}: {gpu_name or '?'} · driver {driver or '?'} · "
                f"{ffmpeg_v.split()[2] if ffmpeg_v else '?'} · git {(git_commit or '?')[:8]}{'-dirty' if git_dirty else ''}"
            )
        except Exception as e:
            logging.warning(f"Failed to write pipeline_env.json: {e}")

    def run(self, full_gamut_queue: list[dict], gap_filler_queue: list[dict], enable_gap_filler: bool = True):
        """Main entry point."""
        logging.info("Orchestrator starting:")
        logging.info(f"  Full gamut: {len(full_gamut_queue)} files")
        logging.info(f"  Gap filler: {len(gap_filler_queue)} files ({'enabled' if enable_gap_filler else 'disabled'})")

        # Write the per-session env snapshot so every encode_history entry can reference
        # it via session_id rather than duplicating MBs of environment across thousands of rows.
        self._write_session_env()

        for subdir in ("fetch", "encoded", "force", "whisper_tmp", "ocr_tmp"):
            os.makedirs(os.path.join(self.staging_dir, subdir), exist_ok=True)

        # Reset any non-terminal states from previous crashed runs
        reset_count = self.state.reset_non_terminal()
        if reset_count:
            logging.info(f"  Reset {reset_count} stale entries from previous run")

        # Remove ghost 'done' entries where the source file no longer exists (renamed/deleted).
        # Bounded by a 30s wall-clock deadline — on slow/flaky SMB (common when the
        # NAS is busy) os.path.exists can block per-file for seconds, so 2,700+ paths
        # would hang the orchestrator startup indefinitely. If we don't finish in 30s,
        # skip ghost detection this run — stale done entries are harmless (they just
        # take up space in pipeline_state.db) and the next clean-NAS startup will catch
        # them. The scanner's hourly pass also prunes ghost entries.
        done_paths = self.state.get_files_by_status(FileStatus.DONE)
        if done_paths:
            import time as _time
            from concurrent.futures import ThreadPoolExecutor, as_completed

            deadline = _time.monotonic() + 30.0
            existence: dict[str, bool] = {}
            with ThreadPoolExecutor(max_workers=16) as pool:
                futures = {pool.submit(os.path.exists, p): p for p in done_paths}
                for fut in as_completed(futures):
                    p = futures[fut]
                    try:
                        existence[p] = fut.result(timeout=max(0.5, deadline - _time.monotonic()))
                    except Exception:
                        existence[p] = True  # assume exists on timeout — safer than deleting
                    if _time.monotonic() > deadline:
                        # time's up — cancel outstanding
                        for pending_fut in futures:
                            pending_fut.cancel()
                        break
            checked = len(existence)
            if checked < len(done_paths):
                logging.info(
                    f"  Ghost check deadline hit — probed {checked}/{len(done_paths)} "
                    f"done entries. Continuing (stale entries will be cleaned next run)."
                )
            ghosts = [p for p, exists in existence.items() if not exists]
            if ghosts:
                self.state.remove_ghosts(ghosts)
                logging.info(f"  Removed {len(ghosts)} ghost entries (files renamed/deleted)")

        # Delete stale tmp files left by any pipeline stage (gap filler, mux,
        # audio transcode, full-gamut NVENC, legacy naslib). These are leftovers
        # from runs interrupted between the tmp-mux and the final rename — they
        # block subsequent rename attempts (WinError 183), the scanner indexes
        # them as if real media, and they waste disk. Pass 1: sweep whatever the
        # state DB knows about. Pass 2: walk the NAS itself because a crash
        # could have written a tmp file that was never recorded in state.
        state_tmp_paths = [
            p for p in self.state.all_filepaths()
            if p.endswith(_PIPELINE_TMP_SUFFIXES)
        ] if hasattr(self.state, "all_filepaths") else []
        if state_tmp_paths:
            deleted = 0
            for p in state_tmp_paths:
                try:
                    if os.path.exists(p):
                        os.remove(p)
                    deleted += 1
                except OSError as e:
                    logging.debug(f"Could not remove stale pipeline tmp {p}: {e}")
            self.state.remove_ghosts(state_tmp_paths)
            logging.info(
                f"  Cleaned {deleted}/{len(state_tmp_paths)} stale pipeline tmp files "
                f"from state (suffixes: {_PIPELINE_TMP_SUFFIXES})"
            )

        # NAS walk: find tmp files that were never in state (crashed before
        # state.set_file). Same 16-worker + 30s deadline pattern as the ghost
        # check above — SMB per-file stat can block, so a hard deadline keeps
        # orchestrator startup bounded. Anything we miss this pass will be
        # caught on the next startup.
        try:
            from paths import NAS_MOVIES as _NAS_MOVIES
            from paths import NAS_SERIES as _NAS_SERIES

            nas_roots = [str(_NAS_MOVIES), str(_NAS_SERIES)]
        except Exception:
            nas_roots = []

        nas_tmp_found: list[str] = []
        if nas_roots:
            import time as _time
            from concurrent.futures import ThreadPoolExecutor

            deadline = _time.monotonic() + 30.0

            def _walk_for_tmps(root: str) -> list[str]:
                found: list[str] = []
                if not os.path.isdir(root):
                    return found
                for dirpath, _dirs, filenames in os.walk(root):
                    if _time.monotonic() > deadline:
                        break
                    for fname in filenames:
                        if fname.endswith(_PIPELINE_TMP_SUFFIXES):
                            found.append(os.path.join(dirpath, fname))
                return found

            with ThreadPoolExecutor(max_workers=16) as pool:
                futures = [pool.submit(_walk_for_tmps, root) for root in nas_roots]
                for fut in futures:
                    try:
                        remaining = max(0.5, deadline - _time.monotonic())
                        nas_tmp_found.extend(fut.result(timeout=remaining))
                    except Exception:
                        # Best-effort — a timeout or SMB hiccup just means we miss
                        # this sweep. The next startup will catch anything we leave.
                        pass

            # Dedupe against state sweep so we don't double-delete.
            already = set(state_tmp_paths)
            nas_tmp_found = [p for p in nas_tmp_found if p not in already]

            if nas_tmp_found:
                nas_deleted = 0
                for p in nas_tmp_found:
                    try:
                        os.remove(p)
                        nas_deleted += 1
                    except OSError as e:
                        logging.debug(f"Could not remove NAS-walked tmp {p}: {e}")
                logging.info(
                    f"  NAS walk cleaned {nas_deleted}/{len(nas_tmp_found)} stale "
                    f"pipeline tmp files not tracked in state"
                )

        # Clean orphaned fetch/encoded files from previous runs. Retry once on transient
        # Windows file locks — the common case is the old pipeline's ffmpeg still holding
        # the handle for a moment after the subprocess exits. Left orphans waste disk and
        # can mislead the fetch-buffer accounting.
        import time as _time

        for subdir in ("fetch", "encoded"):
            d = os.path.join(self.staging_dir, subdir)
            if not os.path.isdir(d):
                continue
            cleaned = 0
            still_locked = []
            for f in os.listdir(d):
                path = os.path.join(d, f)
                try:
                    os.remove(path)
                    cleaned += 1
                except OSError:
                    still_locked.append(f)
            if still_locked:
                _time.sleep(1)
                for f in list(still_locked):
                    path = os.path.join(d, f)
                    try:
                        os.remove(path)
                        cleaned += 1
                        still_locked.remove(f)
                    except OSError:
                        pass
            if cleaned:
                logging.info(f"  Cleaned {cleaned} orphaned files from {subdir}/")
            if still_locked:
                logging.warning(
                    f"  {len(still_locked)} file(s) in {subdir}/ still locked after retry — "
                    f"leaving them; they'll be reclaimed by the next restart. First: {still_locked[0][:80]}"
                )

        self.state.compact()
        full_gamut_queue = self.control.apply_queue_overrides(full_gamut_queue)
        gap_filler_queue = self.control.apply_queue_overrides(gap_filler_queue)

        threads = {}
        # Spin up N GPU worker threads (RTX 40-series has 2 NVENC chips → N=2 safe).
        for i in range(self._gpu_concurrency):
            name = f"gpu-encode-{i}"
            threads[name] = threading.Thread(
                target=self._gpu_worker,
                args=(full_gamut_queue, gap_filler_queue, i),
                daemon=True,
                name=name,
            )
        # Single fetch worker. SMB upload is already saturated by one transfer; a second
        # thread only adds contention. fetch_file claims its target under state._lock, so
        # even with N>1 two workers would never pick the same path — we just don't need N>1.
        fetch_concurrency = max(1, int(self.config.get("fetch_concurrency", 1)))
        for i in range(fetch_concurrency):
            name = f"fetch-{i}" if fetch_concurrency > 1 else "fetch"
            threads[name] = threading.Thread(
                target=self._fetch_worker,
                args=(full_gamut_queue, i),
                daemon=True,
                name=name,
            )
        if enable_gap_filler:
            threads["gap_filler"] = threading.Thread(
                target=self._gap_filler_worker, args=(gap_filler_queue,), daemon=True, name="gap-filler"
            )

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

    def _gpu_worker(self, queue: list[dict], gap_queue: list[dict], worker_id: int = 0):
        """Full gamut encodes. Holds one GPU semaphore slot per file.

        Multiple workers run in parallel — the semaphore caps concurrency to the configured
        `gpu_concurrency` (default 2 for RTX 40-series dual NVENC). Pickers use a shared
        `_dispatched` set to avoid two workers grabbing the same file.
        """
        tag_prefix = f"gpu{worker_id}"
        logging.info(f"GPU worker {worker_id} started")
        processed = 0

        while not self._shutdown.is_set():
            with self._dispatched_lock:
                item = self._pick_next_locked(queue)
                if item is not None:
                    self._dispatched.add(item["filepath"])

            if item is None:
                if self._all_done(queue):
                    break
                self._shutdown.wait(timeout=5)
                continue

            filepath = item["filepath"]
            processed += 1

            while self.control.is_encode_paused() and not self._shutdown.is_set():
                self._shutdown.wait(timeout=5)

            # Tell the network worker what we need fetched (per-worker tracking via shared set).
            self._set_gpu_wants(filepath)

            logging.info(
                f"\n[{tag_prefix} {processed}/{len(queue)}] "
                f"{item['filename']} ({format_bytes(item.get('file_size_bytes', 0))})"
            )

            # Encode under the GPU semaphore, then release it BEFORE uploading —
            # upload is network-bound, so holding the GPU slot through it blocks
            # the next encode for no reason.
            with self._gpu_semaphore:
                encode_ok = full_gamut(filepath, item, self.config, self.state, self.staging_dir)

            if encode_ok:
                try:
                    finalize_upload(filepath, self.state, self.config)
                except Exception as e:
                    logging.error(f"Upload failed for {os.path.basename(filepath)}: {e}")
                    self.state.stats["errors"] = self.state.stats.get("errors", 0) + 1
                    self.state.save()
            else:
                self.state.stats["errors"] = self.state.stats.get("errors", 0) + 1
                self.state.save()

            self._set_gpu_wants(None, previous=filepath)

        self._set_gpu_wants(None)
        logging.info(f"GPU worker {worker_id} finished")

    # NOTE: the force-stack mechanism was removed. Previously the GPU worker popped
    # entries from `control/priority.json -> force` and ran them ahead of the regular
    # queue. If a user needs to force a re-encode today they delete the state DB row
    # and let the queue builder pick the file up again.

    # =========================================================================
    # Fetch Worker — pre-fetch files to keep the GPU encoders fed
    # =========================================================================

    def _fetch_worker(self, queue: list[dict], worker_id: int = 0):
        """Pull files from NAS → staging, in priority order.

        Priority order:
        1. Fetch what a GPU worker is blocked on (never starve the encoders)
        2. Pre-fetch next queue items (up to the adaptive cap)

        After each successful fetch, runs cheap pre-processing (language detection +
        external sub scan) so the encode startup is basically instant.
        """
        tag = f"Fetch worker {worker_id}" if worker_id >= 0 else "Fetch worker"
        logging.info(f"{tag} started")
        max_buffer = self.config.get("max_fetch_buffer_bytes", 2000 * 1024**3)
        # Cap the count of pre-fetched-but-not-yet-encoded files, independent of bytes.
        max_prefetched = max(4, 3 * self._gpu_concurrency)

        while not self._shutdown.is_set():
            did_work = False

            if self.control.is_fetch_paused():
                self._shutdown.wait(timeout=5)
                continue

            if self._get_fetch_buffer_used() >= max_buffer:
                self._shutdown.wait(timeout=5)
                continue

            prefetched_count = self._count_prefetched()
            prefetch_full = prefetched_count >= max_prefetched

            # === Priority 1: Fetch what the GPU workers are blocked on ===
            for gpu_wants in self._get_gpu_wants():
                existing = self.state.get_file(gpu_wants)
                status = existing["status"] if existing else None
                if status not in (None, FileStatus.PENDING.value):
                    continue  # already fetched or in some other state
                entry = self._lookup_file(gpu_wants)
                if not entry:
                    entry = {
                        "filepath": gpu_wants,
                        "filename": os.path.basename(gpu_wants),
                        "file_size_bytes": 0,
                        "library_type": "movie" if "Movies" in gpu_wants else "series",
                    }
                result = fetch_file(entry, self.staging_dir, self.config, self.state)
                if result is not None:
                    self._post_fetch(entry)
                    did_work = True
                    break
            if did_work:
                continue

            if prefetch_full:
                self._shutdown.wait(timeout=10)
                continue

            # === Priority 2: Pre-fetch next queue items ===
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
                    self._post_fetch(item)
                    did_work = True
                    break

            if not did_work:
                self._shutdown.wait(timeout=5)

        logging.info(f"{tag} finished")

    def _post_fetch(self, item: dict) -> None:
        """Eager CPU work on a freshly-fetched file so the GPU worker doesn't pay the cost.

        Runs:
        - Language detection on undetermined audio/sub tracks (text-based, no whisper).
        - External sub scan — find sidecar .srt/.ass files next to the source.

        Results are written into state.extras under `detected_streams` and `external_subs`;
        full_gamut reads them and skips its own detection. If anything goes wrong here we
        just log + move on — the encode will re-detect at worst case.
        """
        filepath = item.get("filepath")
        if not filepath:
            return
        try:
            # Local import to avoid circular imports at module load.
            from pipeline.language import detect_all_languages

            enriched = detect_all_languages(item, use_whisper=False)
            payload: dict = {}
            if enriched:
                payload["detected_audio"] = enriched.get("audio_streams")
                payload["detected_subs"] = enriched.get("subtitle_streams")
        except Exception as e:
            logging.debug(f"  post-fetch language detect failed ({os.path.basename(filepath)}): {e}")
            payload = {}

        try:
            from pipeline.full_gamut import _find_external_subs

            ext_subs = _find_external_subs(filepath)
            if ext_subs:
                payload["external_subs"] = ext_subs
        except Exception as e:
            logging.debug(f"  post-fetch sub scan failed ({os.path.basename(filepath)}): {e}")

        if payload:
            payload["pre_processed"] = True
            try:
                self.state.set_file(filepath, FileStatus.PROCESSING, **payload)
            except Exception as e:
                logging.debug(f"  post-fetch state write failed: {e}")

    # =========================================================================
    # Gap Filler Worker — CPU work always, GPU (whisper) between encodes
    # =========================================================================

    def _gap_filler_worker(self, queue: list[dict]):
        """Gap filler: one SSH-heavy worker + one local-quick worker.

        Heavy (SSH): track stripping + sub muxing via remote mkvmerge on SERVER.
        Quick (local): rename, metadata, foreign sub delete — instant NAS ops.

        Files can split between both queues (e.g. rename+strip both needed). The
        two workers don't conflict: mkvmerge writes a tmp file, mkvpropedit/rename
        operate on the original.
        """
        import queue as queue_mod

        from pipeline.gap_filler import (
            _rename_file,
            _scan_external_subs,
            analyse_gaps,
            gap_fill,
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

        # Single circuit breaker for the SSH path. threshold=5, cooldown=300s matches
        # the overnight-2026-04-23 forensic — 5 consecutive SSH+docker failures is the
        # point where "transient" is no longer a credible explanation and we back off.
        breaker = CircuitBreaker(threshold=5, cooldown_secs=300, name="heavy_worker.SRV")

        def heavy_worker(name: str, machine: dict):
            while not self._shutdown.is_set():
                # Back off if the breaker is OPEN. wait_if_open raises CircuitBreakerOpen
                # if shutdown fires while we're waiting — catch that and exit cleanly.
                # Otherwise we block (polling) until the breaker ages into HALF_OPEN and
                # a trial is allowed through.
                try:
                    breaker.wait_if_open(shutdown=self._shutdown)
                except CircuitBreakerOpen:
                    logging.info(f"[{name}] worker exiting — breaker open at shutdown")
                    break

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
                gaps._remote_machine = machine
                was_open_before = breaker.is_open()
                success = False
                try:
                    success = bool(gap_fill(filepath, item, gaps, self.config, self.state))
                except Exception as e:
                    logging.error(f"[{name}] gap_fill raised on {item['filename']}: {e}")
                    success = False

                # Cross-check state: if the file landed in ERROR, treat as failure
                # regardless of the return value. Belt-and-braces — the discipline
                # contract forbids trusting return codes without state verification.
                entry = self.state.get_file(filepath)
                if entry and (entry.get("status") or "").lower() == "error":
                    success = False
                    self.state.stats["errors"] = self.state.stats.get("errors", 0) + 1
                    self.state.save()

                breaker.record(success)
                if not was_open_before and breaker.is_open():
                    logging.warning(
                        f"[{name}] circuit breaker OPENED (5 consecutive failures) — "
                        f"worker will pause for 300s"
                    )

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
                    old_path = filepath
                    new_path = _rename_file(filepath, gaps.clean_name)
                    if new_path:
                        filepath = new_path
                        filename = gaps.clean_name
                        # Patch report with new path/filename
                        try:
                            from tools.report_lock import read_report, write_report

                            rpt = read_report()
                            for entry in rpt.get("files", []):
                                if entry.get("filepath") == old_path:
                                    entry["filepath"] = new_path
                                    entry["filename"] = gaps.clean_name
                                    break
                            write_report(rpt)
                        except Exception as e:
                            logging.debug(f"  Report patch failed for {filename}: {e}")

                # TMDb metadata
                if gaps.needs_metadata:
                    try:
                        from pipeline.metadata import enrich_and_tag

                        tmdb_data = enrich_and_tag(filepath, filename, library_type)
                        # Patch TMDb directly into the report (update_entry doesn't read MKV tags)
                        if tmdb_data:
                            from tools.report_lock import read_report, write_report

                            try:
                                rpt = read_report()
                                for entry in rpt.get("files", []):
                                    if entry.get("filepath") == filepath or entry.get("filepath") == item.get(
                                        "filepath"
                                    ):
                                        entry["tmdb"] = tmdb_data
                                        break
                                write_report(rpt)
                            except Exception as e:
                                logging.debug(f"  TMDb report patch failed for {filename}: {e}")
                    except Exception as e:
                        logging.debug(f"  TMDb tagging failed for {filename}: {e}")

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
                except Exception as e:
                    logging.warning(f"  Report update failed for {filename}: {e}")

                quick_queue.task_done()

        from pipeline.nas_worker import SERVER

        threads = []
        # Single SSH heavy worker against SERVER. When SERVER is configured it's always
        # the better path (native Docker, NFS-mounted media — much faster than Synology).
        # If not configured, skip heavy ops entirely rather than fall back to NAS SSH.
        if SERVER.get("host"):
            threads.append(threading.Thread(target=heavy_worker, args=("SRV", SERVER), daemon=True))
        else:
            logging.info("  Heavy worker skipped (SERVER_SSH_HOST not set)")
        # Single local quick worker (rename/metadata/delete — instant ops).
        threads.append(threading.Thread(target=quick_worker, args=("QUICK",), daemon=True))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        logging.info("Gap filler finished")

    # =========================================================================
    # Helpers
    # =========================================================================

    def _pick_next(self, queue: list[dict]) -> dict | None:
        """Non-locked variant — acquires the lock. Use _pick_next_locked from GPU workers."""
        with self._dispatched_lock:
            return self._pick_next_locked(queue)

    def _pick_next_locked(self, queue: list[dict]) -> dict | None:
        """Pick next item from full gamut queue. Prefers already-fetched files.

        Caller MUST hold `_dispatched_lock`. Skips items already in `_dispatched`.
        """
        # First pass: find a file that's already fetched (status=PROCESSING, local file exists)
        for item in queue:
            fp = item["filepath"]
            if fp in self._dispatched:
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
            if fp in self._dispatched:
                continue
            if self.control.should_skip(fp):
                continue
            existing = self.state.get_file(fp)
            status = existing["status"] if existing else None
            if status in (FileStatus.DONE.value, FileStatus.ERROR.value, FileStatus.PROCESSING.value):
                continue
            return item
        return None

    def _all_done(self, queue: list[dict]) -> bool:
        with self._dispatched_lock:
            dispatched = set(self._dispatched)
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

    def _count_prefetched(self) -> int:
        """Number of files in state that are fetched-but-not-yet-done.

        Used to cap aggressive pre-fetch: it's pointless to keep pulling files from the NAS
        when dozens are already sitting on the staging drive waiting for the encoder. Counts
        any non-terminal, non-pending file (processing/uploading/encoding/etc.) — those have
        a local fetch file consuming buffer.
        """
        active_statuses = {
            FileStatus.PROCESSING.value,
            FileStatus.FETCHING.value,
            FileStatus.UPLOADING.value,
        }
        count = 0
        for fp, entry in self.state.get_all_files().items():
            status = (entry.get("status") or "").lower()
            if status in active_statuses and entry.get("local_path"):
                count += 1
        return count

    def _lookup_file(self, filepath: str) -> dict | None:
        """Look up a file in the media report."""
        try:
            from tools.report_lock import read_report

            report = read_report()
            for f in report.get("files", []):
                if f.get("filepath") == filepath:
                    return f
        except Exception as e:
            logging.debug(f"Media report lookup failed for {filepath}: {e}")
        return None
