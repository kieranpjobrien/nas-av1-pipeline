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
from pipeline.state import FileStatus, PipelineState, is_terminal
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
        # Files currently being prepped by a prep worker. Prevents two prep
        # workers grabbing the same file. Same role for prep as _dispatched
        # has for GPU.
        self._prepping: set[str] = set()
        self._prepping_lock = threading.Lock()
        # Files currently being uploaded by an upload worker. Same role for
        # upload as _dispatched has for GPU.
        self._uploading: set[str] = set()
        self._uploading_lock = threading.Lock()

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

    def _write_heavy_worker_status(
        self,
        configured: bool,
        queued_count: int,
        host: str | None,
    ) -> None:
        """Write the heavy gap_filler worker status to a file the dashboard can read.

        Surfaces the "SERVER_SSH_HOST not set + heavy queue not empty" failure
        mode that previously only appeared as a single INFO log line per pass
        (so 1,264 queued tasks could sit invisible for hours overnight). The
        dashboard /api/health endpoint and tools/invariants.py both consume
        this file.
        """
        import json as _json
        from datetime import datetime, timezone

        status = {
            "configured": configured,
            "queued_count": queued_count,
            "host": host,
            "last_check": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "blocked": (not configured) and queued_count > 0,
        }
        try:
            path = os.path.join(self.staging_dir, "heavy_worker_state.json")
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                _json.dump(status, f, indent=2, ensure_ascii=False)
            os.replace(tmp, path)
        except OSError as e:
            logging.warning(f"Failed to write heavy_worker_state.json: {e}")

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

        # Clean orphaned fetch/encoded files from previous runs.
        #
        # 2026-04-30 incident: this used to blindly delete every file in
        # ``encoded/`` and ``fetch/`` on startup. The upload worker had died
        # mid-day on a JSON corruption error; 66 fully-encoded files were
        # sitting in ``encoded/`` waiting to be uploaded with the matching
        # state row at status='uploading'. The next pipeline restart wiped
        # all 66 — ~11 hours of GPU encode work — because the cleanup didn't
        # check whether the state DB still referenced those paths.
        #
        # New rule: a file is only "orphaned" if NO live state row points
        # at it. We collect every ``local_path`` from rows in non-terminal
        # status (processing, uploading, fetching, pending with prep_data)
        # and skip files matching those paths. Anything left is safe to
        # remove — it really was abandoned by a previous crashed run.
        import time as _time
        import json as _json

        live_paths: set[str] = set()
        try:
            for _fp, row in self.state.get_all_files().items():
                status = (row.get("status") or "").lower()
                # Terminal statuses don't have in-flight files in encoded/fetch
                if status in ("done", "skipped", "error", "flagged_undetermined"):
                    continue
                # _row_to_dict already merged extras into row, so prep_data /
                # local_path / output_path / actual_input live at the top level.
                for k in ("local_path", "output_path", "actual_input"):
                    p = row.get(k)
                    if p:
                        live_paths.add(os.path.normcase(os.path.abspath(p)))
                pd = row.get("prep_data")
                if isinstance(pd, dict):
                    for k in ("local_path", "output_path", "actual_input"):
                        p = pd.get(k)
                        if p:
                            live_paths.add(os.path.normcase(os.path.abspath(p)))
        except Exception as e:
            logging.warning(
                f"  Could not enumerate live state rows for safe cleanup ({e!r}); "
                f"falling back to age-based: only files >24h old will be removed"
            )
            live_paths = set()

        now = _time.time()
        AGE_FALLBACK_SECS = 24 * 3600  # if state lookup failed, only kill very old files

        for subdir in ("fetch", "encoded"):
            d = os.path.join(self.staging_dir, subdir)
            if not os.path.isdir(d):
                continue
            cleaned = 0
            preserved = 0
            still_locked = []
            for f in os.listdir(d):
                path = os.path.join(d, f)
                norm = os.path.normcase(os.path.abspath(path))
                # Hard skip: live state row points at this file
                if norm in live_paths:
                    preserved += 1
                    continue
                # Soft skip when we couldn't enumerate state: only delete >24h
                if not live_paths:
                    try:
                        if (now - os.path.getmtime(path)) < AGE_FALLBACK_SECS:
                            preserved += 1
                            continue
                    except OSError:
                        pass
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
            if cleaned or preserved:
                logging.info(
                    f"  {subdir}/: cleaned {cleaned} orphaned, preserved {preserved} "
                    f"referenced by live state"
                )
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

        # Prep workers — run all CPU-bound prep work (filename clean,
        # language detect with whisper, qualify gate, external sub scan,
        # container remux) AHEAD of the GPU worker. Multiple instances run
        # in parallel so a slow whisper escalation on one file doesn't
        # starve the GPU pipeline. CPU-only, never touches NVENC, safe.
        prep_concurrency = max(0, int(self.config.get("prep_concurrency", 2)))
        for i in range(prep_concurrency):
            name = f"prep-{i}"
            threads[name] = threading.Thread(
                target=self._prep_worker,
                args=(full_gamut_queue, i),
                daemon=True,
                name=name,
            )

        # Upload workers — run finalize_upload (NAS upload, verify, atomic
        # replace, mkvpropedit tags, Plex scan trigger) AFTER the GPU
        # encode. Decouples the SMB upload from the GPU thread so the GPU
        # dives straight into the next encode. Network-bound, CPU-light.
        upload_concurrency = max(0, int(self.config.get("upload_concurrency", 1)))
        for i in range(upload_concurrency):
            name = f"upload-{i}" if upload_concurrency > 1 else "upload"
            threads[name] = threading.Thread(
                target=self._upload_worker,
                args=(i,),
                daemon=True,
                name=name,
            )

        # Refresh worker — re-reads media_report.json on mtime change and
        # merges any new files into the live queues. Lets Sonarr/Radarr
        # drop-ins become next-up without waiting for a pipeline restart.
        # Disabled when refresh_interval_secs <= 0 (e.g. unit tests).
        refresh_interval = float(self.config.get("queue_refresh_interval_secs", 60.0))
        if refresh_interval > 0:
            threads["refresh"] = threading.Thread(
                target=self._refresh_worker,
                args=(full_gamut_queue, gap_filler_queue, refresh_interval),
                daemon=True,
                name="queue-refresh",
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

            # GPU semaphore is now threaded INTO full_gamut so it's held only
            # around the actual NVENC encode subprocess (_run_encode). Prep
            # steps (filename clean, language detect, qualify, external subs,
            # container remux, command build) and verify run WITHOUT holding
            # the slot — they're pure CPU/disk and previously parked the slot
            # idle for ~30-90s per file. With 2 GPU workers the slot freed by
            # one worker's prep can immediately be claimed by the other's
            # encode, keeping the NVENC chips warm.
            encode_ok = full_gamut(
                filepath,
                item,
                self.config,
                self.state,
                self.staging_dir,
                gpu_semaphore=self._gpu_semaphore,
            )

            if encode_ok:
                # Upload + verify + replace + tags + Plex scan run on the
                # dedicated upload worker. The GPU thread leaves the row at
                # status=UPLOADING (set inside full_gamut) and moves on to
                # the next encode immediately. This is what keeps NVENC at
                # 100% — no more "GPU thread spends 30-60s pushing bytes
                # back over SMB while NVENC sits idle".
                #
                # Fallback: if upload_concurrency is 0 (e.g. unit tests, or
                # a deliberate config), run inline as before.
                upload_inline = int(self.config.get("upload_concurrency", 1)) <= 0
                if upload_inline:
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
            # Snapshot the queue under the dispatched lock so the refresh
            # worker's mid-iteration mutations don't race. fetch_file is
            # network-bound (slow) — we MUST iterate the snapshot, not the
            # live queue, otherwise holding the lock would serialise fetches.
            with self._dispatched_lock:
                queue_snapshot = list(queue)
            for item in queue_snapshot:
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
        """Gap filler with drain-and-rescan loop: runs one pass then re-scans.

        Each pass: snapshot the queue, build heavy (SSH mkvmerge) + quick
        (local rename/metadata/sub-delete) op queues from non-terminal
        items, drain them, then sleep briefly before the next pass. New
        files added by the refresh worker between passes get picked up
        on the next iteration.

        Heavy (SSH): track stripping + sub muxing via remote mkvmerge on SERVER.
        Quick (local): rename, metadata, foreign sub delete — instant NAS ops.
        """
        rescan_interval = float(self.config.get("gap_filler_rescan_interval_secs", 60.0))
        pass_num = 0

        while not self._shutdown.is_set():
            pass_num += 1
            processed = self._gap_filler_pass(queue, pass_num)
            if self._shutdown.is_set():
                break
            # Wait between passes. Idle pause is longer (no new work likely)
            # than busy pause (refresh worker may have added more).
            wait_secs = rescan_interval if processed == 0 else 5.0
            self._shutdown.wait(timeout=wait_secs)

        logging.info(f"Gap filler finished (drain-and-rescan, {pass_num} pass(es))")

    def _gap_filler_pass(self, queue: list[dict], pass_num: int) -> int:
        """One drain pass over the gap_filler queue. Returns items processed."""
        import queue as queue_mod

        from pipeline.gap_filler import (
            _rename_file,
            _scan_external_subs,
            analyse_gaps,
            gap_fill,
        )

        # Two queues with independent gates:
        #   mux_queue   — track strip + external sub mux (mkvmerge over SSH)
        #   quick_queue — rename, mkvpropedit metadata, foreign sub delete (local, instant)
        #
        # Audio transcode is deliberately NOT a gap_filler responsibility —
        # it requires fetching whole files over SMB and running ffmpeg locally,
        # which is bandwidth-heavy and not what the user wants happening
        # autonomously. Files needing audio transcode are flagged for
        # diagnostic purposes but skipped here; they belong in the encode
        # pipeline (full_gamut) on explicit user request.
        mux_queue: queue_mod.Queue = queue_mod.Queue()
        quick_queue: queue_mod.Queue = queue_mod.Queue()
        stats_lock = threading.Lock()
        mux_count = [0]
        quick_count = [0]

        # Build separate operation queues from all gap filler items.
        # Snapshot under the lock so the refresh worker's appends don't race
        # with this iteration. Gap_filler currently builds its op queues
        # once at startup — files added by the refresher AFTER this point
        # won't be picked up until a future "drain & rescan" feature.
        with self._dispatched_lock:
            queue_snapshot = list(queue)
        for item in queue_snapshot:
            filepath = item["filepath"]
            existing = self.state.get_file(filepath)
            status = existing["status"] if existing else None
            # Skip terminal statuses (DONE + FLAGGED_*) and ERROR. ERROR is
            # transient — retried on the next queue build — but currently we
            # skip it here too because the queue builder will reset it.
            if status == FileStatus.ERROR.value or (status and is_terminal(status)):
                continue

            gaps = analyse_gaps(item, self.config)
            gaps.needs_language_detect = False  # whisper handled separately

            # Quick ops: rename, metadata, foreign sub delete — queue independently
            if gaps.needs_filename_clean or gaps.needs_metadata or gaps.needs_foreign_sub_cleanup:
                quick_queue.put((item, gaps))

            # Mux ops: track strip + external sub mux. Audio-transcode is NOT
            # a gap_filler responsibility — those files are skipped here and
            # left for the user to re-encode through full_gamut if desired.
            if gaps.needs_track_removal or gaps.needs_sub_mux:
                mux_queue.put((item, gaps))

        mux_total = mux_queue.qsize()
        quick_total = quick_queue.qsize()
        logging.info(
            f"Gap filler: {mux_total} mux (mkvmerge SSH) + {quick_total} quick (rename/meta/delete)"
        )

        # Single circuit breaker for the SSH path. threshold=5, cooldown=300s matches
        # the overnight-2026-04-23 forensic — 5 consecutive SSH+docker failures is the
        # point where "transient" is no longer a credible explanation and we back off.
        breaker = CircuitBreaker(threshold=5, cooldown_secs=300, name="gap_filler.mux.SRV")

        def mux_worker(name: str, machine: dict):
            """SSH-gated worker: track strip + external sub mux via remote mkvmerge.

            Audio transcode is NOT handled here — gap_filler doesn't transcode
            audio. Files queued have only mux gaps (track_removal or sub_mux).
            """
            while not self._shutdown.is_set():
                try:
                    breaker.wait_if_open(shutdown=self._shutdown)
                except CircuitBreakerOpen:
                    logging.info(f"[{name}] worker exiting — breaker open at shutdown")
                    break

                try:
                    item, gaps = mux_queue.get(timeout=2)
                except queue_mod.Empty:
                    break

                filepath = item["filepath"]
                with stats_lock:
                    mux_count[0] += 1
                    p = mux_count[0]

                # Re-scan for external subs (deferred from queue building)
                _scan_external_subs(filepath, gaps)

                if not (gaps.needs_track_removal or gaps.needs_sub_mux):
                    mux_queue.task_done()
                    continue

                logging.info(f"[{name} {p}/{mux_total}] {gaps.describe()} | {item['filename']}")
                gaps._remote_machine = machine
                was_open_before = breaker.is_open()
                success = False
                try:
                    success = bool(gap_fill(filepath, item, gaps, self.config, self.state))
                except Exception as e:
                    logging.error(f"[{name}] gap_fill raised on {item['filename']}: {e}")
                    success = False

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

                mux_queue.task_done()

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
        # Mux worker — backend is config-selectable:
        #   "local"  (default 2026-04-29) — runs mkvmerge.exe locally against
        #            UNC paths. Slower (SMB-bound) but doesn't load the NAS.
        #   "remote" — SSH+Docker against SERVER. Faster, but stresses Synology.
        #
        # Status side-effect: write a state file the dashboard / invariants
        # can read regardless of which backend is in use, so the user always
        # sees mux queue + worker availability.
        backend = (self.config.get("gap_filler_mux_backend") or "local").lower()
        backend_available = False
        backend_host = None
        if backend == "remote":
            backend_available = bool(SERVER.get("host"))
            backend_host = SERVER.get("host") or None
        elif backend == "local":
            from pipeline import local_mux as _local_mux
            backend_available = _local_mux.is_available()
            backend_host = "local" if backend_available else None

        self._write_heavy_worker_status(
            configured=backend_available,
            queued_count=mux_total,
            host=backend_host,
        )

        if backend_available:
            threads.append(threading.Thread(
                target=mux_worker,
                # `machine` is only meaningful for the remote backend — local
                # ignores it. Pass SERVER either way; the dispatcher in
                # gap_filler._strip_tracks selects by backend.
                args=("SRV" if backend == "remote" else "LOCAL", SERVER if backend == "remote" else {}),
                daemon=True,
            ))
        elif mux_total > 0:
            if backend == "remote":
                logging.warning(
                    f"  Mux worker DISABLED — {mux_total} files queued but "
                    f"SERVER_SSH_HOST is not set. Set it in .env or switch "
                    f"gap_filler_mux_backend to 'local'."
                )
            else:
                logging.warning(
                    f"  Mux worker DISABLED — {mux_total} files queued but "
                    f"local mkvmerge.exe was not found. Install MKVToolNix "
                    f"or switch gap_filler_mux_backend to 'remote'."
                )
        else:
            logging.info(f"  Mux worker idle (backend={backend}, queue empty)")
        # Single local quick worker (rename/metadata/delete — instant ops).
        threads.append(threading.Thread(target=quick_worker, args=("QUICK",), daemon=True))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        total = mux_count[0] + quick_count[0]
        if total or pass_num == 1:
            logging.info(f"Gap filler pass {pass_num}: {mux_count[0]} mux + {quick_count[0]} quick processed")
        return total

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
            # Skip currently-mid-flight + terminal (DONE / FLAGGED_*) + ERROR
            if status and (
                status == FileStatus.PROCESSING.value
                or status == FileStatus.ERROR.value
                or is_terminal(status)
            ):
                continue
            return item
        return None

    # =========================================================================
    # Upload worker — runs after the GPU encode lands; ships bytes back to NAS
    # =========================================================================

    def _upload_worker(self, worker_id: int = 0):
        """Pick UPLOADING rows and run finalize_upload on them.

        finalize_upload handles SMB upload + duration verify + atomic
        replace of the original on the NAS + mkvpropedit tag write +
        Plex library scan trigger. Network-bound, CPU-light.

        Decoupling this from the GPU worker is what keeps NVENC busy:
        as soon as encode lands, the GPU thread sets status=UPLOADING and
        moves to the next encode. This worker picks up the UPLOADING row
        and ships bytes back over SMB while the GPU encodes the next one.

        Coordinates via a simple ``_uploading`` set so multiple upload
        workers (if configured) can't double-process the same file.
        """
        from pipeline.full_gamut import finalize_upload

        tag = f"upload-{worker_id}"
        logging.info(f"{tag} started")

        while not self._shutdown.is_set():
            picked = self._pick_for_upload()
            if picked is None:
                self._shutdown.wait(timeout=5)
                continue

            try:
                finalize_upload(picked, self.state, self.config)
            except Exception as e:
                logging.error(f"{tag}: finalize_upload failed for "
                              f"{os.path.basename(picked)}: {e}")
                self.state.stats["errors"] = self.state.stats.get("errors", 0) + 1
                self.state.save()
            finally:
                self._release_upload(picked)

        logging.info(f"{tag} finished")

    def _pick_for_upload(self) -> str | None:
        """Find one filepath in UPLOADING status that isn't already being uploaded."""
        try:
            conn = self.state._get_conn()
            rows = conn.execute(
                "SELECT filepath FROM pipeline_files WHERE status = 'uploading'"
            ).fetchall()
        except Exception:
            return None

        for (fp,) in rows:
            with self._uploading_lock:
                if fp in self._uploading:
                    continue
                self._uploading.add(fp)
                return fp
        return None

    def _release_upload(self, filepath: str) -> None:
        with self._uploading_lock:
            self._uploading.discard(filepath)

    # =========================================================================
    # Prep worker — CPU-bound stage between fetch and GPU encode
    # =========================================================================

    def _prep_worker(self, queue: list[dict], worker_id: int = 0):
        """Run all CPU prep work (steps 1-5) ahead of the GPU encode.

        Picks files where:
          * fetch has landed (status=PROCESSING with local_path on disk)
          * prep hasn't run yet (prep_done not set in extras)
          * not currently being prepped by another prep worker (we use a
            lightweight in-memory ``_prepping`` set to avoid duplication)

        Calls :func:`pipeline.full_gamut.prepare_for_encode` which mutates
        state on completion (FLAGGED_*, DONE-already-compliant, or
        prep_done=True with prep_data extras). The GPU worker then picks
        up only files with prep_done=True so it never waits on whisper /
        remux / language detection.

        Backpressure: if there are already N+ files prepped-and-not-yet-
        encoded (configurable via ``prep_buffer_max``, default 3), pause
        so we don't burn CPU producing more than the GPU can consume.
        """
        from pipeline.full_gamut import prepare_for_encode

        tag = f"prep-{worker_id}"
        logging.info(f"{tag} started")
        prep_buffer_max = max(1, int(self.config.get("prep_buffer_max", 3)))

        while not self._shutdown.is_set():
            # Backpressure: count files already prepped-and-waiting on GPU.
            # If at cap, sleep a bit; the GPU worker will drain.
            prepped_count = self._count_prepped_waiting()
            if prepped_count >= prep_buffer_max:
                self._shutdown.wait(timeout=10)
                continue

            picked = self._pick_for_prep(queue)
            if picked is None:
                # Nothing to prep right now — wait briefly for fetch worker
                # to land more files, then re-scan.
                self._shutdown.wait(timeout=5)
                continue

            filepath = picked["filepath"]
            try:
                prep_data = prepare_for_encode(
                    filepath, picked, self.config, self.state, self.staging_dir
                )
                if prep_data is None:
                    logging.info(f"{tag}: prep parked {os.path.basename(filepath)} "
                                 "(flagged / nothing-to-do / fetch failed)")
                else:
                    logging.info(f"{tag}: prep done for {os.path.basename(filepath)}")
            except Exception as e:
                logging.warning(f"{tag}: prep crashed on {os.path.basename(filepath)}: {e}")
            finally:
                self._release_prep(filepath)

        logging.info(f"{tag} finished")

    def _count_prepped_waiting(self) -> int:
        """How many files are prepped (prep_done=True) but not yet encoded?

        Cheap state DB scan. Used by prep workers for backpressure so we
        don't waste CPU prepping ahead of what the GPU can consume.
        """
        try:
            conn = self.state._get_conn()
            row = conn.execute(
                "SELECT COUNT(*) FROM pipeline_files WHERE status = 'processing' "
                "AND extras LIKE '%\"prep_done\": true%' "
                "AND extras NOT LIKE '%\"stage\": \"encoding\"%' "
                "AND extras NOT LIKE '%\"stage\": \"pending_upload\"%'"
            ).fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def _pick_for_prep(self, queue: list[dict]) -> dict | None:
        """Pick the next queue item that's been fetched but not prepped.

        Coordinates between prep workers via the ``_prepping`` set so two
        workers can't grab the same file. Returns None if nothing's ready.
        """
        with self._dispatched_lock:
            queue_snapshot = list(queue)

        for item in queue_snapshot:
            fp = item["filepath"]
            with self._prepping_lock:
                if fp in self._prepping:
                    continue
            existing = self.state.get_file(fp)
            if not existing:
                continue
            status = existing.get("status")
            if status != FileStatus.PROCESSING.value:
                continue
            if existing.get("prep_done"):
                continue
            local = existing.get("local_path")
            if not (local and os.path.exists(local)):
                continue
            with self._prepping_lock:
                if fp in self._prepping:
                    continue
                self._prepping.add(fp)
            return item
        return None

    def _release_prep(self, filepath: str) -> None:
        with self._prepping_lock:
            self._prepping.discard(filepath)

    # =========================================================================
    # Queue refresh — picks up Sonarr/Radarr drop-ins mid-session
    # =========================================================================

    def _refresh_worker(
        self,
        full_gamut_queue: list[dict],
        gap_filler_queue: list[dict],
        interval_secs: float,
    ) -> None:
        """Periodically re-read media_report.json and merge new files.

        Runs as a background thread. Polls the report's mtime every
        ``interval_secs`` seconds; when it changes, calls
        :meth:`_merge_new_files` to append unseen entries to the live
        queues. Files that turn out to be already-known, terminal, or
        in-flight are filtered out by ``categorise_entry`` + the
        path-set check inside the merge.

        Mutations happen under ``_dispatched_lock`` so iterating workers
        (GPU pickers, gap_filler) don't race with appends.
        """
        from paths import MEDIA_REPORT

        report_path = str(MEDIA_REPORT)
        last_mtime = 0.0
        try:
            last_mtime = os.path.getmtime(report_path)
        except OSError:
            pass

        logging.info(
            f"Queue refresh worker started (poll every {interval_secs:.0f}s, "
            f"watching {report_path})"
        )
        while not self._shutdown.is_set():
            self._shutdown.wait(timeout=interval_secs)
            if self._shutdown.is_set():
                break

            try:
                mtime = os.path.getmtime(report_path)
            except OSError:
                continue
            if mtime <= last_mtime:
                continue
            last_mtime = mtime

            try:
                added_full, added_gap = self._merge_new_files(
                    full_gamut_queue, gap_filler_queue, report_path
                )
                if added_full or added_gap:
                    logging.info(
                        f"Queue refresh: +{added_full} full_gamut, "
                        f"+{added_gap} gap_filler (now {len(full_gamut_queue)} / "
                        f"{len(gap_filler_queue)})"
                    )
            except Exception as e:
                logging.warning(f"Queue refresh failed (non-fatal): {e}")

        logging.info("Queue refresh worker finished")

    def _merge_new_files(
        self,
        full_gamut_queue: list[dict],
        gap_filler_queue: list[dict],
        report_path: str,
    ) -> tuple[int, int]:
        """Read the report and append any new entries to the live queues.

        Returns ``(added_full, added_gap)`` counts. Idempotent: paths that
        are already in either queue are skipped, as are paths whose state
        DB row is terminal (DONE/FLAGGED_*) or actively in-flight.

        Re-sorts both queues smallest-first after appending so the
        fetch-worker / GPU-pickers walk them in the same order they
        would after a fresh ``build_queues`` startup.
        """
        import json as _json

        from pipeline.__main__ import categorise_entry
        from pipeline.gap_filler import analyse_gaps

        with open(report_path, encoding="utf-8") as f:
            report = _json.load(f)

        # Snapshot the current queue paths to avoid duplicate appends.
        # Acquire the lock long enough to copy the path sets — workers
        # may iterate concurrently but won't mutate, so this is safe.
        with self._dispatched_lock:
            known_full = {item.get("filepath") for item in full_gamut_queue}
            known_gap = {item.get("filepath") for item in gap_filler_queue}

        new_full: list[dict] = []
        new_gap: list[dict] = []
        for entry in report.get("files", []) or []:
            fp = entry.get("filepath")
            if not fp:
                continue
            if fp in known_full or fp in known_gap:
                continue
            category, item = categorise_entry(entry, self.config, self.state, self.control)
            if category == "full_gamut":
                new_full.append(item)
            elif category == "gap_filler":
                new_gap.append(item)

        if not new_full and not new_gap:
            return (0, 0)

        with self._dispatched_lock:
            full_gamut_queue.extend(new_full)
            gap_filler_queue.extend(new_gap)
            # Match the startup queue order — see pipeline.__main__ for the
            # default + rationale (largest-first to make the ETA shrink).
            order = (self.config.get("encode_queue_order") or "largest_first").lower()
            full_gamut_queue.sort(
                key=lambda x: x.get("file_size_bytes", 0),
                reverse=(order == "largest_first"),
            )

            def _gap_sort_key(e: dict) -> tuple[int, int]:
                gaps = analyse_gaps(e, self.config)
                return (1 if gaps.needs_fetch else 0, e.get("file_size_bytes", 0))

            gap_filler_queue.sort(key=_gap_sort_key)

        return (len(new_full), len(new_gap))

    def _all_done(self, queue: list[dict]) -> bool:
        # Snapshot both the dispatched set AND the queue under the lock so
        # the refresh worker's appends don't race with this iteration. Per-
        # item state lookups can happen outside the lock (state.get_file
        # has its own internal lock).
        with self._dispatched_lock:
            dispatched = set(self._dispatched)
            queue_snapshot = list(queue)
        for item in queue_snapshot:
            fp = item["filepath"]
            existing = self.state.get_file(fp)
            status = existing["status"] if existing else None
            # ERROR counts as "settled" because retry happens on the next
            # queue build, not within this one. FLAGGED_* and DONE count as
            # settled (the is_terminal check covers both).
            settled = status and (status == FileStatus.ERROR.value or is_terminal(status))
            if not settled:
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
