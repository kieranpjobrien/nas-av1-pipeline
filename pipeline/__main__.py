"""CLI entry point — run via `python -m pipeline` or `uv run python -m pipeline`."""

import argparse
import copy
import faulthandler
import json
import logging
import os
import sys
from datetime import datetime

# Defensive re-set of JSONEncoder defaults (2026-05-23). Across 5 incidents
# we've caught JSONEncoder.key_separator silently mutated from ': ' to a
# random interned string (utf-8, frame, search, status, status). Hardware
# / driver interned-string-pointer corruption is the working hypothesis
# (consistent with 0x7E BSOD history). Resetting at process start gives a
# known-good baseline; hot-path writes also pass separators=(",", ": ")
# explicitly so they survive future corruption — see
# pipeline.orchestrator._write_heavy_worker_status for context.
import json.encoder as _json_encoder
_json_encoder.JSONEncoder.key_separator = ": "
_json_encoder.JSONEncoder.item_separator = ", "

# Pipeline mode forces whisper to run on CPU. The GPU is owned by NVENC for
# the live encode workers, and running whisper on the same chip caused a
# BSOD on 2026-04-21 (rule 9a). CPU + faster-whisper int8 is fast enough
# (~5-15s per file at tiny model) that it can run inline without bottlenecking
# fetch or encode workers. Set BEFORE pipeline.language imports faster_whisper
# so the flag is picked up at first model load.
os.environ.setdefault("WHISPER_FORCE_CPU", "1")

# Crash diagnostics — Python segfaulted at 0xc0000005 in python314.dll on
# 2026-04-27 22:47 with no log line, no traceback, no clue. faulthandler
# catches SIGSEGV / fatal errors and dumps a Python-level stack trace of
# EVERY thread to its registered file before the interpreter dies.
# That's how we'll know whether the segfault was in faster-whisper /
# ctranslate2, an ffmpeg subprocess interaction, our own threading, or
# CPython itself. The handler must be installed BEFORE any C extension
# loads native code, hence wired in at the top of the entry module.
_FAULT_LOG_PATH = os.path.join(
    os.environ.get("AV1_STAGING") or r"F:\AV1_Staging",
    "pipeline_faulthandler.log",
)
try:
    os.makedirs(os.path.dirname(_FAULT_LOG_PATH), exist_ok=True)
    _fault_log = open(_FAULT_LOG_PATH, "a", encoding="utf-8", buffering=1)
    _fault_log.write(
        f"\n--- pipeline start {datetime.now().isoformat(timespec='seconds')} (pid={os.getpid()}) ---\n"
    )
    _fault_log.flush()
    faulthandler.enable(file=_fault_log, all_threads=True)
except OSError:
    # Disk full / permissions / weird drive layout — fall back to stderr.
    faulthandler.enable(all_threads=True)

from paths import MEDIA_REPORT, STAGING_DIR  # noqa: E402
from pipeline.compliance import video_is_finished  # noqa: E402
from pipeline.config import GRADE_CQ_TOLERANCE, build_config  # noqa: E402
from pipeline.control import PipelineControl  # noqa: E402
from pipeline.state import FileStatus, PipelineState, is_terminal  # noqa: E402


def setup_logging(staging_dir: str):
    log_file = os.path.join(staging_dir, "pipeline.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    sys.stdout.reconfigure(line_buffering=True)


def _build_full_gamut_item(entry: dict) -> dict:
    """Project a media_report entry into the queue-item shape the GPU/fetch
    workers consume. Pulled out of :func:`categorise_entry` so the AV1
    force-reencode path can reuse it without duplicating field plucking.

    Carries the full ``tmdb`` blob through so downstream consumers
    (``derive_grade`` for content-grade CQ, ``finalize_upload``'s
    standards-compliance verify for foreign-language audio, etc.) have
    the genres / keywords / original_language data they need.

    Pre-2026-05-08 the item dropped tmdb, which silently downgraded
    every encode's grade to ``default`` — Avengers IW (blockbuster
    target cq=25) re-encoded at the default cq=22, getting a 5%
    shrink instead of the ~25% it should have. Same root cause
    blocked verify on foreign-language films (Crouching Tiger 'chi',
    Seven Samurai 'jpn') because verify's KEEP_LANGS check needs
    original_language to know which non-English audio is legitimate.
    """
    video = entry.get("video", {}) or {}
    codec_raw = video.get("codec_raw", "")
    return {
        "filepath": entry.get("filepath", ""),
        "filename": entry.get("filename", ""),
        "file_size_bytes": entry.get("file_size_bytes", 0),
        "file_size_gb": entry.get("file_size_gb", 0),
        "duration_seconds": entry.get("duration_seconds", 0),
        "video_codec": video.get("codec", codec_raw),
        "resolution": video.get("resolution_class", ""),
        "bitrate_kbps": entry.get("overall_bitrate_kbps", 0) or 0,
        "hdr": video.get("hdr", False),
        "bit_depth": video.get("bit_depth", 8),
        "audio_streams": entry.get("audio_streams", []),
        "subtitle_streams": entry.get("subtitle_streams", []),
        "subtitle_count": entry.get("subtitle_count", 0),
        "library_type": entry.get("library_type", ""),
        "tmdb": entry.get("tmdb") or {},
    }


def _ffprobe_video_codec(filepath: str, *, timeout: int = 15) -> str | None:
    """Return the live on-disk video codec name (e.g. 'av1', 'hevc', 'h264'),
    or None on probe failure.

    Used by the codec-mismatch auto-reset rule in ``categorise_entry`` to
    verify that the report's codec field actually matches the file. The
    report rebuilds on a schedule and can be stale after a re-encode lands;
    relying on it alone caused 15 DONE-AV1 rows to be wrongly reset on
    2026-06-01 (media_report still said 'hevc' for them).

    Light call — ~50 ms per probe, only fires on DONE rows whose report
    codec disagrees with 'av1'. Small set, infrequent path, cost negligible.
    """
    import subprocess
    if not os.path.exists(filepath):
        return None
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name",
             "-of", "csv=p=0", filepath],
            capture_output=True, text=True, timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None
    return (out.stdout or "").strip().rstrip(",").lower() or None


def _stamp_force_reencode(
    state: PipelineState,
    filepath: str,
    existing: dict | None,
    *,
    reason: str,
) -> None:
    """Set ``force_reencode=true`` on the state row so the AV1-source
    guard in ``full_gamut`` lets the encode proceed.

    Preserves any existing non-terminal status (PENDING / QUEUED /
    QUALIFYING). Creates a fresh PENDING row when there isn't one.
    Called from the AV1 branches of ``categorise_entry`` when routing
    to full_gamut — without this stamp the file gets silently DONE'd by
    the guard at full_gamut.py:689. (2026-05-22 incident: 183 priority
    AV1 paths got silently DONE'd before this stamp was added.)
    """
    # Pick a sane non-terminal status. Existing rows here are guaranteed
    # non-terminal (terminal-skip ran above) so just reuse whatever it
    # had, otherwise PENDING.
    if existing and existing.get("status"):
        try:
            status_to_use = FileStatus(existing["status"])
        except ValueError:
            status_to_use = FileStatus.PENDING
    else:
        status_to_use = FileStatus.PENDING
    state.set_file(
        filepath,
        status_to_use,
        force_reencode=True,
        reason=f"force_reencode set by categorise_entry: {reason}",
    )


def categorise_entry(
    entry: dict,
    config: dict,
    state: PipelineState,
    control: PipelineControl,
    priority_paths: set[str] | None = None,
) -> tuple[str, dict | None]:
    """Decide which queue (if any) a media-report entry belongs to.

    Returns ``(category, queue_item)``:
      * ``("full_gamut", item_dict)`` — needs full re-encode
      * ``("gap_filler", entry)`` — already AV1 but needs cleanup work
      * ``("skip", None)`` — terminal state, control-skipped, or empty entry

    Side effects: marks unprobeable entries (codec_raw missing) as
    ``FLAGGED_CORRUPT`` in the state DB so they surface in the Flagged
    pane instead of silently rotting in PENDING.

    Used at startup by :func:`build_queues` AND mid-session by the
    orchestrator's refresh worker so a Sonarr/Radarr drop-in becomes
    next-up automatically without waiting for a pipeline restart.

    Force-reencode of already-AV1 files: when the user clicks "Queue for
    re-encode" on the dashboard, the requeue endpoint sets
    ``force_reencode=true`` in the row's ``extras`` JSON. Without that
    flag the AV1 branch below would route to gap_filler/skip and the
    user's queue action would be a silent no-op (the only thing it'd
    achieve is flipping status to ``pending``, which is invisible to
    the queue builder for AV1 files). The flag is cleared in
    ``full_gamut`` on a successful DONE transition; if the encode
    fails the flag stays set so the next queue-build pass picks it up
    again automatically.
    """
    from pipeline.gap_filler import analyse_gaps

    filepath = entry.get("filepath", "")
    video = entry.get("video", {})
    codec_raw = video.get("codec_raw", "")

    if not filepath:
        return ("skip", None)

    if control.should_skip(filepath):
        return ("skip", None)

    # Already terminal? Mostly skip. DONE means encoded successfully;
    # FLAGGED_* means qualify/audit deliberately parked the file.
    # Earlier versions only skipped "done", so flagged rows landed back in
    # the queue and got re-encoded with the wrong audio.
    #
    # AUTO-RESET (2026-05-17): a flagged_corrupt / flagged_foreign_audio /
    # flagged_undetermined row whose underlying file has been REFRESHED on
    # disk since the flag was applied gets resurrected as pending. The
    # canonical case: user deletes a corrupt source from NAS, Sonarr /
    # Radarr re-downloads a clean release at the same path. Without this
    # the new file sits invisible because the picker skips terminal rows
    # forever — user has to manually clear the state row. The reset is
    # gated on report.file_mtime > state.last_updated + 60s (clock-skew
    # tolerance) so a small post-encode mtime wiggle on the SAME file
    # doesn't cause a re-encode loop.
    existing = state.get_file(filepath)
    if existing and is_terminal(existing["status"]):
        st = existing["status"]
        if st in ("flagged_corrupt", "flagged_foreign_audio", "flagged_undetermined"):
            file_mtime = entry.get("file_mtime", 0) or 0
            flag_time = 0.0
            last_updated = existing.get("last_updated")
            if last_updated:
                try:
                    flag_time = datetime.fromisoformat(last_updated).timestamp()
                except (ValueError, TypeError):
                    pass
            if file_mtime > flag_time + 60:
                logging.info(
                    f"  Auto-reset {st} → pending: file refreshed on disk since flag was set "
                    f"({os.path.basename(filepath)}, "
                    f"file_mtime={file_mtime:.0f} > flag_time={flag_time:.0f})"
                )
                state.set_file(
                    filepath,
                    FileStatus.PENDING,
                    stage=None,
                    error=None,
                    reason=f"auto-reset from {st} — file refreshed on disk",
                )
                # Fall through to normal categorisation against the fresh entry.
                existing = state.get_file(filepath)
            else:
                return ("skip", None)
        elif st in ("done", "replaced"):
            # Explicit re-encode request wins over terminal-skip (2026-06-01).
            # force_reencode=true on a DONE row means the operator wants
            # this file re-done despite being complete — e.g. an AV1 file
            # that needs the colour-tag / black-level fix or a CQ change,
            # added via priority or the requeue button. WITHOUT this, a
            # done AV1 file can NEVER be re-encoded through priority: this
            # terminal-skip fires (returning skip at the bottom of the
            # branch) before the priority override / force_reencode routing
            # in the av1 block below ever runs. That's the bug behind "I
            # keep adding the 7 remaining AV1 re-encodes to priority and
            # they never get sorted" — they skipped here, then the prune
            # dropped them from priority.json within 10s.
            if existing.get("force_reencode"):
                # Fall through (do NOT return) to the codec-routing blocks
                # below, which send force_reencode AV1 (and any non-AV1) to
                # full_gamut. Note this is a bare `if` with `pass`-by-
                # omission: the subsequent codec checks are `elif`, so when
                # force_reencode is set we skip them entirely and drop out
                # of the terminal block to normal routing.
                existing = state.get_file(filepath)  # refresh, no-op but explicit
            # DONE consistency check (2026-05-24). If the state row says
            # DONE/REPLACED but the file on disk isn't AV1, the row is
            # lying. Causes seen:
            #   * priority-API auto-seed inserted PENDING rows that got
            #     transitioned to DONE without an actual encode happening
            #     (the file is still its original h264/hevc — pipeline
            #     never touched it).
            #   * Sonarr / qbittorrent / manual restore replaced our AV1
            #     output with a non-AV1 release post-encode (Crash,
            #     Toy Soldiers — mtime hours/days after done).
            #   * cq_resync sweep marked DONE on files that were never
            #     actually re-encoded.
            # In all cases: codec on disk says we have work to do. Reset
            # to pending with force_reencode so the pipeline picks it up.
            # AV1 DONE rows are left alone — they're correctly complete.
            #
            # 2026-06-01: codec_raw comes from media_report.json which is
            # rebuilt by the scanner on a schedule and can be stale. The
            # auto-reset misfired on 15 DONE rows where report said "hevc"
            # but ffprobe (truth) said "av1". To prevent this, ffprobe the
            # file before trusting the report. Cheap (~50 ms per row,
            # fires only on DONE-claimed-non-AV1 entries — small set).
            elif codec_raw and not video_is_finished(codec_raw):
                # Verify via ffprobe before resetting — report may be stale.
                live_codec = _ffprobe_video_codec(filepath)
                if live_codec and video_is_finished(live_codec):
                    # Report is stale; on-disk file IS AV1. Leave DONE alone.
                    logging.info(
                        f"  Auto-reset skipped: report says codec={codec_raw} "
                        f"but ffprobe says {live_codec} (already AV1) — "
                        f"media_report is stale, no action: "
                        f"{os.path.basename(filepath)}"
                    )
                    return ("skip", None)
                logging.info(
                    f"  Auto-reset {st} → pending: state was {st}, report "
                    f"codec={codec_raw}, ffprobe codec={live_codec or 'unknown'} — "
                    f"genuinely not AV1, re-encoding {os.path.basename(filepath)}"
                )
                state.set_file(
                    filepath,
                    FileStatus.PENDING,
                    stage=None,
                    error=None,
                    reason=f"auto-reset from {st} — ffprobe-verified codec is {live_codec or codec_raw}, not a finished (AV1/HEVC) codec",
                    force_reencode=True,
                )
                # Fall through to normal categorisation against the fresh entry.
                existing = state.get_file(filepath)
            else:
                return ("skip", None)
        else:
            # flagged_manual — never auto-reset. The user's park button
            # must require user action to clear.
            return ("skip", None)

    # Unprobeable: ffprobe couldn't determine the video codec. Earlier
    # versions silently skipped these files at queue-build time, so
    # corrupt / truncated files sat in PENDING forever, never visible to
    # the user. Flag them so the Flagged pane surfaces them.
    if not codec_raw:
        state.set_file(
            filepath,
            FileStatus.FLAGGED_CORRUPT,
            stage="scan",
            reason="ffprobe could not determine video codec",
        )
        return ("skip", None)

    if codec_raw == "av1":
        # Priority override (2026-05-22). If the operator has put this
        # filepath on control/priority.json, treat it as force_reencode
        # regardless of audit data. Operator intent is the highest
        # signal — without this override, AV1 files with no audit blob
        # (e.g. recent Sonarr drops the scanner hasn't audited yet) get
        # silently skipped even when prioritised. The bite that surfaced
        # this: 139 of 187 priority paths fell through to skip because
        # categorise_entry didn't see the priority list. The
        # priority-resort then had only ~4 items to lift to the front
        # and the GPU worker moved on to non-priority files while the
        # priority bucket was effectively empty in-queue.
        #
        # IMPORTANT: routing to full_gamut alone is not enough. full_gamut
        # has an AV1-source guard (full_gamut.py:689) that refuses to
        # re-encode AV1 unless force_reencode=true is stamped on the
        # state row. The 2026-05-22 09:15 follow-up bite: 183 priority
        # AV1 paths got routed to full_gamut, hit the guard, marked DONE
        # as "av1 source preserved (no force_reencode flag)" without any
        # actual encode. We MUST stamp force_reencode here so the guard
        # lets the encode through.
        if priority_paths and filepath in priority_paths:
            if not (existing and existing.get("force_reencode")):
                _stamp_force_reencode(state, filepath, existing,
                                      reason="priority.json membership")
            return ("full_gamut", _build_full_gamut_item(entry))

        # CQ adherence check (2026-05-21). Policy: an AV1 file whose
        # current_cq disagrees with target_cq is NOT compliant — it's
        # off-spec for the current grade rule and must be re-encoded.
        # Pre-fix, qualify gated compliance on codec + audio config
        # only, so a Bluey episode encoded at CQ 30 under the older
        # policy stayed DONE forever even though tv_animation grade
        # now targets CQ 37. Operator's stated rule: "if they're too
        # low then they're not done — that needs to be stopped."
        # Note this routes BOTH too_low (cur<tgt, higher quality than
        # target → re-encode shrinks) AND too_high (cur>tgt, lower
        # quality → re-encode improves), since the rule is parity
        # with target, not a direction.
        # Audit blob is populated by the scanner from MKV CQ tag, or
        # bitrate-inferred when no tag. inferred_uncertain rows are
        # still actioned — re-encoding will produce a confidently-
        # tagged file that becomes 'optimal' on the next audit pass.
        audit = entry.get("audit") or {}
        cur_cq = audit.get("current_cq")
        tgt_cq = audit.get("target_cq")
        if cur_cq is not None and tgt_cq is not None and abs(cur_cq - tgt_cq) > GRADE_CQ_TOLERANCE:
            # Same AV1-source-guard story as the priority branch above —
            # routing to full_gamut without force_reencode causes the
            # full_gamut.py:689 guard to mark this DONE as "av1 source
            # preserved" without encoding. Stamp the flag so the encode
            # actually runs.
            if not (existing and existing.get("force_reencode")):
                _stamp_force_reencode(state, filepath, existing,
                                      reason=f"cq off-target ({cur_cq} vs {tgt_cq})")
            return ("full_gamut", _build_full_gamut_item(entry))
        # User-initiated force re-encode wins over the codec check.
        # Without this an already-AV1 file at the wrong CQ can never be
        # re-encoded — the queue builder would route it to gap_filler
        # (audio strip / sub stamp only) or skip outright.
        if existing and existing.get("force_reencode"):
            return ("full_gamut", _build_full_gamut_item(entry))
        gaps = analyse_gaps(entry, config)
        # gap_filler explicitly does NOT do audio transcodes (see
        # pipeline/gap_filler.py:389-394 — fetch+ffmpeg+upload is heavy
        # and excluded). Pre-2026-05-12 we still routed AV1 files with
        # AC-3/DTS/etc. audio to gap_filler, which ran its other ops
        # (track strip, tags) and then marked DONE — leaving the wrong
        # audio in place. That's a Rule-1 violation (DONE-on-a-lie).
        # LotR Return of the King shipped with AC-3 5.1 + commentary
        # sub because of this. Route audio-transcode files to full_gamut
        # so the encoder actually does the transcode.
        if gaps.needs_audio_transcode:
            return ("full_gamut", _build_full_gamut_item(entry))
        if gaps.needs_anything:
            return ("gap_filler", entry)
        return ("skip", None)

    # Non-AV1 → full re-encode
    return ("full_gamut", _build_full_gamut_item(entry))


def _prune_done_from_priority(staging_dir: str | None = None,
                              state: "PipelineState | None" = None) -> int:
    """Remove DONE / flagged_* rows from ``control/priority.json -> paths``.

    Priority is "lift these to the front" — once a file's been encoded
    (or terminally flagged) it has no business on the list anymore. The
    pre-2026-05-13 behaviour was append-only: the list grew forever and
    the operator had no signal of progress through it.

    Called at every queue rebuild so the list is self-pruning. Returns
    the number of entries removed.
    """
    if staging_dir is None:
        staging_dir = str(STAGING_DIR)
    prio_path = os.path.join(staging_dir, "control", "priority.json")
    if not os.path.exists(prio_path):
        return 0
    try:
        with open(prio_path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return 0
    paths = data.get("paths") or []
    if not paths or state is None:
        return 0
    # Anything terminal — done OR any flagged_* — drops off.
    TERMINAL = {"done", "flagged_corrupt", "flagged_foreign_audio",
                "flagged_undetermined", "flagged_manual"}
    kept = []
    removed = 0
    for fp in paths:
        row = state.get_file(fp)
        if row and (row.get("status") or "").lower() in TERMINAL:
            # KEEP a terminal row the operator explicitly wants re-encoded.
            # A DONE row with force_reencode=true is an ACTIVE re-encode
            # request (priority-add / requeue of an already-done AV1 file
            # for the colour-tag / black-level / CQ fixes), not a completed
            # item. Pruning it drops the operator's intent — the exact bug
            # behind "the 7 AV1 re-encodes keep vanishing from priority".
            # The flag clears (force_reencode=False) on the next successful
            # DONE in full_gamut, so the row prunes normally once the
            # re-encode actually lands. 2026-06-01.
            if row.get("force_reencode"):
                kept.append(fp)
                continue
            removed += 1
            continue
        kept.append(fp)
    if removed > 0:
        data["paths"] = kept
        # Atomic rewrite: write to .tmp + replace, so a crash mid-write
        # doesn't leave a truncated file.
        tmp = prio_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, prio_path)
        except OSError:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass
            return 0
    return removed


def _read_priority_paths(staging_dir: str | None = None) -> set[str]:
    """Return the set of paths in ``control/priority.json -> paths``.

    Items whose filepath is in this set get bumped to the front of
    ``full_gamut_queue`` regardless of size. Used for one-off test
    runs (e.g. "encode these 30 specific files first overnight to
    verify the fix actually shrinks them"). Empty list / missing
    file = no bump (queue stays in its normal size order).

    The 2026-05-08 incident review noted the old force-stack
    mechanism was removed without a replacement. This is a lighter
    replacement: read-once-at-build-time, no run-time IPC.

    Note: this READS only — pruning of completed entries is handled
    by ``_prune_done_from_priority`` which runs alongside.
    """
    if staging_dir is None:
        staging_dir = str(STAGING_DIR)
    prio_path = os.path.join(staging_dir, "control", "priority.json")
    if not os.path.exists(prio_path):
        return set()
    try:
        with open(prio_path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return set()
    return set(data.get("paths") or [])


def _sort_full_gamut(queue: list, config: dict, priority_paths: set[str]) -> None:
    """In-place sort of the full_gamut queue.

    Order:
      1. Priority paths (per ``control/priority.json -> paths``) ALWAYS
         smallest-first within the bucket. Users prioritise small files
         to get a burst of quick wins ("150 smallest HEVC/H264"), not
         to wait through the biggest of the small set first. Pre-2026-
         05-20 the priority bucket honoured the global encode_queue_order
         which was largest_first by default — so prioritising "150
         smallest" delivered the largest of those 150 first. That's
         the opposite of intent.
      2. Everything else, sorted by size in the configured direction
         (``encode_queue_order``, default ``largest_first`` so big
         files burn down the ETA first).
    """
    order = (config.get("encode_queue_order") or "largest_first").lower()
    largest_first_default = order == "largest_first"

    def _key(item: dict) -> tuple:
        is_priority = 0 if item.get("filepath") in priority_paths else 1
        size = item.get("file_size_bytes", 0)
        # Within the priority bucket: always smallest-first (positive
        # size for natural ascending sort). Outside the bucket: honour
        # the global config.
        if is_priority == 0:
            return (is_priority, size)
        return (is_priority, -size if largest_first_default else size)

    queue.sort(key=_key)


def build_queues(report_path: str, config: dict, state: PipelineState, control: PipelineControl):
    """Build separate queues for full_gamut and gap_filler from the media report."""
    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)

    from pipeline.gap_filler import analyse_gaps

    full_gamut_queue = []
    gap_filler_queue = []

    # Read priority paths BEFORE categorisation so categorise_entry can
    # honour the operator's "encode these first" override (priority list
    # wins over the AV1-no-audit silent skip). Pre-2026-05-22 the priority
    # paths were read AFTER the categorisation loop, only for sort — which
    # meant 139 of 187 priority paths got skipped at categorise time and
    # never reached the queue to be sorted.
    pruned = _prune_done_from_priority(state=state)
    if pruned:
        logging.info(f"Priority prune: dropped {pruned} done/flagged entries from priority.json")
    priority_paths = _read_priority_paths()

    for entry in report.get("files", []):
        category, item = categorise_entry(entry, config, state, control, priority_paths=priority_paths)
        if category == "full_gamut":
            full_gamut_queue.append(item)
        elif category == "gap_filler":
            gap_filler_queue.append(item)
    _sort_full_gamut(full_gamut_queue, config, priority_paths)
    if priority_paths:
        n_prio = sum(1 for it in full_gamut_queue if it.get("filepath") in priority_paths)
        logging.info(
            f"Priority bump active: {n_prio} of {len(full_gamut_queue)} full_gamut "
            f"items lifted to the front of the queue"
        )

    # Gap filler: NAS-only work first (no fetch), then by size
    # needs_fetch is True only for audio transcode — everything else runs on NAS
    def _gap_sort_key(entry):
        gaps = analyse_gaps(entry, config)
        return (1 if gaps.needs_fetch else 0, entry.get("file_size_bytes", 0))

    gap_filler_queue.sort(key=_gap_sort_key)

    return full_gamut_queue, gap_filler_queue


def main():
    parser = argparse.ArgumentParser(description="AV1 Media Pipeline — one pass, everything done")
    parser.add_argument("--report", type=str, default=str(MEDIA_REPORT))
    parser.add_argument("--staging", type=str, default=str(STAGING_DIR))
    parser.add_argument("--state-file", type=str, default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-replace", action="store_true")
    parser.add_argument(
        "--no-gap-filler", action="store_true", help="Disable gap filler (GPU encodes only, no cleanup)"
    )
    parser.add_argument("--gap-filler-only", action="store_true", help="Run gap filler only (no GPU encodes)")
    parser.add_argument("--max-staging-gb", type=int, default=None)
    parser.add_argument("--max-fetch-gb", type=int, default=None)
    args = parser.parse_args()

    # Config
    overrides_path = os.path.join(args.staging, "control", "config_overrides.json")
    file_overrides = {}
    if os.path.exists(overrides_path):
        try:
            with open(overrides_path, encoding="utf-8") as f:
                file_overrides = json.load(f)
        except Exception:
            pass
    config = build_config(file_overrides)

    if args.no_replace:
        config["replace_original"] = False
    if args.max_staging_gb is not None:
        config["max_staging_bytes"] = args.max_staging_gb * 1024**3
    if args.max_fetch_gb is not None:
        config["max_fetch_buffer_bytes"] = args.max_fetch_gb * 1024**3

    # Staging
    os.makedirs(args.staging, exist_ok=True)
    setup_logging(args.staging)

    # ProcessRegistry reconcile at session start. Before we launch any worker
    # threads, drop entries whose PIDs are dead or recycled — ghost entries from
    # previous sessions that crashed without cleaning up would otherwise block
    # our own registration for the same role.
    from pathlib import Path

    from pipeline.process_registry import ProcessRegistry

    registry_path = Path(args.staging) / "control" / "agents.registry.json"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry = ProcessRegistry(registry_path)
    dead = registry.reconcile()
    logging.info(f"Reaped {len(dead)} dead registry entries: {dead}")

    # State
    db_path = args.state_file or os.path.join(args.staging, "pipeline_state.db")
    state = PipelineState(db_path)
    serializable_config = copy.deepcopy(config)
    if isinstance(serializable_config.get("lossless_audio_codecs"), set):
        serializable_config["lossless_audio_codecs"] = sorted(serializable_config["lossless_audio_codecs"])
    state.set_meta("config", serializable_config)
    state.save()

    # Control
    control = PipelineControl(args.staging)

    # Build queues
    if not os.path.exists(args.report):
        logging.error(f"Report not found: {args.report}")
        sys.exit(1)

    full_gamut_queue, gap_filler_queue = build_queues(args.report, config, state, control)

    logging.info(f"Full gamut: {len(full_gamut_queue)} files to encode")
    logging.info(f"Gap filler: {len(gap_filler_queue)} files to clean up")

    if args.dry_run:
        logging.info("\nDRY RUN -- full gamut queue:")
        for item in full_gamut_queue[:20]:
            codec = item.get("video_codec", "?")
            res = item.get("resolution", "?")
            logging.info(f"  {codec} {res:6s} {item['filename']}")
        if len(full_gamut_queue) > 20:
            logging.info(f"  ... and {len(full_gamut_queue) - 20} more")
        logging.info("\nDRY RUN -- gap filler queue:")
        from pipeline.gap_filler import analyse_gaps

        for entry in gap_filler_queue[:20]:
            gaps = analyse_gaps(entry, config)
            logging.info(f"  {gaps.describe():30s} {entry['filename']}")
        if len(gap_filler_queue) > 20:
            logging.info(f"  ... and {len(gap_filler_queue) - 20} more")
        return

    if not full_gamut_queue and not gap_filler_queue:
        logging.info("Nothing to process!")
        return

    # Run orchestrator under the process registry so a crashed session's
    # entry is still reaped next time (reconcile above) and live entries
    # are visible to tools like `tools/invariants.py`.
    from pipeline.orchestrator import Orchestrator

    orchestrator = Orchestrator(config, state, args.staging, control)

    with registry.register("pipeline", sys.argv):
        if args.gap_filler_only:
            orchestrator.run([], gap_filler_queue, enable_gap_filler=True)
        else:
            orchestrator.run(full_gamut_queue, gap_filler_queue, enable_gap_filler=not args.no_gap_filler)


if __name__ == "__main__":
    main()
