"""Gap Filler: CPU-only cleanup for already-AV1 files.

Intelligently chooses the cheapest method:
- mkvmerge on NAS for track removal (no fetch, ~30s)
- mkvpropedit on NAS for metadata (no fetch, ~1s)
- os.rename on NAS for filename cleaning (no fetch, instant)
- Fetch + ffmpeg for audio codec change (~5 min)

Most operations need NO FETCH — they work directly on the NAS file.
"""

import logging
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from typing import Optional

from pipeline.config import KEEP_LANGS
from pipeline.ffmpeg import (
    _should_transcode_audio,
    format_bytes,
)
from pipeline.gap_fill_lock import GapFillLockTimeout, gap_fill_lock
from pipeline.report import update_entry
from pipeline.state import FileStatus, PipelineState
from pipeline.streams import parse_sub_stream
from pipeline.subs import pick_english_sidecars, scan_sidecars

_MKVMERGE_SEARCH = [
    r"C:\Program Files\MKVToolNix\mkvmerge.exe",
    r"C:\Program Files (x86)\MKVToolNix\mkvmerge.exe",
]

_MKVPROPEDIT_SEARCH = [
    r"C:\Program Files\MKVToolNix\mkvpropedit.exe",
    r"C:\Program Files (x86)\MKVToolNix\mkvpropedit.exe",
]


def _find_tool(name: str, search_paths: list[str]) -> Optional[str]:
    found = shutil.which(name)
    if found:
        return found
    for path in search_paths:
        if os.path.isfile(path):
            return path
    return None


@dataclass
class GapAnalysis:
    """What a file needs to be fully 'done'."""

    needs_track_removal: bool = False
    needs_audio_transcode: bool = False
    needs_metadata: bool = False
    needs_filename_clean: bool = False
    needs_language_detect: bool = False
    needs_sub_mux: bool = False
    needs_foreign_sub_cleanup: bool = False
    audio_keep_indices: list[int] = field(default_factory=list)
    sub_keep_indices: list[int] = field(default_factory=list)
    audio_transcode_indices: list[int] = field(default_factory=list)
    external_subs: list[str] = field(default_factory=list)
    foreign_external_subs: list[str] = field(default_factory=list)
    clean_name: Optional[str] = None

    @property
    def needs_fetch(self) -> bool:
        """Only audio codec transcoding requires fetching to local."""
        return self.needs_audio_transcode

    @property
    def needs_anything(self) -> bool:
        return (
            self.needs_track_removal
            or self.needs_audio_transcode
            or self.needs_metadata
            or self.needs_filename_clean
            or self.needs_language_detect
            or self.needs_sub_mux
            or self.needs_foreign_sub_cleanup
        )

    def describe(self) -> str:
        parts = []
        if self.needs_track_removal:
            parts.append("strip tracks")
        if self.needs_sub_mux:
            parts.append(f"mux {len(self.external_subs)} subs")
        if self.needs_foreign_sub_cleanup:
            parts.append(f"delete {len(self.foreign_external_subs)} foreign subs")
        if self.needs_audio_transcode:
            parts.append("transcode audio")
        if self.needs_metadata:
            parts.append("write metadata")
        if self.needs_filename_clean:
            parts.append("clean filename")
        if self.needs_language_detect:
            parts.append("detect languages")
        return " + ".join(parts) if parts else "nothing"


def _scan_external_subs(filepath: str, gaps: GapAnalysis) -> None:
    """Scan for external subtitle files next to the MKV. Called lazily during gap_fill.

    Idempotent: sets ``gaps._external_scan_done = True`` after the first run and
    short-circuits on subsequent calls. The orchestrator's heavy_worker AND
    gap_fill() both call this function — without the guard, the second call
    appended another sidecar to ``gaps.external_subs``, producing an output
    MKV with 2 English subtitle tracks and failing "exactly 1 English"
    compliance (root cause of subs % trending DOWN after strip was enabled).

    Delegates to :mod:`pipeline.subs` for sidecar enumeration and the
    "pick one English, delete the rest" split. See that module for the
    documented semantic tightenings vs. the old inline version (stricter stem
    match, richer language parser, HI detection that also catches ``cc``).
    """
    if getattr(gaps, "_external_scan_done", False):
        return

    sidecars = scan_sidecars(filepath)
    to_mux, to_delete = pick_english_sidecars(sidecars)
    gaps.external_subs.extend(s.path for s in to_mux)
    gaps.foreign_external_subs.extend(s.path for s in to_delete)
    if gaps.external_subs:
        gaps.needs_sub_mux = True
        gaps.needs_track_removal = True
    if gaps.foreign_external_subs:
        gaps.needs_foreign_sub_cleanup = True

    gaps._external_scan_done = True


def analyse_gaps(file_entry: dict, config: dict) -> GapAnalysis:
    """Analyse what an already-AV1 file needs to be fully done."""
    gaps = GapAnalysis()
    # Attach config so downstream workers (e.g. _strip_tracks_locally) can
    # consult thresholds without changing every call signature.
    gaps._config = config  # type: ignore[attr-defined]
    audio_streams = file_entry.get("audio_streams", [])
    sub_streams = file_entry.get("subtitle_streams", [])

    # Audio codec check
    for i, a in enumerate(audio_streams):
        codec = (a.get("codec_raw") or a.get("codec", "")).lower()
        if codec in ("eac3", "e-ac-3"):
            gaps.audio_keep_indices.append(i)
        else:
            if _should_transcode_audio(a, config):
                gaps.needs_audio_transcode = True
                gaps.audio_transcode_indices.append(i)
            gaps.audio_keep_indices.append(i)

    # Foreign-audio strip planning. Mirrors ffmpeg._select_audio_streams so
    # the gap_filler path produces the same result as the full encode path.
    # GATED by config["strip_non_english_audio"]. When stripping is on HOLD
    # we don't plan any audio track removal — the file is still fine for
    # every other gap-fill action (metadata, filename clean, sub mux).
    if config.get("strip_non_english_audio", True) and len(audio_streams) > 1:
        policy = config.get("audio_keep_policy", "original_language")
        clean_audio_keep: list[int] | None = None

        if policy == "original_language":
            from pipeline.streams import (
                parse_audio_stream,
                select_audio_keep_indices_by_original_language,
            )

            tmdb = file_entry.get("tmdb") or {}
            original_language = (tmdb.get("original_language") or "").strip().lower() or None
            if original_language:
                parsed = [parse_audio_stream(a, i) for i, a in enumerate(audio_streams)]
                kept = select_audio_keep_indices_by_original_language(
                    parsed,
                    original_language,
                    keep_english_too=bool(config.get("audio_keep_english_with_original", False)),
                )
                if kept is not None:
                    clean_audio_keep = kept

        if clean_audio_keep is None:
            # Legacy "english_und" policy or no-TMDb fallback.
            clean_audio_keep = [0]  # always keep stream 0
            for i, a in enumerate(audio_streams):
                if i == 0:
                    continue
                lang = (a.get("language") or a.get("detected_language") or "und").lower().strip()
                if lang in KEEP_LANGS:
                    clean_audio_keep.append(i)

        if len(clean_audio_keep) < len(audio_streams):
            gaps.needs_track_removal = True
            gaps.audio_keep_indices = clean_audio_keep

    # Subtitle selection delegates to pipeline.streams.select_sub_keep_indices
    # so gap_filler stays in lock-step with the encode-time policy:
    #   - always keep forced/foreign-parts subs
    #   - keep ONE English (prefer non-HI; fall back to HI if that's all we have)
    #   - strip everything else
    #
    # The selector now returns None when ANY sub track has unresolved language
    # (inviolate rule 2026-04-29 — never strip what we can't identify). In that
    # case we DON'T set needs_track_removal; instead the file is flagged for
    # language detection below and stays untouched until whisper resolves it.
    #
    # GATED by config["strip_non_english_subs"]. When stripping is on HOLD
    # we keep every existing sub. `sub_keep_indices` stays empty; the
    # `_strip_tracks_on_nas` None path (``sub_keep_ids=None``) then signals
    # "keep all" to mkvmerge.
    if config.get("strip_non_english_subs", True):
        from pipeline.streams import select_sub_keep_indices

        parsed_subs = [parse_sub_stream(raw, index=i) for i, raw in enumerate(sub_streams)]
        sub_keep = select_sub_keep_indices(parsed_subs)

        if sub_keep is not None:
            # Either nothing to strip (keep == streams) OR the keep-set is the
            # plan. Record the plan in either case so callers can introspect.
            gaps.sub_keep_indices = sub_keep
            if len(sub_keep) < len(sub_streams):
                gaps.needs_track_removal = True
        # else: sub_keep is None — file has unresolved sub languages. Defer
        # entirely. needs_track_removal stays False; gaps.sub_keep_indices
        # stays empty (caller's None-path through _strip_tracks_on_nas treats
        # empty as "keep all" if needs_track_removal isn't set).

    # TMDb metadata check
    if not file_entry.get("tmdb"):
        gaps.needs_metadata = True

    # Filename check
    try:
        from pipeline.filename import clean_filename

        clean = clean_filename(file_entry["filepath"], file_entry.get("library_type", ""))
        if clean and clean != file_entry["filename"]:
            gaps.needs_filename_clean = True
            gaps.clean_name = clean
    except (ImportError, Exception):
        pass

    # Undetermined language check
    for a in audio_streams:
        lang = (a.get("language") or "und").lower().strip()
        if lang in ("und", "unk", "") and not a.get("detected_language"):
            gaps.needs_language_detect = True
            break
    for s in sub_streams:
        lang = (s.get("language") or "und").lower().strip()
        if lang in ("und", "unk", "") and not s.get("detected_language"):
            gaps.needs_language_detect = True
            break

    # External subtitle check — deferred to gap_fill() to avoid slow NAS scans
    # during queue building. Set a flag so gap_fill knows to check.
    #
    # DO NOT auto-set needs_sub_mux from file_entry.external_subtitles here:
    # the strip+mux path uses audio_keep_indices, which is ONLY populated for
    # already-EAC-3 audio tracks. If we flag sub_mux on files that also need
    # audio transcode, mkvmerge strips all audio (destructive — 256 files lost
    # 2026-04-22 when this was patched in incorrectly).
    gaps._check_external_subs = True
    if gaps.external_subs:
        gaps.needs_sub_mux = True

    return gaps


def gap_fill(
    filepath: str,
    file_entry: dict,
    gaps: GapAnalysis,
    config: dict,
    state: PipelineState,
) -> bool:
    """Fill gaps for an already-AV1 file. Returns True on success.

    Intelligently chooses the cheapest method per operation.
    Most operations work directly on the NAS — no fetch needed.
    """
    filename = file_entry["filename"]
    library_type = file_entry.get("library_type", "")

    # Short-circuit if the state already says DONE for this filepath. The gap_filler queue is
    # built from media_report which can lag reality — a file we already re-encoded can end up
    # back in the queue with stale track info, and we don't want to overwrite its DONE state
    # with an ERROR just because the analysis was based on outdated metadata.
    existing = state.get_file(filepath)
    if existing and (existing.get("status") or "").lower() == FileStatus.DONE.value:
        logging.info(
            f"  Skipping {filename}: already DONE in state "
            f"(media_report analysis likely stale)."
        )
        return True

    # Verify file still exists (may have been renamed by another worker)
    if not os.path.exists(filepath):
        # Try the clean name in the same directory
        if gaps.clean_name:
            alt_path = os.path.join(os.path.dirname(filepath), gaps.clean_name)
            if os.path.exists(alt_path):
                filepath = alt_path
                filename = gaps.clean_name
                gaps.needs_filename_clean = False  # already clean
            else:
                logging.warning(f"  File not found (renamed?): {filename}")
                return True  # not an error, just already handled
        else:
            logging.warning(f"  File not found: {filename}")
            return True

    # Deferred external sub check (avoids slow NAS scans during queue building)
    if getattr(gaps, "_check_external_subs", False):
        _scan_external_subs(filepath, gaps)

    if not gaps.needs_anything:
        # Re-probe before marking DONE. The cached gap analysis was built from
        # media_report which can lag reality — an external sub may have been dropped in
        # since, or the audio count may have changed on a re-encode/merge by another
        # worker. If a fresh probe disagrees with the cached analysis we re-run the
        # gap_fill with the new entry instead of committing a stale DONE.
        try:
            from pipeline.report import build_file_entry, probe_file

            probe_data = probe_file(filepath)
            if probe_data:
                fresh_entry = build_file_entry(filepath, probe_data, library_type)
                # Preserve tmdb/detection data from the cached entry so we don't
                # falsely mark needs_metadata / needs_language_detect after re-probe.
                for k in ("tmdb",):
                    if file_entry.get(k) and not fresh_entry.get(k):
                        fresh_entry[k] = file_entry[k]
                for stream_key in ("audio_streams", "subtitle_streams"):
                    old_streams = file_entry.get(stream_key) or []
                    new_streams = fresh_entry.get(stream_key) or []
                    for j, s in enumerate(new_streams):
                        if j < len(old_streams):
                            for field in (
                                "detected_language",
                                "detection_confidence",
                                "detection_method",
                                "whisper_attempted",
                            ):
                                if old_streams[j].get(field) and not s.get(field):
                                    s[field] = old_streams[j][field]
                fresh_gaps = analyse_gaps(fresh_entry, config)
                _scan_external_subs(filepath, fresh_gaps)
                if fresh_gaps.needs_anything:
                    logging.info(
                        f"  Re-probe disagrees with cached analysis for {filename}: "
                        f"{fresh_gaps.describe()} — re-running gap_fill with fresh entry."
                    )
                    return gap_fill(filepath, fresh_entry, fresh_gaps, config, state)
        except Exception as e:
            # Re-probe is a best-effort sanity check; a failure here shouldn't block
            # the DONE marking that the caller already decided was warranted.
            logging.debug(f"  Re-probe before DONE short-circuit failed (non-fatal): {e}")

        state.set_file(filepath, FileStatus.DONE, mode="gap_filler", reason="nothing to do")
        return True

    logging.info(f"Gap fill: {filename} ({gaps.describe()})")
    state.set_file(filepath, FileStatus.PROCESSING, stage="gap_fill", mode="gap_filler")

    try:
        # Language detection first (informs track selection)
        if gaps.needs_language_detect:
            try:
                from pipeline.language import detect_all_languages

                enriched = detect_all_languages(file_entry, use_whisper=False)
                if enriched:
                    file_entry.update(enriched)
                    # Re-analyse with detected languages
                    gaps = analyse_gaps(file_entry, config)
                    logging.info(f"  Languages detected, re-analysed: {gaps.describe()}")
            except Exception as e:
                logging.warning(f"  Language detection failed: {e}")

        # Track removal and/or sub muxing (remote mkvmerge — no SMB transfer).
        # If remote SSH isn't configured or the strip fails, we DON'T error the whole file —
        # we skip the strip and continue with local ops (filename clean, metadata, foreign
        # sub delete). Those are genuine wins that don't require SSH. Only the file that
        # needed strip stays partially untouched, not the rest of the queue.
        #
        # Audio transcode is NOT a gap_filler responsibility — those files
        # require fetch+ffmpeg+upload (SMB-bound, slow, bandwidth-heavy) and
        # the user has explicitly excluded them from autonomous gap-fill.
        # If the file ALSO needs audio transcode, we still do the track strip
        # (cheap, remote) and leave the audio for the user to handle via
        # full_gamut on demand.
        track_strip_deferred = False
        if gaps.needs_track_removal or gaps.needs_sub_mux:
            machine = getattr(gaps, "_remote_machine", None)
            strip_ok = False
            try:
                strip_ok = _strip_tracks(filepath, gaps, config, machine=machine)
            except RuntimeError as e:
                logging.warning(f"  Track strip deferred (config): {e}")
                track_strip_deferred = True
            except Exception as e:
                logging.warning(f"  Track strip failed ({e}) — continuing with local ops")
                track_strip_deferred = True

            if strip_ok:
                # Delete external sub files after successful mux
                if gaps.needs_sub_mux:
                    for sub_path in gaps.external_subs:
                        try:
                            os.remove(sub_path)
                            logging.info(f"  Muxed and removed: {os.path.basename(sub_path)}")
                        except OSError:
                            pass
            elif not track_strip_deferred:
                # Attempted + failed for a real reason (not config): mark that specifically.
                track_strip_deferred = True
                logging.warning("  Track strip failed — continuing with local ops")

        # Delete foreign external subs (not muxed, just cleaned up)
        if gaps.needs_foreign_sub_cleanup:
            for sub_path in gaps.foreign_external_subs:
                try:
                    os.remove(sub_path)
                    logging.info(f"  Deleted foreign sub: {os.path.basename(sub_path)}")
                except OSError:
                    pass

        # Filename clean (os.rename on NAS)
        if gaps.needs_filename_clean and gaps.clean_name:
            new_path = _rename_file(filepath, gaps.clean_name)
            if new_path:
                filepath = new_path  # update path for subsequent operations

        # TMDb metadata (mkvpropedit on NAS)
        if gaps.needs_metadata:
            try:
                from pipeline.metadata import enrich_and_tag

                tmdb_data = enrich_and_tag(filepath, os.path.basename(filepath), library_type)
                if tmdb_data:
                    logging.info("  TMDb: written")
            except (ImportError, Exception) as e:
                logging.debug(f"  TMDb skipped: {e}")

        # Update media report
        try:
            update_entry(filepath, library_type)
        except Exception as e:
            logging.warning(f"  Report update failed: {e}")

        # If track strip was deferred (SSH unavailable / rc=137 / exception), the file is
        # NOT done — its audio/sub tracks still need stripping. Marking DONE here lost 65
        # files overnight 2026-04-23 because the queue builder skips DONE rows. The correct
        # status is ERROR so the next queue build retries, while also surfacing the problem
        # to the user via the Errors page.
        if track_strip_deferred:
            state.set_file(
                filepath,
                FileStatus.ERROR,
                mode="gap_filler",
                stage="track_strip",
                error="track strip failed (ssh unavailable or remote failure)",
            )
            logging.warning(
                f"  ERROR (track strip failed): {filename} "
                f"— local ops applied, but track strip still owed. Will retry on next queue build."
            )
        else:
            state.set_file(filepath, FileStatus.DONE, mode="gap_filler")
            logging.info(f"  DONE: Gap filled: {filename}")
        state.stats["gap_filled"] = state.stats.get("gap_filled", 0) + 1
        state.save()
        return True

    except Exception as e:
        logging.error(f"Gap fill failed for {filename}: {e}")
        state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="gap_fill")
        return False


def _strip_tracks(
    filepath: str,
    gaps: GapAnalysis,
    config: dict,
    machine: dict | None = None,
) -> bool:
    """Dispatch the track strip + sub mux to the configured backend.

    ``config["gap_filler_mux_backend"]`` selects between:

      * ``"local"`` (default 2026-04-29) — runs mkvmerge.exe on this machine
        against UNC paths. Slower (SMB-bound, ~2-3 min per file) but doesn't
        load the NAS CPU. Single concurrent worker.
      * ``"remote"`` — SSHes to NAS and runs mkvmerge inside the mkvworker
        Docker container. Fast (~10s per file) but stresses Synology I/O
        — concurrent SSH+Docker+mkvmerge has triggered OOM-kill cascades.

    Returns True on success. False on a real failure (mkvmerge nonzero exit,
    verify failed, replace failed). Raises RuntimeError when the chosen
    backend is unavailable for a config-level reason (no SSH host, no local
    mkvmerge.exe) so the caller logs a specific deferral message.
    """
    backend = (config.get("gap_filler_mux_backend") or "local").lower()
    if backend == "remote":
        return _strip_tracks_on_nas(filepath, gaps, machine=machine)
    if backend == "local":
        return _strip_tracks_locally(filepath, gaps)
    raise RuntimeError(
        f"unknown gap_filler_mux_backend: {backend!r} (expected 'local' or 'remote')"
    )


def _post_mux_verify_and_replace(filepath: str, tmp_unc: str, gaps: GapAnalysis, src_size: int, label: str) -> bool:
    """Shared verify + atomic replace for both local and remote mux paths.

    Both backends write their output to ``filepath + ".gapfill_tmp.mkv"``.
    This function: confirms the tmp exists, sanity-checks size, ffprobes
    for stream counts, then atomically replaces the original. Cleans up
    the tmp on any failure.

    Returns True on success, False on any verify failure.
    """
    # Confirm tmp file exists. SMB cache can lag NFS writes by up to a few
    # seconds when the remote backend wrote it.
    for _ in range(5):
        if os.path.exists(tmp_unc):
            break
        time.sleep(1)
    if not os.path.exists(tmp_unc):
        logging.error(f"  Output file not found after mkvmerge ({label})")
        return False

    try:
        dst_size = os.path.getsize(tmp_unc)
        # Size sanity. Floor at 10% — large bitmap-sub strips can legitimately
        # drop substantial bytes.
        if dst_size < src_size * 0.1:
            logging.error(f"  Output too small ({format_bytes(dst_size)} vs {format_bytes(src_size)})")
            os.remove(tmp_unc)
            return False

        # Strict ffprobe check. Catches the empty --audio-tracks → zero-audio
        # output bug from 2026-04-22.
        expected_audio = len(gaps.audio_keep_indices) if gaps.audio_keep_indices else 1
        try:
            probe_cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", tmp_unc]
            pr = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=60)
            if pr.returncode != 0:
                logging.error(f"  Post-mkvmerge ffprobe failed (rc={pr.returncode})")
                os.remove(tmp_unc)
                return False
            import json as _json
            probed = _json.loads(pr.stdout or "{}")
            streams = probed.get("streams", []) or []
            audio_n = sum(1 for s in streams if s.get("codec_type") == "audio")
            video_n = sum(1 for s in streams if s.get("codec_type") == "video")
            if video_n < 1:
                logging.error("  Post-mkvmerge verify: 0 video streams — aborting replace")
                os.remove(tmp_unc)
                return False
            if audio_n < expected_audio:
                logging.error(
                    f"  Post-mkvmerge verify: expected {expected_audio} audio stream(s), "
                    f"got {audio_n} — aborting replace (would lose "
                    f"{expected_audio - audio_n} audio track(s))"
                )
                os.remove(tmp_unc)
                return False
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as ve:
            logging.error(f"  Post-mkvmerge verify errored: {ve} — aborting replace to be safe")
            try:
                os.remove(tmp_unc)
            except OSError:
                pass
            return False

        os.replace(tmp_unc, filepath)
        saved = src_size - dst_size
        logging.info(
            f"  Stripped ({label}): {format_bytes(src_size)} -> {format_bytes(dst_size)} "
            f"({format_bytes(abs(saved))} {'saved' if saved > 0 else 'added'})  "
            f"[verify: {video_n}v {audio_n}a]"
        )
        return True
    except Exception as e:
        logging.error(f"  Replace failed: {e}")
        return False
    finally:
        if os.path.exists(tmp_unc):
            try:
                os.remove(tmp_unc)
            except OSError:
                pass


def _build_keep_ids_from_identify(id_data: dict, gaps: GapAnalysis) -> tuple[list[int] | None, list[int] | None, bool]:
    """Translate the relative gap_filler indices into absolute mkvmerge track IDs.

    Shared between local + remote backends — same translation either way.
    Returns ``(audio_keep_ids, sub_keep_ids, no_subs)``.
    """
    audio_track_ids = [t["id"] for t in id_data.get("tracks", []) if t["type"] == "audio"]
    sub_track_ids = [t["id"] for t in id_data.get("tracks", []) if t["type"] == "subtitles"]

    audio_keep_ids = None
    if gaps.audio_keep_indices and audio_track_ids:
        audio_keep_ids = [audio_track_ids[i] for i in gaps.audio_keep_indices if i < len(audio_track_ids)]

    sub_keep_ids = None
    no_subs = False

    if gaps.external_subs and sub_track_ids:
        # Muxing external subs — strip ALL internal English, keep only forced
        forced_keep = []
        for track in id_data.get("tracks", []):
            if track["type"] == "subtitles":
                props = track.get("properties", {})
                name = (props.get("track_name") or "").lower()
                if "forced" in name or "foreign" in name or props.get("forced_track"):
                    forced_keep.append(track["id"])
        if forced_keep:
            sub_keep_ids = forced_keep
        else:
            no_subs = True
    elif gaps.sub_keep_indices and sub_track_ids:
        sub_keep_ids = [sub_track_ids[i] for i in gaps.sub_keep_indices if i < len(sub_track_ids)]
    elif not gaps.sub_keep_indices and gaps.needs_track_removal:
        no_subs = True

    return audio_keep_ids, sub_keep_ids, no_subs


def _strip_tracks_locally(filepath: str, gaps: GapAnalysis) -> bool:
    """Run mkvmerge.exe locally against UNC paths. SMB does the I/O.

    Slower than the remote-Docker path (~2-3 min per file at SMB speeds vs
    ~10s on the NAS), but avoids loading the Synology CPU + SSH/Docker
    overhead. Single-worker — concurrent UNC writes to the NAS would
    saturate SMB and add the load risk we're avoiding.
    """
    from pipeline import local_mux

    if not local_mux.is_available():
        raise RuntimeError(
            "local mkvmerge.exe not found — install MKVToolNix or set "
            "gap_filler_mux_backend=remote"
        )

    tmp_unc = filepath + ".gapfill_tmp.mkv"

    # Identify tracks via local mkvmerge
    id_data = local_mux.local_identify(filepath)
    if not id_data:
        logging.error("  Local identify failed")
        return False

    audio_keep_ids, sub_keep_ids, no_subs = _build_keep_ids_from_identify(id_data, gaps)

    # External sidecars — verify each still exists; skip stale.
    external_sub_args = None
    if gaps.external_subs:
        external_sub_args = []
        for sub_path in gaps.external_subs:
            if not os.path.exists(sub_path):
                logging.warning(f"  skip stale sidecar (not on NAS): {os.path.basename(sub_path)}")
                continue
            external_sub_args.append((sub_path, "eng"))

    try:
        src_size = os.path.getsize(filepath)
    except OSError:
        src_size = 1024 * 1024 * 1024
    timeout = max(900, int(src_size / (1024 * 1024)) * 3)

    # Big-file optimisation: stage input to local SSD before mkvmerge.
    #
    # 2026-05-01 finding: mkvmerge running with both INPUT and OUTPUT on UNC
    # paths runs at ~520 KB/s on big files (House of the Dragon S01E06,
    # 9.17 GB → 5+ hour ETA). The slow path is the random/seeky read pattern
    # mkvmerge does against an SMB source — when the encoder's fetch/upload
    # workers are also using SMB, mkvmerge gets pushed down to a tiny share
    # of the bandwidth.
    #
    # Sequential bulk-copy to local SSD is dramatically faster (3-minute
    # 9 GB copy at gigabit vs hours of random SMB reads). After the copy,
    # mkvmerge runs against local SSD I/O — no contention, no random-read
    # penalty — and only the OUTPUT goes back over SMB.
    #
    # Threshold default 2 GB: below that, the in-place UNC path is fast
    # enough that the staging overhead isn't worth it.
    from paths import STAGING_DIR

    config = getattr(gaps, "_config", None) or {}
    threshold = int(config.get("gap_filler_local_stage_threshold_bytes", 2 * 1024**3))

    staged_input: Optional[str] = None
    mkv_input = filepath
    if src_size >= threshold:
        gap_stage_dir = os.path.join(str(STAGING_DIR), "gap_stage")
        os.makedirs(gap_stage_dir, exist_ok=True)
        # Disambiguating prefix in case two passes ever collide on the same name
        staged_name = f"{int(time.time())}_{os.path.basename(filepath)}"
        staged_input = os.path.join(gap_stage_dir, staged_name)
        try:
            copy_t0 = time.monotonic()
            logging.info(
                f"  Staging {src_size/1024**3:.1f} GB to local SSD "
                f"(file > {threshold/1024**3:.1f} GB threshold)"
            )
            shutil.copy2(filepath, staged_input)
            elapsed = time.monotonic() - copy_t0
            logging.info(
                f"  Staged in {elapsed:.0f}s "
                f"({src_size / max(elapsed, 0.001) / 1024**2:.0f} MB/s)"
            )
            mkv_input = staged_input
        except OSError as e:
            logging.warning(
                f"  Local staging failed ({e!r}); falling back to UNC-in-place "
                f"(may be slow under SMB contention)"
            )
            if staged_input and os.path.exists(staged_input):
                try:
                    os.remove(staged_input)
                except OSError:
                    pass
            staged_input = None
            mkv_input = filepath

    # Single-flight lock — same rationale as remote (don't pile up concurrent
    # SMB writes to the NAS from multiple workers).
    shutdown_event = getattr(gaps, "_shutdown_event", None)
    try:
        with gap_fill_lock(role="gap_filler", timeout=600.0, shutdown=shutdown_event):
            result = local_mux.local_strip_and_mux(
                mkv_input,
                tmp_unc,
                audio_keep_ids=audio_keep_ids,
                sub_keep_ids=sub_keep_ids,
                no_subs=no_subs,
                external_sub_paths=external_sub_args,
                timeout=timeout,
            )
    except GapFillLockTimeout as e:
        logging.error(f"  gap_fill_lock timed out / aborted: {e}")
        if staged_input and os.path.exists(staged_input):
            try:
                os.remove(staged_input)
            except OSError:
                pass
        return False

    # Always remove the staged copy whether mkvmerge succeeded or not — the
    # tmp_unc output (if any) is on the NAS, the local staging was just a
    # speed optimisation. Leaving it would waste tens of GB on the SSD.
    if staged_input and os.path.exists(staged_input):
        try:
            os.remove(staged_input)
        except OSError as e:
            logging.warning(f"  Failed to remove staged input {staged_input}: {e!r}")

    if result.returncode >= 2:
        err_lines = [
            ln for ln in ((result.stderr or "") + "\n" + (result.stdout or "")).splitlines()
            if ln.strip() and not ln.lstrip().lower().startswith("mkvmerge v")
        ]
        err = "\n".join(err_lines).strip() or "(no diagnostic output)"
        logging.error(f"  mkvmerge failed (LOCAL) rc={result.returncode}: {err[:1000]}")
        return False

    return _post_mux_verify_and_replace(filepath, tmp_unc, gaps, src_size, "LOCAL")


def _strip_tracks_on_nas(filepath: str, gaps: GapAnalysis, machine: dict | None = None) -> bool:
    """Remove foreign audio/subtitle tracks via remote mkvmerge on NAS or media server.

    Runs mkvmerge inside a Docker container on the target machine via SSH.
    No SMB transfer — direct local/NFS disk I/O. 100x faster.
    """
    from pipeline.nas_worker import (
        SERVER,
        remote_identify,
        remote_strip_and_mux,
        unc_to_container_path,
    )

    if machine is None:
        machine = SERVER  # default to media server (fastest Docker)

    # Skip remote operations entirely if SSH host isn't configured — the SSH call will just
    # error out with "connect to host  port 22" otherwise, leaving an unhelpful log.
    if not machine.get("host"):
        logging.error(
            f"  Remote {machine['label']} SSH host not configured "
            f"(set {machine['label']}_SSH_HOST env var) — skipping track strip."
        )
        # Raise so the caller records a specific error message rather than the generic
        # "track strip failed" — the user needs to know it's a config issue, not a code bug.
        raise RuntimeError(f"ssh host {machine['label']}_SSH_HOST not configured")

    container_path = unc_to_container_path(filepath)
    tmp_path = container_path + ".gapfill_tmp.mkv"

    # Get track IDs from remote mkvmerge --identify
    id_data = remote_identify(machine, container_path)
    if not id_data:
        logging.error(f"  Remote identify failed on {machine['label']}")
        return False

    audio_keep_ids, sub_keep_ids, no_subs = _build_keep_ids_from_identify(id_data, gaps)

    # External subs — convert paths to container-internal form (only difference
    # from the local path; otherwise the keep-id translation is identical).
    external_sub_args = None
    if gaps.external_subs:
        external_sub_args = []
        for sub_path in gaps.external_subs:
            if not os.path.exists(sub_path):
                logging.warning(f"  skip stale sidecar (not on NAS): {os.path.basename(sub_path)}")
                continue
            container_sub = unc_to_container_path(sub_path)
            external_sub_args.append((container_sub, "eng"))

    # Calculate timeout based on file size
    try:
        src_size = os.path.getsize(filepath)
    except OSError:
        src_size = 1024 * 1024 * 1024  # assume 1GB
    timeout = max(300, int(src_size / (1024 * 1024)))

    # Run remote mkvmerge under the cross-process gap-fill lock so only ONE
    # tool is driving SSH+Docker+mkvmerge against the NAS at a time. Two
    # concurrent holders saturate Synology disk I/O, which causes mkvmerge
    # to rc=137 (SIGKILL from kernel OOM-pressure guard). Pattern observed
    # 10+ times on 2026-04-23.
    #
    # The orchestrator attaches its shutdown event to ``gaps._shutdown_event``
    # so Ctrl-C aborts the waiter cleanly; if it's absent we fall back to
    # a plain timeout-only wait.
    shutdown_event = getattr(gaps, "_shutdown_event", None)
    try:
        with gap_fill_lock(
            role="gap_filler", timeout=600.0, shutdown=shutdown_event
        ):
            result = remote_strip_and_mux(
                machine,
                container_path,
                tmp_path,
                audio_keep_ids=audio_keep_ids,
                sub_keep_ids=sub_keep_ids,
                no_subs=no_subs,
                external_sub_paths=external_sub_args,
                timeout=timeout,
            )
    except GapFillLockTimeout as e:
        logging.error(f"  gap_fill_lock timed out / aborted: {e}")
        return False

    if result.returncode >= 2:
        # Filter cosmetic OpenSSH post-quantum warning block ("** ..." lines) and
        # mkvmerge's own banner so the real error is visible.
        combined = (result.stderr or "") + "\n" + (result.stdout or "")
        err_lines = [
            ln for ln in combined.splitlines()
            if not ln.lstrip().startswith("**")
            and not ln.lstrip().lower().startswith("mkvmerge v")
            and ln.strip()
        ]
        err = "\n".join(err_lines).strip() or "(no diagnostic output)"
        logging.error(f"  mkvmerge failed ({machine['label']}) rc={result.returncode}: {err[:1000]}")
        return False

    tmp_unc = filepath + ".gapfill_tmp.mkv"
    return _post_mux_verify_and_replace(filepath, tmp_unc, gaps, src_size, machine.get("label", "?"))


# NOTE: gap_filler does NOT transcode audio. Files with non-EAC-3 audio
# (DTS / FLAC / AAC / MP3 / Opus / etc.) get flagged via
# ``GapAnalysis.needs_audio_transcode`` for diagnostic purposes only —
# acting on it would require fetching the entire file over SMB, running
# ffmpeg locally, and uploading back, which is bandwidth-heavy and not
# something the user wants happening autonomously. The encode pipeline
# (full_gamut) handles audio transcode at encode time; gap_filler stays
# in its lane: track strip + sub mux only. Removed 2026-04-29 to align
# with that boundary.


def _rename_file(filepath: str, clean_name: str) -> Optional[str]:
    """Rename a file on the NAS to a cleaned name. Returns new path or None.

    Special case: if the source ends in `.gapfill_tmp.mkv` and the destination
    already exists, the tmp is a stale leftover from a gap-fill that got
    interrupted between the mkvmerge mux and the final rename. Delete the
    stale tmp so the next scan doesn't pick it up again.
    """
    source_dir = os.path.dirname(filepath)
    new_path = os.path.join(source_dir, clean_name)

    if new_path == filepath:
        return None

    is_stale_tmp = filepath.endswith(".gapfill_tmp.mkv")
    if is_stale_tmp and os.path.exists(new_path):
        try:
            os.remove(filepath)
            logging.info(
                f"  Removed stale gapfill tmp (clean name already present): "
                f"{os.path.basename(filepath)}"
            )
            return new_path  # caller treats this as success — clean file is the real one
        except OSError as e:
            logging.warning(f"  Could not remove stale gapfill tmp: {e}")
            return None

    try:
        os.rename(filepath, new_path)
        logging.info(f"  Renamed: {os.path.basename(filepath)} -> {clean_name}")
        return new_path
    except Exception as e:
        logging.warning(f"  Rename failed: {e}")
        return None
