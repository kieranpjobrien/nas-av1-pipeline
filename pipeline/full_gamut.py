"""Full Gamut: one function, one file, everything done.

Takes a file from the NAS, processes it completely (encode, audio transcode,
language detection, sub/audio stripping, metadata tagging, filename cleaning),
uploads it back, replaces the original, and updates the media report.

No handoffs between threads. No intermediate states. One file, one thread,
start to finish.
"""

import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

from paths import PLEX_TOKEN, PLEX_URL
from pipeline.config import REMUX_EXTENSIONS, get_res_key, resolve_encode_params
from pipeline.ffmpeg import (
    _remux_to_mkv,
    build_ffmpeg_cmd,
    format_bytes,
    format_duration,
    get_duration,
)
from pipeline.language import detect_all_languages
from pipeline.report import update_entry
from pipeline.state import FileStatus, PipelineState
from pipeline.streams import is_hi_external
from pipeline.subs import scan_sidecars


def _probe_full(path: str) -> dict:
    """Run a single ffprobe that captures everything we want in history + integrity.

    Returns a dict like:
        {"format": {...}, "video": {...}, "audio": [...], "subs": [...], "error": "..."}
    Never raises — failures go into the 'error' field so callers can still proceed.
    """
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-print_format", "json",
                "-show_format",
                "-show_streams",
                path,
            ],
            capture_output=True, text=True, timeout=60,
            encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            return {"error": (result.stderr.strip().splitlines() or ["probe failed"])[-1]}
        data = json.loads(result.stdout)
    except Exception as e:
        return {"error": str(e)}

    fmt = data.get("format") or {}
    video: dict = {}
    audio: list = []
    subs: list = []
    for s in data.get("streams") or []:
        st = (s.get("codec_type") or "").lower()
        if st == "video" and not video:
            video = {
                "codec": s.get("codec_name"),
                "profile": s.get("profile"),
                "width": s.get("width"),
                "height": s.get("height"),
                "pix_fmt": s.get("pix_fmt"),
                "bit_rate_kbps": int(s["bit_rate"]) // 1000 if str(s.get("bit_rate", "")).isdigit() else None,
                "r_frame_rate": s.get("r_frame_rate"),
                "color_transfer": s.get("color_transfer"),
                "color_space": s.get("color_space"),
            }
        elif st == "audio":
            audio.append({
                "codec": s.get("codec_name"),
                "channels": s.get("channels"),
                "channel_layout": s.get("channel_layout"),
                "bit_rate_kbps": int(s["bit_rate"]) // 1000 if str(s.get("bit_rate", "")).isdigit() else None,
                "language": (s.get("tags") or {}).get("language"),
            })
        elif st == "subtitle":
            subs.append({
                "codec": s.get("codec_name"),
                "language": (s.get("tags") or {}).get("language"),
            })

    return {
        "format": {
            "name": fmt.get("format_name"),
            "duration_secs": float(fmt["duration"]) if str(fmt.get("duration", "")).replace(".", "", 1).isdigit() else None,
            "size_bytes": int(fmt["size"]) if str(fmt.get("size", "")).isdigit() else None,
            "bit_rate_kbps": int(fmt["bit_rate"]) // 1000 if str(fmt.get("bit_rate", "")).isdigit() else None,
        },
        "video": video,
        "audio": audio,
        "subs": subs,
    }


_RELEASE_GROUP_RE = re.compile(r"-([A-Za-z0-9]+)(?:\.(?:mkv|mp4|avi|m4v|ts))?$")
_SOURCE_TYPE_RE = re.compile(
    r"\b(BluRay|BDRip|BRRip|WEB-?DL|WEBRip|HDTV|HDRip|DVDRip|REMUX|UHD|4K|HDCAM)\b",
    re.IGNORECASE,
)


def _parse_release_info(filename: str) -> dict:
    """Extract release group + source type from a scene-tagged filename.

    `Scrubs.S08E12.1080p.BluRay.DD5.1.x264-GRiMM.mkv` → {group: "GRiMM", source: "BluRay"}
    `Scrubs - S08E12 - My Nah Nah Nah.mkv`            → {group: None, source: None}
    """
    stem = Path(filename).stem
    group_m = _RELEASE_GROUP_RE.search(stem)
    source_m = _SOURCE_TYPE_RE.search(stem)
    return {
        "group": group_m.group(1) if group_m else None,
        "source_type": source_m.group(1).upper() if source_m else None,
    }


# Tag names this writer owns. The merge helper drops existing entries
# with these names before appending the encoder's new values, so a
# subsequent re-encode replaces the encoder block cleanly without
# touching DIRECTOR / GENRE / GRADE_REVIEW / etc.
_ENCODER_OWNED_TAGS = frozenset({"ENCODER", "CQ", "CONTENT_GRADE"})


def _stamp_encode_metadata(
    filepath: str,
    *,
    encoder: str,
    cq: int | None = None,
    content_grade: str | None = None,
) -> bool:
    """Write encode parameters into the MKV's global tags via mkvpropedit.

    Three SimpleTags get added at the global (movie/episode) level:
      * ``ENCODER``         — full param string for human inspection
      * ``CQ``              — integer CQ used (machine-readable)
      * ``CONTENT_GRADE``   — string from content_grade.derive_grade()

    Uses :func:`pipeline.mkv_tags.merge_global_tags` so existing tags
    (TMDb metadata, GRADE_REVIEW, etc.) are preserved. Pre-2026-05-04
    this function naked-wrote --tags global, which mkvpropedit honoured
    by REPLACING the entire global tag block — the subsequent
    write_tmdb_to_mkv pass then wiped this stamp. Sample of 50 latest
    done encodes: 0/50 had CQ tag stamped because of that clobber.

    The audit tool now reads tags via mkvextract (mkvmerge --identify
    only surfaces global-tag *counts*, not values) and compares the
    stamped CQ to what the current grade rules say it should be.

    Returns True on success, False on any tooling error (caller logs and
    moves on — the encode itself already succeeded).
    """
    from pipeline.mkv_tags import merge_global_tags

    new_tags: list[dict] = [{"name": "ENCODER", "value": encoder}]
    if cq is not None:
        new_tags.append({"name": "CQ", "value": str(int(cq))})
    if content_grade:
        new_tags.append({"name": "CONTENT_GRADE", "value": content_grade})

    return merge_global_tags(
        filepath,
        owned_names=_ENCODER_OWNED_TAGS,
        new_tags=new_tags,
    )


def _append_history_jsonl(path, entry: dict) -> None:
    """Append a single JSONL entry with fsync. JSONL tolerates partial writes at the
    line level — the worst case from a crash is one truncated trailing line, which
    readers can skip. fsync after write keeps us honest across power loss."""
    line = json.dumps(entry, ensure_ascii=False) + "\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)
        f.flush()
        try:
            os.fsync(f.fileno())
        except OSError:
            pass


def prepare_for_encode(
    filepath: str,
    item: dict,
    config: dict,
    state: PipelineState,
    staging_dir: str,
) -> dict | None:
    """Run all non-GPU prep steps so the GPU worker can dive straight into encoding.

    Performs steps 1-5 of the full pipeline:
        1. Wait for fetch to complete (no-op if already fetched).
        2. Clean filename.
        3. Detect languages (whisper on CPU per WHISPER_FORCE_CPU=1).
        4. Pre-encode qualification — may FLAG the file early.
        5. External sub scan + container remux.

    Returns a dict with the encode-time inputs the GPU worker needs:
        {
            'clean_name': str | None,
            'actual_input': absolute path to fetched (and possibly remuxed) file,
            'remuxed_path': absolute path of remuxed file or None,
            'external_subs': list of english sidecar sub paths,
            'output_path': absolute path the encode will write to,
        }

    Returns None if the file was either FLAGGED (foreign audio / und),
    already-compliant (NOTHING_TO_DO → marked DONE), or otherwise not
    suitable for encoding. State has already been updated to reflect the
    outcome — the caller just needs to skip this file.

    Designed to run in a SEPARATE prep worker thread so the GPU isn't
    waiting on whisper / remux / language detection. The result is
    persisted to state.extras under ``prep_data`` and ``prep_done=True``
    so a subsequent ``full_gamut()`` call short-circuits past the prep.

    Idempotent: re-running on a file with prep_done=True returns the
    cached prep_data without redoing work.
    """
    filename = item["filename"]
    library_type = item.get("library_type", "")

    # Idempotence: if prep already ran, return the cached result.
    existing = state.get_file(filepath)
    if existing and existing.get("prep_done") and existing.get("prep_data"):
        return existing["prep_data"]

    try:
        # === STEP 1: Wait for fetch ===
        existing = existing or state.get_file(filepath)
        status = existing.get("status") if existing else None
        local_path = existing.get("local_path") if existing else None

        if status == FileStatus.DONE.value:
            logging.info(f"Already done: {filename} — skipping prep.")
            return None

        if not (status == FileStatus.PROCESSING.value and local_path and os.path.exists(local_path)):
            logging.info(f"prep: waiting for fetch: {filename}")
            waited = 0
            while True:
                existing = state.get_file(filepath)
                status = existing.get("status") if existing else None
                local_path = existing.get("local_path") if existing else None
                if status == FileStatus.PROCESSING.value and local_path and os.path.exists(local_path):
                    break
                if status == FileStatus.ERROR.value:
                    logging.error(f"prep: fetch failed: {filename}")
                    return None
                if status == FileStatus.DONE.value:
                    return None
                time.sleep(2)
                waited += 2
                if waited % 120 == 0 and status != FileStatus.FETCHING.value:
                    logging.warning(
                        f"prep: still waiting for fetch after {waited}s "
                        f"(status={status}, not actively fetching): {filename}"
                    )

        # === STEP 2: Clean filename ===
        try:
            from pipeline.filename import clean_filename

            clean_name = clean_filename(filepath, library_type)
            if clean_name and clean_name != os.path.basename(filepath):
                logging.info(f"  prep: clean name: {clean_name}")
        except Exception as e:
            logging.warning(f"  prep: filename cleaner failed on {filename}: {e}")
            clean_name = None

        # === STEP 3: Language detect (with whisper on CPU) ===
        existing = state.get_file(filepath) or existing
        detected_audio = existing.get("detected_audio") if existing else None
        detected_subs = existing.get("detected_subs") if existing else None
        if existing and existing.get("pre_processed"):
            if detected_audio is not None:
                item["audio_streams"] = detected_audio
            if detected_subs is not None:
                item["subtitle_streams"] = detected_subs
        else:
            state.set_file(filepath, FileStatus.PROCESSING, stage="language_detect")
            try:
                enriched = detect_all_languages(item, use_whisper=True)
                if enriched:
                    item.update(enriched)
            except Exception as e:
                logging.warning(f"  prep: language detection failed (non-fatal): {e}")

        # === STEP 4: Qualify gate ===
        existing = state.get_file(filepath) or existing
        qualify_override = bool(existing and existing.get("qualify_override"))

        if qualify_override:
            logging.info(f"  prep: qualify pre-check SKIPPED (user override): {filename}")
        else:
            try:
                from pipeline.qualify import QualifyOutcome, qualify_file

                qresult = qualify_file(item, config, use_whisper=True)
                if qresult.outcome == QualifyOutcome.FLAGGED_FOREIGN:
                    logging.warning(f"  prep: FLAGGED_FOREIGN_AUDIO: {filename} — {qresult.rationale}")
                    state.set_file(
                        filepath,
                        FileStatus.FLAGGED_FOREIGN_AUDIO,
                        mode="full_gamut",
                        stage="qualify",
                        reason=qresult.rationale,
                    )
                    _cleanup(local_path, None, None)
                    return None
                if qresult.outcome == QualifyOutcome.FLAGGED_UND:
                    logging.warning(f"  prep: FLAGGED_UNDETERMINED: {filename} — {qresult.rationale}")
                    state.set_file(
                        filepath,
                        FileStatus.FLAGGED_UNDETERMINED,
                        mode="full_gamut",
                        stage="qualify",
                        reason=qresult.rationale,
                    )
                    _cleanup(local_path, None, None)
                    return None
                if qresult.outcome == QualifyOutcome.NOTHING_TO_DO:
                    logging.info(f"  prep: already compliant: {filename}")
                    state.set_file(
                        filepath, FileStatus.DONE, mode="full_gamut", reason="already compliant"
                    )
                    _cleanup(local_path, None, None)
                    return None
            except Exception as e:
                logging.warning(f"  prep: qualify pre-check failed (non-fatal): {e}")

        # === STEP 5a: External subs ===
        cached_external = existing.get("external_subs") if existing else None
        if existing and existing.get("pre_processed") and cached_external is not None:
            external_subs = cached_external
        else:
            external_subs = _find_external_subs(filepath)

        # === STEP 5b: Container remux ===
        actual_input = local_path
        remuxed_path = None
        ext = Path(local_path).suffix.lower()
        if ext in REMUX_EXTENSIONS:
            logging.info(f"  prep: remuxing {ext} container to MKV...")
            remuxed_path = _remux_to_mkv(local_path)
            if remuxed_path:
                actual_input = remuxed_path

        # === Compute output path so the encode worker doesn't have to ===
        encode_dir = os.path.join(staging_dir, "encoded")
        os.makedirs(encode_dir, exist_ok=True)
        out_stem = Path(clean_name).stem if clean_name else Path(filename).stem
        safe_prefix = hashlib.md5(filepath.encode()).hexdigest()[:12]
        output_path = os.path.join(encode_dir, f"{safe_prefix}_{out_stem}.mkv")

        prep_data = {
            "clean_name": clean_name,
            "actual_input": actual_input,
            "remuxed_path": remuxed_path,
            "external_subs": external_subs or [],
            "output_path": output_path,
        }

        # Persist so subsequent full_gamut / GPU worker picks it up.
        state.set_file(
            filepath,
            FileStatus.PROCESSING,
            stage="prepped",
            prep_done=True,
            prep_data=prep_data,
            # Also write the mutated stream lists back so the encode
            # builder sees the language-detected versions even if it
            # rebuilds item from scratch.
            detected_audio=item.get("audio_streams"),
            detected_subs=item.get("subtitle_streams"),
        )

        return prep_data
    except Exception as e:
        logging.error(f"prepare_for_encode failed for {filename}: {e}")
        state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="prep")
        return None


def full_gamut(
    filepath: str,
    item: dict,
    config: dict,
    state: PipelineState,
    staging_dir: str,
    *,
    gpu_semaphore=None,
) -> bool:
    """Process a single file completely. Returns True on success.

    Steps:
    1. Fetch to local staging
    2. Clean filename
    3. Detect undetermined languages
    4. Find and include external subs
    5. Build ONE ffmpeg command (AV1 + EAC-3 + strip + mux)
    6. Execute encode
    7. Upload to NAS
    8. Verify (duration check)
    9. Replace original (crash-safe)
    10. Write TMDb tags (mkvpropedit, direct on NAS)
    11. Update media report
    12. Trigger Plex scan
    13. Cleanup

    When a separate prep worker has already produced ``prep_data`` (step 1-5
    output cached in state extras), this function short-circuits past the
    prep block and dives straight to the encode (step 6 onwards). That's
    the optimisation: the GPU thread doesn't burn time on CPU prep.
    """
    filename = item["filename"]
    library_type = item.get("library_type", "")

    try:
        # If a prep worker has already done steps 1-5, short-circuit past
        # the entire prep block and go straight to encode. The GPU thread
        # is precious — every second it spends on CPU prep is a second of
        # NVENC idle.
        existing_pre = state.get_file(filepath)
        if existing_pre and existing_pre.get("prep_done") and existing_pre.get("prep_data"):
            return _encode_only(filepath, item, config, state, staging_dir, gpu_semaphore)

        # === STEP 1: Fetch ===
        # Wait for network worker to fetch this file (it should be pre-fetching ahead).
        # Only fetch ourselves as a last resort if the file never appears.
        existing = state.get_file(filepath)
        status = existing.get("status") if existing else None
        local_path = existing.get("local_path") if existing else None

        # Already done — bail cleanly rather than waiting forever for a fetch that won't come.
        # (The orchestrator's force-stack check should prevent this, but belt-and-braces.)
        if status == FileStatus.DONE.value:
            logging.info(f"Already done: {filename} — skipping full_gamut.")
            return True

        # Wait for file to be ready (status=PROCESSING, set after copy completes).
        # No timers — just block until the network worker signals completion.
        if not (status == FileStatus.PROCESSING.value and local_path and os.path.exists(local_path)):
            logging.info(f"Waiting for fetch: {filename}")
            waited = 0
            while True:
                existing = state.get_file(filepath)
                status = existing.get("status") if existing else None
                local_path = existing.get("local_path") if existing else None
                if status == FileStatus.PROCESSING.value and local_path and os.path.exists(local_path):
                    break
                if status == FileStatus.ERROR.value:
                    logging.error(f"Fetch failed: {filename}")
                    return False
                if status == FileStatus.DONE.value:
                    logging.info(f"Became done while waiting for fetch: {filename} — bailing.")
                    return True
                time.sleep(2)
                waited += 2
                # Log once per 2 min and only if status isn't FETCHING (which means progress is happening).
                # Avoids the "Still waiting 60s/120s/180s..." spam while fetch is actually in flight.
                if waited % 120 == 0 and status != FileStatus.FETCHING.value:
                    logging.warning(
                        f"Still waiting for fetch after {waited}s "
                        f"(status={status}, not actively fetching): {filename}"
                    )
            logging.info(f"Fetched: {filename}")

        # === STEP 2: Clean filename ===
        # Loud failure mode: if the filename cleaner errors, we WANT to know — silently
        # falling back to the dirty name means we commit standards-violating files.
        # (Real failure today: Begin Again shipped with its scene-tagged name because
        # clean_filename raised and we caught it without logging.)
        try:
            from pipeline.filename import clean_filename

            clean_name = clean_filename(filepath, library_type)
            if clean_name and clean_name != os.path.basename(filepath):
                logging.info(f"  Clean name: {clean_name}")
        except Exception as e:
            logging.warning(f"  Filename cleaner failed on {filename}: {e}")
            clean_name = None

        # === STEP 3: Detect undetermined languages ===
        # The fetch worker runs this eagerly on fetch-complete and caches results in state,
        # so encoding startup is instant for pre-fetched files. If the cache is present we
        # use it; otherwise we detect inline (old behaviour, e.g. for files we fetched
        # ourselves above). Refresh `existing` first in case the post-fetch hook wrote the
        # cache after we grabbed the earlier snapshot.
        existing = state.get_file(filepath) or existing
        detected_audio = existing.get("detected_audio") if existing else None
        detected_subs = existing.get("detected_subs") if existing else None
        if existing and existing.get("pre_processed"):
            if detected_audio is not None:
                item["audio_streams"] = detected_audio
            if detected_subs is not None:
                item["subtitle_streams"] = detected_subs
            logging.info("  Language detection: using pre-computed results")
        else:
            state.set_file(filepath, FileStatus.PROCESSING, stage="language_detect")
            try:
                # use_whisper=True runs the CPU faster-whisper ladder for any
                # `und` audio tracks. WHISPER_FORCE_CPU=1 is set in pipeline
                # startup so this never contends with NVENC.
                enriched = detect_all_languages(item, use_whisper=True)
                if enriched:
                    item.update(enriched)
                    logging.info("  Language detection complete")
            except Exception as e:
                logging.warning(f"  Language detection failed (non-fatal): {e}")

        # === STEP 3b: Pre-encode qualification ===
        # Catches the foreign-audio class (Bluey-Swedish-dub, Amelie-English-dub-only,
        # Spirited-Away-English-dub-only) BEFORE we burn 5-15 min of GPU time
        # producing a flagged-but-encoded AV1 file. Whisper runs on CPU here
        # via WHISPER_FORCE_CPU=1 (set in pipeline startup) so we can call
        # qualify with use_whisper=True without contending with NVENC.
        # Whisper results from the earlier detect step are cached, so this
        # call is cheap when the previous step already resolved the und tracks.
        #
        # User-override bypass: if the user clicked "Encode anyway" on the
        # Flagged UI for this file, the state row carries qualify_override=True
        # in its extras JSON. We respect that and skip the pre-check entirely.
        existing = state.get_file(filepath) or existing
        qualify_override = bool(existing and existing.get("qualify_override"))

        if qualify_override:
            logging.info(f"  Qualify pre-check SKIPPED (user override): {filename}")
        else:
            try:
                from pipeline.qualify import QualifyOutcome, qualify_file

                qresult = qualify_file(item, config, use_whisper=True)
                if qresult.outcome == QualifyOutcome.FLAGGED_FOREIGN:
                    logging.warning(
                        f"  FLAGGED_FOREIGN_AUDIO: {filename} — {qresult.rationale}"
                    )
                    state.set_file(
                        filepath,
                        FileStatus.FLAGGED_FOREIGN_AUDIO,
                        mode="full_gamut",
                        stage="qualify",
                        reason=qresult.rationale,
                    )
                    _cleanup(local_path, None, None)
                    return False
                if qresult.outcome == QualifyOutcome.FLAGGED_UND:
                    logging.warning(
                        f"  FLAGGED_UNDETERMINED: {filename} — {qresult.rationale}"
                    )
                    state.set_file(
                        filepath,
                        FileStatus.FLAGGED_UNDETERMINED,
                        mode="full_gamut",
                        stage="qualify",
                        reason=qresult.rationale,
                    )
                    _cleanup(local_path, None, None)
                    return False
                if qresult.outcome == QualifyOutcome.NOTHING_TO_DO:
                    # Already compliant — no encode needed.
                    logging.info(f"  Already compliant: {filename}")
                    state.set_file(
                        filepath, FileStatus.DONE, mode="full_gamut", reason="already compliant"
                    )
                    _cleanup(local_path, None, None)
                    return True
                # QUALIFIED: continue with the existing encode flow. The keep
                # indices are computed inside build_ffmpeg_cmd from item's stream
                # lists, which reflect the language detection above.
            except Exception as e:
                # Qualification itself shouldn't be a hard blocker — log and continue.
                # The current language detection above is still in effect.
                logging.warning(f"  Qualify pre-check failed (non-fatal): {e}")

        # === STEP 4: Find external subs ===
        # Also eagerly computed by the fetch worker — use cached list if present.
        cached_external = existing.get("external_subs") if existing else None
        if existing and existing.get("pre_processed") and cached_external is not None:
            external_subs = cached_external
        else:
            external_subs = _find_external_subs(filepath)
        if external_subs:
            logging.info(f"  Found {len(external_subs)} external subtitle file(s)")

        # === STEP 5: Build ffmpeg command ===
        state.set_file(filepath, FileStatus.PROCESSING, stage="encoding")
        encode_dir = os.path.join(staging_dir, "encoded")
        os.makedirs(encode_dir, exist_ok=True)

        # Output filename: use clean name if available
        out_stem = Path(clean_name).stem if clean_name else Path(filename).stem
        safe_prefix = hashlib.md5(filepath.encode()).hexdigest()[:12]
        output_path = os.path.join(encode_dir, f"{safe_prefix}_{out_stem}.mkv")

        # Remux if container is problematic
        actual_input = local_path
        remuxed_path = None
        ext = Path(local_path).suffix.lower()
        if ext in REMUX_EXTENSIONS:
            logging.info(f"  Remuxing {ext} container to MKV...")
            remuxed_path = _remux_to_mkv(local_path)
            if remuxed_path:
                actual_input = remuxed_path

        # Build the ONE ffmpeg command (including external subs from Bazarr)
        encode_start = time.time()
        # Filter external subs: only regular English (not HI) — 1 sub per file.
        # NOTE: HI detection is delegated to pipeline.streams.is_hi_external
        # which also catches ``cc`` tokens. The old inline check only matched
        # ``.hi.`` and ``.sdh.``. Behaviour on existing sidecars is unchanged
        # — we just now also strip Closed-Caption variants.
        eng_external = []
        for s in external_subs:
            fn = os.path.basename(s)
            fn_lower = fn.lower()
            is_eng = ".en." in fn_lower or ".eng." in fn_lower
            if is_eng and not is_hi_external(fn):
                eng_external.append(s)
                break  # only 1 regular English sub
        if eng_external:
            logging.info(f"  Muxing {len(eng_external)} external English subtitle(s)")
        cmd = build_ffmpeg_cmd(
            actual_input, output_path, item, config, include_subs=True, external_subs=eng_external or None
        )

        logging.info("  Encoding: AV1 + EAC-3 audio + strip foreign tracks")
        get_res_key(item)
        params = resolve_encode_params(config, item)
        logging.info(
            f"  {library_type.upper()} | {item.get('resolution', '?')} | "
            f"HDR: {item.get('hdr', False)} | CQ: {params.get('cq', '?')} | "
            f"Preset: {params.get('preset', '?')}"
        )

        # === STEP 6: Execute encode ===
        # The GPU semaphore is held ONLY around _run_encode — the actual
        # NVENC subprocess. Prep work above (filename clean, language
        # detect, qualify, external subs, container remux, command build)
        # and verify/upload below are all CPU/disk/network and would
        # otherwise sit holding a slot the other GPU worker could use.
        # That's where most of our GPU idle time was coming from.
        encode_info: dict = {}
        if gpu_semaphore is not None:
            with gpu_semaphore:
                success = _run_encode(
                    cmd, actual_input, output_path, item, config, state, filepath, result_out=encode_info
                )
        else:
            success = _run_encode(
                cmd, actual_input, output_path, item, config, state, filepath, result_out=encode_info
            )
        if not success:
            _cleanup(local_path, remuxed_path, output_path)
            return False
        # Stash for later history write — full_gamut spans two functions so shuttle via
        # state.extras. Temporary: cleared again on DONE.
        state.set_file(
            filepath,
            FileStatus.PROCESSING,  # unchanged status, just extras
            encode_retry_mode=encode_info.get("retry_mode"),
            encode_attempts=encode_info.get("attempts"),
            ffmpeg_stats={k: v for k, v in encode_info.items() if k.startswith("ffmpeg_")},
            encode_params_used=dict(params),
        )

        encode_elapsed = time.time() - encode_start
        output_size = os.path.getsize(output_path)
        input_size = os.path.getsize(actual_input)
        saved = input_size - output_size
        ratio = (1 - output_size / input_size) * 100 if input_size > 0 else 0

        logging.info(
            f"  Encoded in {format_duration(encode_elapsed)}: "
            f"{format_bytes(input_size)} -> {format_bytes(output_size)} "
            f"({ratio:.1f}% reduction, {format_bytes(abs(saved))} {'saved' if saved > 0 else 'added'})"
        )

        # Cleanup local fetch file (free staging space)
        _cleanup(local_path, remuxed_path)

        # === Stage info for finalize_upload ===
        # finalize_upload is called inline by the GPU worker after we return True —
        # it reads these fields from state to drive the upload + verify + replace.
        final_name = clean_name if clean_name else Path(filename).stem + ".mkv"
        if not final_name.endswith(".mkv"):
            final_name = Path(final_name).stem + ".mkv"

        state.set_file(
            filepath,
            FileStatus.UPLOADING,
            stage="pending_upload",
            output_path=output_path,
            encode_time_secs=round(encode_elapsed, 1),
            output_size_bytes=output_size,
            input_size_bytes=input_size,
            bytes_saved=saved,
            compression_ratio=round(ratio, 1),
            final_name=final_name,
            library_type=library_type,
            duration_seconds=item.get("duration_seconds", 0),
        )

        logging.info(f"  Encoded, ready for upload: {final_name}")
        return True

    except Exception as e:
        logging.error(f"Full gamut failed for {filename}: {e}")
        state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="full_gamut")
        return False


def _encode_only(
    filepath: str,
    item: dict,
    config: dict,
    state: PipelineState,
    staging_dir: str,
    gpu_semaphore=None,
) -> bool:
    """Run encode (step 6) using already-cached prep_data. Returns True/False.

    Called by ``full_gamut()`` when the prep worker has already produced
    ``prep_data`` in state extras. Skips steps 1-5 entirely. Restores the
    language-detected stream lists from the cached state so the ffmpeg
    command builder sees the same streams the prep worker analysed.
    """
    filename = item["filename"]
    library_type = item.get("library_type", "")

    existing = state.get_file(filepath) or {}
    prep_data = existing.get("prep_data") or {}

    # Stale-prep guard (added 2026-04-29 after The Lost Thing incident):
    # prep_data persists in state across pipeline restarts, but the local
    # fetch + remux files in F:\AV1_Staging\fetch\ get cleaned on startup.
    # If we trust stale prep_data without verifying disk presence, the
    # encode fires immediately against a missing file → ffmpeg ENOENT,
    # status row gets confused (the fetch worker's later state write can
    # race-overwrite the ERROR back to PROCESSING).
    if prep_data:
        actual_input = prep_data.get("actual_input")
        local_path = existing.get("local_path")
        # The actual input is usually the .remux.mkv (for .avi/.m2ts/etc.)
        # or the local fetch directly. If neither exists, prep_data is stale.
        if actual_input and not os.path.exists(actual_input):
            logging.warning(
                f"_encode_only: cached prep_data points to missing input "
                f"({os.path.basename(actual_input)}) — invalidating and re-prepping"
            )
            prep_data = {}
        elif local_path and not os.path.exists(local_path):
            logging.warning(
                f"_encode_only: local fetch missing ({os.path.basename(local_path)}) — invalidating prep_data"
            )
            prep_data = {}

    if not prep_data:
        # Defensive: either caller's prep_done check missed it, or our stale
        # guard above invalidated it. Fall back to inline prep.
        logging.warning(f"_encode_only: prep_data missing/stale for {filename}, falling back to inline prep")
        prep_result = prepare_for_encode(filepath, item, config, state, staging_dir)
        if prep_result is None:
            return False
        prep_data = prep_result

    # Restore mutated stream lists from prep cache.
    if existing.get("detected_audio") is not None:
        item["audio_streams"] = existing["detected_audio"]
    if existing.get("detected_subs") is not None:
        item["subtitle_streams"] = existing["detected_subs"]

    clean_name = prep_data.get("clean_name")
    actual_input = prep_data.get("actual_input")
    remuxed_path = prep_data.get("remuxed_path")
    external_subs = prep_data.get("external_subs") or []
    output_path = prep_data.get("output_path")
    local_path = existing.get("local_path")

    try:
        # === STEP 5 (cmd build only — remux already happened in prep) ===
        state.set_file(filepath, FileStatus.PROCESSING, stage="encoding")

        # Filter external subs to one English non-HI sub.
        eng_external = []
        for s in external_subs:
            fn_lower = os.path.basename(s).lower()
            is_eng = ".en." in fn_lower or ".eng." in fn_lower
            if is_eng and not is_hi_external(os.path.basename(s)):
                eng_external.append(s)
                break
        if eng_external:
            logging.info(f"  Muxing {len(eng_external)} external English subtitle(s) (cached)")

        cmd = build_ffmpeg_cmd(
            actual_input, output_path, item, config,
            include_subs=True, external_subs=eng_external or None,
        )

        encode_start = time.time()
        logging.info("  Encoding (post-prep): AV1 + EAC-3 audio + strip foreign tracks")
        get_res_key(item)
        params = resolve_encode_params(config, item)
        logging.info(
            f"  {library_type.upper()} | {item.get('resolution', '?')} | "
            f"HDR: {item.get('hdr', False)} | CQ: {params.get('cq', '?')} | "
            f"Preset: {params.get('preset', '?')}"
        )

        # === STEP 6: Execute encode under GPU semaphore ===
        encode_info: dict = {}
        if gpu_semaphore is not None:
            with gpu_semaphore:
                success = _run_encode(
                    cmd, actual_input, output_path, item, config, state, filepath, result_out=encode_info
                )
        else:
            success = _run_encode(
                cmd, actual_input, output_path, item, config, state, filepath, result_out=encode_info
            )
        if not success:
            _cleanup(local_path, remuxed_path, output_path)
            return False

        state.set_file(
            filepath,
            FileStatus.PROCESSING,
            encode_retry_mode=encode_info.get("retry_mode"),
            encode_attempts=encode_info.get("attempts"),
            ffmpeg_stats={k: v for k, v in encode_info.items() if k.startswith("ffmpeg_")},
            encode_params_used=dict(params),
        )

        encode_elapsed = time.time() - encode_start
        output_size = os.path.getsize(output_path)
        input_size = os.path.getsize(actual_input)
        saved = input_size - output_size
        ratio = (1 - output_size / input_size) * 100 if input_size > 0 else 0

        logging.info(
            f"  Encoded in {format_duration(encode_elapsed)}: "
            f"{format_bytes(input_size)} -> {format_bytes(output_size)} "
            f"({ratio:.1f}% reduction, {format_bytes(abs(saved))} {'saved' if saved > 0 else 'added'})"
        )

        _cleanup(local_path, remuxed_path)

        final_name = clean_name if clean_name else Path(filename).stem + ".mkv"
        if not final_name.endswith(".mkv"):
            final_name = Path(final_name).stem + ".mkv"

        state.set_file(
            filepath,
            FileStatus.UPLOADING,
            stage="pending_upload",
            output_path=output_path,
            encode_time_secs=round(encode_elapsed, 1),
            output_size_bytes=output_size,
            input_size_bytes=input_size,
            bytes_saved=saved,
            compression_ratio=round(ratio, 1),
            final_name=final_name,
            library_type=library_type,
            duration_seconds=item.get("duration_seconds", 0),
        )

        logging.info(f"  Encoded, ready for upload: {final_name}")
        return True
    except Exception as e:
        logging.error(f"_encode_only failed for {filename}: {e}")
        state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="encode")
        return False


def finalize_upload(filepath: str, state: PipelineState, config: dict) -> bool:
    """Upload encoded file to NAS, verify, replace original, tag, report, Plex.

    Called inline by the GPU worker immediately after full_gamut returns True.
    No separate upload thread — the GPU semaphore has already been released by
    the time we get here, so the next encode can start while we ship bytes back.
    """
    entry = state.get_file(filepath)
    if not entry:
        return False

    output_path = entry.get("output_path")
    final_name = entry.get("final_name", os.path.basename(filepath))
    library_type = entry.get("library_type", "")
    input_size = entry.get("input_size_bytes", 0)
    output_size = entry.get("output_size_bytes", 0)
    saved = entry.get("bytes_saved", 0)
    encode_time = entry.get("encode_time_secs", 0)
    input_duration = entry.get("duration_seconds", 0)
    ratio = entry.get("compression_ratio", 0)

    if not output_path or not os.path.exists(output_path):
        logging.error(f"  Upload: encoded file missing: {final_name}")
        state.set_file(filepath, FileStatus.ERROR, error="encoded file missing", stage="upload")
        return False

    source_dir = os.path.dirname(filepath)
    dest_path = os.path.join(source_dir, final_name + ".av1.tmp")
    final_path = os.path.join(source_dir, final_name)

    # === Upload ===
    state.set_file(filepath, FileStatus.UPLOADING, stage="upload")
    logging.info(f"  Uploading: {final_name} ({format_bytes(output_size)})")
    upload_start = time.time()
    try:
        shutil.copy2(output_path, dest_path)
    except Exception as e:
        state.set_file(filepath, FileStatus.ERROR, error=f"upload failed: {e}", stage="upload")
        return False
    upload_elapsed = time.time() - upload_start
    upload_speed = output_size / upload_elapsed / (1024**2) if upload_elapsed > 0 else 0
    logging.info(f"  Uploaded in {format_duration(upload_elapsed)} ({upload_speed:.0f} MB/s)")

    # Cleanup local encoded file
    try:
        os.remove(output_path)
    except OSError:
        pass

    # === Verify ===
    # Three tiers, chosen to match the empirical ffmpeg/NVENC behaviour we've actually seen:
    #   - within max(2s, 2% of input): accept — normal container/rounding drift.
    #   - within 0.8x-1.2x: log a warning but STILL accept — visually fine in practice,
    #     and re-encoding wastes GPU when the output was probably usable.
    #   - outside 0.8x-1.2x: real broken encode (20%+ off). Auto-retry once, then park.
    # Anything that gets past the 20% threshold is deterministic garbage (seen today on VFR
    # sources and the raw-DTS-in-.mkv files). Retry cap prevents GPU loop on those.
    MAX_DURATION_RETRIES = 1
    duration_tolerance_fixed = config.get("verify_duration_tolerance_secs", 2.0)
    duration_tolerance_pct = config.get("verify_duration_tolerance_pct", 0.02)  # 2%
    output_duration = get_duration(dest_path) or 0
    if input_duration > 0:
        diff = abs(input_duration - output_duration)
        ratio = output_duration / input_duration if input_duration else 1.0
        # Dynamic tolerance — scales with content length. A 50-min episode gets ~60s
        # grace; a 3-min short gets 3.6s. Prevents false alarms on long-form content.
        allowed_drift = max(duration_tolerance_fixed, input_duration * duration_tolerance_pct)
        if diff > allowed_drift:
            if ratio > 1.2 or ratio < 0.8:
                # Clearly broken (>20% off). Clean up the output file before retry/park.
                try:
                    os.remove(dest_path)
                except OSError:
                    pass
                prev = state.get_file(filepath) or {}
                retry_count = int(prev.get("duration_retry_count", 0) or 0)
                if retry_count < MAX_DURATION_RETRIES:
                    # First (or first N) time — discard + reset to PENDING so the pipeline
                    # picks it up again. Bump the counter so the second attempt can't loop.
                    logging.error(
                        f"  Duration mismatch (broken encode, ratio {ratio:.2f}, retry {retry_count + 1}/{MAX_DURATION_RETRIES}): "
                        f"input={input_duration:.0f}s, output={output_duration:.0f}s — resetting to pending."
                    )
                    state.set_file(
                        filepath,
                        FileStatus.PENDING,
                        error=None,
                        stage=None,
                        reason=f"auto-retry {retry_count + 1}/{MAX_DURATION_RETRIES}: duration mismatch {input_duration:.0f}s→{output_duration:.0f}s",
                        duration_retry_count=retry_count + 1,
                    )
                else:
                    # Already retried and still broken — park in ERROR with the retry count
                    # recorded for audit. User can manually reset from the Errors page if
                    # they want to try again (e.g. after tweaking config).
                    logging.error(
                        f"  Duration mismatch persists after {retry_count} retries (ratio {ratio:.2f}): "
                        f"input={input_duration:.0f}s, output={output_duration:.0f}s — parking in ERROR."
                    )
                    state.set_file(
                        filepath,
                        FileStatus.ERROR,
                        error=f"duration mismatch after {retry_count} auto-retries ({input_duration:.0f}s vs {output_duration:.0f}s)",
                        stage="verify",
                        duration_retry_count=retry_count + 1,
                    )
            else:
                # 2%-20% off — drift is real but not catastrophic. Log a warning so the
                # user can spot a pattern, but DON'T reject the encode. In practice these
                # files play fine; rejecting them costs GPU for no benefit. The > 20%
                # branch above handles genuinely broken cases.
                logging.warning(
                    f"  Duration drift (accepting anyway): input={input_duration:.1f}s, "
                    f"output={output_duration:.1f}s, {(ratio - 1) * 100:+.1f}%"
                )
                # fall through to replace + DONE below

    # === Output integrity check ===
    # Guards against the "ffmpeg exited early, wrote a few seconds of frames, stamped
    # full duration in the container header" failure mode. Two gates:
    #   (a) output must have a video stream at all;
    #   (b) output average bitrate must clear a codec-aware floor.
    #
    # Codec-aware floor: AV1 is 2-3x more efficient than H.264/HEVC, so a 30%-of-
    # source threshold (safe for H.264-in/H.264-out) wrongly rejects legitimate
    # AV1 encodes of simple content (sitcoms, animation) that compress to 5-15%.
    # We use an absolute 200 kbps floor unconditionally, plus a codec-aware ratio
    # floor: 5% for AV1 (catches "truncated or silent video" only), 30% for
    # everything else.
    #
    # Also captures the full probe for encode_history below — one ffprobe call, reused.
    output_probe = _probe_full(dest_path)
    if output_probe.get("error"):
        # Probe failure used to be a warning that fell through — so a file ffprobe couldn't
        # parse (truncated container header, unknown codec tag, etc.) skipped BOTH the
        # integrity check AND the standards-compliance check and was committed to the
        # library. Now it's a hard ERROR and the file is parked for manual review.
        logging.error(
            f"  Output integrity probe failed ({output_probe['error']}) — parking in ERROR."
        )
        try:
            os.remove(dest_path)
        except OSError:
            pass
        state.set_file(
            filepath,
            FileStatus.ERROR,
            error=f"probe failed on staging output: {output_probe['error']}",
            stage="verify",
        )
        return False
    else:
        out_video = output_probe.get("video") or {}
        out_codec = (out_video.get("codec") or "").lower()
        out_bitrate_kbps = (
            out_video.get("bit_rate_kbps")
            or (output_probe.get("format") or {}).get("bit_rate_kbps")
            or 0
        )
        input_bitrate_kbps = int((input_size / input_duration * 8 / 1000)) if input_duration > 0 else 0
        # AV1 is much more efficient than H.264/HEVC — use a lower ratio floor so
        # well-compressed AV1 output of simple content doesn't trip the check.
        min_ratio = 0.05 if out_codec in ("av1", "av1_nvenc") else 0.3
        min_abs_kbps = 200
        integrity_ok = (
            bool(out_codec)
            and out_bitrate_kbps >= min_abs_kbps
            and (input_bitrate_kbps == 0 or out_bitrate_kbps >= input_bitrate_kbps * min_ratio)
        )
        if not integrity_ok:
            logging.error(
                f"  Output integrity check FAILED: codec={out_codec!r} "
                f"output_bitrate={out_bitrate_kbps}kbps input_bitrate={input_bitrate_kbps}kbps "
                f"(minimum: {min_abs_kbps}kbps and >={min_ratio * 100:.0f}% of source)"
            )
            try:
                os.remove(dest_path)
            except OSError:
                pass
            MAX_INTEGRITY_RETRIES = 1
            prev = state.get_file(filepath) or {}
            retry_count = int(prev.get("integrity_retry_count", 0) or 0)
            if retry_count < MAX_INTEGRITY_RETRIES:
                state.set_file(
                    filepath,
                    FileStatus.PENDING,
                    error=None,
                    stage=None,
                    reason=f"auto-retry {retry_count + 1}/{MAX_INTEGRITY_RETRIES}: output bitrate {out_bitrate_kbps}kbps too low",
                    integrity_retry_count=retry_count + 1,
                )
            else:
                state.set_file(
                    filepath,
                    FileStatus.ERROR,
                    error=f"output integrity failed after {retry_count} retries (bitrate {out_bitrate_kbps}kbps)",
                    stage="verify",
                    integrity_retry_count=retry_count + 1,
                )
            return False

    # === Standards compliance check ===
    # Beyond "the encode ran without crashing" we also insist every output file meets
    # the library's policy:
    #   - video codec = AV1
    #   - every audio track in {eac3, truehd, or explicitly-configured lossless passthrough}
    #   - every audio + sub language in KEEP_LANGS (no foreign audio/subs left over)
    #   - target filename has scene tags cleaned (checked post-replace below, since the
    #     rename-to-clean-name happens during replace)
    # Opus was previously accepted but is now transcoded to EAC-3 — Sonos Arc cannot
    # decode Opus passthrough so Plex transcoded it on every play; pre-transcoding
    # once eliminates that cost.
    # If the encoder somehow leaves a non-conforming file we park it in ERROR rather
    # than commit it to the library. The command builder SHOULD prevent this; the check
    # is belt-and-braces for edge cases (e.g. strange stream configurations).
    if output_probe and not output_probe.get("error"):
        from pipeline.config import KEEP_LANGS

        out_video = output_probe.get("video") or {}
        out_audio = output_probe.get("audio") or []
        out_subs = output_probe.get("subs") or []
        target_audio_codecs = {"eac3", "truehd"}
        lossless_codecs = {c.lower() for c in config.get("lossless_audio_codecs") or []}

        violations: list[str] = []
        if (out_video.get("codec") or "").lower() not in ("av1", "av1_nvenc"):
            violations.append(f"video codec {out_video.get('codec')!r} is not AV1")
        # ZERO-AUDIO is a violation. The previous version iterated `for a in out_audio:`
        # and recorded no violations when the list was empty — silently shipping
        # audio-less files. 1,787 files lost this way. Never again.
        if not out_audio:
            violations.append("output has zero audio streams")
        for i, a in enumerate(out_audio):
            codec = (a.get("codec") or "").lower()
            lang = (a.get("language") or "").lower().strip()
            if codec not in target_audio_codecs and codec not in lossless_codecs:
                violations.append(f"audio track {i}: codec {codec!r} not in target set")
            if lang and lang not in KEEP_LANGS:
                violations.append(f"audio track {i}: language {lang!r} not in KEEP_LANGS")
        for i, s in enumerate(out_subs):
            lang = (s.get("language") or "").lower().strip()
            if lang and lang not in KEEP_LANGS:
                violations.append(f"sub track {i}: language {lang!r} not in KEEP_LANGS")

        if violations:
            logging.error(f"  Standards compliance FAILED for {final_name}:")
            for v in violations:
                logging.error(f"    - {v}")
            try:
                os.remove(dest_path)
            except OSError:
                pass
            state.set_file(
                filepath,
                FileStatus.ERROR,
                error=f"standards compliance: {violations[0]}" + (f" (+{len(violations) - 1} more)" if len(violations) > 1 else ""),
                stage="verify",
                compliance_violations=violations,
            )
            return False

    # === Stream-level integrity check ===
    # The 2026-04-13/15 distributed-gap-filler sprint produced ~960 files
    # with valid metadata but corrupt AV1 streams (Matroska "element exceeds
    # master element" + libdav1d "obu_forbidden_bit" damage). Header probes
    # missed all of them. To prevent the same class shipping again, we
    # decode the first 10 seconds of the encoded output via ``ffmpeg -v error
    # -f null -``. Any decode-error output means the file is structurally
    # damaged. Better to ERROR-park here than replace the user's source
    # with garbage we can't even play back.
    #
    # Cost: ~300-500 ms on a clean file (header + first GOP only). Worth it.
    integrity_signatures = (
        "exceeds containing master element", "exceeds max length",
        "unknown-sized element", "inside parent with finite size",
        "obu_forbidden_bit out of range", "failed to parse temporal unit",
        "unknown obu type", "overrun in obu bit buffer", "error parsing obu data",
        "invalid data found when processing input", "error submitting packet to decoder",
    )
    try:
        result = subprocess.run(
            ["ffmpeg", "-v", "error", "-hide_banner",
             "-i", dest_path, "-t", "10", "-f", "null", "-"],
            capture_output=True, text=True, timeout=60,
            encoding="utf-8", errors="replace",
        )
        stderr_lo = (result.stderr or "").lower()
        hits = [sig for sig in integrity_signatures if sig in stderr_lo]
    except subprocess.TimeoutExpired:
        hits = ["integrity_check_timeout"]
    except Exception as e:  # noqa: BLE001
        # Don't gate on the check itself failing — log + proceed
        logging.warning(f"  Integrity check error (proceeding): {e!r}")
        hits = []

    if hits:
        logging.error(f"  Integrity FAILED for {final_name}: {', '.join(hits[:3])}")
        # Keep the corrupt output in place under .corrupt so we can examine
        # it post-mortem rather than silently deleting evidence.
        corrupt_path = dest_path + ".corrupt"
        try:
            os.replace(dest_path, corrupt_path)
            logging.error(f"  Corrupt output preserved at: {corrupt_path}")
        except OSError as e:
            logging.error(f"  Could not preserve corrupt output: {e}")
        state.set_file(
            filepath,
            FileStatus.ERROR,
            error=f"corruption detected post-encode: {hits[0]}",
            stage="integrity",
            corruption_signatures=hits,
        )
        return False

    # === Replace original (crash-safe) ===
    # Backup policy: DO NOT auto-delete the .original.bak. We leave it in place so
    # Synology's #recycle captures a safety copy on any subsequent housekeeping, AND
    # any tool that wants to verify the replacement (e.g. a nightly audit) can still
    # compare the sizes. Cleanup of old .bak files is a separate, manual/scheduled step.
    backup_path = filepath + ".original.bak"
    try:
        if os.path.exists(final_path) and final_path != filepath:
            os.remove(final_path)
            logging.info(f"  Removed existing target: {final_name}")
        if os.path.exists(filepath) and not os.path.exists(backup_path):
            os.rename(filepath, backup_path)
        if os.path.exists(dest_path):
            os.rename(dest_path, final_path)
            logging.info(f"  Replaced: {final_name} (backup kept at .original.bak)")
        # NOTE: we intentionally DO NOT remove backup_path here. See commit message.
    except Exception as e:
        state.set_file(filepath, FileStatus.ERROR, error=f"replace failed: {e}", stage="replace")
        return False

    # === Stamp encode parameters into MKV global tags ===
    # Without this, every encode is opaque after the fact — we'd never know
    # which CQ a 6-month-old file was encoded at, so we couldn't tell whether
    # it matches the current grade rules. Stamp ENCODER + CQ + CONTENT_GRADE
    # so the audit tool reads them directly. ~50 ms cost, fully recoverable.
    #
    # Pre-2026-05-04 this block referenced an `encode_params_used` variable
    # that doesn't exist in scope (it's defined ~200 lines later when
    # building the history entry). The NameError was caught by the broad
    # `except` and logged as a non-fatal warning — which is why 0/50 of the
    # latest done encodes had a CQ tag stamped despite the code claiming to
    # do so. Now we pull from state_extras directly, matching how the
    # history-entry builder reads it.
    #
    # Failure here is non-fatal — the encode succeeded and the file is in
    # place; missing tags just means the audit tool will fall back to the
    # state DB or flag the file as "unknown CQ" later.
    try:
        stamp_extras = state.get_file(filepath) or {}
        params_used = stamp_extras.get("encode_params_used") or {}
        encoder_value = (
            f"av1_nvenc cq={params_used.get('cq', '?')} "
            f"preset={params_used.get('preset', '?')} "
            f"multipass={params_used.get('multipass', '?')} "
            f"grade={params_used.get('content_grade', '?')} "
            f"base_cq={params_used.get('base_cq', '?')} "
            f"offset={params_used.get('cq_offset', 0):+d}"
        )
        _stamp_encode_metadata(
            final_path,
            encoder=encoder_value,
            cq=params_used.get("cq"),
            content_grade=params_used.get("content_grade"),
        )
    except Exception as e:  # noqa: BLE001
        logging.warning(f"  encode-tag stamp failed (non-fatal): {e!r}")

    # === Post-replace: filename standards check ===
    # If the on-disk filename still matches common scene-tag patterns, clean-filename
    # silently failed at encode time and we shipped a standards violation. Park in ERROR
    # with the clean-name we would have used, so a later re-queue can fix it.
    #
    # SCENE_TAG_RE lives in pipeline.filename (the canonical detector also used
    # by tools/compliance.py). The previous inline copy in this file was a
    # slightly simpler variant; the canonical one is strictly broader (adds
    # dot-dash anchoring for streaming services + scene release-group suffix),
    # so any filename the old version flagged is still flagged.
    from pipeline.filename import SCENE_TAG_RE as _SCENE_TAG_RE

    if _SCENE_TAG_RE.search(final_name):
        try:
            from pipeline.filename import clean_filename
            proposed = clean_filename(final_path, library_type)
        except Exception:
            proposed = None
        # Only reject if the cleaner can actually propose a BETTER name. If the cleaner
        # returns None or the same name, the dirty token (e.g. "MULTI" on Outlander
        # S08E03 MULTI.mkv) is something our cleaner doesn't know how to strip — parking
        # in ERROR forever just creates a stalemate. Accept with a warning instead.
        if proposed and proposed != final_name:
            logging.error(
                f"  Standards compliance FAILED (filename): on-disk name has scene tags: "
                f"{final_name!r} (cleaner proposes: {proposed!r})"
            )
            state.set_file(
                filepath,
                FileStatus.ERROR,
                error=f"standards compliance: dirty filename {final_name!r}",
                stage="verify",
                compliance_violations=[f"filename not cleaned: {final_name!r} -> {proposed!r}"],
            )
            return False
        else:
            logging.warning(
                f"  Filename has scene-tag-like token but cleaner can't propose a better "
                f"alternative: {final_name!r} — accepting. Add a rule to pipeline.filename "
                f"if this should be stripped."
            )

    # === TMDb tags ===
    try:
        from pipeline.metadata import enrich_and_tag

        tmdb_data = enrich_and_tag(final_path, final_name, library_type)
        if tmdb_data:
            logging.info(f"  TMDb: {tmdb_data.get('director', tmdb_data.get('created_by', ['?']))}")
    except Exception as e:
        logging.debug(f"  TMDb tagging failed: {e}")

    # === Sidecar cleanup ===
    # The encode embedded the chosen English sub into the MKV; any external
    # .srt/.ass/.sub/.idx/.vtt next to the new file is now redundant. Delete
    # them so the library only carries MKVs long-term. Bazarr is configured
    # (Sub-Zero remove_HI mod + Custom PP delete-if-HI + must-not-contain
    # regex on the language profile) to NOT re-grab HI variants, and the
    # "Treat Embedded Subtitles as Downloaded" flag means it sees the muxed
    # track and stops looking. Best-effort: failures here don't fail the
    # encode itself.
    try:
        sidecar_dir = os.path.dirname(final_path)
        stem = Path(final_path).stem.lower()
        sub_exts = (".srt", ".ass", ".ssa", ".sub", ".idx", ".vtt")
        deleted_subs: list[str] = []
        for fn in os.listdir(sidecar_dir):
            low = fn.lower()
            if not low.endswith(sub_exts):
                continue
            # Match by stem prefix so `Movie.en.srt` and `Movie.en.hi.srt`
            # both belong to `Movie.mkv`. Cheap startswith — false positives
            # would have to share the exact stem AND a sub extension, which
            # for distinct media files is essentially impossible.
            if not low.startswith(stem):
                continue
            full = os.path.join(sidecar_dir, fn)
            try:
                os.remove(full)
                deleted_subs.append(fn)
            except OSError as e:
                logging.debug(f"  sidecar cleanup failed for {fn}: {e}")
        if deleted_subs:
            logging.info(
                f"  Sidecar cleanup: removed {len(deleted_subs)} external sub(s) "
                f"({', '.join(deleted_subs[:3])}{'...' if len(deleted_subs) > 3 else ''})"
            )
    except OSError as e:
        logging.debug(f"  Sidecar cleanup skipped (OSError): {e}")

    # === Update media report ===
    # Pass through the whisper-enriched stream lists from state so the
    # detected_language fields produced during the encode actually persist
    # to media_report.json. Without this, every re-probe drops the
    # detection and Langs Known never moves up. (2026-04-29 fix)
    enriched: dict = {}
    detected_audio = entry.get("detected_audio")
    detected_subs = entry.get("detected_subs")
    if detected_audio is not None:
        enriched["audio_streams"] = detected_audio
    if detected_subs is not None:
        enriched["subtitle_streams"] = detected_subs
    try:
        update_entry(final_path, library_type, enriched_streams=enriched or None)
    except Exception as e:
        logging.debug(f"  Report update failed: {e}")

    # === Plex scan ===
    _trigger_plex_scan(final_path)

    # === DONE ===
    # Clear the duration_retry_count on success — a file that retried once and then
    # encoded cleanly shouldn't carry the counter forward if it's re-queued later.
    state.set_file(
        filepath,
        FileStatus.DONE,
        final_path=final_path,
        output_size_bytes=output_size,
        input_size_bytes=input_size,
        bytes_saved=saved,
        compression_ratio=ratio,
        encode_time_secs=encode_time,
        upload_time_secs=round(upload_elapsed, 1),
        mode="full_gamut",
        duration_retry_count=0,
        integrity_retry_count=0,
    )

    # Update global stats
    state.stats["completed"] = state.stats.get("completed", 0) + 1
    state.stats["bytes_saved"] = state.stats.get("bytes_saved", 0) + saved
    state.stats["total_encode_time_secs"] = state.stats.get("total_encode_time_secs", 0) + encode_time
    state.stats["total_source_size_bytes"] = state.stats.get("total_source_size_bytes", 0) + input_size
    state.stats["total_content_duration_secs"] = state.stats.get("total_content_duration_secs", 0) + input_duration
    state.save()

    # === Append to encode_history.jsonl (what the dashboard + audits read) ===
    # Rich record: source stream info (from media_report + item) + output stream info
    # (from the probe we did during integrity verify) + per-stage speeds. Lets us do
    # post-hoc sense checks without re-probing files.
    try:
        from datetime import datetime, timezone

        from paths import MEDIA_REPORT, STAGING_DIR
        from server.helpers import read_json_safe

        report = read_json_safe(MEDIA_REPORT) or {}
        report_entry: dict = {}
        for f in report.get("files", []):
            if f.get("filepath") == filepath or f.get("filepath") == final_path:
                report_entry = f
                break
        report_video = report_entry.get("video", {}) or {}

        fetch_time = entry.get("fetch_time_secs") or 0
        def _mbps(bytes_, secs):
            if not bytes_ or not secs:
                return None
            return round(bytes_ / secs / (1024 * 1024), 2)

        # Pull output probe if we have it; otherwise do one now on the final file.
        out_probe = locals().get("output_probe") or {}
        if out_probe.get("error") or not out_probe.get("video"):
            out_probe = _probe_full(final_path)

        input_bitrate_kbps = (
            report_entry.get("overall_bitrate_kbps")
            or (int(input_size / input_duration * 8 / 1000) if input_duration > 0 else None)
        )

        # Pull Tier 1 telemetry stashed earlier during encode
        state_extras = state.get_file(filepath) or {}
        ffmpeg_stats = state_extras.get("ffmpeg_stats") or {}
        encode_retry_mode = state_extras.get("encode_retry_mode") or "none"
        encode_attempts = state_extras.get("encode_attempts") or 1
        encode_params_used = state_extras.get("encode_params_used") or {}

        # Release-info (always on, cheap)
        release_info = _parse_release_info(entry.get("filename") or final_name)

        history_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "filepath": final_path,
            "filename": final_name,
            "library_type": library_type,
            "tier": entry.get("tier") or entry.get("tier_name") or "",
            "mode": entry.get("mode") or "full_gamut",
            "release": release_info,

            # Sizes + timings + speeds
            "input_bytes": input_size,
            "output_bytes": output_size,
            "saved_bytes": saved,
            "compression_ratio": round(output_size / input_size, 3) if input_size > 0 else 0,
            "encode_time_secs": round(encode_time, 1) if encode_time else 0,
            "fetch_time_secs": round(fetch_time, 1),
            "upload_time_secs": round(upload_elapsed, 1),
            "fetch_speed_mb_s": _mbps(input_size, fetch_time),
            "upload_speed_mb_s": _mbps(output_size, upload_elapsed),
            "encode_speed_x_realtime": (
                round(input_duration / encode_time, 2)
                if encode_time and input_duration else None
            ),

            # Durations — catch timestamp-bug outputs via side-by-side comparison
            "input_duration_secs": round(input_duration, 1) if input_duration else None,
            "output_duration_secs": (
                round((out_probe.get("format") or {}).get("duration_secs") or 0, 1) or None
            ),

            # Tier 1 — encoder config + retry telemetry
            "encode_params": encode_params_used,         # cq, preset, multipass, lookahead, maxrate, bufsize
            "ffmpeg_stats": ffmpeg_stats,                # speed, fps, dup, drop, frame, size from stderr
            "retry": {
                "ffmpeg_retry_mode": encode_retry_mode,  # none / no_subs / audio_copy
                "ffmpeg_attempts": encode_attempts,
                "duration_retry_count": state_extras.get("duration_retry_count", 0),
                "integrity_retry_count": state_extras.get("integrity_retry_count", 0),
            },

            # Source stream details (pre-encode) — preserved so we can compare later
            "source": {
                "video": {
                    "codec": report_video.get("codec") or report_video.get("codec_raw"),
                    "resolution_class": report_video.get("resolution_class"),
                    "width": report_video.get("width"),
                    "height": report_video.get("height"),
                    "hdr": bool(report_video.get("hdr")),
                    "bit_depth": report_video.get("bit_depth"),
                    "bitrate_kbps": input_bitrate_kbps,
                },
                "audio": [
                    {
                        "codec": a.get("codec") or a.get("codec_raw"),
                        "language": a.get("language"),
                        "channels": a.get("channels"),
                        "bitrate_kbps": a.get("bitrate_kbps"),
                        "lossless": a.get("lossless"),
                    }
                    for a in (report_entry.get("audio_streams") or [])
                ],
                "subs": [
                    {"codec": s.get("codec"), "language": s.get("language")}
                    for s in (report_entry.get("subtitle_streams") or [])
                ],
                "external_subs": [
                    {"filename": s.get("filename"), "language": s.get("language")}
                    for s in (report_entry.get("external_subtitles") or [])
                ],
            },

            # Output stream details (post-encode) — live probe of the file we just wrote
            "output": {
                "video": out_probe.get("video") or {},
                "audio": out_probe.get("audio") or [],
                "subs": out_probe.get("subs") or [],
                "format": out_probe.get("format") or {},
            },
        }
        history_file = STAGING_DIR / "encode_history.jsonl"
        _append_history_jsonl(history_file, history_entry)
    except Exception as e:
        logging.debug(f"  History append failed (non-fatal): {e}")

    logging.info(f"  DONE: {final_name}")
    return True


def _run_encode(
    cmd: list[str],
    input_path: str,
    output_path: str,
    item: dict,
    config: dict,
    state: PipelineState,
    filepath: str,
    result_out: dict | None = None,
) -> bool:
    """Execute the ffmpeg encode command with up to three attempts.

    1. Full command as built.
    2. If subtitle codec rejected → retry without subs.
    3. If audio timestamps corrupted (common on DTS-HD MA → EAC-3) → retry with audio copy.

    Progress is parsed from stderr (frame=/fps=/time=/speed= lines) and pushed into
    pipeline state so the dashboard can show live % / speed / ETA per file.

    If `result_out` is provided, on success it is populated with:
        retry_mode        — "none" | "no_subs" | "audio_copy"
        attempts          — number of attempts taken (1..3)
        ffmpeg_speed etc. — ffmpeg's own final stats line
    """
    from pipeline.ffmpeg import build_ffmpeg_cmd

    retry_mode = "none"
    # 4 attempts: original (with hwaccel) → no_hwaccel → no_subs → audio_copy
    # no_hwaccel is checked first because NVDEC-incompatible sources (10-bit H.264,
    # MPEG-4 ASP, some edge cases) fail on decode before the subtitle or audio
    # stages are even reached — retrying those with sw decode resolves them.
    attempts_total = 4
    duration_secs = item.get("duration_seconds") or 0

    for attempt in range(attempts_total):
        if attempt == 0:
            pass  # original cmd (hwaccel on by default)
        elif retry_mode == "no_hwaccel":
            cmd = build_ffmpeg_cmd(input_path, output_path, item, config, use_hwaccel=False)
            logging.warning("  Retrying with software decode (NVDEC incompatible source)")
        elif retry_mode == "no_subs":
            cmd = build_ffmpeg_cmd(input_path, output_path, item, config, include_subs=False)
            logging.warning("  Retrying without subtitles")
        elif retry_mode == "audio_copy":
            cmd = _build_audio_copy_cmd(cmd)
            logging.warning("  Retrying with audio passthrough (DTS timestamp workaround)")
        else:
            break

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf-8",
                errors="replace",
            )
            stderr = _stream_encode_progress(process, state, filepath, duration_secs)

            if process.returncode == 0:
                if not os.path.exists(output_path):
                    continue
                if result_out is not None:
                    result_out["retry_mode"] = retry_mode
                    result_out["attempts"] = attempt + 1
                return True

            if os.path.exists(output_path):
                os.remove(output_path)

            stderr_low = stderr.lower()
            # NVDEC decode failure — retry with software decode (libavcodec).
            #
            # CRITICAL: scan only the TAIL of stderr, not the full output. ffmpeg's
            # startup banner always contains strings like "--enable-cuvid",
            # "--enable-nvdec", "--enable-cuda-llvm" in its configuration dump, so
            # naive substring matching on the whole stderr triggers this fallback
            # on ANY non-zero exit regardless of the real cause. We only want to
            # see ERROR messages, which appear near the end of stderr.
            #
            # Patterns are specific error-message fragments rather than feature
            # keywords — matches NVDEC/CUVID runtime failures without false-positiving
            # on the banner or unrelated CUDA diagnostics.
            error_tail = "\n".join(stderr_low.strip().split("\n")[-20:])
            hwaccel_failure_markers = (
                "cuvid error",
                "cuvid decoder",
                "cuviddecoder",
                "cuvidcreatedecoder",
                "cuda_error_",
                "hwaccel initialisation returned error",
                "hwaccel initialization returned error",
                "failed setup for format cuda",
                "no decoder could be found for codec",
                "impossible to convert between the formats",
            )
            if attempt == 0 and any(m in error_tail for m in hwaccel_failure_markers):
                retry_mode = "no_hwaccel"
                continue
            if attempt == 0 and ("subtitle" in stderr_low or "codec none" in stderr_low):
                retry_mode = "no_subs"
                continue
            if "non-monotonic dts" in stderr_low or "non monotonic dts" in stderr_low:
                if retry_mode != "audio_copy":
                    retry_mode = "audio_copy"
                    continue

            logging.error(f"  Encode failed (exit {process.returncode})")
            for line in stderr.strip().split("\n")[-5:]:
                logging.error(f"    ffmpeg: {line}")
            state.set_file(filepath, FileStatus.ERROR, error=f"ffmpeg exit {process.returncode}", stage="encoding")
            return False

        except Exception as e:
            logging.error(f"  Encode exception: {e}")
            state.set_file(filepath, FileStatus.ERROR, error=str(e), stage="encoding")
            if os.path.exists(output_path):
                os.remove(output_path)
            return False

    state.set_file(filepath, FileStatus.ERROR, error="encode failed after retries", stage="encoding")
    return False


def _stream_encode_progress(process, state: PipelineState, filepath: str, duration_secs: float) -> str:
    """Consume ffmpeg's stable `-progress pipe:1` key=value output on stdout, emit state updates.

    Also drains stderr on a background thread so it doesn't deadlock the subprocess (stderr
    still carries warnings + errors which the caller needs for the retry detection).

    Each progress snapshot ends with `progress=continue` (or `progress=end` at finish). We
    push a state update on each snapshot boundary, throttled to one per ~1.5s to keep the
    SQLite write volume sane.

    Enforces a wall-clock deadline of ``max(1800, duration_secs * 10)`` seconds. A hung
    ffmpeg used to block a GPU worker forever; now we kill the process and return the
    captured stderr so the caller can record ERROR.
    """
    import threading

    stderr_buf: list[str] = []

    def _drain_stderr():
        assert process.stderr is not None
        for line in iter(process.stderr.readline, ""):
            if not line:
                break
            stderr_buf.append(line.rstrip("\n"))

    t = threading.Thread(target=_drain_stderr, daemon=True)
    t.start()

    # Wall-clock deadline: 10x content duration, minimum 30 minutes. A healthy NVENC AV1
    # encode runs at 1-3x realtime, so 10x gives a huge margin while still catching truly
    # hung processes (e.g. stuck ffmpeg at 0% CPU, banner-print timeouts, driver hangs).
    deadline = time.time() + max(1800.0, float(duration_secs) * 10.0)

    snapshot: dict[str, str] = {}
    last_update = 0.0
    assert process.stdout is not None
    timed_out = False
    for raw in iter(process.stdout.readline, ""):
        if not raw:
            break
        if time.time() > deadline:
            logging.error(
                f"  Encode exceeded wall-clock deadline ({int(max(1800.0, duration_secs * 10.0))}s) "
                f"for {os.path.basename(filepath)} — killing"
            )
            try:
                process.kill()
            except Exception:
                pass
            timed_out = True
            break
        line = raw.strip()
        if not line:
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        snapshot[key.strip()] = val.strip()
        if key.strip() != "progress":
            continue
        # snapshot is complete (progress=continue or progress=end)
        now = time.time()
        if snapshot["progress"] != "end" and now - last_update < 1.5:
            snapshot.clear()
            continue
        last_update = now
        try:
            fps_s = snapshot.get("fps", "0")
            speed_s = snapshot.get("speed", "0x").rstrip("x")
            out_time_us_s = snapshot.get("out_time_us") or snapshot.get("out_time_ms")
            # out_time_us is microseconds; out_time_ms is (misnamed) also microseconds per ffmpeg docs
            out_time_us = int(out_time_us_s) if out_time_us_s and out_time_us_s.isdigit() else 0
            elapsed_out = out_time_us / 1_000_000
            fps = float(fps_s) if fps_s else 0.0
            speed = float(speed_s) if speed_s else 0.0
            pct = int(elapsed_out / duration_secs * 100) if duration_secs > 0 else None
            eta_secs = (
                (duration_secs - elapsed_out) / speed if speed > 0 and duration_secs else None
            )
            eta_text = None
            if eta_secs and eta_secs > 0:
                h, rem = divmod(int(eta_secs), 3600)
                m_, _s = divmod(rem, 60)
                eta_text = f"{h}h {m_:02d}m" if h else f"{m_}m {_s:02d}s"
            state.set_file(
                filepath,
                FileStatus.PROCESSING,
                stage="encoding",
                progress_pct=pct,
                speed=f"{speed}x",
                fps=round(fps, 1),
                eta_text=eta_text,
            )
        except (ValueError, KeyError):
            pass
        snapshot.clear()

    # Bound process.wait too — if the stdout loop exits cleanly but the process hasn't
    # actually exited (rare but seen on driver hangs) we don't want to block forever.
    remaining = max(1.0, deadline - time.time())
    try:
        process.wait(timeout=remaining if not timed_out else 5.0)
    except subprocess.TimeoutExpired:
        logging.error(f"  Encode still alive after deadline — killing {os.path.basename(filepath)}")
        try:
            process.kill()
        except Exception:
            pass
        try:
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            pass
        timed_out = True
    t.join(timeout=5)
    stderr = "\n".join(stderr_buf)
    if timed_out:
        stderr = (stderr + "\nENCODE TIMEOUT: killed after wall-clock deadline").strip()
    return stderr


def _build_audio_copy_cmd(cmd: list[str]) -> list[str]:
    """Rewrite an ffmpeg command to use audio passthrough instead of transcode.

    Strips per-stream -c:a:N / -b:a:N pairs (added by build_ffmpeg_cmd) and inserts a single
    global -c:a copy just before the output path. Faster and sidesteps DTS timestamp bugs.

    Asserts that the rewritten command still maps audio — if the input command had
    no audio map at all we'd produce a zero-audio output on the retry. Refuse.
    """
    out = []
    skip_next = False
    for tok in cmd:
        if skip_next:
            skip_next = False
            continue
        # Strip per-stream audio codec/bitrate flags
        if tok.startswith(("-c:a", "-b:a", "-filter:a", "-ac:a")):
            skip_next = True
            continue
        out.append(tok)
    # Insert -c:a copy right before the output path (final argument)
    if out:
        output = out[-1]
        out = out[:-1] + ["-c:a", "copy", output]

    # INVARIANT: rewritten command must still contain at least one -map 0:a* flag.
    # If it doesn't, running it would produce zero-audio output with rc=0 — a
    # silent-damage path. Fail here rather than ship the audio-less encode.
    has_audio_map = any(
        out[i] == "-map" and out[i + 1].startswith("0:a")
        for i in range(len(out) - 1)
        if out[i] == "-map"
    )
    if not has_audio_map:
        raise ValueError(
            "_build_audio_copy_cmd refused: rewritten command has no `-map 0:a*` — "
            "retrying this would produce a zero-audio output. Original cmd was "
            f"missing an audio map entirely: {cmd!r}"
        )

    return out


def _find_external_subs(filepath: str) -> list[str]:
    """Find external subtitle files (.srt, .ass, .ssa, .sub) alongside the MKV.

    Delegates to :func:`pipeline.subs.scan_sidecars`. The stem-match rule is
    stricter than the old inline ``startswith(stem[:20])`` (now requires the
    full stem), which is the correct behaviour — the 20-char prefix would
    false-match siblings of e.g. ``The Office`` onto ``The Office (UK)``.
    """
    return [s.path for s in scan_sidecars(filepath)]


def _cleanup(*paths: str | None) -> None:
    """Remove local staging files, ignoring errors."""
    for p in paths:
        if p and os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


_plex_scan_lock = None
_last_plex_scan = 0


def _trigger_plex_scan(filepath: str) -> None:
    """Trigger a Plex library scan (debounced, non-blocking)."""
    global _last_plex_scan
    import threading

    if not PLEX_URL or not PLEX_TOKEN:
        return

    now = time.time()
    if now - _last_plex_scan < 30:
        return  # debounce: max one scan per 30 seconds
    _last_plex_scan = now

    def _scan():
        try:
            from urllib.request import Request, urlopen
            from xml.etree import ElementTree

            headers = {"X-Plex-Token": PLEX_TOKEN, "Accept": "application/xml"}

            # Determine which section(s) to scan
            sections_req = Request(f"{PLEX_URL}/library/sections", headers=headers)
            resp = urlopen(sections_req, timeout=10)
            root = ElementTree.fromstring(resp.read())
            scanned = 0
            for section in root.findall(".//Directory"):
                section_key = section.get("key")
                if section_key:
                    scan_req = Request(
                        f"{PLEX_URL}/library/sections/{section_key}/refresh",
                        headers=headers,
                    )
                    urlopen(scan_req, timeout=10)
                    scanned += 1
            if scanned:
                logging.info(f"  Triggered Plex scan ({scanned} sections)")
        except Exception as e:
            logging.debug(f"  Plex scan failed (best-effort): {e}")

    threading.Thread(target=_scan, daemon=True, name="plex-scan").start()
