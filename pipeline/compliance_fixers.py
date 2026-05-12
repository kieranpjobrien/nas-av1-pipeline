"""In-place fixers for FIXABLE compliance violations.

Each fixer takes the encoded output file path + the violation that
fired, runs ``mkvmerge`` (stream removal) or ``mkvpropedit`` (tag
stamp) or ``os.rename`` (filename clean), and returns True/False
on success. The caller (finalize_upload) re-runs compliance after
all fixers have attempted.

Why in-place rather than re-encode: every fixable violation is a
metadata or stream-mux issue that ``mkvmerge``/``mkvpropedit`` can
resolve in seconds against the existing encoded video bitstream.
Re-encoding would just reproduce the same bytes (potentially with
the same bug) at 60+ minutes of GPU cost. Fixing the existing
output is strictly better.

The fixers follow the discipline-contract guards:
  * Output of the fix must pass an integrity probe before it
    replaces the original .av1.tmp (no zero-stream, no truncation).
  * Fix never deletes the input until the output is verified.
  * If any fixer fails, the .av1.tmp is left as-is so the caller's
    "refuse to ship" path can preserve the source.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path

from pipeline.compliance import Violation

MKVMERGE = r"C:/Program Files/MKVToolNix/mkvmerge.exe"
MKVPROPEDIT = r"C:/Program Files/MKVToolNix/mkvpropedit.exe"


def _mkvmerge_drop_streams(
    src: str,
    *,
    drop_audio_indices: list[int] | None = None,
    drop_sub_indices: list[int] | None = None,
) -> bool:
    """Run mkvmerge to produce a copy of ``src`` with the specified audio
    and/or subtitle stream indices dropped, then atomically replace ``src``.

    Returns True on success. False if mkvmerge fails or the output looks
    truncated (≥50% of source size guard, mirrors mkv_tags._try_remux_in_place).

    ``drop_*_indices`` are PER-TYPE indices as ffprobe sees them (sub index
    0 = first subtitle stream). mkvmerge's ``--audio-tracks`` /
    ``--subtitle-tracks`` flags however use **global** mkvmerge track IDs,
    which include video + audio + subtitles in source-stream order. So a
    file with 1 video + 4 audio + 26 subs has audio at IDs 1-4 and subs at
    IDs 5-30; per-type sub index 25 maps to global track ID 30.

    Pre-2026-05-12 this function passed per-type indices directly to
    ``--subtitle-tracks`` and ``--audio-tracks``. mkvmerge interpreted them
    as global IDs, kept the wrong tracks (or all of them on a no-match
    fallback), and the compliance gate saw the same violations on every
    re-encode — that's the GoodFellas / Mary Poppins / The Favourite /
    Toni Erdmann / Blue Valentine / Jurassic World loop. The breaker
    counter climbed but the underlying fix never worked.
    """
    if not drop_audio_indices and not drop_sub_indices:
        return True  # nothing to drop

    src_path = Path(src)
    tmp_out = str(src_path.parent / (src_path.name + ".compliance_tmp.mkv"))

    # One probe for both audio + sub fixes — saves an SMB round-trip.
    from pipeline.full_gamut import _probe_full
    probe = _probe_full(src)
    # ``_probe_full`` returns ``video`` as a SINGLE DICT (the first video
    # stream), not a list. ``len(dict)`` returns the key count (~9 fields),
    # not 1 — pre-2026-05-12 that's exactly the bug that made mkvmerge
    # silently return rc=1 with empty stderr for every compliance fix
    # against an encoded output. The Wild Robot / Happy Gilmore 2 /
    # Superbad / From Russia with Love wave was this. Truth: video is
    # 1 if the dict is non-empty, else 0. audio / subs ARE lists.
    n_video = 1 if probe.get("video") else 0
    n_audio = len(probe.get("audio") or [])
    n_sub = len(probe.get("subs") or [])
    audio_id_offset = n_video             # audio global IDs: [n_video, n_video+n_audio)
    sub_id_offset = n_video + n_audio     # sub global IDs:   [offset, offset+n_sub)

    cmd = [MKVMERGE, "-o", tmp_out]

    if drop_audio_indices is not None:
        keep_audio_per_type = [i for i in range(n_audio) if i not in drop_audio_indices]
        if not keep_audio_per_type:
            logging.error(
                f"compliance fix refuses to drop ALL audio tracks (would produce zero-audio file): {src}"
            )
            return False
        keep_audio_global = [audio_id_offset + i for i in keep_audio_per_type]
        cmd.extend(["--audio-tracks", ",".join(str(i) for i in keep_audio_global)])

    if drop_sub_indices is not None:
        keep_sub_per_type = [i for i in range(n_sub) if i not in drop_sub_indices]
        if keep_sub_per_type:
            keep_sub_global = [sub_id_offset + i for i in keep_sub_per_type]
            cmd.extend(["--subtitle-tracks", ",".join(str(i) for i in keep_sub_global)])
        else:
            cmd.append("--no-subtitles")

    cmd.append(src)

    src_size = os.path.getsize(src)
    # Timeout scales with file size. mkvmerge stream-copy reads + writes the
    # whole file over SMB at ~50-100 MB/s in practice. 600s flat was fine for
    # 1-3 GB TV episodes but timed out on 25 GB+ 4K HDR films — Dark Knight
    # Rises (27.5 GB), The Last Boy Scout, Varsity Blues, Caddyshack, etc.
    # 18 files stuck in "compliance unfixed" today because of this single
    # hardcoded bound. Formula: 60 s overhead + 1 s per 10 MB of source ≈
    # 100 MB/s amortised. 30 GB → 60 + 3000 = ~50 min ceiling, plenty.
    timeout_secs = max(600, 60 + int(src_size / (10 * 1024 * 1024)))
    try:
        out = subprocess.run(cmd, capture_output=True, timeout=timeout_secs)
    except Exception as e:
        logging.error(
            f"compliance fix mkvmerge raised (timeout={timeout_secs}s, "
            f"src_size={src_size/1024**3:.1f}GB): {e}"
        )
        if os.path.exists(tmp_out):
            try:
                os.remove(tmp_out)
            except OSError:
                pass
        return False
    if out.returncode != 0:
        logging.error(
            f"compliance fix mkvmerge rc={out.returncode}: "
            f"{out.stderr.decode('utf-8','replace')[:200]}"
        )
        if os.path.exists(tmp_out):
            try:
                os.remove(tmp_out)
            except OSError:
                pass
        return False

    # Sanity guard: output must be ≥ 50% of input (mirrors mkv_tags
    # remux guard — protects against the 4 KB stub class).
    if not os.path.exists(tmp_out):
        logging.error(f"compliance fix produced no output file: {tmp_out}")
        return False
    out_size = os.path.getsize(tmp_out)
    if out_size < src_size * 0.5:
        logging.error(
            f"compliance fix output too small ({out_size/1024**2:.1f} MB vs "
            f"{src_size/1024**2:.1f} MB source) — refusing replace"
        )
        try:
            os.remove(tmp_out)
        except OSError:
            pass
        return False

    # ---- PROOF-OF-WORK validation (2026-05-12) ------------------------
    # Pre-fix the function returned True whenever mkvmerge produced a
    # >=50%-size output, regardless of whether the drop actually happened.
    # The track-ID translation bug (per-type vs global) meant mkvmerge
    # happily produced a 95%-of-source output with the WRONG tracks
    # kept — same filter syntax, same exit code 0, same caller-visible
    # success. Compliance gate then re-probed, saw the same violations,
    # REFUSE. Loop. The fixer is the contract; if it can't verify the
    # drop, it MUST return False so the breaker eventually catches it
    # rather than the cohort silently expanding.
    out_probe = _probe_full(tmp_out)
    n_out_audio = len(out_probe.get("audio") or [])
    n_out_sub = len(out_probe.get("subs") or [])
    expected_audio = n_audio - (len(drop_audio_indices) if drop_audio_indices else 0)
    expected_sub = n_sub - (len(drop_sub_indices) if drop_sub_indices else 0)
    if n_out_audio != expected_audio or n_out_sub != expected_sub:
        logging.error(
            f"compliance fix track-count mismatch: expected "
            f"audio={expected_audio} sub={expected_sub}, "
            f"got audio={n_out_audio} sub={n_out_sub} — mkvmerge did "
            f"NOT drop the requested tracks. Refusing to replace. "
            f"cmd was: {' '.join(cmd[:8])}..."
        )
        try:
            os.remove(tmp_out)
        except OSError:
            pass
        return False

    # Atomic replace.
    os.replace(tmp_out, src)
    return True


def fix_extra_eng_subs(filepath: str, v: Violation) -> bool:
    return _mkvmerge_drop_streams(filepath, drop_sub_indices=v.data["indices"])


def fix_foreign_subs(filepath: str, v: Violation) -> bool:
    return _mkvmerge_drop_streams(filepath, drop_sub_indices=v.data["indices"])


def fix_foreign_audio(filepath: str, v: Violation) -> bool:
    return _mkvmerge_drop_streams(filepath, drop_audio_indices=v.data["indices"])


def fix_commentary_audio(filepath: str, v: Violation) -> bool:
    return _mkvmerge_drop_streams(filepath, drop_audio_indices=v.data["indices"])


def fix_missing_encode_tags(
    filepath: str, v: Violation, *, encode_params: dict
) -> bool:
    """Stamp ENCODER + CQ + CONTENT_GRADE via merge_global_tags."""
    from pipeline.mkv_tags import merge_global_tags

    cq = encode_params.get("cq")
    grade = encode_params.get("content_grade") or "default"
    if cq is None:
        logging.error("fix_missing_encode_tags: encode_params has no cq")
        return False
    encoder_str = (
        f"av1_nvenc cq={cq} preset={encode_params.get('preset','p7')} "
        f"multipass={encode_params.get('multipass','fullres')} "
        f"grade={grade} base_cq={encode_params.get('base_cq', cq)} "
        f"offset={'+' if (encode_params.get('cq_offset') or 0) >= 0 else ''}"
        f"{encode_params.get('cq_offset') or 0}"
    )
    new_tags = [
        {"name": "ENCODER", "value": encoder_str},
        {"name": "CQ", "value": str(cq)},
        {"name": "CONTENT_GRADE", "value": grade},
    ]
    try:
        return merge_global_tags(
            filepath,
            owned_names={"ENCODER", "CQ", "CONTENT_GRADE", "BASE_CQ"},
            new_tags=new_tags,
        )
    except Exception as e:
        logging.error(f"fix_missing_encode_tags failed: {e}")
        return False


def fix_cq_mismatch(filepath: str, v: Violation, *, encode_params: dict) -> bool:
    """Same fix as missing — re-stamp the CQ + ENCODER from encode_params."""
    return fix_missing_encode_tags(filepath, v, encode_params=encode_params)


def fix_grade_mismatch(filepath: str, v: Violation, *, encode_params: dict) -> bool:
    return fix_missing_encode_tags(filepath, v, encode_params=encode_params)


def fix_missing_tmdb_tags(filepath: str, v: Violation, *, item: dict) -> bool:
    """Re-stamp TMDb metadata from item.tmdb via the metadata writer."""
    from pipeline.metadata import enrich_and_tag

    tmdb = item.get("tmdb") or {}
    if not tmdb:
        return False
    library_type = item.get("library_type", "")
    try:
        enrich_and_tag(filepath, os.path.basename(filepath), library_type)
        return True
    except Exception as e:
        logging.error(f"fix_missing_tmdb_tags failed: {e}")
        return False


def fix_filename_mismatch(filepath: str, v: Violation) -> str | None:
    """Rename the file on NAS to the expected canonical name. Returns the new
    path on success, None on failure. Caller must update state.filepath."""
    expected = v.data.get("expected")
    if not expected:
        return None
    new_path = os.path.join(os.path.dirname(filepath), expected)
    if new_path == filepath:
        return filepath  # already matches
    try:
        os.rename(filepath, new_path)
        return new_path
    except OSError as e:
        logging.error(f"fix_filename_mismatch failed: {e}")
        return None


# Dispatch table: violation tag -> fixer callable. The fixer signature is
# (filepath, violation, **context) -> bool | str | None.
# Context kwargs are passed by name from finalize_upload.
FIXERS = {
    "extra_eng_subs": fix_extra_eng_subs,
    "foreign_subs": fix_foreign_subs,
    "foreign_audio": fix_foreign_audio,
    "commentary_audio": fix_commentary_audio,
    "missing_encode_tags": fix_missing_encode_tags,
    "cq_mismatch": fix_cq_mismatch,
    "grade_mismatch": fix_grade_mismatch,
    "missing_tmdb_tags": fix_missing_tmdb_tags,
    "filename_mismatch": fix_filename_mismatch,
}
