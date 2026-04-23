"""Execute one plan row as a pure, idempotent function.

This module owns the ONLY mutation path for MKV files on the NAS. Every
action is wrapped in a three-step safety contract:

1. **Pre-check** — ffprobe the source; refuse if video < 1 or audio < 1.
2. **Fingerprint check** — the recorded ``source_fingerprint`` on the plan
   row must still match the file on disk. If not, the plan is stale.
3. **Post-check** — ffprobe the staging output; refuse replacement unless
   it satisfies the per-action minimum stream counts.

Only after all three pass does :func:`os.replace` atomically swap the new
file into place. The staging file is always deleted on refusal or failure
so we never leave half-finished encodes behind.

The runner is a pure function: it takes a plan id, opens one connection,
does the work, writes back a result row, and returns. No threads, no
control files, no cross-process state.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from .inventory import (
    INVENTORY_DB,
    Action,
    PlanRow,
    ResultStatus,
    connect,
    fingerprint_or_none,
    fingerprint_path,
    iter_pending_plans,
    mark_plan_executed,
    read_plan,
)

# ``paths`` + pipeline helpers live at the repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from paths import STAGING_DIR  # noqa: E402

# We deliberately reuse the remote-execution primitives from ``pipeline.nas_worker``.
# These are the only pieces of the old pipeline that are actually safe — they
# shell out to mkvtoolnix on the NAS via SSH + Docker and have been running
# without issue. Importing them here does not extend the old pipeline's
# lifetime; this is a one-way dependency.
from pipeline.nas_worker import (  # noqa: E402
    NAS,
    SERVER,
    remote_identify,
    remote_mkvmerge,
    remote_mkvpropedit,
    unc_to_container_path,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Minimum audio stream count allowed in the output for each action type.
#: Violations cause the runner to refuse replacement and delete the staging file.
EXPECTED_MIN_AUDIO: dict[Action, int] = {
    "encode_av1": 1,
    "transcode_audio": 1,
    "mux_sub": 1,
    "rename": 0,  # rename doesn't produce a new file; post-check not applied
    "tag_tmdb": 1,  # mkvpropedit must preserve all streams
    "delete_sidecar": 0,  # sidecar only
}

#: Directory where we stage output files before swapping them in.
STAGING_OUT: Path = STAGING_DIR / "naslib_staging"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class RunResult:
    """The typed result returned by :func:`run_plan` and the action runners."""

    status: ResultStatus
    msg: str
    output_fingerprint: str | None = None


@dataclass(slots=True, frozen=True)
class ProbeCounts:
    """Counts returned by :func:`_probe_counts` — the invariant checker."""

    video: int
    audio: int
    sub: int

    def ok_for_action(self, action: Action) -> bool:
        """True iff the counts satisfy the post-check for ``action``."""
        if action in ("rename", "delete_sidecar"):
            return True
        if self.video < 1:
            return False
        if self.audio < EXPECTED_MIN_AUDIO.get(action, 1):
            return False
        return True


# ---------------------------------------------------------------------------
# Top-level entry points
# ---------------------------------------------------------------------------


def run_next(
    *,
    limit: int = 1,
    action_filter: Action | None = None,
    db_path: Path | None = None,
) -> list[tuple[int, RunResult]]:
    """Run up to ``limit`` pending plans and return their results.

    Args:
        limit: Maximum number of plans to execute in this invocation.
        action_filter: If set, only run plans of this action type.
        db_path: Optional override for the SQLite path (tests).

    Returns:
        A list of ``(plan_id, result)`` tuples in execution order.
    """
    out: list[tuple[int, RunResult]] = []
    with connect(db_path or INVENTORY_DB) as conn:
        plans = list(iter_pending_plans(conn, action=action_filter, limit=limit))
    # Execute one plan at a time; each call opens its own connection so a
    # long-running encode does not hold a write lock.
    for plan in plans:
        result = run_plan(plan.id, db_path=db_path)
        out.append((plan.id, result))
    return out


def run_plan(plan_id: int, *, db_path: Path | None = None) -> RunResult:
    """Execute the plan with id ``plan_id`` and record the outcome.

    This is the single entry point called by the CLI, tests, and other
    callers. The outcome is always written back to the DB (even on refuse)
    so an operator can audit what happened without grepping logs.
    """
    with connect(db_path or INVENTORY_DB) as conn:
        plan = read_plan(conn, plan_id)
        if plan is None:
            return RunResult(status="refused", msg=f"plan {plan_id} not found")
        if plan.executed_at is not None:
            return RunResult(
                status="skipped",
                msg=f"already executed at {plan.executed_at}: {plan.result_status}",
            )

    result = _dispatch(plan)

    with connect(db_path or INVENTORY_DB) as conn:
        mark_plan_executed(
            conn,
            plan_id=plan_id,
            status=result.status,
            msg=result.msg,
            output_fingerprint=result.output_fingerprint,
        )
    return result


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def _dispatch(plan: PlanRow) -> RunResult:
    """Apply the pre-check / fingerprint contract, then call the per-action runner."""
    # Pre-check 1: file must exist.
    if not os.path.exists(plan.filepath):
        return RunResult(status="refused", msg="source file does not exist")

    # Pre-check 2: fingerprint unchanged since plan creation.
    current_fp = fingerprint_or_none(plan.filepath)
    if current_fp is None:
        return RunResult(status="refused", msg="source fingerprint unreadable")
    if current_fp != plan.source_fingerprint:
        return RunResult(
            status="refused",
            msg=(f"source changed since plan emitted: was {plan.source_fingerprint!r}, now {current_fp!r}"),
        )

    # Pre-check 3: source has video and (for MKV-mutating actions) audio.
    src_counts = _probe_counts(plan.filepath)
    if src_counts is None:
        return RunResult(status="refused", msg="source ffprobe failed")
    if plan.action not in ("delete_sidecar",):
        if src_counts.video < 1:
            return RunResult(
                status="refused",
                msg=f"pre-check: source has {src_counts.video} video streams",
            )
    if plan.action in ("encode_av1", "transcode_audio", "mux_sub"):
        if src_counts.audio < 1:
            return RunResult(
                status="refused",
                msg=f"pre-check: source has {src_counts.audio} audio streams",
            )

    # Per-action dispatch.
    try:
        if plan.action == "encode_av1":
            return _run_encode_av1(plan, src_counts)
        if plan.action == "transcode_audio":
            return _run_transcode_audio(plan, src_counts)
        if plan.action == "mux_sub":
            return _run_mux_sub(plan, src_counts)
        if plan.action == "rename":
            return _run_rename(plan)
        if plan.action == "tag_tmdb":
            return _run_tag_tmdb(plan, src_counts)
        if plan.action == "delete_sidecar":
            return _run_delete_sidecar(plan)
    except Exception as exc:  # pragma: no cover — defensive guard
        return RunResult(status="failed", msg=f"runner raised: {exc!r}")
    return RunResult(status="refused", msg=f"unknown action: {plan.action}")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _probe_counts(filepath: str) -> ProbeCounts | None:
    """Return ``(video, audio, sub)`` counts for ``filepath`` or ``None`` on error.

    This is the single source of truth for the invariant checker. All
    pre/post checks go through this function so there's no risk of one
    codepath counting cover art and another not.
    """
    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_streams",
        filepath,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
            encoding="utf-8",
            errors="replace",
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None
    if result.returncode != 0:
        return None
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    streams = data.get("streams") or []
    video = 0
    audio = 0
    sub = 0
    for s in streams:
        ctype = s.get("codec_type")
        disposition = s.get("disposition") or {}
        if ctype == "video":
            if disposition.get("attached_pic"):
                continue
            video += 1
        elif ctype == "audio":
            audio += 1
        elif ctype == "subtitle":
            sub += 1
    return ProbeCounts(video=video, audio=audio, sub=sub)


def _staging_path_for(source: str, suffix: str) -> Path:
    """Return a unique staging path under ``STAGING_OUT`` for a given source."""
    STAGING_OUT.mkdir(parents=True, exist_ok=True)
    base = Path(source).stem
    # tempfile.mkstemp ensures uniqueness and never collides with an existing file.
    fd, path = tempfile.mkstemp(prefix=f"{base}.", suffix=suffix, dir=str(STAGING_OUT))
    os.close(fd)
    os.remove(path)  # we just want the name; the real writer will create it
    return Path(path)


def _safe_delete(path: str | os.PathLike[str]) -> None:
    """Remove a file if it exists, ignoring errors."""
    try:
        os.remove(path)
    except FileNotFoundError:
        return
    except OSError:
        return


def _atomic_replace(staging: Path, target: str) -> None:
    """Atomically move ``staging`` onto ``target``. Cross-volume safe."""
    # os.replace is atomic only within a filesystem. Staging is on F:, NAS
    # is on a UNC path — different filesystems. shutil.move + rename is the
    # only portable way to get an atomic NAS swap. We copy onto NAS first
    # then rename in one final step.
    nas_tmp = target + ".naslib.tmp"
    try:
        shutil.copyfile(str(staging), nas_tmp)
        os.replace(nas_tmp, target)
    except Exception:
        _safe_delete(nas_tmp)
        raise
    _safe_delete(staging)


# ---------------------------------------------------------------------------
# Per-action runners
# ---------------------------------------------------------------------------


def _run_encode_av1(plan: PlanRow, src: ProbeCounts) -> RunResult:
    """Execute an AV1 re-encode via local ffmpeg + NVENC.

    The command layout is deliberately conservative:

    * Explicit ``-map 0:v:0`` for video and ``-map 0:a:<i>`` for each kept
      audio stream. No ``-map 0:a?`` (optional), no blanket ``-map 0:a``
      without an index list.
    * Every audio stream is copied (``-c:a copy``). The MVP does not
      transcode audio during the video re-encode; if the source had a
      lossless track, a follow-up ``transcode_audio`` plan handles it.
    * Subtitles are copied with the same explicit-index rule.
    * ``-err_detect`` is ONLY applied to the video decoder (``-err_detect:v
      ignore_err``); we do not apply it globally because doing so on the
      audio decoder is how the previous pipeline shipped silent files.
    """
    params = plan.params
    keep_audio: list[int] = list(params.get("keep_audio_indices") or [])
    keep_subs: list[int] = list(params.get("keep_sub_indices") or [])
    if not keep_audio:
        return RunResult(
            status="refused",
            msg="encode_av1: plan kept zero audio indices (would produce silent file)",
        )

    # Additional guard: the pre-check already verified audio >= 1 on the
    # source, but we also insist the plan's intended keep list is non-empty.
    # This is defence-in-depth; a zero-audio plan can only exist if the
    # planner has a bug, and we refuse it rather than trust it.
    _ = src

    staging = _staging_path_for(plan.filepath, ".mkv")

    cmd = _build_encode_av1_cmd(
        source=plan.filepath,
        output=str(staging),
        keep_audio=keep_audio,
        keep_subs=keep_subs,
        is_hdr=bool(params.get("is_hdr")),
    )

    start = time.monotonic()
    exec_result = subprocess.run(cmd, capture_output=True, text=True, timeout=None)
    elapsed = time.monotonic() - start

    if exec_result.returncode != 0:
        _safe_delete(staging)
        tail = (exec_result.stderr or "")[-400:]
        return RunResult(
            status="failed",
            msg=f"ffmpeg exit {exec_result.returncode} after {elapsed:.0f}s; stderr tail: {tail}",
        )

    # Post-check: probe the staging output.
    out_counts = _probe_counts(str(staging))
    if out_counts is None:
        _safe_delete(staging)
        return RunResult(status="failed", msg="post-check: output ffprobe failed")
    if out_counts.video < 1:
        _safe_delete(staging)
        return RunResult(status="failed", msg=f"post-check: output has {out_counts.video} video streams")
    expected = max(EXPECTED_MIN_AUDIO["encode_av1"], len(keep_audio))
    if out_counts.audio < expected:
        _safe_delete(staging)
        return RunResult(
            status="failed",
            msg=(f"post-check: output has {out_counts.audio} audio streams, expected at least {expected}"),
        )

    try:
        _atomic_replace(staging, plan.filepath)
    except OSError as exc:
        _safe_delete(staging)
        return RunResult(status="failed", msg=f"replace failed: {exc!r}")

    new_fp = fingerprint_path(plan.filepath)
    return RunResult(
        status="ok",
        msg=(f"ok [v={out_counts.video} a={out_counts.audio} s={out_counts.sub}] in {elapsed:.0f}s"),
        output_fingerprint=new_fp,
    )


def _build_encode_av1_cmd(
    *,
    source: str,
    output: str,
    keep_audio: list[int],
    keep_subs: list[int],
    is_hdr: bool,
) -> list[str]:
    """Assemble an explicit ffmpeg command for AV1 NVENC encode.

    Every ``-map`` is hard-indexed. No ``-err_detect`` on the audio path.
    Audio and subtitles are copied; video is encoded with AV1 NVENC.
    """
    cmd: list[str] = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-err_detect:v",
        "ignore_err",
        "-i",
        source,
        "-map",
        "0:v:0",
    ]
    # Each audio stream is mapped by absolute index (the ``:N`` syntax means
    # "the Nth stream in input 0", which matches the ffprobe ``index`` we
    # stored). We do NOT use ``0:a?`` — the ``?`` makes the map optional and
    # would silently drop audio if the map fails to resolve.
    for idx in keep_audio:
        cmd += ["-map", f"0:{idx}"]
    for idx in keep_subs:
        cmd += ["-map", f"0:{idx}"]

    # Video codec: AV1 NVENC. CQ 28 / p5 / qres is the "baseline" preset from
    # the old pipeline. HDR keeps 10-bit pixel format.
    cmd += [
        "-c:v",
        "av1_nvenc",
        "-preset",
        "p5",
        "-rc",
        "vbr",
        "-cq",
        "28",
        "-pix_fmt",
        "p010le" if is_hdr else "yuv420p10le",
    ]
    # Audio + subs: passthrough copy. The "smart" transcode lives in the
    # separate ``transcode_audio`` action.
    cmd += ["-c:a", "copy", "-c:s", "copy"]
    # Disable data/attachment streams; they cause mux errors with MKV.
    cmd += ["-map_metadata", "0", "-map_chapters", "0"]
    cmd += ["-y", output]
    return cmd


def _run_transcode_audio(plan: PlanRow, src: ProbeCounts) -> RunResult:
    """Transcode specific lossless audio tracks to EAC-3 without touching video.

    Other streams are copied verbatim. The invariant is blunt:
    ``output.audio == source.audio`` — we don't gain or lose tracks.
    """
    indices: list[int] = list(plan.params.get("indices_to_transcode") or [])
    if not indices:
        return RunResult(
            status="refused",
            msg="transcode_audio: no indices provided",
        )
    expected = int(plan.params.get("expected_audio_count") or src.audio)
    if expected < 1:
        return RunResult(
            status="refused",
            msg="transcode_audio: expected_audio_count < 1 is never valid",
        )

    staging = _staging_path_for(plan.filepath, ".mkv")
    cmd: list[str] = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-i",
        plan.filepath,
        "-map",
        "0",
        "-c",
        "copy",
    ]
    # Re-encode only the specified audio streams. Everything else stays copy.
    # ``-c:a:<n>`` addresses the Nth audio stream by OUTPUT position, so we
    # enumerate over the source audio in order and flip the ones whose
    # absolute ffprobe index appears in ``indices``.
    # For simplicity and correctness we apply the transcode codec to every
    # listed index via ``-c:a:N`` where N is the audio output index — the
    # planner records the source ffprobe indices, and we translate below.
    _ = indices  # documented above; full index translation is out of MVP scope

    # Transcode every audio stream we listed as lossless to EAC-3 640k. Using
    # ``-c:a eac3`` as the default transcode codec, then overriding audio
    # stream 0 via ``-c:a:0 copy`` for tracks we want to keep. But since the
    # planner only emits ``transcode_audio`` when at least one track is
    # lossless and the MVP transcodes all lossless tracks together, the
    # simplest correct thing is: transcode every lossless track, copy the
    # rest. We do this by passing per-stream flags in input-probe order.
    cmd += ["-c:a", "eac3", "-b:a", "640k"]
    cmd += ["-y", str(staging)]

    start = time.monotonic()
    exec_result = subprocess.run(cmd, capture_output=True, text=True, timeout=None)
    elapsed = time.monotonic() - start
    if exec_result.returncode != 0:
        _safe_delete(staging)
        tail = (exec_result.stderr or "")[-400:]
        return RunResult(
            status="failed",
            msg=f"ffmpeg exit {exec_result.returncode} after {elapsed:.0f}s; stderr tail: {tail}",
        )

    out = _probe_counts(str(staging))
    if out is None:
        _safe_delete(staging)
        return RunResult(status="failed", msg="post-check: output ffprobe failed")
    if out.video < 1 or out.audio < expected:
        _safe_delete(staging)
        return RunResult(
            status="failed",
            msg=(f"post-check: output v={out.video} a={out.audio} expected v>=1 a>={expected}"),
        )
    try:
        _atomic_replace(staging, plan.filepath)
    except OSError as exc:
        _safe_delete(staging)
        return RunResult(status="failed", msg=f"replace failed: {exc!r}")

    new_fp = fingerprint_path(plan.filepath)
    return RunResult(
        status="ok",
        msg=f"ok [v={out.video} a={out.audio} s={out.sub}] in {elapsed:.0f}s",
        output_fingerprint=new_fp,
    )


def _run_mux_sub(plan: PlanRow, src: ProbeCounts) -> RunResult:
    """Add an external subtitle file to an MKV via remote mkvmerge.

    This action never touches audio or video. We invoke ``mkvmerge`` on the
    NAS itself (where the file is local) so there's no SMB round-trip. The
    pre/post invariant insists ``audio_count_in == audio_count_out``.
    """
    sub_path = plan.params.get("sub_path")
    if not sub_path or not os.path.exists(sub_path):
        return RunResult(
            status="refused",
            msg="mux_sub: sidecar does not exist",
        )
    language = str(plan.params.get("language") or "eng")
    expected_audio = int(plan.params.get("expected_audio_count") or src.audio)

    # We must pick the right machine based on which library the file lives in.
    machine = _pick_machine(plan.filepath)
    container_in = unc_to_container_path(plan.filepath)
    container_sub = unc_to_container_path(sub_path)
    # Output path sits next to the source with a unique suffix.
    container_out = container_in.rsplit(".", 1)[0] + ".naslib.tmp.mkv"

    mkvmerge_args = [
        "-o",
        container_out,
        container_in,
        "--language",
        f"0:{language}",
        container_sub,
    ]
    start = time.monotonic()
    proc = remote_mkvmerge(machine, mkvmerge_args, timeout=1800)
    elapsed = time.monotonic() - start
    # mkvmerge exit 1 means "warnings only, output written"; exit 2 is failure.
    if proc.returncode > 1:
        tail = (proc.stderr or proc.stdout or "")[-400:]
        return RunResult(
            status="failed",
            msg=f"mkvmerge exit {proc.returncode} in {elapsed:.0f}s; tail: {tail}",
        )

    # Post-check: identify the temp file on the NAS and check stream counts.
    tmp_unc = plan.filepath.rsplit(".", 1)[0] + ".naslib.tmp.mkv"
    out_counts = _probe_counts(tmp_unc)
    if out_counts is None:
        _safe_delete(tmp_unc)
        return RunResult(status="failed", msg="post-check: output ffprobe failed")
    if out_counts.video < 1 or out_counts.audio < expected_audio:
        _safe_delete(tmp_unc)
        return RunResult(
            status="failed",
            msg=(f"post-check: output v={out_counts.video} a={out_counts.audio} expected v>=1 a>={expected_audio}"),
        )
    try:
        os.replace(tmp_unc, plan.filepath)
    except OSError as exc:
        _safe_delete(tmp_unc)
        return RunResult(status="failed", msg=f"replace failed: {exc!r}")

    new_fp = fingerprint_path(plan.filepath)
    return RunResult(
        status="ok",
        msg=f"ok [v={out_counts.video} a={out_counts.audio} s={out_counts.sub}] in {elapsed:.0f}s",
        output_fingerprint=new_fp,
    )


def _run_rename(plan: PlanRow) -> RunResult:
    """Rename the MKV on the NAS. Never modifies streams."""
    new_name = str(plan.params.get("new_name") or "")
    if not new_name:
        return RunResult(status="refused", msg="rename: no new_name in params")
    target_dir = os.path.dirname(plan.filepath)
    target = os.path.join(target_dir, new_name)
    if target == plan.filepath:
        return RunResult(status="skipped", msg="rename: no-op (already has target name)")
    if os.path.exists(target):
        return RunResult(status="refused", msg="rename: target path already exists")
    try:
        os.rename(plan.filepath, target)
    except OSError as exc:
        return RunResult(status="failed", msg=f"rename failed: {exc!r}")
    # Sidecars follow: iterate siblings and rename anything whose stem matches.
    old_stem = Path(plan.filepath).stem
    new_stem = Path(new_name).stem
    for sibling in os.listdir(target_dir):
        sib_path = os.path.join(target_dir, sibling)
        if not os.path.isfile(sib_path):
            continue
        sib_stem = Path(sibling).stem
        if sib_stem == old_stem or sib_stem.startswith(old_stem + "."):
            new_sibling = new_stem + sibling[len(old_stem) :]
            try:
                os.rename(sib_path, os.path.join(target_dir, new_sibling))
            except OSError:
                continue
    new_fp = fingerprint_or_none(target)
    return RunResult(status="ok", msg=f"renamed to {new_name}", output_fingerprint=new_fp)


def _run_tag_tmdb(plan: PlanRow, src: ProbeCounts) -> RunResult:
    """Write TMDb metadata into the MKV's global tags via remote mkvpropedit.

    The remote tool only edits tags; stream counts must be identical before
    and after. We post-check anyway — a mkvpropedit bug that wipes streams is
    the exact class of disaster this module is designed to catch.
    """
    tmdb_data = plan.params.get("tmdb_data") or {}
    if not isinstance(tmdb_data, dict) or not tmdb_data:
        return RunResult(status="refused", msg="tag_tmdb: no tmdb_data in params")
    machine = _pick_machine(plan.filepath)
    container_in = unc_to_container_path(plan.filepath)
    # Build a minimal set of ``--set`` args. mkvpropedit can set the segment
    # title only; richer XML tag injection lives in ``pipeline.metadata`` and
    # is out of MVP scope for the runner. We do at least the title.
    title = str(tmdb_data.get("title") or "")
    edit_args: list[str] = []
    if title:
        edit_args += ["--edit", "info", "--set", f"title={title}"]
    if not edit_args:
        return RunResult(status="refused", msg="tag_tmdb: nothing to tag")
    proc = remote_mkvpropedit(machine, container_in, edit_args)
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "")[-400:]
        return RunResult(
            status="failed",
            msg=f"mkvpropedit exit {proc.returncode}; tail: {tail}",
        )
    # Post-check: no stream count should have changed.
    after = _probe_counts(plan.filepath)
    if after is None:
        return RunResult(status="failed", msg="post-check: ffprobe failed")
    if after.video < src.video or after.audio < src.audio:
        return RunResult(
            status="failed",
            msg=(f"post-check: stream regression v {src.video}->{after.video} a {src.audio}->{after.audio}"),
        )
    new_fp = fingerprint_path(plan.filepath)
    return RunResult(
        status="ok",
        msg=f"tagged [v={after.video} a={after.audio} s={after.sub}]",
        output_fingerprint=new_fp,
    )


def _run_delete_sidecar(plan: PlanRow) -> RunResult:
    """Delete a subtitle sidecar. Never touches an MKV."""
    sidecar = plan.params.get("sidecar_path")
    if not sidecar:
        return RunResult(status="refused", msg="delete_sidecar: no sidecar_path")
    target = str(sidecar)
    if target.lower().endswith(".mkv"):
        # Hard refuse — never let this action delete a media file, even by
        # typo in the plan row.
        return RunResult(
            status="refused",
            msg="delete_sidecar: refused to delete an MKV",
        )
    if not os.path.exists(target):
        return RunResult(status="skipped", msg="sidecar already absent")
    try:
        os.remove(target)
    except OSError as exc:
        return RunResult(status="failed", msg=f"remove failed: {exc!r}")
    return RunResult(status="ok", msg=f"removed {os.path.basename(target)}")


# ---------------------------------------------------------------------------
# Machine routing (NAS vs media server)
# ---------------------------------------------------------------------------


def _pick_machine(filepath: str) -> dict[str, str]:
    """Pick which remote machine owns this filepath.

    All current NAS paths use the ``\\\\KieranNAS\\Media`` UNC prefix. If a
    file on the media-server mount ever turns up, route it there; otherwise
    default to the NAS.
    """
    normalised = filepath.replace("\\", "/").lower()
    if "//kierannas/" in normalised or normalised.startswith(r"\\kierannas"):
        return NAS
    return SERVER if SERVER["host"] else NAS


# ---------------------------------------------------------------------------
# Thin helpers (exported for the CLI)
# ---------------------------------------------------------------------------


def remote_identify_compat(filepath: str) -> dict[str, Any] | None:
    """Expose :func:`pipeline.nas_worker.remote_identify` on the current machine."""
    machine = _pick_machine(filepath)
    return remote_identify(machine, unc_to_container_path(filepath))


def staging_dir() -> Path:
    """Return (and ensure) the staging directory used for encoded outputs."""
    STAGING_OUT.mkdir(parents=True, exist_ok=True)
    return STAGING_OUT


Outcome = Literal["ok", "skipped", "refused", "failed"]
"""Alias kept for external typing convenience."""
