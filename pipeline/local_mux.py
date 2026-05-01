"""Local mkvmerge execution path for gap_filler track strip + sub mux.

Counterpart to ``pipeline.nas_worker`` (which runs mkvmerge inside a Docker
container on the NAS over SSH). The user wanted a local backend in 2026-04-29
because:

  * Concurrent SSH+Docker+mkvmerge on the Synology had previously triggered
    OOM-kills (rc=137) — running it remotely is fast (~10s/file) but stresses
    the NAS, and a single bad file can cascade.
  * Running locally is slower (~2-3 min per file at SMB throughput) but the
    NAS only does what it's good at: serve bytes. No CPU/memory load there.

Architecture: mkvmerge.exe runs locally on Windows. mkvmerge supports UNC
paths natively, so we hand it ``\\\\KieranNAS\\Media\\...`` for both the
input and output (writing to a sibling ``.gapfill_tmp.mkv``). No local
staging — SMB does the I/O, mkvmerge does demux/mux. After success, an
atomic ``os.replace`` swaps the tmp into place.

Same safety gates as remote_strip_and_mux:
  * Empty audio_keep_ids or sub_keep_ids (without no_subs=True) is rejected
    — an empty ``--audio-tracks`` list would strip all audio (the
    2026-04-22 256-file incident).
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional


_MKVMERGE_SEARCH = (
    r"C:\Program Files\MKVToolNix\mkvmerge.exe",
    r"C:\Program Files (x86)\MKVToolNix\mkvmerge.exe",
)


def _find_mkvmerge() -> Optional[str]:
    """Return absolute path to a local mkvmerge binary, or None.

    Checks PATH first, then the standard MKVToolNix install locations.
    """
    found = shutil.which("mkvmerge")
    if found:
        return found
    for candidate in _MKVMERGE_SEARCH:
        if os.path.isfile(candidate):
            return candidate
    return None


def is_available() -> bool:
    """Cheap check used by the orchestrator to decide whether the local mux
    worker is wireable. False means mkvmerge.exe couldn't be located."""
    return _find_mkvmerge() is not None


def local_identify(filepath: str, timeout: int = 60) -> Optional[dict]:
    """Run ``mkvmerge --identify --identification-format json`` on a local path.

    ``filepath`` may be a UNC path; mkvmerge handles that natively. Returns
    parsed JSON on success, None on parse failure or rc >= 2 (warnings, rc=1,
    are still considered usable).
    """
    import json as _json

    exe = _find_mkvmerge()
    if not exe:
        logging.error("local_identify: mkvmerge.exe not found")
        return None
    cmd = [exe, "--identify", "--identification-format", "json", filepath]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, encoding="utf-8", errors="replace"
        )
    except subprocess.TimeoutExpired:
        logging.error(f"local_identify: timed out after {timeout}s on {os.path.basename(filepath)}")
        return None
    if result.returncode > 1 or not result.stdout:
        return None
    try:
        return _json.loads(result.stdout)
    except _json.JSONDecodeError:
        return None


def local_strip_and_mux(
    input_path: str,
    output_path: str,
    audio_keep_ids: list[int] | None = None,
    sub_keep_ids: list[int] | None = None,
    no_subs: bool = False,
    external_sub_paths: list[tuple[str, str]] | None = None,
    timeout: int = 900,
    progress_stall_secs: int = 90,
) -> subprocess.CompletedProcess:
    """Run mkvmerge locally to produce a stripped/muxed output file.

    Mirrors the contract of ``pipeline.nas_worker.remote_strip_and_mux``:
    same args, same return type, same safety gates.

    ``input_path`` and ``output_path`` may be UNC (``\\\\KieranNAS\\Media\\...``)
    or local — mkvmerge handles both. SMB I/O happens inside mkvmerge for UNC
    paths; no local staging needed.

    Args:
        input_path: source MKV path (UNC or local).
        output_path: destination tmp path; caller does the atomic swap.
        audio_keep_ids: absolute mkvmerge track IDs to keep, or None for all.
        sub_keep_ids: absolute mkvmerge track IDs to keep, or None for all.
        no_subs: if True, drop ALL subtitles (overrides sub_keep_ids).
        external_sub_paths: list of ``(path, lang)`` for sidecars to mux in.
        timeout: subprocess wall-clock cap in seconds.

    Returns the CompletedProcess. Caller checks ``.returncode`` —
    rc=0 success, rc=1 warnings (still usable), rc>=2 fatal.

    Raises ValueError on the empty-keep-list gate (matches nas_worker).
    """
    if audio_keep_ids is not None and len(audio_keep_ids) == 0:
        raise ValueError(
            "local_strip_and_mux refused: audio_keep_ids is an empty list. "
            "Pass None to keep all audio tracks, or a non-empty list. "
            "Sending --audio-tracks with no IDs would strip all audio (destructive)."
        )
    if sub_keep_ids is not None and len(sub_keep_ids) == 0 and not no_subs:
        raise ValueError(
            "local_strip_and_mux refused: sub_keep_ids is an empty list "
            "without no_subs=True. Pass no_subs=True to strip all subs "
            "explicitly, or pass None to keep all."
        )

    exe = _find_mkvmerge()
    if not exe:
        raise RuntimeError("mkvmerge.exe not found — install MKVToolNix or set MKVMERGE_PATH")

    args = [exe, "-o", output_path]

    if audio_keep_ids is not None:
        args.extend(["--audio-tracks", ",".join(str(i) for i in audio_keep_ids)])

    if no_subs:
        args.append("--no-subtitles")
    elif sub_keep_ids is not None:
        args.extend(["--subtitle-tracks", ",".join(str(i) for i in sub_keep_ids)])

    args.append(input_path)

    if external_sub_paths:
        for sub_path, lang in external_sub_paths:
            args.extend(["--language", f"0:{lang}", sub_path])

    # Progress watchdog: kill mkvmerge if the output file hasn't grown for
    # ``progress_stall_secs``. 2026-05-01 House S01E17 case: mkvmerge wrote
    # 140 MB then froze for 16+ minutes producing zero bytes — manual kill
    # was the only way out. The wallclock ``timeout`` is a fallback for
    # genuinely-slow-but-progressing runs; the stall watchdog targets the
    # frozen-zero-progress class specifically.
    proc = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8", errors="replace"
    )
    last_size = -1
    last_progress_t = time.monotonic()
    deadline = time.monotonic() + timeout
    while True:
        try:
            stdout, stderr = proc.communicate(timeout=5)
            # process exited
            return subprocess.CompletedProcess(args, proc.returncode, stdout=stdout, stderr=stderr)
        except subprocess.TimeoutExpired:
            now = time.monotonic()
            if now > deadline:
                logging.error(f"local_strip_and_mux: wallclock timeout {timeout}s exceeded; killing mkvmerge")
                proc.kill()
                stdout, stderr = proc.communicate(timeout=10)
                return subprocess.CompletedProcess(args, proc.returncode or -1, stdout=stdout, stderr=stderr)
            # Check output file size for progress
            try:
                cur_size = os.path.getsize(output_path)
            except OSError:
                cur_size = last_size
            if cur_size != last_size:
                last_size = cur_size
                last_progress_t = now
            elif now - last_progress_t >= progress_stall_secs:
                logging.error(
                    f"local_strip_and_mux: mkvmerge stalled — output stuck at "
                    f"{cur_size / 1024**2:.0f} MB for {int(now - last_progress_t)}s "
                    f"(threshold {progress_stall_secs}s); killing"
                )
                proc.kill()
                stdout, stderr = proc.communicate(timeout=10)
                return subprocess.CompletedProcess(args, proc.returncode or -1, stdout=stdout, stderr=stderr)


def local_mkvpropedit(filepath: str, edit_args: list[str], timeout: int = 60) -> subprocess.CompletedProcess:
    """Run mkvpropedit locally on a UNC or local path."""
    exe = shutil.which("mkvpropedit")
    if not exe:
        for candidate in (
            r"C:\Program Files\MKVToolNix\mkvpropedit.exe",
            r"C:\Program Files (x86)\MKVToolNix\mkvpropedit.exe",
        ):
            if os.path.isfile(candidate):
                exe = candidate
                break
    if not exe:
        raise RuntimeError("mkvpropedit.exe not found")
    cmd = [exe, filepath] + list(edit_args)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, encoding="utf-8", errors="replace")
