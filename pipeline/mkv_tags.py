"""Shared MKV global-tag read + merge utilities.

mkvpropedit's ``--tags global:file.xml`` flag *replaces* the entire
global-tag block. Multiple writers in this codebase want to own
different parts of the global tag set:

  * ``pipeline.full_gamut._stamp_encode_metadata`` writes
    ``ENCODER`` / ``CQ`` / ``CONTENT_GRADE``
  * ``pipeline.metadata.write_tmdb_to_mkv`` writes
    ``DIRECTOR`` / ``GENRE`` / ``ACTOR`` / ``WRITTEN_BY`` / etc.
  * ``pipeline.grade_review`` writes ``GRADE_REVIEW`` / ``GRADE_REVIEW_AT``

Before this module each writer ran ``mkvpropedit --tags global:...``
with only the tags it cared about, so whichever writer ran *last*
wiped the others. The pre-2026-05-04 sample of 50 latest done encodes
showed 0/50 had CQ stamped — the encoder wrote it, then the TMDb
writer clobbered it, every time.

The fix is :func:`merge_global_tags` — each writer declares which tag
names it ``owns`` (the names it would have written on previous runs).
We read the existing tag block via ``mkvextract tags``, drop entries
whose name is in the owned-set, append the writer's new entries, and
push the union back. Anything outside the owned-set is preserved
verbatim.

Why mkvextract and not mkvmerge --identify: ``mkvmerge --identify``
surfaces global tags only as a count (``global_tags[].num_entries``)
and never the actual names/values. ``mkvextract <file> tags -`` writes
the full Matroska tag XML to stdout, which we parse here.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import xml.sax.saxutils as _xml


class MkvTagWriteError(RuntimeError):
    """Raised when mkvpropedit returns a hard failure (rc >= 2).

    Carries the actual error string from mkvpropedit so callers can
    surface a useful message ("file not Matroska or could not be found",
    "permission denied", "format error") instead of a generic
    "mkvpropedit failed".
    """

    def __init__(self, message: str, *, returncode: int, filepath: str):
        super().__init__(message)
        self.returncode = returncode
        self.filepath = filepath


def _find_mkvextract() -> str | None:
    """Locate the mkvextract binary on Windows / PATH. Cheap so not cached."""
    exe = shutil.which("mkvextract")
    if exe:
        return exe
    for candidate in (
        r"C:\Program Files\MKVToolNix\mkvextract.exe",
        r"C:\Program Files (x86)\MKVToolNix\mkvextract.exe",
    ):
        if os.path.isfile(candidate):
            return candidate
    return None


def read_global_tags(filepath: str, *, timeout: int = 60) -> list[dict]:
    """Return ``[{'name': str, 'value': str}]`` for every global SimpleTag.

    Returns ``[]`` on tool failure (mkvextract not installed, file
    missing, parse error). Track-level tags (per-stream DURATION,
    per-stream ENCODER) are filtered out — only tags whose Targets
    element is empty or carries the global TargetTypeValue (50) survive.

    Order is preserved so a round-trip read → write doesn't shuffle the
    on-disk layout.
    """
    exe = _find_mkvextract()
    if not exe:
        return []
    try:
        result = subprocess.run(
            [exe, filepath, "tags", "-"],
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    if result.returncode != 0 or not result.stdout:
        return []
    raw = result.stdout
    if raw.startswith("﻿"):
        raw = raw[1:]
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return []

    out: list[dict] = []
    for tag in root.findall("Tag"):
        targets = tag.find("Targets")
        if targets is None:
            is_global = True
        else:
            children = list(targets)
            if not children:
                is_global = True
            elif (
                len(children) == 1
                and targets.find("TargetTypeValue") is not None
                and (targets.find("TargetTypeValue").text or "").strip() == "50"
            ):
                is_global = True
            else:
                is_global = False
        if not is_global:
            continue
        for simple in tag.findall("Simple"):
            name_el = simple.find("Name")
            value_el = simple.find("String")
            if name_el is None:
                continue
            name = (name_el.text or "").strip()
            if not name:
                continue
            value = (value_el.text if value_el is not None else "") or ""
            out.append({"name": name, "value": str(value)})
    return out


def _build_tag_xml(tags: list[dict]) -> str:
    """Render a list of {name, value} tags as mkvpropedit-compatible XML.

    Always emits at TargetTypeValue 50 (movie/episode = global). Empty
    tag list produces an empty <Tags/> block, which clears all global
    tags — useful only as the inner of a clear operation.
    """
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<!DOCTYPE Tags SYSTEM "matroskatags.dtd">',
        "<Tags>",
    ]
    if tags:
        parts.append("  <Tag>")
        parts.append("    <Targets><TargetTypeValue>50</TargetTypeValue></Targets>")
        for t in tags:
            name = _xml.escape(t["name"])
            value = _xml.escape(str(t["value"]))
            parts.append(
                f"    <Simple><Name>{name}</Name><String>{value}</String></Simple>"
            )
        parts.append("  </Tag>")
    parts.append("</Tags>")
    return "\n".join(parts)


def _parse_mkvpropedit_error(raw: str) -> str:
    """Pick the actual ``Error:`` line out of mkvpropedit's verbose output.

    Output usually looks like:
        "The file is being analyzed.\\n"
        "Error: Modification of properties in the section ...\\n"
    The progress-line-as-error parsing trap meant earlier toasts said
    "Cannot write tag: The file is being analyzed." which was useless.
    """
    err = ""
    for line in (raw or "").splitlines():
        stripped = line.strip()
        if stripped.startswith("Error:"):
            err = stripped[len("Error:"):].strip()
            break
    if not err:
        lines = [ln.strip() for ln in (raw or "").splitlines() if ln.strip()]
        err = lines[-1] if lines else ""
    return err[:300]


# Errors that mean the file's structure is broken in a way mkvpropedit
# can't navigate, but mkvmerge CAN read + remux into a clean structure.
# When we hit one of these on a tag write, retry after a stream-copy
# remux. The remuxed file replaces the original (atomic rename) so
# subsequent operations see the cleaned structure.
_REMUX_RECOVERABLE_PATTERNS = (
    "no corresponding level 1 element was found",
    "the file has not been modified",
    "could not find a valid",  # mkvpropedit "could not find a valid Tracks element"
)


def _try_remux_in_place(filepath: str, *, timeout: int = 600) -> bool:
    """Stream-copy ``filepath`` through mkvmerge to repair its container.

    Used as a recovery step when mkvpropedit can't navigate the file's
    EBML structure to write tags. mkvmerge is more tolerant of malformed
    containers than mkvpropedit — it can read a file with damaged
    SeekHead / Cues / etc. and write out a clean copy.

    Replaces the original via ``os.replace`` only on mkvmerge rc 0/1
    (1 = warnings but output usable). Source file untouched on hard
    failure (rc >= 2) — caller falls through to the original tag-write
    error.

    Returns True if the remux landed and the original was replaced.
    """
    import shutil as _shutil  # noqa: PLC0415
    import subprocess as _subprocess  # noqa: PLC0415

    mkvm = _shutil.which("mkvmerge")
    if not mkvm:
        for candidate in (
            r"C:\Program Files\MKVToolNix\mkvmerge.exe",
            r"C:\Program Files (x86)\MKVToolNix\mkvmerge.exe",
        ):
            if os.path.isfile(candidate):
                mkvm = candidate
                break
    if not mkvm:
        return False

    tmp_out = filepath + ".remux_tmp.mkv"
    if os.path.exists(tmp_out):
        try:
            os.remove(tmp_out)
        except OSError:
            return False
    try:
        result = _subprocess.run(
            [mkvm, "-o", tmp_out, filepath],
            capture_output=True, text=True, timeout=timeout,
            encoding="utf-8", errors="replace",
        )
    except (OSError, _subprocess.TimeoutExpired) as e:
        logging.warning(f"  remux subprocess error on {os.path.basename(filepath)}: {e!r}")
        try:
            os.remove(tmp_out)
        except OSError:
            pass
        return False

    # rc 0 = success, rc 1 = warnings (output still usable), rc >=2 = fail.
    if result.returncode >= 2 or not os.path.exists(tmp_out):
        logging.warning(
            f"  remux failed for {os.path.basename(filepath)}: "
            f"rc={result.returncode} {_parse_mkvpropedit_error(result.stdout)}"
        )
        try:
            os.remove(tmp_out)
        except OSError:
            pass
        return False

    # === SAFETY GUARDS before atomic replace ===
    # The 2026-05-06 incident: Million Dollar Baby had a valid EBML header
    # but ZERO tracks (some pre-existing structural issue). mkvmerge
    # happily produced a 4 KB header-only stub and rc=0; my naive
    # os.replace wiped a 3 GB source file with the stub. NEVER AGAIN.
    #
    # Three checks, ALL must pass before we replace:
    #   1. Output size is at least 50% of source (mkvmerge stream-copy
    #      overhead is well under that on real content)
    #   2. mkvmerge --identify on the output reports >=1 track
    #   3. Track count matches the source's track count exactly (no
    #      silent track loss)
    try:
        src_size = os.path.getsize(filepath)
        out_size = os.path.getsize(tmp_out)
    except OSError:
        src_size = out_size = 0

    if src_size > 0 and out_size < src_size * 0.5:
        logging.error(
            f"  remux output suspiciously small "
            f"({out_size:,} B vs source {src_size:,} B) — REFUSING to replace, "
            f"file would have been destroyed"
        )
        try:
            os.remove(tmp_out)
        except OSError:
            pass
        return False

    # Probe both files for track count
    def _track_count(path: str) -> int:
        try:
            r = _subprocess.run(
                [mkvm, "--identification-format", "json", "--identify", path],
                capture_output=True, text=True, timeout=60,
                encoding="utf-8", errors="replace",
            )
            if r.returncode != 0:
                return -1
            import json as _json
            data = _json.loads(r.stdout or "{}")
            return len(data.get("tracks") or [])
        except Exception:
            return -1

    src_tracks = _track_count(filepath)
    out_tracks = _track_count(tmp_out)
    if out_tracks < 1:
        logging.error(
            f"  remux output has 0 tracks — REFUSING to replace, "
            f"file would have been destroyed (source had {src_tracks} tracks)"
        )
        try:
            os.remove(tmp_out)
        except OSError:
            pass
        return False
    if src_tracks > 0 and out_tracks != src_tracks:
        logging.error(
            f"  remux track-count mismatch: source {src_tracks}, output {out_tracks} — "
            f"REFUSING to replace, would lose {src_tracks - out_tracks} track(s)"
        )
        try:
            os.remove(tmp_out)
        except OSError:
            pass
        return False

    # Guards passed. Atomic replace.
    try:
        os.replace(tmp_out, filepath)
    except OSError as e:
        logging.warning(f"  remux replace failed: {e!r}")
        try:
            os.remove(tmp_out)
        except OSError:
            pass
        return False

    logging.info(
        f"  Remuxed {os.path.basename(filepath)} via mkvmerge — "
        f"container structure repaired ({out_tracks} tracks, {out_size:,} B)"
    )
    return True


def _write_tag_xml(filepath: str, xml_body: str, *, timeout: int = 60) -> bool:
    """Write tag XML via mkvpropedit.

    Returns True on rc < 2 (success or warnings). Raises
    :class:`MkvTagWriteError` with the actual error string on rc >= 2 so
    callers can surface a precise reason — generic "mkvpropedit failed"
    is useless when the underlying problem is "file no longer exists"
    or "not a valid Matroska file".

    On structural-mismatch errors (the 2026-05-06 "no corresponding level 1
    element was found" class) the function attempts to repair the file by
    remuxing it through mkvmerge — which is more tolerant of malformed
    EBML than mkvpropedit — and retries the write once. Files that fail
    on a fundamentally-corrupt source (Paths of Glory class) still error
    out cleanly because mkvmerge also bails on those.
    """
    from pipeline import local_mux  # noqa: PLC0415

    def _attempt_write() -> tuple[int, str]:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".xml", delete=False, encoding="utf-8"
        ) as f:
            f.write(xml_body)
            xml_path = f.name
        try:
            result = local_mux.local_mkvpropedit(
                filepath, ["--tags", f"global:{xml_path}"], timeout=timeout
            )
            return result.returncode, ((result.stdout or result.stderr) or "")
        finally:
            try:
                os.remove(xml_path)
            except OSError:
                pass

    rc, raw = _attempt_write()
    if rc < 2:
        return True

    err = _parse_mkvpropedit_error(raw)
    err_low = err.lower()
    is_structural = any(p in err_low for p in _REMUX_RECOVERABLE_PATTERNS)

    if is_structural:
        logging.info(
            f"  mkvpropedit hit structural error on {os.path.basename(filepath)} "
            f"({err[:80]}) — attempting mkvmerge remux + retry"
        )
        if _try_remux_in_place(filepath):
            rc2, raw2 = _attempt_write()
            if rc2 < 2:
                return True
            # Remux succeeded but tag write still fails — log both
            err2 = _parse_mkvpropedit_error(raw2)
            logging.warning(
                f"  Tag write still failed after remux on {os.path.basename(filepath)}: {err2[:120]}"
            )
            err = err2
            rc = rc2

    logging.warning(
        "  mkvpropedit rc=%s on %s: %s",
        rc, os.path.basename(filepath), err,
    )
    raise MkvTagWriteError(
        err or f"mkvpropedit returned rc={rc}",
        returncode=rc,
        filepath=filepath,
    )


def merge_global_tags(
    filepath: str,
    *,
    owned_names: set[str] | frozenset[str],
    new_tags: list[dict],
    timeout: int = 60,
) -> bool:
    """Patch the global tag block, preserving tags outside ``owned_names``.

    Args:
        filepath: MKV path (UNC or local).
        owned_names: Tag names the caller claims authority over. Existing
            global tags with these names get dropped before the merge.
            Compared case-insensitively.
        new_tags: ``[{'name', 'value'}]`` to append after the dropouts.
        timeout: mkvpropedit timeout in seconds.

    Returns True on mkvpropedit rc < 2 (success or warnings). False on
    hard failure — caller decides whether to surface or retry.

    Pass ``new_tags=[]`` with a populated ``owned_names`` to *clear*
    those tags without writing replacements (used by
    ``grade_review.clear_grade_review``).
    """
    upper_owned = {n.upper() for n in owned_names}
    existing = read_global_tags(filepath, timeout=timeout)
    preserved = [t for t in existing if t["name"].upper() not in upper_owned]
    merged = preserved + new_tags
    return _write_tag_xml(filepath, _build_tag_xml(merged), timeout=timeout)
