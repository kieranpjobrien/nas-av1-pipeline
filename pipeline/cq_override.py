"""Per-file CQ override stored in pipeline_state.db extras.

Lets the user nudge a single file's encode CQ without touching the
grade rules. Use case: the dashboard shows "Proposed CQ 28" for an
upcoming non-AV1 encode; the user clicks +1 because they know this
particular film has fine grain that needs more bits, or -1 because
it's a flat-shaded animated comedy that can take harsher compression
than the grade rule chose.

The override is read by ``pipeline.full_gamut`` right after
``resolve_encode_params`` and replaces ``params["cq"]``. State DB is
the source of truth — no duplicate config files, no environment vars.
The encoder logs both the auto-computed value and the override so the
history entry shows what was overridden.

Bounds: clamped to ``[18, 45]`` matching ``content_grade``'s absolute
floor/ceiling. Anything outside that range is rejected at the API.

Persistence model:
- Write: stamp ``cq_override`` into ``extras`` JSON for the row.
- Read: pull from ``extras`` on each encoder pass.
- Clear: drop the key (not setting to ``None``).

Once a file finishes encoding, the override stays on the row — the
extras blob is also where ``encode_params_used`` lives so the audit
can see what CQ was actually used. Setting cq_override on a done file
only affects the NEXT re-encode (which there usually won't be unless
the audit flags the file as too_low or the user requeues).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

# Same bounds as content_grade._ABSOLUTE_MIN_CQ / _ABSOLUTE_MAX_CQ.
# AV1 supports 0-63 but anything below 18 is wasteful (visually lossless,
# huge files) and above 45 produces visible blocking on simple content.
CQ_MIN = 18
CQ_MAX = 45


def get_override(state_db: str | Path, filepath: str) -> int | None:
    """Return the user-set CQ override for ``filepath``, or ``None``.

    Quietly returns ``None`` for any DB error, missing row, or invalid
    extras JSON — the override is best-effort metadata, not a hard
    requirement. Caller falls back to the computed grade target.
    """
    try:
        con = sqlite3.connect(str(state_db))
        cur = con.cursor()
        row = cur.execute(
            "SELECT extras FROM pipeline_files WHERE filepath = ?", (filepath,)
        ).fetchone()
        con.close()
    except sqlite3.Error:
        return None
    if not row or not row[0]:
        return None
    try:
        extras = json.loads(row[0])
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    cq = extras.get("cq_override")
    if not isinstance(cq, int):
        return None
    if not (CQ_MIN <= cq <= CQ_MAX):
        # Invalid value somehow landed in the DB — treat as no override
        # rather than clamp silently. Caller decides.
        logging.warning(f"  cq_override out of bounds for {filepath}: {cq}")
        return None
    return cq


def set_override(state_db: str | Path, filepath: str, cq: int) -> bool:
    """Store ``cq_override`` in the row's extras blob.

    Creates the row if it doesn't exist (status='pending'). Validates
    bounds — raises ``ValueError`` for out-of-range CQs so the API
    layer can return a 400.

    Returns True on a successful write, False on DB error.
    """
    if not isinstance(cq, int) or not (CQ_MIN <= cq <= CQ_MAX):
        raise ValueError(f"cq must be an int in [{CQ_MIN}, {CQ_MAX}]; got {cq!r}")

    try:
        con = sqlite3.connect(str(state_db))
        cur = con.cursor()
        row = cur.execute(
            "SELECT extras FROM pipeline_files WHERE filepath = ?", (filepath,)
        ).fetchone()
        if row is None:
            extras: dict[str, Any] = {"cq_override": cq}
            cur.execute(
                "INSERT INTO pipeline_files (filepath, status, extras) VALUES (?, 'pending', ?)",
                (filepath, json.dumps(extras)),
            )
        else:
            try:
                extras = json.loads(row[0]) if row[0] else {}
            except (TypeError, ValueError, json.JSONDecodeError):
                extras = {}
            extras["cq_override"] = cq
            cur.execute(
                "UPDATE pipeline_files SET extras = ? WHERE filepath = ?",
                (json.dumps(extras), filepath),
            )
        con.commit()
        con.close()
        return True
    except sqlite3.Error as e:
        logging.warning(f"  set_override DB error for {filepath}: {e}")
        return False


def clear_override(state_db: str | Path, filepath: str) -> bool:
    """Remove ``cq_override`` from the row's extras. No-op if absent.

    Returns True if the row was found and updated (or already had no
    override), False on DB error.
    """
    try:
        con = sqlite3.connect(str(state_db))
        cur = con.cursor()
        row = cur.execute(
            "SELECT extras FROM pipeline_files WHERE filepath = ?", (filepath,)
        ).fetchone()
        if row is None or not row[0]:
            con.close()
            return True  # nothing to clear
        try:
            extras = json.loads(row[0])
        except (TypeError, ValueError, json.JSONDecodeError):
            con.close()
            return True
        if "cq_override" not in extras:
            con.close()
            return True
        del extras["cq_override"]
        cur.execute(
            "UPDATE pipeline_files SET extras = ? WHERE filepath = ?",
            (json.dumps(extras), filepath),
        )
        con.commit()
        con.close()
        return True
    except sqlite3.Error as e:
        logging.warning(f"  clear_override DB error for {filepath}: {e}")
        return False


def compute_proposed_cq(item: dict, config: dict | None = None) -> dict:
    """Return ``{cq, base_cq, content_grade, cq_offset, res_key}`` for an entry.

    Uses the same logic as ``resolve_encode_params`` but without the
    NVENC-specific fields (preset, multipass, etc.). Lets the API
    surface a "what the encoder would pick" value for files that
    haven't been encoded yet, so the user sees a Proposed CQ in the
    Inspector and can adjust it before the file enters the queue.
    """
    from pipeline.config import build_config  # noqa: PLC0415
    from pipeline.content_grade import target_cq  # noqa: PLC0415

    if config is None:
        config = build_config()

    library_type = item.get("library_type", "movie")
    content_type = "series" if library_type in ("series", "show", "tv", "anime") else "movie"

    # Item shape from /api/file-detail: media_report entry has video.resolution_class
    # and video.hdr; the `resolve_encode_params` callsite uses the older flat
    # `resolution` / `hdr` keys. Accept both.
    video = item.get("video") or {}
    resolution = video.get("resolution_class") or item.get("resolution") or "1080p"
    is_hdr = video.get("hdr") if "hdr" in video else item.get("hdr", False)

    if resolution == "4K" and is_hdr:
        res_key = "4K_HDR"
    elif resolution == "4K":
        res_key = "4K_SDR"
    elif resolution in ("1080p", "720p", "480p", "SD"):
        res_key = resolution
    else:
        res_key = "SD"

    base_cq = config["cq"].get(content_type, {}).get(res_key, 30)
    final_cq, content_grade, applied_offset = target_cq(base_cq, item)

    return {
        "cq": final_cq,
        "base_cq": base_cq,
        "cq_offset": applied_offset,
        "content_grade": content_grade,
        "res_key": res_key,
        "content_type": content_type,
    }
