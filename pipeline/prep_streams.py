"""Pre-encode stream cleanup — strip the things compliance would have
dropped post-encode, but on the LOCAL fetched file BEFORE the GPU
spins up.

The 2026-05-12 / 2026-05-13 "compliance unfixed" loops cost ~10h of
GPU time per day on encodes that the post-encode fixer couldn't
repair. Root cause was architectural: the encoder ran on the full
source (all foreign audio, all foreign subs, commentary tracks), the
gate detected the violations post-encode, then mkvmerge tried to
patch the .av1.tmp output on NAS (slow SMB writes, sequential-index
bugs, stale-probe bugs, etc.).

Better model — the user's original ask weeks ago: do every fixable
strip on the LOCAL fetched file (fast SSD, single mkvmerge call) so
the encoder consumes a guaranteed-clean input. Post-encode
compliance becomes a thin verifier with nothing to fix — its fixer
dispatch becomes dead code by construction.

This module provides:
  * ``compute_audio_drop_indices(item, config)`` — per-type audio
    indices to drop. Reuses the encoder's existing
    ``_select_audio_streams`` to stay consistent with the
    long-running policy work (original-language, KEEP_LANGS,
    keep-1-2-track guard).
  * ``compute_sub_drop_indices(item, config)`` — per-type sub
    indices to drop. Mirrors the foreign_subs +
    extra_eng_subs violations in compliance.py.
  * ``strip_streams_locally(path, item, config)`` — apply the drops
    via :func:`pipeline.compliance_fixers._mkvmerge_drop_streams`.
    Atomic replace; proof-of-work guard re-probes the output and
    refuses to ship if the drop didn't actually happen.

Called from ``pipeline.full_gamut.prepare_for_encode`` after fetch +
qualify + remux, before the GPU encode dispatch. If strip fails
(genuine mkvmerge error, not a stale-index issue), the file is
marked ERROR before the encode is even attempted — zero GPU waste.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Optional


# Keep regex shared with compliance.py — title-based commentary detection.
# Must match the same patterns the post-encode gate uses, else prep won't
# strip things the verifier would later refuse.
_COMMENTARY_TITLE_RE = re.compile(
    r"\b(commentary|director'?s|isolated\s*music|audio\s*description|"
    r"making[- ]of)\b",
    re.I,
)

# Languages allowed for subtitles. Mirrors compliance.py's foreign_subs
# check. English in all its ISO variants + und + zxx (no dialogue).
_SUB_KEEP_LANGS = {"eng", "en", "und", "zxx"}

# Maximum number of "regular" (non-forced, non-SDH) English sub tracks
# the policy allows. Anything beyond gets dropped as extra_eng_subs.
# Matches the constant in compliance.py.
MAX_REGULAR_ENGLISH_SUBS = 1


def compute_audio_drop_indices(item: dict, config: dict) -> list[int]:
    """Per-type audio indices to drop (the inverse of what the encoder
    would keep). Uses the encoder's existing selector so prep and
    encode-time decisions never diverge.

    Returns an empty list if no drops are needed (or if the policy is
    off).
    """
    from pipeline.ffmpeg import _select_audio_streams

    audio_streams = item.get("audio_streams") or []
    if not audio_streams:
        return []

    # Encoder's selector returns "keep these per-type indices" or None
    # meaning "keep all" (the no-strip cases like 1-2 tracks or
    # missing TMDb).
    keep = _select_audio_streams(item, config)
    if keep is None:
        # Encoder doesn't strip — neither does prep.
        keep_set = set(range(len(audio_streams)))
    else:
        keep_set = set(keep)

    # Title-based commentary strip is layered on TOP of the language
    # filter. Even an English-titled commentary track should be dropped
    # by config flag default.
    if config.get("strip_commentary_audio", True):
        for i, a in enumerate(audio_streams):
            title = (a.get("title") or "").strip()
            if title and _COMMENTARY_TITLE_RE.search(title):
                keep_set.discard(i)

    drop = sorted(i for i in range(len(audio_streams)) if i not in keep_set)
    return drop


def compute_sub_drop_indices(item: dict, config: dict) -> list[int]:
    """Per-type sub indices to drop.

    Two categories:
      * foreign_subs — language not in eng/und/zxx
      * extra_eng_subs — regular (non-forced, non-SDH) English subs
        beyond MAX_REGULAR_ENGLISH_SUBS

    Mirrors compliance.py's check_compliance sub-violation logic so
    the verify-only post-encode gate has nothing to refuse.
    """
    sub_streams = item.get("subtitle_streams") or []
    if not sub_streams:
        return []

    if not config.get("strip_non_english_subs", True):
        return []

    drop: set[int] = set()
    eng_regular_seen: list[int] = []
    for i, s in enumerate(sub_streams):
        lang = (s.get("language") or "").lower().strip()
        title = (s.get("title") or "").lower()

        # Foreign-language subtitle — drop unconditionally.
        if lang and lang not in _SUB_KEEP_LANGS:
            drop.add(i)
            continue

        # English / und / zxx subs — track which are "regular" so we can
        # cap them at MAX_REGULAR_ENGLISH_SUBS. Forced and SDH stay.
        if lang in ("eng", "en"):
            is_forced = "forced" in title or s.get("forced") is True
            is_sdh = (
                "sdh" in title
                or "(cc)" in title
                or "hearing impaired" in title
                or s.get("hearing_impaired") is True
            )
            if not is_forced and not is_sdh:
                eng_regular_seen.append(i)

    # Anything past the per-policy max regular-English count gets dropped.
    if len(eng_regular_seen) > MAX_REGULAR_ENGLISH_SUBS:
        drop.update(eng_regular_seen[MAX_REGULAR_ENGLISH_SUBS:])

    return sorted(drop)


def strip_streams_locally(
    local_path: str,
    item: dict,
    config: dict,
) -> tuple[bool, str]:
    """Strip foreign/commentary/extra-sub tracks from ``local_path`` by
    writing the cleaned output to a SIBLING PATH — never modify the
    fetched source file. The encoder consumes the stripped sibling.

    The 2026-05-13 21:03 architectural fix: previous versions used
    ``_mkvmerge_drop_streams`` which atomic-replaced the source.
    That replace fought Windows antivirus / file-cache locks on the
    freshly-fetched file — observed PermissionError 13 failures.
    Writing to a NEW path bypasses the entire lock class because
    nothing competes for the source.

    Returns ``(ok, stripped_path_or_message)``.
      * ``ok=True``  — caller's input is now at ``stripped_path_or_message``
        (could be ``local_path`` itself if nothing needed stripping, or
        a new sibling path if streams were dropped).
      * ``ok=False`` — strip attempted but failed; the value is a
        human-readable reason for state.set_file(ERROR).
    """
    drop_audio = compute_audio_drop_indices(item, config)
    drop_sub = compute_sub_drop_indices(item, config)

    if not drop_audio and not drop_sub:
        # No work needed — encoder uses the fetched file directly.
        return (True, local_path)

    # Safety: never strip ALL audio. The encoder needs at least one
    # audio track to map; an all-audio drop would produce a silent
    # file that the post-encode gate would refuse anyway.
    audio_streams = item.get("audio_streams") or []
    if drop_audio and len(drop_audio) >= len(audio_streams):
        return (False, f"refusing to drop all {len(audio_streams)} audio tracks")

    from pipeline.compliance_fixers import _mkvmerge_drop_streams_to_path

    n_a = len(audio_streams)
    n_s = len(item.get("subtitle_streams") or [])
    # Sibling path next to the fetched file — same directory, suffix
    # ``.stripped.mkv`` so the cleanup pass can find both halves.
    stripped_path = local_path + ".stripped.mkv"
    logging.info(
        f"  prep: stripping streams locally → {os.path.basename(stripped_path)} — "
        f"drop audio={drop_audio} (keep {n_a - len(drop_audio)}) "
        f"sub={drop_sub} (keep {n_s - len(drop_sub)})"
    )

    try:
        ok = _mkvmerge_drop_streams_to_path(
            local_path,
            stripped_path,
            drop_audio_indices=drop_audio or None,
            drop_sub_indices=drop_sub or None,
        )
    except Exception as e:
        return (False, f"local mkvmerge strip raised {type(e).__name__}: {e}")

    if not ok:
        return (False, "local mkvmerge strip failed (rc!=0 or proof-of-work mismatch)")
    # Success — return the stripped sibling path. The encoder consumes
    # that path. Caller owns cleanup of both files when encode is done.
    return (True, stripped_path)

    return (True, f"stripped {len(drop_audio)} audio + {len(drop_sub)} sub track(s)")
