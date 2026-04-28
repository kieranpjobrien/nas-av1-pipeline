"""Sidecar subtitle scanner — single source of truth for on-disk external subs.

Consolidates sidecar scanning logic previously duplicated across:
  - tools/scanner.py::extract_info (external_subs block in media-report probe)
  - pipeline/gap_filler.py::_scan_external_subs
  - pipeline/full_gamut.py::_find_external_subs
  - tools/mux_external_subs.py (external-sub filter inside find_candidates)

The canonical filter is :func:`pick_english_sidecars` which splits sidecars
into "mux this one" vs "delete these" piles using the same logic gap_filler
has been running in production.

NOTE ON SEMANTIC DIFFERENCES preserved (documented, not fixed):
  - Extension list: tools/scanner.py includes ``.vtt`` and ``.idx`` (for
    media-report completeness); gap_filler / full_gamut only look at
    ``.srt/.ass/.ssa/.sub`` when deciding what to mux. This module exposes
    :data:`SCAN_EXTS` (all six) and :data:`MUX_EXTS` (four muxable) so
    callers pick the right set.
  - Stem matching: scanner.py matches ``stem`` or ``stem + "."`` exactly
    (case-insensitive). gap_filler / full_gamut use a looser
    ``filename.startswith(stem[:20])`` prefix (case-sensitive). This module
    follows the scanner.py rule because it's more correct — the 20-char
    prefix would false-match "The Office" sidecars onto "The Office (UK)".
    Existing data flow through gap_filler's scanner output hasn't shown the
    false-match pathology so this tightening is low-risk.
  - Language parsing: scanner.py walks all dot parts looking for a 2/3-letter
    alpha code (first wins, rest become flags). gap_filler only checks for
    .en./.eng./.en-/.eng- tokens. We adopt scanner.py's richer parser so
    non-English sidecars get correct language codes for downstream display.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from pipeline.config import ENG_LANGS
from pipeline.streams import is_hi_external

# All sidecar types we parse into media-report entries.
SCAN_EXTS: frozenset[str] = frozenset({".srt", ".ass", ".ssa", ".sub", ".vtt", ".idx"})

# Subset that we're willing to mux into an MKV as a stream (image-based and
# streaming-only formats like .idx / .vtt get handled separately if at all).
MUX_EXTS: frozenset[str] = frozenset({".srt", ".ass", ".ssa", ".sub"})

# Flag tokens that live in the dotted suffix of a sidecar filename.
_FLAG_TOKENS: frozenset[str] = frozenset({"hi", "sdh", "cc", "forced", "foreign"})


@dataclass
class SidecarSub:
    """A subtitle sidecar located next to a video file."""

    path: str
    filename: str
    stem: str
    language: str
    is_forced: bool
    is_hi: bool


def _parse_language_and_flags(suffix: str) -> tuple[str, list[str]]:
    """Parse the dotted suffix after the video stem into (language, flags).

    Example: ``"en.hi"`` -> ``("en", ["hi"])``; ``"forced"`` -> ``("und", ["forced"])``.

    Follows scanner.py's rule: first 2- or 3-letter alpha token wins as the
    language; everything else becomes a flag.
    """
    language = "und"
    flags: list[str] = []
    for part in (p for p in suffix.split(".") if p):
        if len(part) in (2, 3) and part.isalpha() and language == "und":
            language = part.lower()
        else:
            flags.append(part.lower())
    return language, flags


def scan_sidecars(video_path: str, exts: frozenset[str] = MUX_EXTS) -> list[SidecarSub]:
    """Enumerate subtitle sidecars next to ``video_path``.

    Matches siblings whose stem equals the video stem (case-insensitive) or
    begins with ``video_stem + "."``. Returns a list of :class:`SidecarSub`
    in directory-listing order.

    ``exts`` defaults to :data:`MUX_EXTS` (the files we'd mux). Pass
    :data:`SCAN_EXTS` if you want every subtitle sidecar (including ``.vtt``
    and ``.idx``) for report-building.
    """
    parent = os.path.dirname(video_path)
    video_stem = Path(video_path).stem
    video_stem_lower = video_stem.lower()

    results: list[SidecarSub] = []
    try:
        entries = os.listdir(parent)
    except OSError:
        return results

    for name in entries:
        ext = Path(name).suffix.lower()
        if ext not in exts:
            continue

        sib_path = os.path.join(parent, name)
        if not os.path.isfile(sib_path):
            continue

        sib_stem = Path(name).stem
        sib_stem_lower = sib_stem.lower()
        # Exact match or stem + "."-prefixed extension-suffix (e.g. "Foo.en")
        if not (sib_stem_lower == video_stem_lower or sib_stem_lower.startswith(video_stem_lower + ".")):
            continue

        # Everything after the video stem, stripped of a leading dot.
        suffix = sib_stem[len(video_stem):].lstrip(".")
        language, flags = _parse_language_and_flags(suffix)
        is_forced = "forced" in flags or "foreign" in flags
        # HI detection reuses the filename-based rule from pipeline.streams.
        is_hi = is_hi_external(name)

        results.append(
            SidecarSub(
                path=sib_path,
                filename=name,
                stem=sib_stem,
                language=language,
                is_forced=is_forced,
                is_hi=is_hi,
            )
        )

    return results


def pick_english_sidecars(sidecars: list[SidecarSub]) -> tuple[list[SidecarSub], list[SidecarSub]]:
    """Split sidecars into (to_mux, to_delete).

    Rule (2026-04-28 update):
      - Pick exactly one English sidecar to mux. Prefer regular non-HI;
        fall back to HI if that's all that's available.
      - Everything else (foreign-language sidecars, duplicates, the
        unchosen English variant) goes to ``to_delete``.

    Why the change: Bazarr's providers sometimes only have HI subs
    available for older / niche titles. The previous "regular only"
    rule left those files with no embedded sub at all, and Bazarr
    kept re-grabbing the HI sidecar each scan. Accepting HI as a
    fallback gives every encoded MKV one English track, which then
    satisfies Bazarr's "Treat Embedded Subtitles as Downloaded" check.

    Sidecars are considered in the order given; callers wanting a
    deterministic pick should sort first.
    """
    to_mux: list[SidecarSub] = []
    to_delete: list[SidecarSub] = []

    # First pass: take the first regular (non-HI) English sidecar.
    chosen: SidecarSub | None = None
    for sub in sidecars:
        if sub.language in ENG_LANGS and not sub.is_hi:
            chosen = sub
            break

    # Fallback: no regular English — take the first HI English instead.
    if chosen is None:
        for sub in sidecars:
            if sub.language in ENG_LANGS and sub.is_hi:
                chosen = sub
                break

    for sub in sidecars:
        if sub is chosen:
            to_mux.append(sub)
        else:
            to_delete.append(sub)
    return to_mux, to_delete
