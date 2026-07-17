"""Single source of truth for "is this output ready to ship?"

The encoder ``finalize_upload`` calls :func:`check_compliance` BEFORE the
atomic replace step. Any violation either gets auto-fixed in-place
(mkvmerge stream-drop, mkvpropedit tag-stamp, os.rename) or refuses
the replace entirely so the source stays intact.

The audit tool ``tools.audit_recent_encodes`` calls the SAME function
so what the audit reports is exactly what verify enforces.

Categories
----------

* ``FIXABLE`` — an in-place fix exists. The encoder runs the fix, re-probes,
  re-checks, and proceeds to atomic replace. Examples: extra English sub
  (mkvmerge stream-remove), missing MKV tag (mkvpropedit stamp).

* ``REFUSE`` — no in-place fix; the output is wrong in a way that requires
  re-encoding (CQ doesn't match grade target) or re-acquiring (AV1 grew
  more than the 5% noise threshold). Encoder deletes ``.av1.tmp``, parks
  state row in ``error``, source on NAS is untouched.

* ``UNRECOVERABLE`` — out of band issue (source vanished mid-encode,
  encoder produced corrupt bytes). Encoder also refuses, surfaces for
  manual triage. Same effect as REFUSE; the category just signals
  "this is not a normal failure mode, look at it."

The 5% growth threshold (rule 9 in user's 2026-05-10 conversation) gives
a small noise budget — NVENC second-pass on the same source produces
files that vary by a percent or so between runs even at identical CQ.
Anything beyond 5% is the wrong-direction-encode symptom (Saving Private
Ryan 18 GB → 47 GB, ratio 2.59) and never legitimate.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

# Audio codec policy: EAC-3 is the target (Sonos Arc decodes natively),
# TrueHD is the Atmos passthrough exception (object layer preservation).
# Lossless codecs (FLAC/PCM/DTS-HD MA) are accepted only when the user's
# config explicitly lists them in ``lossless_audio_codecs``.
TARGET_AUDIO_CODECS = frozenset({"eac3", "truehd"})


# Video codec policy: which SOURCE codecs count as a finished target and so
# do NOT need re-encoding. Relaxed 2026-07-18 from AV1-only to also accept
# HEVC (h265): HEVC is now preferred at acquisition (Radarr/Sonarr custom
# formats) and re-encoding it to AV1 for a ~10-20% size gain isn't worth the
# GPU cost. Debloat still shrinks oversized HEVC to AV1 opportunistically.
# NOTE: this governs SOURCE acceptance / completion stats only. The post-encode
# OUTPUT check (``check_compliance`` below) stays AV1-only — when the pipeline
# actually encodes, it still produces AV1.
def video_is_finished(codec_raw: str | None) -> bool:
    """True if the on-disk video codec is a finished target (AV1 or HEVC)."""
    c = (codec_raw or "").lower()
    return "av1" in c or "hevc" in c

# Subtitle policy: at most 1 regular English sub. Forced-flag subs are a
# separate slot (foreign-dialogue snippets) and are always kept regardless
# of the regular-English count.
MAX_REGULAR_ENGLISH_SUBS = 1

# Required MKV global tags on every encode output. ENCODER tells you who
# encoded the file (and at what params); CQ + CONTENT_GRADE let the audit
# tool pick the file's bucket without re-deriving from media_report.
REQUIRED_ENCODE_TAGS = ("ENCODER", "CQ", "CONTENT_GRADE")


class Category(str, Enum):
    FIXABLE = "fixable"
    REFUSE = "refuse"
    UNRECOVERABLE = "unrecoverable"


@dataclass(frozen=True)
class Violation:
    """A single compliance failure with enough context for triage + fix.

    ``tag`` is the machine-readable kind ("extra_eng_subs", "missing_cq_tag",
    etc.) — fixers dispatch on it. ``message`` is human-readable; goes into
    state.error and the audit report. ``data`` carries the indices /
    expected values / etc. that the fixer needs.
    """

    tag: str
    message: str
    category: Category
    data: dict[str, Any]


def check_compliance(
    *,
    filepath: str,
    item: dict,
    encode_params: dict,
    output_probe: dict,
    mkv_tags: dict[str, str],
    input_size_bytes: int,
    output_size_bytes: int,
    source_was_av1: bool,
    config: dict,
) -> list[Violation]:
    """Return every violation the file has. Empty list = clean.

    Caller must pass:
      * ``filepath`` — final NAS destination (UNC \\\\KieranNAS\\... form).
      * ``item`` — the queue item (carries ``tmdb``, ``library_type``, etc.).
      * ``encode_params`` — the dict from :func:`pipeline.config.resolve_encode_params`
        used for this encode (``cq``, ``content_grade``, ``base_cq``, ``cq_offset``).
      * ``output_probe`` — :func:`pipeline.full_gamut._probe_full` of the encoded output.
      * ``mkv_tags`` — flat ``{TAG_NAME: value}`` dict from ``mkvextract tags``.
      * ``input_size_bytes`` / ``output_size_bytes`` — for the AV1 growth check.
      * ``source_was_av1`` — whether the input codec was AV1 (drives growth rule).
      * ``config`` — pipeline config (for KEEP_LANGS, lossless_audio_codecs, etc.).
    """
    violations: list[Violation] = []
    if output_probe.get("error"):
        violations.append(Violation(
            tag="probe_error",
            message=f"ffprobe failed on output: {output_probe['error']}",
            category=Category.UNRECOVERABLE,
            data={"error": output_probe["error"]},
        ))
        return violations

    out_video = output_probe.get("video") or {}
    out_audio = output_probe.get("audio") or []
    out_subs = output_probe.get("subs") or []

    # === Video codec ===
    v_codec = (out_video.get("codec") or "").lower()
    if v_codec not in ("av1", "av1_nvenc"):
        violations.append(Violation(
            tag="video_codec_wrong",
            message=f"video codec is {v_codec!r}, expected av1",
            category=Category.REFUSE,
            data={"actual": v_codec},
        ))

    # === Audio: codec ===
    if not out_audio:
        # ZERO-AUDIO is the 2026-04-23 incident class — refuse to ship.
        violations.append(Violation(
            tag="zero_audio",
            message="output has zero audio streams",
            category=Category.REFUSE,
            data={},
        ))

    lossless_codecs = {c.lower() for c in config.get("lossless_audio_codecs") or []}
    bad_codec_indices: list[int] = []
    for i, a in enumerate(out_audio):
        codec = (a.get("codec") or "").lower()
        if codec not in TARGET_AUDIO_CODECS and codec not in lossless_codecs:
            bad_codec_indices.append(i)
    if bad_codec_indices:
        violations.append(Violation(
            tag="audio_codec_wrong",
            message=(
                f"{len(bad_codec_indices)} audio track(s) with non-target codec: "
                f"{[out_audio[i].get('codec') for i in bad_codec_indices]}"
            ),
            category=Category.REFUSE,  # codec wrong = needs transcode = re-encode
            data={"indices": bad_codec_indices},
        ))

    # === Audio: language (KEEP_LANGS + original_language equivalents) ===
    from pipeline.config import KEEP_LANGS
    from pipeline.qualify import equivalence_bucket

    orig_lang = ((item.get("tmdb") or {}).get("original_language") or "").lower().strip()
    allowed_audio_langs: set[str] = set(KEEP_LANGS)
    if orig_lang:
        # Use the reverse-aware lookup so TMDb's "cn" (legacy Chinese code)
        # finds the {zh, cn, chi, zho, yue, cmn, ...} bucket and the actual
        # MKV 'chi' tag doesn't get refused as foreign. Pre-2026-05-17 the
        # direct lookup only knew the canonical 'zh' key and "In the Mood
        # for Love" (TMDb returns cn) tripped foreign_audio.
        allowed_audio_langs |= equivalence_bucket(orig_lang)

    foreign_audio_indices: list[int] = []
    for i, a in enumerate(out_audio):
        lang = (a.get("language") or "").lower().strip()
        if lang and lang not in allowed_audio_langs:
            foreign_audio_indices.append(i)
    if foreign_audio_indices:
        violations.append(Violation(
            tag="foreign_audio",
            message=(
                f"{len(foreign_audio_indices)} audio track(s) in foreign language "
                f"(allowed: KEEP_LANGS + original_language={orig_lang!r}): "
                f"{[out_audio[i].get('language') for i in foreign_audio_indices]}"
            ),
            category=Category.FIXABLE,  # mkvmerge can drop the offending tracks
            data={"indices": foreign_audio_indices},
        ))

    # === Audio: commentary tracks (title-based) ===
    commentary_re = re.compile(
        r"\b(commentary|director'?s|isolated\s*music|audio\s*description|"
        r"making[- ]of)\b",
        re.IGNORECASE,
    )
    commentary_indices: list[int] = []
    for i, a in enumerate(out_audio):
        title = (a.get("title") or "")
        if title and commentary_re.search(title):
            commentary_indices.append(i)
    if commentary_indices:
        violations.append(Violation(
            tag="commentary_audio",
            message=(
                f"{len(commentary_indices)} commentary/extras audio track(s) "
                f"survived strip: {[out_audio[i].get('title','')[:40] for i in commentary_indices]}"
            ),
            category=Category.FIXABLE,
            data={"indices": commentary_indices},
        ))

    # === Subtitles ===
    # Forced and SDH/HI subs each occupy a SEPARATE slot from "regular
    # English" — they carry different content (forced: foreign-dialogue
    # translation; SDH: sound effects + speaker IDs for the
    # hearing-impaired) and the user's setup wants both alongside the
    # regular English dialogue track. Pre-2026-05-14 this loop excluded
    # only forced from the regular count, then flagged extra_eng_subs
    # when a file had regular + SDH (e.g. Slow Horses S05E05 Circus:
    # 1 forced + 1 regular + 1 SDH → compliance counted regular+SDH=2
    # and refused). prep_streams.compute_sub_drop_indices already does
    # the right thing (excludes both forced AND SDH from the regular
    # count); compliance must match or files survive prep cleanly and
    # then loop on the breaker here.
    from pipeline.streams import is_forced_internal, is_hi_internal

    foreign_sub_indices: list[int] = []
    extra_eng_sub_indices: list[int] = []
    eng_regular_seen: list[int] = []
    for i, s in enumerate(out_subs):
        lang = (s.get("language") or "").lower().strip()
        if lang and lang not in KEEP_LANGS:
            foreign_sub_indices.append(i)
            continue
        # Forced subs — different slot, don't count toward the regular cap.
        # Use the canonical disposition+title helper (is_forced_internal) so
        # this matches the encoder's keep-decision (parse_sub_stream). A
        # disposition-forced track with no "forced" in its title must not be
        # miscounted as a regular English sub — that trips extra_eng_subs and
        # loops the prep circuit breaker (the 2026-05-14 Slow Horses class).
        if is_forced_internal(s):
            continue
        # SDH / HI subs — also a different slot. Use the canonical
        # disposition + title regex from pipeline.streams so this stays
        # in sync with the encoder's HI detection (which also drives
        # _map_subtitle_streams's eng-regular-keep decision).
        if is_hi_internal(s):
            continue
        eng_regular_seen.append(i)

    if foreign_sub_indices:
        violations.append(Violation(
            tag="foreign_subs",
            message=(
                f"{len(foreign_sub_indices)} foreign sub track(s) survived strip: "
                f"{[out_subs[i].get('language') for i in foreign_sub_indices]}"
            ),
            category=Category.FIXABLE,
            data={"indices": foreign_sub_indices},
        ))
    if len(eng_regular_seen) > MAX_REGULAR_ENGLISH_SUBS:
        # Keep the first regular English; mkvmerge drops the rest.
        extra_eng_sub_indices = eng_regular_seen[MAX_REGULAR_ENGLISH_SUBS:]
        violations.append(Violation(
            tag="extra_eng_subs",
            message=(
                f"{len(eng_regular_seen)} regular English sub tracks "
                f"(max {MAX_REGULAR_ENGLISH_SUBS}); will drop indices {extra_eng_sub_indices}"
            ),
            category=Category.FIXABLE,
            data={"indices": extra_eng_sub_indices},
        ))

    # === MKV global tags: encode metadata ===
    missing_encode_tags = [t for t in REQUIRED_ENCODE_TAGS if t not in mkv_tags]
    if missing_encode_tags:
        violations.append(Violation(
            tag="missing_encode_tags",
            message=f"MKV global tags missing: {missing_encode_tags}",
            category=Category.FIXABLE,
            data={"tags": missing_encode_tags},
        ))

    # CQ written must match what the encoder used.
    expected_cq = encode_params.get("cq")
    actual_cq = mkv_tags.get("CQ")
    if expected_cq is not None and actual_cq and str(expected_cq) != str(actual_cq):
        violations.append(Violation(
            tag="cq_mismatch",
            message=f"MKV CQ tag {actual_cq!r} != encode_params cq {expected_cq}",
            category=Category.FIXABLE,
            data={"actual": actual_cq, "expected": expected_cq},
        ))

    # CONTENT_GRADE must match.
    expected_grade = encode_params.get("content_grade")
    actual_grade = mkv_tags.get("CONTENT_GRADE")
    if expected_grade and actual_grade and expected_grade != actual_grade:
        violations.append(Violation(
            tag="grade_mismatch",
            message=f"MKV CONTENT_GRADE tag {actual_grade!r} != encode_params {expected_grade!r}",
            category=Category.FIXABLE,
            data={"actual": actual_grade, "expected": expected_grade},
        ))

    # NOTE: TMDb tag check is intentionally NOT here. TMDb metadata
    # gets stamped post-replace (after the atomic rename to the final
    # path), so at this pre-replace gate the tags are legitimately
    # absent. The audit tool — which runs against already-shipped
    # files — does its own TMDb tag check separately.

    # === Filename ===
    # Compare basename WITHOUT extension. An extension change (.mp4 -> .mkv,
    # .ts -> .mkv) is a normal and CORRECT consequence of transcoding —
    # the encoder always outputs .mkv, the atomic replace lands the new
    # file at the .mkv target, and the source's old extension stays only
    # on the .original.bak backup. Pre-2026-05-13 this check compared
    # full basenames including extension; .mp4 sources flagged every
    # time (Miller's Girl / Trainspotting / The Menu on 2026-05-13) and
    # the fixer couldn't rename the SOURCE while the encoder was still
    # using it — the violation re-fired forever.
    #
    # The check still catches typo'd source filenames ("Crisismkv" vs
    # "Crisis.mkv") because the basename-without-ext differs there.
    expected_filename = item.get("final_name") or item.get("filename")
    actual_filename = os.path.basename(filepath)
    if expected_filename:
        exp_stem, _exp_ext = os.path.splitext(expected_filename)
        act_stem, _act_ext = os.path.splitext(actual_filename)
        if exp_stem != act_stem:
            violations.append(Violation(
                tag="filename_mismatch",
                message=f"filename is {actual_filename!r}, expected {expected_filename!r}",
                category=Category.FIXABLE,
                data={"expected": expected_filename, "actual": actual_filename},
            ))

    # === Output growth is NEVER a compliance failure ===
    # User policy (re-stated angrily on 2026-05-12 and again on 2026-05-16):
    # quality is the goal, size is not. An AV1 output that is larger than
    # the source is acceptable and must not be refused. The old AV1→AV1
    # growth REFUSE rule has been removed in full. The size delta is still
    # surfaced via the encode_summary log line + dashboard for the user's
    # awareness; it just no longer blocks the upload.

    return violations


def categorise(violations: list[Violation]) -> dict[Category, list[Violation]]:
    """Group violations by category for the verify dispatcher."""
    out: dict[Category, list[Violation]] = {c: [] for c in Category}
    for v in violations:
        out[v.category].append(v)
    return out
