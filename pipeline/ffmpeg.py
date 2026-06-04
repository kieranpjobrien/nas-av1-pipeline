"""FFmpeg command builders for AV1 encoding and audio remux.
Extracted from encoding.py — pure functions that build ffmpeg commands.
No state management, no file I/O beyond ffprobe queries."""

import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Optional

from pipeline.config import KEEP_LANGS, resolve_encode_params
from pipeline.streams import (
    normalise_codec,
    parse_sub_stream,
)


# Codecs where NVDEC has a reputation for silent degradation rather than a
# clean failure — speed drops to ~1 fps without an error code, and the
# reactive retry path (which depends on ffmpeg exiting non-zero) never
# triggers. We force software decode upfront for these. The 2026-05-03
# Any Given Sunday VC-1 incident motivated the list; add codecs as we
# discover more cases.
_NVDEC_SILENT_DEGRADATION_CODECS = frozenset({
    "vc1",     # SMPTE 421M — the original case (HD-DVD / older Blu-ray)
    "wmv3",    # WMV9 ASF, same family as VC-1, similarly flaky
    "mpeg2",   # interlaced MPEG-2 from old broadcast captures
    "mpeg2video",
})


def format_bytes(b: int) -> str:
    if b >= 1024**4:
        return f"{b / 1024**4:.2f} TB"
    if b >= 1024**3:
        return f"{b / 1024**3:.1f} GB"
    if b >= 1024**2:
        return f"{b / 1024**2:.0f} MB"
    return f"{b / 1024:.0f} KB"


def format_duration(secs: float) -> str:
    if secs < 60:
        return f"{secs:.0f}s"
    if secs < 3600:
        return f"{secs / 60:.0f}m {secs % 60:.0f}s"
    return f"{secs / 3600:.0f}h {(secs % 3600) / 60:.0f}m"


def _probe_source_color(input_path: str) -> dict[str, Optional[str]]:
    """ffprobe the source's video colour tags. Returns a dict with
    color_primaries / color_transfer / color_space / color_range —
    ``None`` for any field the source doesn't tag explicitly.

    Background — two recurring bugs solved by passing source tags through:

    1. 2026-05-26: SDR encodes had no -color_primaries / -color_trc /
       -colorspace flags. AV1 stream went out tagged "unspecified",
       players default-guessed the matrix, 10-bit SDR landed on
       BT.2020 → green / purple tint (1917, The Drama, Avatar, etc.).
    2. 2026-05-28: SDR encodes had no -color_range flag. NVENC silently
       re-scaled limited-range inputs, lifting the black floor from
       Y=16 to ~Y=43-53. User noticed "blacks not black enough" on
       Lion (2016). signalstats confirmed the lift.

    The fix passes the SOURCE's actual tags through; the SDR branch in
    ``build_ffmpeg_cmd`` falls back to BT.709 + tv (limited) range when
    the source is untagged — matches virtually all SDR content correctly.
    """
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries",
         "stream=color_primaries,color_transfer,color_space,color_range",
         "-of", "json", input_path],
        capture_output=True, text=True, timeout=15
    )
    if out.returncode != 0:
        return {"color_primaries": None, "color_transfer": None,
                "color_space": None, "color_range": None}
    try:
        info = json.loads(out.stdout)
        s = (info.get("streams") or [{}])[0]
    except (json.JSONDecodeError, IndexError, AttributeError):
        return {"color_primaries": None, "color_transfer": None,
                "color_space": None, "color_range": None}
    # ffprobe returns "unknown" / "reserved" for unspecified; normalise.
    def _norm(v: Optional[str]) -> Optional[str]:
        if v in (None, "", "unknown", "reserved"):
            return None
        return v
    return {
        "color_primaries": _norm(s.get("color_primaries")),
        "color_transfer": _norm(s.get("color_transfer")),
        "color_space": _norm(s.get("color_space")),
        "color_range": _norm(s.get("color_range")),
    }


def get_duration(filepath: str) -> Optional[float]:
    """Get file duration via ffprobe."""
    try:
        cmd = [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            str(filepath),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, encoding="utf-8", errors="replace")
        if result.returncode == 0:
            import json

            data = json.loads(result.stdout)
            return float(data.get("format", {}).get("duration", 0))
    except Exception as e:
        logging.debug(f"ffprobe duration failed for {filepath}: {e}")
    return None


def _should_transcode_audio(audio: dict, config: dict) -> bool:
    """Decide whether an audio stream should be transcoded to EAC-3.

    Passthrough (no transcode) for:
      - EAC-3 (already target codec, includes EAC-3-JOC which carries Atmos)
      - TrueHD — the primary Atmos carrier. Sonos Arc / Beam Gen 2 decode
        TrueHD-Atmos natively over HDMI eARC. Transcoding to EAC-3 7.1
        would drop the object layer and flatten overhead effects to
        front-wall. The size cost (TrueHD is ~3-5 GB on a 4K remux) is
        worth it for titles where the user actually has Atmos gear.

    Everything else (Opus, DTS, DTS-HD MA, FLAC, PCM, AC-3, AAC, MP3)
    transcodes to EAC-3. Opus was previously passthrough on the assumption
    of "efficient lossy, already good", but Sonos Arc cannot decode Opus
    natively — Plex transcodes it on every play. Pre-transcoding to EAC-3
    once here removes the per-play transcode cost. DTS-HD MA is lossless
    but not Atmos; 640k EAC-3 is transparent on Sonos-class gear.
    """
    codec = normalise_codec(audio.get("codec_raw") or audio.get("codec"))
    if codec == "eac3":
        return False
    if codec == "truehd":
        return False  # preserve Atmos object layer
    return True


def has_bulky_audio(item: dict, config: dict) -> bool:
    """Check if any audio stream in an item would benefit from transcoding."""
    for audio in item.get("audio_streams", []):
        if _should_transcode_audio(audio, config):
            return True
    return False


def _select_audio_streams(item: dict, config: dict) -> list[int] | None:
    """Determine which audio stream indices to keep.

    Returns list of input stream indices to map, or None to keep all.

    Dispatches on ``config["audio_keep_policy"]``:
      * ``"original_language"`` (default) — keep tracks matching TMDb
        ``original_language``. Strips foreign dubs including English dubs
        of foreign-origin films. Falls back to "english_und" when there's
        no TMDb data. Never strips `und` tracks whisper hasn't resolved.
      * ``"english_und"`` (legacy) — keep stream 0 + every English/und
        track. Subject to the historical "don't strip 1-2 tracks" guard
        (not worth the complexity for trivially-small files).
    """
    if not config.get("strip_non_english_audio", True):
        return None

    audio_streams = item.get("audio_streams", [])
    if not audio_streams:
        return None

    policy = config.get("audio_keep_policy", "original_language")

    if policy == "original_language":
        from pipeline.streams import (
            parse_audio_stream,
            select_audio_keep_indices_by_original_language,
        )

        tmdb = (item.get("tmdb") or {})
        original_language = (tmdb.get("original_language") or "").strip().lower() or None

        if original_language:
            parsed = [parse_audio_stream(a, i) for i, a in enumerate(audio_streams)]
            kept = select_audio_keep_indices_by_original_language(
                parsed,
                original_language,
                keep_english_too=bool(config.get("audio_keep_english_with_original", False)),
            )
            if kept is None:
                return None
            stripped = len(audio_streams) - len(kept)
            logging.info(
                f"  Keeping {len(kept)} of {len(audio_streams)} audio streams "
                f"(original_language={original_language}, stripped {stripped} foreign dubs)"
            )
            return kept
        # No TMDb signal — fall through to legacy rule rather than strip blind.

    # Legacy "english_und" policy (also the no-TMDb fallback for original_language).
    if len(audio_streams) <= 2:
        return None  # not worth stripping 1-2 tracks

    # Inviolate rule (2026-04-29): never strip an audio track without first
    # knowing its language. The fallback policy KEPT und tracks by accident
    # (KEEP_LANGS includes "und"), but make it explicit + log when we defer
    # so the user can see when language detection is the bottleneck.
    from pipeline.streams import all_languages_known

    if not all_languages_known(audio_streams):
        unresolved = sum(
            1 for a in audio_streams
            if (a.get("language") or "").lower().strip() in {"", "und", "unk"}
            and (a.get("detected_language") or "").lower().strip() in {"", "und", "unk"}
        )
        logging.info(
            f"  Audio strip deferred — {unresolved}/{len(audio_streams)} track(s) "
            f"have unresolved language. Keeping all audio."
        )
        return None

    keep = {0}  # always keep first stream (original language)
    for i, audio in enumerate(audio_streams):
        lang = (audio.get("language") or "").lower().strip()
        if lang in KEEP_LANGS:
            keep.add(i)

    if len(keep) >= len(audio_streams):
        return None  # keeping everything anyway

    kept = sorted(keep)
    stripped = len(audio_streams) - len(kept)
    logging.info(
        f"  Keeping {len(kept)} of {len(audio_streams)} audio streams (stripped {stripped} non-English tracks)"
    )
    return kept


def _map_subtitle_streams(
    cmd: list[str],
    item: dict,
    config: dict,
    *,
    external_subs_present: bool = False,
) -> list[int] | None:
    """Add per-stream subtitle mappings, keeping only English/undefined tracks.

    Returns the list of INPUT subtitle indices that were mapped, in output
    order, so the caller can stamp matching per-stream metadata on the right
    output index. Returns ``None`` when it emitted a blanket ``-map 0:s?``
    (all source subs, input order preserved). This is the single source of
    truth for the keep-decision — the metadata-stamping loop in the caller
    consumes this list rather than re-deriving it, so the two can never drift
    (pre-fix the loop ignored ``external_subs_present`` and the forced-language
    gate, mis-indexing the external sidecar's language/disposition metadata).

    If strip_non_english_subs is disabled, maps all subs with -map 0:s?.

    Uses pipeline.streams.parse_sub_stream + is_hi_internal for HI detection.
    The HI rule is stricter than the old inline check (it also catches ``cc``
    and disposition flags) — see pipeline/streams.py for details.

    ``external_subs_present`` (added 2026-05-10): when an English external
    sidecar (.en.srt) is being muxed in alongside, skip the internal regular
    English track to keep the policy "max 1 English sub". Pre-fix, The Office
    S03E16 / Veep / Futurama outputs ended up with 2 English subs because
    this function happily mapped an internal PGS/SubRip English while
    ``external_subs`` was simultaneously muxed in the caller. Forced /
    foreign-language subs are still mapped — they're not duplicates of the
    external English sidecar.
    """
    if not config.get("strip_non_english_subs", True):
        cmd.extend(["-map", "0:s?"])
        return None

    raw_subs = item.get("subtitle_streams", [])
    if not raw_subs:
        cmd.extend(["-map", "0:s?"])  # no metadata — let ffmpeg figure it out
        return None

    # Inviolate rule (2026-04-29): never strip a sub track without first
    # knowing its language. If ANY track is `und`/empty with no whisper
    # detection, defer entirely and map all subs.
    from pipeline.streams import all_languages_known

    parsed_subs = [parse_sub_stream(raw, index=i) for i, raw in enumerate(raw_subs)]
    if not all_languages_known(parsed_subs):
        unresolved = sum(
            1 for s in parsed_subs
            if not (s.language and s.language.lower() not in {"", "und", "unk"})
            and not (s.detected_language and s.detected_language.lower() not in {"", "und", "unk"})
        )
        logging.info(
            f"  Sub strip deferred — {unresolved}/{len(parsed_subs)} track(s) "
            f"have unresolved language. Mapping all subs as-is."
        )
        cmd.extend(["-map", "0:s?"])
        return None

    # Keep exactly 1 regular English sub + forced. Strip HI, duplicates, foreign.
    #
    # Per-index maps use the ``?`` (optional) suffix because subtitle streams
    # are legitimately optional in this pipeline:
    #   * Bazarr backfills missing subs post-encode — a zero-sub source is
    #     expected, not an error.
    #   * ``_remux_to_mkv`` drops incompatible subs (e.g. mov_text from .mp4)
    #     on its retry attempt, leaving ``item["subtitle_streams"]`` stale
    #     relative to the remuxed input file.
    # Without ``?``, ffmpeg aborts with "Stream map 0:s:N matches no streams"
    # instead of silently skipping the missing track.
    #
    # NOTE: this is INTENTIONALLY different from the audio-map policy
    # (CLAUDE.md rule 10). Audio is mandatory — a silent audio drop is the
    # incident we wrote discipline rules around. Subs are optional — silent
    # skip is the desired behaviour.
    from pipeline.config import ENG_LANGS, KEEP_LANGS

    # Allowed languages for FORCED subs. Pre-2026-05-14 this branch
    # mapped every forced track regardless of language, on the theory
    # that forced subs are "language-agnostic helpers" for non-dialogue
    # content (signs, brief alien-language passages). That's right for
    # English-region releases — but foreign-region sources ship forced
    # narrative tracks meant for *their* audience (Resident Alien
    # S01E07 had ``language=tur title="Turkish [ForcedNarrative]"``).
    # Mapping that into our English-target output is exactly what
    # compliance.py and prep_streams.py both refuse.
    #
    # Policy: gate forced subs by KEEP_LANGS (eng/en/und/zxx) — same
    # set compliance.check_compliance uses for foreign_subs at
    # compliance.py:218. Compliance does NOT expand the sub-keep set
    # by original_language the way it does for audio; the encoder
    # mirrors that so a Japanese-origin film's ``jpn`` forced
    # narrative is dropped here AND refused at the gate, not kept
    # here and refused at the gate (which would loop forever).
    allowed_forced_langs: set[str] = set(KEEP_LANGS)

    mapped_indices: list[int] = []
    # If an external English sidecar is being muxed in by the caller, treat
    # the regular-English slot as already-claimed so we don't keep a second
    # internal English. Forced subs in an allowed language are still mapped
    # — they don't collide with the external English sidecar.
    found_regular_eng = external_subs_present
    for i, sub in enumerate(parsed_subs):
        if sub.is_forced:
            if sub.language in allowed_forced_langs:
                cmd.extend(["-map", f"0:s:{i}?"])
                mapped_indices.append(i)
            # else: foreign-language forced narrative — drop, matching
            # prep_streams.compute_sub_drop_indices and compliance.py.
        elif sub.language in ENG_LANGS and not sub.is_hi and not found_regular_eng:
            cmd.extend(["-map", f"0:s:{i}?"])
            mapped_indices.append(i)
            found_regular_eng = True

    if not mapped_indices and not external_subs_present:
        # No English subs found AND no external sidecar — map all to be safe
        # (the source might have unlabelled English subs).
        cmd.extend(["-map", "0:s?"])
        return None
    if len(mapped_indices) < len(raw_subs):
        stripped = len(raw_subs) - len(mapped_indices)
        logging.info(f"  Stripped {stripped} non-English subtitle stream(s)")
    return mapped_indices


def _parse_sub_language(filepath: str) -> str:
    """Extract language code from Bazarr subtitle filename.

    Patterns: Movie.en.srt, Movie.en.hi.srt, Movie.en.forced.srt
    Returns ISO 639 code or 'eng' as default.
    """
    from pathlib import Path

    stem = Path(filepath).stem  # e.g. "Movie (2020).en.hi"
    parts = stem.rsplit(".", 3)
    # Walk backwards through dot-separated parts looking for a 2-3 char lang code
    lang_codes = {
        "en",
        "eng",
        "fr",
        "fre",
        "de",
        "deu",
        "ger",
        "es",
        "spa",
        "it",
        "ita",
        "pt",
        "por",
        "nl",
        "nld",
        "dut",
        "ja",
        "jpn",
        "ko",
        "kor",
        "zh",
        "zho",
        "chi",
        "ru",
        "rus",
        "ar",
        "ara",
        "hi",
        "hin",
        "sv",
        "swe",
        "no",
        "nor",
        "da",
        "dan",
        "fi",
        "fin",
        "pl",
        "pol",
        "tr",
        "tur",
        "cs",
        "ces",
        "cze",
        "hu",
        "hun",
        "ro",
        "ron",
        "rum",
        "el",
        "ell",
        "gre",
        "he",
        "heb",
        "th",
        "tha",
        "vi",
        "vie",
        "id",
        "ind",
        "ms",
        "msa",
        "may",
    }
    for part in reversed(parts[1:]):  # skip the main title
        p = part.lower()
        if p == "hi" or p == "sdh":
            pass
        elif p == "forced":
            pass
        elif p in lang_codes:
            return p
    return "eng"


def build_ffmpeg_cmd(
    input_path: str,
    output_path: str,
    item: dict,
    config: dict,
    include_subs: bool = True,
    external_subs: list[str] | None = None,
    use_hwaccel: bool = True,
) -> list[str]:
    """Build the ffmpeg command for NVENC AV1 encoding.

    Refuses to build a command for sources with zero audio streams — this is how
    the 1,787-file audio-loss incident got so large: the pipeline would happily
    re-encode an already-damaged file and produce another damaged output. There
    is no legitimate reason to run this builder on a zero-audio source; callers
    should filter those out upstream and surface them as errors.

    Args:
        use_hwaccel: If True (default), decode on the GPU via NVDEC (``-hwaccel cuda
            -hwaccel_output_format cuda``) so frames flow NVDEC → CUDA memory →
            NVENC without a PCIe round-trip through system RAM. Cuts ffmpeg CPU
            from 5+ cores of software decode down to demux + mux only.
            _run_encode flips this to False for the retry attempt if the first
            attempt's stderr mentions CUDA/CUVID/NVDEC/hwaccel — NVDEC does not
            support every codec/profile (e.g. 10-bit H.264, MPEG-4 ASP), so we
            need a graceful fallback path.
    """
    is_hdr = item.get("hdr", False)
    params = resolve_encode_params(config, item)

    # REFUSE-TO-BUILD: zero-audio sources are either scanner false positives (fix
    # the scanner) or pre-existing damage (delete + re-source). Either way, never
    # emit an encode command for them.
    audio_streams = item.get("audio_streams") or []
    if not audio_streams:
        raise ValueError(
            f"build_ffmpeg_cmd refused: source has zero audio streams ({input_path}). "
            "Zero-audio sources must be filtered out upstream — encoding them would "
            "just propagate the damage into an AV1 copy."
        )

    # Hard cap on output duration — `+genpts` plus EAC-3 audio transcoding can produce
    # output files whose container duration is inflated 20-30% over the source (seen in
    # the wild on My Cousin Vinny and Trennung mit Hindernissen). Passing `-t` to ffmpeg
    # truncates the output at exactly the source duration, which prevents the verify
    # step from rejecting an otherwise-fine encode. We add a 1s pad so we don't clip
    # the last frame on files whose duration report rounds down.
    source_duration = item.get("duration_seconds") or get_duration(input_path) or 0

    # Pixel format: 10-bit for HDR (mandatory), also 10-bit for SDR (banding resistance)
    pix_fmt = config.get("pixel_format_hdr" if is_hdr else "pixel_format_sdr", "yuv420p10le")

    # Determine which audio streams to keep before building the command
    audio_keep = _select_audio_streams(item, config)

    cmd = [
        "ffmpeg",
        "-y",
    ]

    # Proactive NVDEC-flaky-codec detection.
    #
    # 2026-05-03 finding: Any Given Sunday (1999) Director's Cut is a VC-1
    # source. NVDEC technically supports VC-1 but in practice silently
    # degrades to ~0.8 fps — the encoder loop sees NO error, just glacial
    # progress (0.046× realtime, 56h ETA on a 2.5h movie). The reactive
    # retry path in _run_encode only fires when ffmpeg exits non-zero,
    # which never happens here.
    #
    # 2026-05-05 follow-up: the original check only read ``item["video_codec"]``
    # which the queue builder stores as the human-readable display string
    # ("VC-1"), not the ffmpeg codec_raw ("vc1"). String compare against a
    # set of codec_raw values failed for VC-1, the file slid into NVDEC's
    # stuck mode again at 0.013x with a 201h ETA. Fix:
    #   1. Fall back to ``item.video.codec_raw`` when ``video_codec`` doesn't
    #      hit the set — the report has the raw form even when the queue
    #      item only has the display string.
    #   2. Normalise both sides by stripping non-alphanumerics, so any of
    #      "VC-1" / "vc-1" / "vc1" / "MPEG-2 Video" all collapse to the
    #      canonical form (vc1, mpeg2video) that the set holds.
    #
    # Force software decode upfront for codecs that have a reputation for
    # this kind of silent NVDEC degradation. CPU decode is slower than NVDEC
    # but vastly faster than NVDEC's stuck-mode, and NVENC encode runs at
    # full speed regardless.
    def _normalise_codec(s: str) -> str:
        return "".join(c for c in (s or "").lower() if c.isalnum())

    src_codec_raw = item.get("video_codec") or ""
    if not src_codec_raw:
        src_codec_raw = (item.get("video") or {}).get("codec_raw") or ""
    src_codec_norm = _normalise_codec(src_codec_raw)
    # Also try codec_raw as a second-chance read — display strings like
    # "VC-1" exist in item.video_codec but normalise to "vc1" only via
    # the strip; if the queue builder ever changes shape this fallback
    # keeps the check working.
    if src_codec_norm not in _NVDEC_SILENT_DEGRADATION_CODECS:
        alt = (item.get("video") or {}).get("codec_raw") or ""
        if _normalise_codec(alt) in _NVDEC_SILENT_DEGRADATION_CODECS:
            src_codec_norm = _normalise_codec(alt)
    if use_hwaccel and src_codec_norm in _NVDEC_SILENT_DEGRADATION_CODECS:
        logging.warning(
            f"  Forcing software decode for {src_codec_raw or src_codec_norm} source "
            f"(NVDEC known-flaky on this codec)"
        )
        use_hwaccel = False

    # GPU-resident decode path. ``-hwaccel cuda`` tells ffmpeg to hand decoding to
    # NVDEC (a separate hardware block from NVENC — no contention on Ada RTX cards).
    # ``-hwaccel_output_format cuda`` keeps decoded frames in CUDA memory so they
    # go NVDEC → NVENC without copying through system RAM. Without this, ffmpeg
    # burns 5+ CPU cores on libavcodec software decode (observed 2026-04-24, 520%
    # ffmpeg CPU with GPU encoder chip only at 52% utilisation because the CPU
    # decode couldn't feed it fast enough).
    #
    # Caveat: NVDEC doesn't support every codec/profile (10-bit H.264 High-10,
    # MPEG-4 ASP, some exotic H.264 chroma subsampling). ``_run_encode`` in
    # full_gamut.py detects hwaccel-related decode errors in stderr and retries
    # with use_hwaccel=False before falling through to the subtitle / audio retries.
    if use_hwaccel:
        cmd.extend(["-hwaccel", "cuda", "-hwaccel_output_format", "cuda"])

    cmd.extend([
        # Scope `ignore_err` to VIDEO only. Global `-err_detect ignore_err` was the root
        # cause of silent audio loss: combined with `-map 0:a?`, a corrupt audio header
        # caused ffmpeg to skip the audio stream and exit 0 with a zero-audio output.
        "-err_detect:v",
        "ignore_err",
        # Regenerate timestamps from frame order — fixes "Non-monotonic DTS" errors on output
        # EAC-3 streams when the source is DTS-HD MA (seen on Vinny, Dances With Wolves).
        "-fflags",
        "+genpts",
        "-i",
        input_path,
        # Emit machine-readable progress to stdout. Much cleaner than parsing stderr, since
        # ffmpeg may change its human-facing format at any time but the `-progress` key=value
        # protocol is stable. -nostats silences the human-facing stderr progress rewrites.
        "-progress",
        "pipe:1",
        "-nostats",
    ])

    # Add external subtitle files as additional inputs
    if external_subs:
        for sub_path in external_subs:
            cmd.extend(["-i", sub_path])

    # Map only the first video stream from input 0
    cmd.extend(["-map", "0:v:0"])

    # Audio stream mapping. Use `-map 0:a` (NOT `0:a?`) — the `?` (optional) form
    # silently produced zero-audio output when combined with a corrupt audio header
    # or a stale audio_streams list. We already refused empty audio_streams at the
    # top of this function, so a non-optional map is safe and will fail loudly if
    # the source genuinely has no audio.
    if audio_keep is not None:
        for idx in audio_keep:
            cmd.extend(["-map", f"0:a:{idx}"])
    else:
        cmd.extend(["-map", "0:a"])

    mapped_sub_inputs: list[int] | None = None
    if include_subs:
        mapped_sub_inputs = _map_subtitle_streams(
            cmd, item, config, external_subs_present=bool(external_subs)
        )

    # Map external subtitle inputs (inputs 1, 2, 3, ...)
    if external_subs:
        for i in range(len(external_subs)):
            cmd.extend(["-map", f"{i + 1}:s"])

    # Pixel format conversion.
    #
    # * With hwaccel (frames live in CUDA memory): use the ``scale_cuda`` video
    #   filter to do the format conversion on the GPU. Using ``-pix_fmt`` alone
    #   makes ffmpeg insert a CPU-side ``auto_scale`` filter that can't accept
    #   CUDA-memory input, which fails at filter-graph init with
    #   "Impossible to convert between the formats supported by the filter
    #   'Parsed_null_0' and the filter 'auto_scale_0'". Observed 2026-04-24.
    # * Without hwaccel (frames in system RAM via libavcodec decode): the
    #   classic ``-pix_fmt`` form is correct. scale_cuda would fail because the
    #   input isn't in CUDA memory.
    if use_hwaccel:
        cmd.extend(["-vf", f"scale_cuda=format={pix_fmt}"])

    # Video: NVENC AV1
    cmd.extend(
        [
            "-c:v",
            config["video_codec"],
            "-cq",
            str(params["cq"]),
            "-preset",
            params["preset"],
            "-tune",
            "hq",
            "-rc",
            "vbr",
            "-b:v",
            "0",
        ]
    )
    # ``-pix_fmt`` only when hwaccel is OFF. With hwaccel, format was already
    # set by ``scale_cuda=format=...`` above, and adding ``-pix_fmt`` forces
    # ffmpeg to re-insert an auto_scale CPU filter, defeating the whole point.
    if not use_hwaccel:
        cmd.extend(["-pix_fmt", pix_fmt])

    # Keyframe interval (GOP cap). 2026-06-01: discovered In the Mood for
    # Love (4K HDR AV1) had ZERO keyframes in a 60s / 1439-frame window —
    # av1_nvenc with no explicit -g uses a default GOP so long the stream
    # is effectively all-inter after the opening IDR. Symptom: video
    # freezes / skips while audio plays fine, because any dropped frame or
    # seek can't resync until the (distant) next keyframe. 4K 10-bit HDR is
    # worst-hit (highest decode load + no recovery points). This gap is
    # UNCONDITIONAL — every AV1 file we've encoded lacks a keyframe cap.
    #
    # Fix: cap the GOP. `-g` is a frame count; a fixed 240 gives a keyframe
    # at least every 10s @ 24fps / 8s @ 30fps / 4s @ 60fps — all healthy
    # for seek granularity + error recovery. NVENC still inserts extra
    # keyframes at scene cuts on top of this cap. Negligible size cost
    # (a few % more keyframes), and keyframes are higher-quality anyway, so
    # no conflict with the quality-first policy. Configurable via
    # `gop_max_frames` (default 240).
    gop = int(config.get("gop_max_frames", 240) or 0)
    if gop > 0:
        cmd.extend(["-g", str(gop)])

    # Multipass
    if params["multipass"] != "disabled":
        cmd.extend(["-multipass", params["multipass"]])

    # Lookahead
    if params["lookahead"] > 0:
        cmd.extend(["-rc-lookahead", str(params["lookahead"])])

    # Spatial/temporal AQ
    cmd.extend(["-spatial-aq", "1"])
    # Temporal AQ: profile can override, otherwise movies only
    temporal_aq = params.get("temporal_aq")
    if temporal_aq is True or (temporal_aq is None and params["content_type"] == "movie"):
        cmd.extend(["-temporal-aq", "1"])

    # Rate cap
    if params["maxrate"]:
        cmd.extend(["-maxrate", params["maxrate"]])
    if params["bufsize"]:
        cmd.extend(["-bufsize", params["bufsize"]])

    # HDR handling: tonemap to SDR or preserve metadata
    do_tonemap = is_hdr and params.get("profile") == "tonemap"
    if do_tonemap:
        # HDR->SDR tone-mapping: BT.2020 PQ -> BT.709 with Hable curve
        cmd.extend(
            [
                "-vf",
                "zscale=t=linear:npl=100,format=gbrpf32le,"
                "zscale=p=bt709,tonemap=hable:desat=0,"
                "zscale=t=bt709:m=bt709:r=tv,format=yuv420p10le",
                "-color_primaries",
                "bt709",
                "-color_trc",
                "bt709",
                "-colorspace",
                "bt709",
                "-color_range",
                "tv",
            ]
        )
    elif is_hdr:
        # HDR preserved: probe source for actual range (most HDR is tv,
        # but some HDR10+ / Dolby Vision masters are full).
        src_color = _probe_source_color(input_path)
        cmd.extend(
            [
                "-color_primaries",
                "bt2020",
                "-color_trc",
                "smpte2084",
                "-colorspace",
                "bt2020nc",
                "-color_range",
                src_color["color_range"] or "tv",
            ]
        )
    else:
        # SDR (2026-05-26 + 2026-05-28): explicit colour tags are
        # mandatory. Pre-fix the SDR branch emitted no -color_*
        # flags at all; first iteration added primaries/trc/colorspace
        # (fixed purple tints on 1917/Drama/Avatar/Groundhog Day).
        # Second iteration adds -color_range — NVENC was silently
        # re-scaling limited-range inputs and lifting the black floor
        # from Y=16 to ~Y=43-53 (Lion 2016 case; user reported
        # "blacks not black enough").
        # Strategy: pass through the source's actual tags when
        # tagged, fall back to BT.709 + tv (limited range) when not.
        # Virtually all SDR content is BT.709 limited-range so the
        # fallback matches reality; rare exceptions get their
        # source tags preserved.
        src_color = _probe_source_color(input_path)
        primaries = src_color["color_primaries"] or "bt709"
        transfer = src_color["color_transfer"] or "bt709"
        matrix = src_color["color_space"] or "bt709"
        rng = src_color["color_range"] or "tv"
        cmd.extend(
            [
                "-color_primaries", primaries,
                "-color_trc", transfer,
                "-colorspace", matrix,
                "-color_range", rng,
            ]
        )

    # Audio handling — codec settings use OUTPUT stream indices (which differ from
    # input indices when non-English audio streams have been stripped).
    # audio_streams is guaranteed non-empty (refused at top of function).
    if config["audio_mode"] == "copy":
        cmd.extend(["-c:a", "copy"])
    elif config["audio_mode"] == "smart":
        loudnorm = config.get("audio_loudnorm", False)
        # Iterate over the streams we're actually keeping
        kept_streams = (
            [(idx, audio_streams[idx]) for idx in audio_keep] if audio_keep else list(enumerate(audio_streams))
        )
        for out_idx, (_, audio) in enumerate(kept_streams):
            if _should_transcode_audio(audio, config):
                channels = audio.get("channels", 2)
                bitrate = (
                    config["audio_eac3_surround_bitrate"] if channels > 2 else config["audio_eac3_stereo_bitrate"]
                )
                if loudnorm:
                    cmd.extend([f"-filter:a:{out_idx}", "loudnorm=I=-24:LRA=7:TP=-2"])
                cmd.extend(
                    [
                        f"-c:a:{out_idx}",
                        "eac3",
                        f"-b:a:{out_idx}",
                        bitrate,
                    ]
                )
            else:
                cmd.extend([f"-c:a:{out_idx}", "copy"])

    # Subtitles: copy all (when mapped)
    if include_subs:
        cmd.extend(["-c:s", "copy"])

    # Strip encoder metadata bloat (scene group tags, encoder info) BEFORE we
    # write our own — ``-map_metadata -1`` drops global metadata. Per-stream
    # language tags also get lost in transcode (E-AC-3 encoder doesn't carry
    # the source's language tag through), so we explicitly re-stamp them
    # below from item.audio_streams / item.subtitle_streams. Pre-2026-05-04
    # this was missing entirely — every encoded file ended up with UND on
    # all audio + sub tracks despite the language detection running and
    # storing the right values in the state DB.
    cmd.extend(["-map_metadata", "-1"])

    # Restore per-stream language tags for kept INTERNAL audio streams.
    # item["audio_streams"] is what the orchestrator merged from
    # detected_audio (whisper-resolved languages) so it's the
    # source-of-truth. audio_keep is the list of input indices being
    # mapped (or None = all); the output index is the position in that
    # iteration order.
    audio_streams_data = item.get("audio_streams") or []
    if audio_streams_data:
        if audio_keep is not None:
            kept_audio_indices = list(audio_keep)
        else:
            kept_audio_indices = list(range(len(audio_streams_data)))
        for out_idx, in_idx in enumerate(kept_audio_indices):
            if in_idx >= len(audio_streams_data):
                continue
            track = audio_streams_data[in_idx]
            lang = (track.get("language") or "").strip().lower()
            if lang and lang not in ("und", "unk"):
                cmd.extend([f"-metadata:s:a:{out_idx}", f"language={lang}"])
            title = (track.get("title") or "").strip()
            if title:
                cmd.extend([f"-metadata:s:a:{out_idx}", f"title={title}"])

    # Same treatment for INTERNAL subtitle streams. _map_subtitle_streams
    # is the single source of truth for which input indices were mapped and
    # in what output order; we consume its return value directly rather than
    # re-deriving the keep-decision (the old re-derivation ignored the
    # external-sidecar regular-eng skip AND the forced-language gate, so it
    # drifted from the real mapping and mis-indexed the metadata stamps —
    # including the external sidecar's out_idx below).
    if include_subs:
        sub_data = item.get("subtitle_streams") or []
        if mapped_sub_inputs is None:
            # Blanket ``-map 0:s?`` — all source subs, output order == input.
            kept_sub_indices = list(range(len(sub_data)))
        else:
            kept_sub_indices = mapped_sub_inputs

        for out_idx, in_idx in enumerate(kept_sub_indices):
            if in_idx >= len(sub_data):
                continue
            track = sub_data[in_idx]
            lang = (track.get("language") or "").strip().lower()
            if lang and lang not in ("und", "unk"):
                cmd.extend([f"-metadata:s:s:{out_idx}", f"language={lang}"])
            # Preserve forced disposition + title so compliance can tell the
            # forced track apart from the regular English track. -map_metadata -1
            # nukes everything from source — without re-stamping here, a kept
            # forced sub looks identical to a regular sub at the post-encode
            # gate and trips extra_eng_subs. Slow Horses S05E03/S05E05 hit
            # exactly this on 2026-05-14 (source had [Forced, "", SDH] eng;
            # encoder kept [0,1], stripped metadata, compliance saw 2 untitled
            # eng → refuse). parse_sub_stream is the source of truth for
            # is_forced (title regex + disposition.forced).
            from pipeline.streams import parse_sub_stream
            sub_obj = parse_sub_stream(track, index=in_idx)
            if sub_obj.is_forced:
                cmd.extend([f"-disposition:s:{out_idx}", "forced"])
                # Re-stamp a canonical title so the compliance title-regex
                # path also catches it (defense-in-depth — disposition alone
                # is enough today but title gives a human-readable signal
                # in mediainfo / mkvinfo output too).
                cmd.extend([f"-metadata:s:s:{out_idx}", "title=Forced"])
            elif sub_obj.is_hi:
                # Same problem class as forced (Little Mermaid 2026-05-17):
                # source had 2 SDH eng tracks ("English SDH - Songs only",
                # "English SDH"). _map_subtitle_streams' regular-eng selector
                # dropped both (is_hi=True), mapped == 0, fallback ``-map 0:s?``
                # kept both, then ``-map_metadata -1`` nuked title+disposition,
                # output had 2 untitled eng with hearing_impaired=0 →
                # compliance counted both as regular → PREP MISS. Re-stamp
                # hearing_impaired disposition + a canonical title so
                # is_hi_internal (disposition path) and the title-regex path
                # both classify the output correctly. Preserve the source
                # title if it carried useful detail (e.g. "Songs only");
                # otherwise stamp "SDH" so the title regex matches.
                cmd.extend([f"-disposition:s:{out_idx}", "hearing_impaired"])
                src_title = (track.get("title") or "").strip()
                hi_title = src_title if src_title else "SDH"
                cmd.extend([f"-metadata:s:s:{out_idx}", f"title={hi_title}"])

    # Set language metadata for EXTERNAL subtitle streams (Bazarr sidecars
    # we mapped as additional inputs above). Output index continues after
    # the internal subs — use the COUNT ACTUALLY MAPPED (len of kept indices),
    # not len(source subs). When the strip dropped tracks (or the external
    # sidecar claimed the regular-eng slot), source-count overshoots and the
    # external sub's metadata lands on a non-existent output index, so ffmpeg
    # silently no-ops it and the sidecar ships with no language tag / no HI
    # disposition.
    if external_subs:
        internal_sub_count = len(kept_sub_indices) if include_subs else 0
        for i, sub_path in enumerate(external_subs):
            lang = _parse_sub_language(sub_path)
            out_idx = internal_sub_count + i
            cmd.extend([f"-metadata:s:s:{out_idx}", f"language={lang}"])
            # Mark hearing-impaired subs
            basename = os.path.basename(sub_path).lower()
            if ".hi." in basename or ".sdh." in basename:
                cmd.extend([f"-disposition:s:{out_idx}", "hearing_impaired"])

    # Pre-encode TMDb metadata: container-level title/date/comment go in via
    # ffmpeg `-metadata` so the encoded MKV carries them as soon as it lands.
    # Rich XML tags (DIRECTOR/CAST/GENRE/...) still flow through mkvpropedit
    # post-encode — ffmpeg can't write Matroska's structured Tag elements.
    cmd.extend(_build_tmdb_metadata_args(item))

    # Hard duration cap (see note at top of fn)
    if source_duration > 0:
        cmd.extend(["-t", f"{source_duration + 1:.3f}"])

    # Output (mkv container — no -movflags needed)
    cmd.append(output_path)

    return cmd


def _build_tmdb_metadata_args(item: dict) -> list[str]:
    """Container-level `-metadata` flags built from the TMDb-enriched entry.

    Returns a flat ffmpeg-arg list. Empty list if the item has no TMDb data
    (still safe to extend cmd with). Only writes simple top-level fields:

      * ``title``    — cleaned filename if available, else TMDb title
      * ``date``     — release_year for movies, first_air_year for series
      * ``language`` — TMDb ``original_language`` (ISO 639-1)
      * ``comment``  — a stable marker so future runs can recognise that this
                       file already passed through our pipeline

    Rich tags (director, cast, genres, etc.) are still written post-encode
    via mkvpropedit because Matroska's XML <Tag> structure isn't reachable
    from ffmpeg's flat ``-metadata`` interface.
    """
    args: list[str] = []
    tmdb = item.get("tmdb") or {}
    if not tmdb:
        return args

    title = tmdb.get("title") or tmdb.get("name")
    if not title:
        # Fall back to the filename without extension — better than nothing,
        # and avoids ffmpeg leaving the title field blank.
        fn = item.get("filename") or ""
        title = os.path.splitext(fn)[0] if fn else None
    if title:
        args.extend(["-metadata", f"title={title}"])

    year = tmdb.get("release_year") or tmdb.get("first_air_year")
    if year:
        args.extend(["-metadata", f"date={year}"])

    original_language = (tmdb.get("original_language") or "").strip().lower()
    if original_language:
        args.extend(["-metadata", f"language={original_language}"])

    args.extend(["-metadata", "comment=encoded by NASCleanup AV1 pipeline"])

    return args


def build_audio_remux_cmd(
    input_path: str, output_path: str, item: dict, config: dict, include_subs: bool = True
) -> list[str]:
    """Build ffmpeg command that copies video but transcodes bulky audio to EAC-3.

    Refuses to build for zero-audio sources — see `build_ffmpeg_cmd` docstring.
    """
    audio_streams = item.get("audio_streams") or []
    if not audio_streams:
        raise ValueError(
            f"build_audio_remux_cmd refused: source has zero audio streams ({input_path}). "
            "Zero-audio sources must be filtered out upstream."
        )

    audio_keep = _select_audio_streams(item, config)
    source_duration = item.get("duration_seconds") or get_duration(input_path) or 0

    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        input_path,
        "-map",
        "0:v:0",
    ]
    # Hard (non-optional) audio map — see comment in build_ffmpeg_cmd.
    if audio_keep is not None:
        for idx in audio_keep:
            cmd.extend(["-map", f"0:a:{idx}"])
    else:
        cmd.extend(["-map", "0:a"])

    if include_subs:
        _map_subtitle_streams(cmd, item, config)

    # Video: copy (already AV1)
    cmd.extend(["-c:v", "copy"])

    # Audio: smart transcode (output indices, not input).
    # audio_streams is guaranteed non-empty (refused at top of function).
    loudnorm = config.get("audio_loudnorm", False)
    kept_streams = (
        [(idx, audio_streams[idx]) for idx in audio_keep] if audio_keep else list(enumerate(audio_streams))
    )
    for out_idx, (_, audio) in enumerate(kept_streams):
        if _should_transcode_audio(audio, config):
            channels = audio.get("channels", 2)
            bitrate = config["audio_eac3_surround_bitrate"] if channels > 2 else config["audio_eac3_stereo_bitrate"]
            if loudnorm:
                cmd.extend([f"-filter:a:{out_idx}", "loudnorm=I=-24:LRA=7:TP=-2"])
            cmd.extend(
                [
                    f"-c:a:{out_idx}",
                    "eac3",
                    f"-b:a:{out_idx}",
                    bitrate,
                ]
            )
        else:
            cmd.extend([f"-c:a:{out_idx}", "copy"])

    # Subtitles: copy
    if include_subs:
        cmd.extend(["-c:s", "copy"])

    # Hard duration cap (same guard as build_ffmpeg_cmd)
    if source_duration > 0:
        cmd.extend(["-t", f"{source_duration + 1:.3f}"])

    cmd.append(output_path)
    return cmd


def _remux_to_mkv(input_path: str) -> Optional[str]:
    """Remux a problematic container to .mkv (stream copy, no re-encoding).

    If the first attempt fails (commonly due to incompatible subtitle formats
    like mov_text), retries without subtitles.

    Returns the remuxed file path on success, or None on failure.
    """
    remuxed_path = input_path + ".remux.mkv"
    # AVI/MPEG containers often have unset timestamps — generate them
    needs_genpts = Path(input_path).suffix.lower() in {".avi", ".mpg", ".mpeg", ".vob"}
    genpts_flags = ["-fflags", "+genpts"] if needs_genpts else []
    base_cmd = ["ffmpeg", "-y"] + genpts_flags + ["-i", input_path]
    attempts = [
        # First video + all audio + all subs (skips data streams, cover art)
        (base_cmd + ["-map", "0:v:0", "-map", "0:a", "-map", "0:s?", "-c", "copy", remuxed_path], None),
        # Drop subs too (handles mov_text / other incompatible sub formats)
        (base_cmd + ["-map", "0:v:0", "-map", "0:a", "-c", "copy", remuxed_path], "retrying without subtitles"),
    ]

    logging.info(f"Remuxing to MKV: {os.path.basename(input_path)}")

    last_stderr = ""
    for i, (cmd, retry_msg) in enumerate(attempts):
        if retry_msg:
            logging.info(f"  {retry_msg}")
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            if result.returncode == 0:
                logging.info(f"Remuxed: {format_bytes(os.path.getsize(remuxed_path))}")
                return remuxed_path

            last_stderr = result.stderr
            logging.warning(f"Remux attempt {i + 1}/{len(attempts)} failed (exit {result.returncode})")

        except Exception as e:
            logging.error(f"Remux exception: {e}")

        if os.path.exists(remuxed_path):
            os.remove(remuxed_path)

    # All attempts failed — log stderr from last attempt
    logging.error("Remux failed after all attempts")
    for line in last_stderr.strip().split("\n")[-5:]:
        logging.error(f"  ffmpeg: {line}")

    return None
