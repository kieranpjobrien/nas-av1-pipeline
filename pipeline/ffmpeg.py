"""FFmpeg command builders for AV1 encoding and audio remux.
Extracted from encoding.py — pure functions that build ffmpeg commands.
No state management, no file I/O beyond ffprobe queries."""

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


def _map_subtitle_streams(cmd: list[str], item: dict, config: dict) -> None:
    """Add per-stream subtitle mappings, keeping only English/undefined tracks.

    If strip_non_english_subs is disabled, maps all subs with -map 0:s?.

    Uses pipeline.streams.parse_sub_stream + is_hi_internal for HI detection.
    The HI rule is stricter than the old inline check (it also catches ``cc``
    and disposition flags) — see pipeline/streams.py for details.
    """
    if not config.get("strip_non_english_subs", True):
        cmd.extend(["-map", "0:s?"])
        return

    raw_subs = item.get("subtitle_streams", [])
    if not raw_subs:
        cmd.extend(["-map", "0:s?"])  # no metadata — let ffmpeg figure it out
        return

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
        return

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
    from pipeline.config import ENG_LANGS

    mapped = 0
    found_regular_eng = False
    for i, sub in enumerate(parsed_subs):
        if sub.is_forced:
            cmd.extend(["-map", f"0:s:{i}?"])
            mapped += 1
        elif sub.language in ENG_LANGS and not sub.is_hi and not found_regular_eng:
            cmd.extend(["-map", f"0:s:{i}?"])
            mapped += 1
            found_regular_eng = True

    if mapped == 0:
        # No English subs found — map all to be safe (might have unlabelled ones)
        cmd.extend(["-map", "0:s?"])
    elif mapped < len(raw_subs):
        stripped = len(raw_subs) - mapped
        logging.info(f"  Stripped {stripped} non-English subtitle stream(s)")


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
    # Force software decode upfront for codecs that have a reputation for
    # this kind of silent NVDEC degradation. CPU decode is slower than NVDEC
    # but vastly faster than NVDEC's stuck-mode, and NVENC encode runs at
    # full speed regardless.
    src_codec = (item.get("video_codec") or "").lower()
    if use_hwaccel and src_codec in _NVDEC_SILENT_DEGRADATION_CODECS:
        logging.warning(
            f"  Forcing software decode for {src_codec.upper()} source "
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

    if include_subs:
        _map_subtitle_streams(cmd, item, config)

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
            ]
        )
    elif is_hdr:
        cmd.extend(
            [
                "-color_primaries",
                "bt2020",
                "-color_trc",
                "smpte2084",
                "-colorspace",
                "bt2020nc",
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

    # Set language metadata for external subtitle streams
    if external_subs:
        # Count internal subtitle streams to get the right output index
        internal_sub_count = len(item.get("subtitle_streams", [])) if include_subs else 0
        for i, sub_path in enumerate(external_subs):
            lang = _parse_sub_language(sub_path)
            out_idx = internal_sub_count + i
            cmd.extend([f"-metadata:s:s:{out_idx}", f"language={lang}"])
            # Mark hearing-impaired subs
            basename = os.path.basename(sub_path).lower()
            if ".hi." in basename or ".sdh." in basename:
                cmd.extend([f"-disposition:s:{out_idx}", "hearing_impaired"])

    # Strip encoder metadata bloat (scene group tags, encoder info) BEFORE we
    # write our own — `-map_metadata -1` drops everything from the source.
    cmd.extend(["-map_metadata", "-1"])

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
