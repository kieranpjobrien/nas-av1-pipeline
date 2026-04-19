"""FFmpeg command builders for AV1 encoding and audio remux.
Extracted from encoding.py — pure functions that build ffmpeg commands.
No state management, no file I/O beyond ffprobe queries."""

import logging
import os
import subprocess
from pathlib import Path
from typing import Optional

from pipeline.config import ENG_LANGS, KEEP_LANGS, resolve_encode_params


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

    Transcodes everything except EAC-3 (already target codec).
    """
    codec_name = (audio.get("codec", "") or "").lower().strip()
    codec_raw = (audio.get("codec_raw", "") or audio.get("codec", "") or "").lower().strip()

    # EAC-3 is the target codec — no point re-encoding
    if codec_raw in ("eac3", "eac-3", "e-ac-3") or codec_name in ("eac3", "eac-3", "e-ac-3"):
        return False

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
    Rule: keep stream 0 (original language) + all English/und tracks.
    """
    if not config.get("strip_non_english_audio", True):
        return None

    audio_streams = item.get("audio_streams", [])
    if len(audio_streams) <= 2:
        return None  # not worth stripping 1-2 tracks

    keep = set()
    keep.add(0)  # always keep first stream (original language)

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
    """
    if not config.get("strip_non_english_subs", True):
        cmd.extend(["-map", "0:s?"])
        return

    subs = item.get("subtitle_streams", [])
    if not subs:
        cmd.extend(["-map", "0:s?"])  # no metadata — let ffmpeg figure it out
        return

    # Keep exactly 1 regular English sub + forced. Strip HI, duplicates, foreign.
    mapped = 0
    found_regular_eng = False
    for i, sub in enumerate(subs):
        lang = (sub.get("language") or "").lower().strip()
        title = (sub.get("title") or "").lower()
        is_forced = "forced" in title or "foreign" in title
        is_hi = "hearing" in title or "sdh" in title or ".hi" in title

        if is_forced:
            cmd.extend(["-map", f"0:s:{i}"])
            mapped += 1
        elif lang in ENG_LANGS and not is_hi and not found_regular_eng:
            cmd.extend(["-map", f"0:s:{i}"])
            mapped += 1
            found_regular_eng = True

    if mapped == 0:
        # No English subs found — map all to be safe (might have unlabelled ones)
        cmd.extend(["-map", "0:s?"])
    elif mapped < len(subs):
        stripped = len(subs) - mapped
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
) -> list[str]:
    """Build the ffmpeg command for NVENC AV1 encoding."""
    is_hdr = item.get("hdr", False)
    params = resolve_encode_params(config, item)

    # Pixel format: 10-bit for HDR (mandatory), also 10-bit for SDR (banding resistance)
    pix_fmt = config.get("pixel_format_hdr" if is_hdr else "pixel_format_sdr", "yuv420p10le")

    # Determine which audio streams to keep before building the command
    audio_keep = _select_audio_streams(item, config)

    cmd = [
        "ffmpeg",
        "-y",
        "-err_detect",
        "ignore_err",  # continue past corrupt data in input
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
    ]

    # Add external subtitle files as additional inputs
    if external_subs:
        for sub_path in external_subs:
            cmd.extend(["-i", sub_path])

    # Map only the first video stream from input 0
    cmd.extend(["-map", "0:v:0"])

    # Audio stream mapping
    if audio_keep is not None:
        for idx in audio_keep:
            cmd.extend(["-map", f"0:a:{idx}"])
    else:
        cmd.extend(["-map", "0:a?"])

    if include_subs:
        _map_subtitle_streams(cmd, item, config)

    # Map external subtitle inputs (inputs 1, 2, 3, ...)
    if external_subs:
        for i in range(len(external_subs)):
            cmd.extend(["-map", f"{i + 1}:s"])

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
            "-pix_fmt",
            pix_fmt,
        ]
    )

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
    # input indices when non-English audio streams have been stripped)
    if config["audio_mode"] == "copy":
        cmd.extend(["-c:a", "copy"])
    elif config["audio_mode"] == "smart":
        audio_streams = item.get("audio_streams", [])
        if not audio_streams:
            cmd.extend(["-c:a", "copy"])
        else:
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

    # Strip encoder metadata bloat (scene group tags, encoder info)
    cmd.extend(["-map_metadata", "-1"])

    # Output (mkv container — no -movflags needed)
    cmd.append(output_path)

    return cmd


def build_audio_remux_cmd(
    input_path: str, output_path: str, item: dict, config: dict, include_subs: bool = True
) -> list[str]:
    """Build ffmpeg command that copies video but transcodes bulky audio to EAC-3."""
    audio_keep = _select_audio_streams(item, config)

    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        input_path,
        "-map",
        "0:v:0",
    ]
    if audio_keep is not None:
        for idx in audio_keep:
            cmd.extend(["-map", f"0:a:{idx}"])
    else:
        cmd.extend(["-map", "0:a?"])

    if include_subs:
        _map_subtitle_streams(cmd, item, config)

    # Video: copy (already AV1)
    cmd.extend(["-c:v", "copy"])

    # Audio: smart transcode (output indices, not input)
    audio_streams = item.get("audio_streams", [])
    if not audio_streams:
        cmd.extend(["-c:a", "copy"])
    else:
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
