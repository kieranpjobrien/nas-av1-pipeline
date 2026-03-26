"""FFmpeg command building and encoding logic."""

import hashlib
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Optional

from pipeline.config import REMUX_EXTENSIONS, resolve_encode_params
from pipeline.state import FileStatus, PipelineState


def get_duration(filepath: str) -> Optional[float]:
    """Get file duration via ffprobe."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_format", str(filepath),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30,
                                encoding="utf-8", errors="replace")
        if result.returncode == 0:
            import json
            data = json.loads(result.stdout)
            return float(data.get("format", {}).get("duration", 0))
    except Exception:
        pass
    return None


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


def _should_transcode_audio(audio: dict, config: dict) -> bool:
    """Decide whether an audio stream should be transcoded to EAC-3.

    Transcodes: lossless codecs (always), DTS core >700kbps, AC-3 >400kbps.
    Copies: AAC, Opus, EAC-3, MP3, low-bitrate AC-3/DTS, anything else efficient.
    """
    lossless_codecs = config.get("lossless_audio_codecs", set())
    codec_name = (audio.get("codec", "") or "").lower().strip()
    codec_raw = (audio.get("codec_raw", "") or audio.get("codec", "") or "").lower().strip()

    # Lossless: always transcode
    if audio.get("lossless", False) or codec_raw in lossless_codecs or codec_name in lossless_codecs:
        return True

    # Bitrate-based thresholds for lossy codecs
    thresholds = config.get("audio_bulky_threshold_kbps", {})
    bitrate = audio.get("bitrate_kbps") or 0
    for codec_pattern, threshold in thresholds.items():
        if codec_pattern in codec_raw or codec_pattern in codec_name:
            if bitrate > threshold:
                return True

    return False


def has_bulky_audio(item: dict, config: dict) -> bool:
    """Check if any audio stream in an item would benefit from transcoding."""
    for audio in item.get("audio_streams", []):
        if _should_transcode_audio(audio, config):
            return True
    return False


def build_ffmpeg_cmd(input_path: str, output_path: str, item: dict, config: dict,
                     include_subs: bool = True) -> list[str]:
    """Build the ffmpeg command for NVENC AV1 encoding."""
    is_hdr = item.get("hdr", False)
    params = resolve_encode_params(config, item)

    # Pixel format: 10-bit for HDR (mandatory), also 10-bit for SDR (banding resistance)
    pix_fmt = config.get("pixel_format_hdr" if is_hdr else "pixel_format_sdr", "yuv420p10le")

    cmd = [
        "ffmpeg", "-y",
        "-err_detect", "ignore_err",  # continue past corrupt data in input
        "-i", input_path,
        # Map only the first video stream, all audio, and (optionally) subs.
        # Excludes data streams, cover art (mjpeg/bmp), and other junk
        # that NVENC or MKV can't handle.
        "-map", "0:v:0",
        "-map", "0:a?",
    ]
    if include_subs:
        cmd.extend(["-map", "0:s?"])

    # Video: NVENC AV1
    cmd.extend([
        "-c:v", config["video_codec"],
        "-cq", str(params["cq"]),
        "-preset", params["preset"],
        "-tune", "hq",
        "-rc", "vbr",
        "-b:v", "0",
        "-pix_fmt", pix_fmt,
    ])

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

    # HDR: preserve colour metadata
    if is_hdr:
        cmd.extend([
            "-color_primaries", "bt2020",
            "-color_trc", "smpte2084",
            "-colorspace", "bt2020nc",
        ])

    # Audio handling
    if config["audio_mode"] == "copy":
        cmd.extend(["-c:a", "copy"])
    elif config["audio_mode"] == "smart":
        audio_streams = item.get("audio_streams", [])
        if not audio_streams:
            cmd.extend(["-c:a", "copy"])
        else:
            loudnorm = config.get("audio_loudnorm", False)
            for i, audio in enumerate(audio_streams):
                if _should_transcode_audio(audio, config):
                    channels = audio.get("channels", 2)
                    bitrate = (config["audio_eac3_surround_bitrate"] if channels > 2
                               else config["audio_eac3_stereo_bitrate"])
                    if loudnorm:
                        cmd.extend([f"-filter:a:{i}", "loudnorm=I=-24:LRA=7:TP=-2"])
                    cmd.extend([
                        f"-c:a:{i}", "eac3",
                        f"-b:a:{i}", bitrate,
                    ])
                else:
                    cmd.extend([f"-c:a:{i}", "copy"])

    # Subtitles: copy all (when mapped)
    if include_subs:
        cmd.extend(["-c:s", "copy"])

    # Output (mkv container — no -movflags needed)
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
        (base_cmd + ["-map", "0:v:0", "-map", "0:a", "-c", "copy", remuxed_path],
         "retrying without subtitles"),
    ]

    logging.info(f"Remuxing to MKV: {os.path.basename(input_path)}")

    last_stderr = ""
    for i, (cmd, retry_msg) in enumerate(attempts):
        if retry_msg:
            logging.info(f"  {retry_msg}")
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
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


def stage_encode(source_filepath: str, item: dict, staging_dir: str,
                 config: dict, state: PipelineState) -> Optional[str]:
    """Encode the fetched file to AV1. Returns output path or None."""
    file_info = state.get_file(source_filepath)
    if not file_info:
        return None

    local_input = file_info.get("local_path")
    if not local_input or not os.path.exists(local_input):
        logging.error(f"Local file missing: {local_input}")
        state.set_file(source_filepath, FileStatus.ERROR, error="local file missing", stage="encode")
        return None

    # Remux problematic containers before encoding
    remuxed_path = None
    if Path(local_input).suffix.lower() in REMUX_EXTENSIONS:
        remuxed_path = _remux_to_mkv(local_input)
        if remuxed_path is None:
            state.set_file(source_filepath, FileStatus.ERROR,
                           error="remux failed", stage="encode")
            return None
        encode_input = remuxed_path
    else:
        encode_input = local_input

    # Output path
    encode_dir = os.path.join(staging_dir, "encoded")
    os.makedirs(encode_dir, exist_ok=True)
    # Always output as .mkv
    out_name = Path(item["filename"]).stem + ".mkv"
    safe_name = hashlib.md5(source_filepath.encode()).hexdigest()[:12] + "_" + out_name
    output_path = os.path.join(encode_dir, safe_name)

    encode_start = time.time()
    state.set_file(source_filepath, FileStatus.ENCODING,
                   local_path=local_input, output_path=output_path,
                   encode_start=encode_start)

    logging.info(f"Encoding: {item['filename']}")
    enc_params = resolve_encode_params(config, item)
    profile_str = f" | Profile: {enc_params['profile']}" if enc_params.get("profile", "baseline") != "baseline" else ""
    logging.info(f"  {enc_params['content_type'].upper()} | {item['resolution']} | "
                 f"HDR: {item.get('hdr', False)} | "
                 f"CQ: {enc_params['cq']} | Preset: {enc_params['preset']} | "
                 f"Multipass: {enc_params['multipass']}{profile_str}")

    try:
        # Try with subs first, retry without if subtitle codec is unsupported
        for include_subs in (True, False):
            cmd = build_ffmpeg_cmd(encode_input, output_path, item, config,
                                   include_subs=include_subs)
            logging.debug(f"  CMD: {' '.join(cmd)}")

            start = time.time()
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf-8",
                errors="replace",
            )
            _, stderr = process.communicate()
            elapsed = time.time() - start

            if process.returncode == 0:
                break  # success

            if os.path.exists(output_path):
                os.remove(output_path)

            # If first attempt failed and subs look like the cause, retry without
            if include_subs and ("subtitle" in stderr.lower() or "codec none" in stderr.lower()):
                logging.warning(f"Encode failed due to subtitle issue, retrying without subs")
                continue

            # Non-subtitle failure or second attempt failed
            logging.error(f"Encode failed (exit {process.returncode})")
            for line in stderr.strip().split("\n")[-5:]:
                logging.error(f"  ffmpeg: {line}")
            state.set_file(source_filepath, FileStatus.ERROR,
                           error=f"ffmpeg exit {process.returncode}", stage="encode")
            if remuxed_path and os.path.exists(remuxed_path):
                os.remove(remuxed_path)
            return None

        # Verify output exists and has reasonable size
        if not os.path.exists(output_path):
            logging.error("Output file not created")
            state.set_file(source_filepath, FileStatus.ERROR,
                           error="output not created", stage="encode")
            return None

        output_size = os.path.getsize(output_path)
        input_size = os.path.getsize(local_input)

        # Sanity check: output shouldn't be larger than input (with some tolerance)
        if output_size > input_size * 1.1:
            logging.warning(f"Output larger than input! {format_bytes(output_size)} > {format_bytes(input_size)}")

        # Duration check
        input_duration = item.get("duration_seconds", 0)
        output_duration = get_duration(output_path) or 0
        tolerance = config["verify_duration_tolerance_secs"]
        if input_duration > 0 and abs(input_duration - output_duration) > tolerance:
            logging.warning(f"Duration mismatch: input={input_duration:.1f}s, output={output_duration:.1f}s")

        saved = input_size - output_size
        ratio = (1 - output_size / input_size) * 100 if input_size > 0 else 0
        speed = input_size / elapsed / (1024**2) if elapsed > 0 else 0

        logging.info(f"Encoded in {format_duration(elapsed)}: "
                     f"{format_bytes(input_size)} -> {format_bytes(output_size)} "
                     f"({ratio:.1f}% reduction, {format_bytes(saved)} saved)")

        state.set_file(source_filepath, FileStatus.ENCODED,
                       output_path=output_path,
                       output_size_bytes=output_size,
                       input_size_bytes=input_size,
                       bytes_saved=saved,
                       compression_ratio=round(ratio, 1),
                       encode_time_secs=round(elapsed, 1),
                       encode_end=time.time())

        # Clean up remuxed intermediate file
        if remuxed_path and os.path.exists(remuxed_path):
            os.remove(remuxed_path)

        # Clean up local fetch copy to free staging space
        if os.path.exists(local_input):
            os.remove(local_input)
            logging.info(f"Cleaned up fetched file: {format_bytes(input_size)} freed")

        state.stats["total_encode_time_secs"] += elapsed

        return output_path

    except Exception as e:
        logging.error(f"Encode exception: {e}")
        state.set_file(source_filepath, FileStatus.ERROR, error=str(e), stage="encode")
        if os.path.exists(output_path):
            os.remove(output_path)
        if remuxed_path and os.path.exists(remuxed_path):
            os.remove(remuxed_path)
        return None


def build_audio_remux_cmd(input_path: str, output_path: str, item: dict,
                          config: dict, include_subs: bool = True) -> list[str]:
    """Build ffmpeg command that copies video but transcodes bulky audio to EAC-3."""
    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-map", "0:v:0",
        "-map", "0:a?",
    ]
    if include_subs:
        cmd.extend(["-map", "0:s?"])

    # Video: copy (already AV1)
    cmd.extend(["-c:v", "copy"])

    # Audio: smart transcode
    audio_streams = item.get("audio_streams", [])
    if not audio_streams:
        cmd.extend(["-c:a", "copy"])
    else:
        loudnorm = config.get("audio_loudnorm", False)
        for i, audio in enumerate(audio_streams):
            if _should_transcode_audio(audio, config):
                channels = audio.get("channels", 2)
                bitrate = (config["audio_eac3_surround_bitrate"] if channels > 2
                           else config["audio_eac3_stereo_bitrate"])
                if loudnorm:
                    cmd.extend([f"-filter:a:{i}", "loudnorm=I=-24:LRA=7:TP=-2"])
                cmd.extend([
                    f"-c:a:{i}", "eac3",
                    f"-b:a:{i}", bitrate,
                ])
            else:
                cmd.extend([f"-c:a:{i}", "copy"])

    # Subtitles: copy
    if include_subs:
        cmd.extend(["-c:s", "copy"])

    cmd.append(output_path)
    return cmd


def stage_audio_remux(source_filepath: str, item: dict, staging_dir: str,
                      config: dict, state: PipelineState) -> Optional[str]:
    """Audio-only remux: copy video, transcode bulky audio to EAC-3. Returns output path or None."""
    file_info = state.get_file(source_filepath)
    if not file_info:
        return None

    local_input = file_info.get("local_path")
    if not local_input or not os.path.exists(local_input):
        logging.error(f"Local file missing: {local_input}")
        state.set_file(source_filepath, FileStatus.ERROR, error="local file missing", stage="audio_remux")
        return None

    encode_dir = os.path.join(staging_dir, "encoded")
    os.makedirs(encode_dir, exist_ok=True)
    out_name = Path(item["filename"]).stem + ".mkv"
    safe_name = hashlib.md5(source_filepath.encode()).hexdigest()[:12] + "_" + out_name
    output_path = os.path.join(encode_dir, safe_name)

    encode_start = time.time()
    state.set_file(source_filepath, FileStatus.ENCODING,
                   local_path=local_input, output_path=output_path,
                   encode_start=encode_start)

    logging.info(f"Audio remux: {item['filename']}")
    audio_streams = item.get("audio_streams", [])
    bulky = [a for a in audio_streams if _should_transcode_audio(a, config)]
    logging.info(f"  {len(bulky)}/{len(audio_streams)} audio streams to transcode")

    try:
        for include_subs in (True, False):
            cmd = build_audio_remux_cmd(local_input, output_path, item, config,
                                        include_subs=include_subs)
            logging.debug(f"  CMD: {' '.join(cmd)}")

            start = time.time()
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                encoding="utf-8", errors="replace",
            )
            _, stderr = process.communicate()
            elapsed = time.time() - start

            if process.returncode == 0:
                break

            if os.path.exists(output_path):
                os.remove(output_path)

            if include_subs and ("subtitle" in stderr.lower() or "codec none" in stderr.lower()):
                logging.warning("Audio remux failed due to subtitle issue, retrying without subs")
                continue

            logging.error(f"Audio remux failed (exit {process.returncode})")
            for line in stderr.strip().split("\n")[-5:]:
                logging.error(f"  ffmpeg: {line}")
            state.set_file(source_filepath, FileStatus.ERROR,
                           error=f"ffmpeg exit {process.returncode}", stage="audio_remux")
            return None

        if not os.path.exists(output_path):
            logging.error("Output file not created")
            state.set_file(source_filepath, FileStatus.ERROR,
                           error="output not created", stage="audio_remux")
            return None

        output_size = os.path.getsize(output_path)
        input_size = os.path.getsize(local_input)

        # Duration check
        input_duration = item.get("duration_seconds", 0)
        output_duration = get_duration(output_path) or 0
        tolerance = config["verify_duration_tolerance_secs"]
        if input_duration > 0 and abs(input_duration - output_duration) > tolerance:
            logging.warning(f"Duration mismatch: input={input_duration:.1f}s, output={output_duration:.1f}s")

        saved = input_size - output_size
        ratio = (1 - output_size / input_size) * 100 if input_size > 0 else 0

        logging.info(f"Audio remux in {format_duration(elapsed)}: "
                     f"{format_bytes(input_size)} -> {format_bytes(output_size)} "
                     f"({ratio:.1f}% reduction, {format_bytes(saved)} saved)")

        state.set_file(source_filepath, FileStatus.ENCODED,
                       output_path=output_path,
                       output_size_bytes=output_size,
                       input_size_bytes=input_size,
                       bytes_saved=saved,
                       compression_ratio=round(ratio, 1),
                       encode_time_secs=round(elapsed, 1),
                       encode_end=time.time(),
                       audio_only=True)

        # Clean up local fetch copy
        if os.path.exists(local_input):
            os.remove(local_input)
            logging.info(f"Cleaned up fetched file: {format_bytes(input_size)} freed")

        state.stats["total_encode_time_secs"] += elapsed

        return output_path

    except Exception as e:
        logging.error(f"Audio remux exception: {e}")
        state.set_file(source_filepath, FileStatus.ERROR, error=str(e), stage="audio_remux")
        if os.path.exists(output_path):
            os.remove(output_path)
        return None
