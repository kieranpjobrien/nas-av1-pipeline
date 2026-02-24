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


def build_ffmpeg_cmd(input_path: str, output_path: str, item: dict, config: dict) -> list[str]:
    """Build the ffmpeg command for NVENC AV1 encoding."""
    is_hdr = item.get("hdr", False)
    params = resolve_encode_params(config, item)

    # Pixel format: 10-bit for HDR (mandatory), also 10-bit for SDR (banding resistance)
    pix_fmt = config.get("pixel_format_hdr" if is_hdr else "pixel_format_sdr", "yuv420p10le")

    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-map", "0",
    ]

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
    # Temporal AQ only for movies (diminishing returns on series, adds encode time)
    if params["content_type"] == "movie":
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
    lossless_codecs = config.get("lossless_audio_codecs", set())
    if config["audio_mode"] == "copy":
        cmd.extend(["-c:a", "copy"])
    elif config["audio_mode"] == "smart":
        audio_streams = item.get("audio_streams", [])
        if not audio_streams:
            cmd.extend(["-c:a", "copy"])
        else:
            for i, audio in enumerate(audio_streams):
                codec_name = (audio.get("codec", "") or "").lower().strip()
                is_lossless = audio.get("lossless", False) or codec_name in lossless_codecs
                if is_lossless:
                    channels = audio.get("channels", 2)
                    bitrate = (config["audio_eac3_surround_bitrate"] if channels > 2
                               else config["audio_eac3_stereo_bitrate"])
                    cmd.extend([
                        f"-c:a:{i}", "eac3",
                        f"-b:a:{i}", bitrate,
                    ])
                else:
                    cmd.extend([f"-c:a:{i}", "copy"])

    # Subtitles: copy all
    cmd.extend(["-c:s", "copy"])

    # Output (mkv container — no -movflags needed)
    cmd.append(output_path)

    return cmd


def _remux_to_mkv(input_path: str) -> Optional[str]:
    """Remux a problematic container to .mkv (stream copy, no re-encoding).

    Returns the remuxed file path on success, or None on failure.
    """
    remuxed_path = input_path + ".remux.mkv"
    cmd = ["ffmpeg", "-y", "-i", input_path, "-map", "0",
           "-c", "copy", remuxed_path]
    logging.info(f"Remuxing to MKV: {os.path.basename(input_path)}")

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            logging.error(f"Remux failed (exit {result.returncode})")
            for line in result.stderr.strip().split("\n")[-5:]:
                logging.error(f"  ffmpeg: {line}")
            if os.path.exists(remuxed_path):
                os.remove(remuxed_path)
            return None

        logging.info(f"Remuxed: {format_bytes(os.path.getsize(remuxed_path))}")
        return remuxed_path

    except Exception as e:
        logging.error(f"Remux exception: {e}")
        if os.path.exists(remuxed_path):
            os.remove(remuxed_path)
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

    state.set_file(source_filepath, FileStatus.ENCODING,
                   local_path=local_input, output_path=output_path)

    cmd = build_ffmpeg_cmd(encode_input, output_path, item, config)
    logging.info(f"Encoding: {item['filename']}")
    enc_params = resolve_encode_params(config, item)
    logging.info(f"  {enc_params['content_type'].upper()} | {item['resolution']} | "
                 f"HDR: {item.get('hdr', False)} | "
                 f"CQ: {enc_params['cq']} | Preset: {enc_params['preset']} | "
                 f"Multipass: {enc_params['multipass']}")
    logging.debug(f"  CMD: {' '.join(cmd)}")

    try:
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

        if process.returncode != 0:
            logging.error(f"Encode failed (exit {process.returncode})")
            for line in stderr.strip().split("\n")[-5:]:
                logging.error(f"  ffmpeg: {line}")
            state.set_file(source_filepath, FileStatus.ERROR,
                           error=f"ffmpeg exit {process.returncode}", stage="encode")
            if os.path.exists(output_path):
                os.remove(output_path)
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
                     f"{format_bytes(input_size)} → {format_bytes(output_size)} "
                     f"({ratio:.1f}% reduction, {format_bytes(saved)} saved)")

        state.set_file(source_filepath, FileStatus.ENCODED,
                       output_path=output_path,
                       output_size_bytes=output_size,
                       input_size_bytes=input_size,
                       bytes_saved=saved,
                       compression_ratio=round(ratio, 1),
                       encode_time_secs=round(elapsed, 1))

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
