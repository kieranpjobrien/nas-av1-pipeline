"""Pipeline configuration defaults and constants."""

# Default configuration — all values can be overridden via CLI args
DEFAULT_CONFIG = {
    # Staging limits (bytes)
    "max_staging_bytes": 2_500_000_000_000,       # 2.5 TB total local staging
    "max_fetch_buffer_bytes": 500_000_000_000,     # 500 GB fetch buffer
    "min_free_space_bytes": 50_000_000_000,        # 50 GB minimum free on staging drive

    # Encoding: NVENC AV1 (RTX 4080)
    "video_codec": "av1_nvenc",
    "cq": {
        "movie":  {"4K_HDR": 22, "4K_SDR": 27, "1080p": 28, "720p": 30, "480p": 30, "SD": 30},
        "series": {"4K_HDR": 24, "4K_SDR": 30, "1080p": 30, "720p": 32, "480p": 32, "SD": 32},
    },
    "nvenc_preset": {
        "movie":  {"4K_HDR": "p7", "4K_SDR": "p5", "1080p": "p5", "720p": "p4", "480p": "p4", "SD": "p4"},
        "series": {"4K_HDR": "p5", "4K_SDR": "p4", "1080p": "p4", "720p": "p4", "480p": "p4", "SD": "p4"},
    },
    "nvenc_multipass": {
        "movie":  {"4K_HDR": "fullres", "4K_SDR": "qres", "1080p": "qres", "720p": "disabled", "480p": "disabled", "SD": "disabled"},
        "series": {"4K_HDR": "qres", "4K_SDR": "disabled", "1080p": "disabled", "720p": "disabled", "480p": "disabled", "SD": "disabled"},
    },
    "nvenc_lookahead": {
        "movie":  {"4K_HDR": 32, "4K_SDR": 24, "1080p": 24, "720p": 16, "480p": 16, "SD": 16},
        "series": {"4K_HDR": 24, "4K_SDR": 16, "1080p": 16, "720p": 16, "480p": 16, "SD": 16},
    },
    "nvenc_maxrate": {
        "movie":  {"4K_HDR": "40M", "4K_SDR": "20M", "1080p": "20M", "720p": None, "480p": None, "SD": None},
        "series": {"4K_HDR": "20M", "4K_SDR": None, "1080p": None, "720p": None, "480p": None, "SD": None},
    },
    "nvenc_bufsize": {
        "movie":  {"4K_HDR": "80M", "4K_SDR": "40M", "1080p": "40M", "720p": None, "480p": None, "SD": None},
        "series": {"4K_HDR": "40M", "4K_SDR": None, "1080p": None, "720p": None, "480p": None, "SD": None},
    },
    "pixel_format_hdr": "p010le",  # 10-bit for HDR (mandatory) — NVENC uses p010le
    "pixel_format_sdr": "p010le",  # 10-bit for SDR too (better banding resistance)

    # Audio: smart mode — lossless→EAC3, lossy→copy
    "audio_mode": "smart",  # "copy" = passthrough, "smart" = lossless→EAC3/lossy→copy
    "audio_eac3_surround_bitrate": "640k",   # EAC3 for surround (>2 channels)
    "audio_eac3_stereo_bitrate": "256k",     # EAC3 for stereo/mono

    # Lossless audio codecs to transcode (all others are copied as-is)
    "lossless_audio_codecs": {"truehd", "dts-hd ma", "dts-hd.ma", "flac", "pcm_s16le",
                              "pcm_s24le", "pcm_s32le", "pcm_f32le", "pcm_s16be",
                              "pcm_s24be", "pcm_s32be", "pcm_f32be", "alac"},

    # Behaviour
    "overwrite_existing": False,
    "replace_original": True,   # Replace original on NAS after verify
    "verify_duration_tolerance_secs": 2.0,

    # Priority tiers (order matters — biggest savings first)
    "priority_tiers": [
        {"name": "H.264 1080p",            "codec": "H.264",        "resolution": "1080p", "min_bitrate_kbps": 0},
        {"name": "Bloated HEVC 1080p",     "codec": "HEVC (H.265)", "resolution": "1080p", "min_bitrate_kbps": 15000},
        {"name": "Bloated HEVC 4K",        "codec": "HEVC (H.265)", "resolution": "4K",    "min_bitrate_kbps": 25000},
        {"name": "H.264 720p/other",       "codec": "H.264",        "resolution": None,    "min_bitrate_kbps": 0},
        {"name": "HEVC 1080p",             "codec": "HEVC (H.265)", "resolution": "1080p", "min_bitrate_kbps": 0, "max_bitrate_kbps": 15000},
        {"name": "HEVC 4K >20Mbps",        "codec": "HEVC (H.265)", "resolution": "4K",    "min_bitrate_kbps": 20000, "max_bitrate_kbps": 25000},
        {"name": "HEVC 4K <=20Mbps",       "codec": "HEVC (H.265)", "resolution": "4K",    "min_bitrate_kbps": 0, "max_bitrate_kbps": 20000},
        {"name": "HEVC 720p/SD + other",   "codec": "HEVC (H.265)", "resolution": None,    "min_bitrate_kbps": 0},
        {"name": "Other codecs",           "codec": None,           "resolution": None,    "min_bitrate_kbps": 0},
    ],
}

# Containers that can cause NVENC failures — remux to .mkv before encoding
REMUX_EXTENSIONS = {".m2ts", ".avi", ".wmv", ".ts", ".m2v", ".vob", ".mpg", ".mpeg", ".mp4"}


def get_res_key(item: dict) -> str:
    """Derive a resolution key (e.g. '4K_HDR', '1080p') from a queue item."""
    resolution = item.get("resolution", "1080p")
    is_hdr = item.get("hdr", False)
    if resolution == "4K" and is_hdr:
        return "4K_HDR"
    if resolution == "4K":
        return "4K_SDR"
    if resolution in ("1080p", "720p", "480p", "SD"):
        return resolution
    return "SD"


def resolve_encode_params(config: dict, item: dict) -> dict:
    """Resolve NVENC encode parameters based on content type and resolution.

    Returns dict with keys: cq, preset, multipass, lookahead, maxrate, bufsize.
    """
    library_type = item.get("library_type", "movie")
    content_type = "series" if library_type in ("series", "show", "tv", "anime") else "movie"
    resolution = item.get("resolution", "1080p")
    is_hdr = item.get("hdr", False)

    # Build resolution key for config lookup
    if resolution == "4K" and is_hdr:
        res_key = "4K_HDR"
    elif resolution == "4K":
        res_key = "4K_SDR"
    elif resolution in ("1080p", "720p", "480p", "SD"):
        res_key = resolution
    else:
        res_key = "SD"

    return {
        "cq": config["cq"].get(content_type, {}).get(res_key, 30),
        "preset": config["nvenc_preset"].get(content_type, {}).get(res_key, "p4"),
        "multipass": config["nvenc_multipass"].get(content_type, {}).get(res_key, "disabled"),
        "lookahead": config["nvenc_lookahead"].get(content_type, {}).get(res_key, 16),
        "maxrate": config["nvenc_maxrate"].get(content_type, {}).get(res_key, None),
        "bufsize": config["nvenc_bufsize"].get(content_type, {}).get(res_key, None),
        "content_type": content_type,
        "res_key": res_key,
    }
