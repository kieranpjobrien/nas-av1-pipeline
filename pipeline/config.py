"""Pipeline configuration defaults and constants."""

# Language sets used across the pipeline for track filtering
KEEP_LANGS: set[str] = {"eng", "en", "english", "und", ""}
ENG_LANGS: set[str] = {"eng", "en", "english"}

# Quality profiles — named presets that modify encoding params.
# "baseline" is the default. "protected" preserves quality (reference films).
# "lossy" trades quality for space savings (sitcoms, reality TV, etc.).
#
# Each profile provides offsets or overrides applied on top of DEFAULT_CONFIG.
# - cq_offset: added to base CQ (negative = better quality, positive = worse)
# - preset_override: force a specific NVENC preset (None = use base)
# - multipass_override: force multipass mode (None = use base)
# - lookahead_override: force lookahead frames (None = use base)
# - temporal_aq: override temporal AQ (None = use base)
QUALITY_PROFILES = {
    "protected": {
        "description": "High quality — reference films, visually important content",
        "cq_offset": -3,
        "preset_override": "p7",
        "multipass_override": "fullres",
        "lookahead_override": 32,
        "temporal_aq": True,
    },
    "baseline": {
        "description": "Standard encoding — good balance of quality and space",
        "cq_offset": 0,
        "preset_override": None,
        "multipass_override": None,
        "lookahead_override": None,
        "temporal_aq": None,
    },
    "lossy": {
        "description": "Aggressive compression — expendable content (sitcoms, reality TV)",
        "cq_offset": 6,
        "preset_override": "p4",
        "multipass_override": "disabled",
        "lookahead_override": 16,
        "temporal_aq": False,
    },
    "tonemap": {
        "description": "HDR→SDR tone-mapping — converts BT.2020 to BT.709",
        "cq_offset": 0,
        "preset_override": None,
        "multipass_override": None,
        "lookahead_override": None,
        "temporal_aq": None,
        "tonemap": True,
    },
}


# Default configuration — all values can be overridden via CLI args
DEFAULT_CONFIG = {
    # Staging limits (bytes)
    "max_staging_bytes": 2_500_000_000_000,  # 2.5 TB total local staging
    "max_fetch_buffer_bytes": 200_000_000_000,  # 200 GB fetch buffer
    "min_free_space_bytes": 50_000_000_000,  # 50 GB minimum free on staging drive
    # Encoding: NVENC AV1 (RTX 4080)
    "video_codec": "av1_nvenc",
    "cq": {
        "movie": {"4K_HDR": 22, "4K_SDR": 27, "1080p": 28, "720p": 30, "480p": 30, "SD": 30},
        "series": {"4K_HDR": 24, "4K_SDR": 30, "1080p": 30, "720p": 32, "480p": 32, "SD": 32},
    },
    "nvenc_preset": {
        "movie": {"4K_HDR": "p7", "4K_SDR": "p5", "1080p": "p5", "720p": "p4", "480p": "p4", "SD": "p4"},
        "series": {"4K_HDR": "p5", "4K_SDR": "p4", "1080p": "p4", "720p": "p4", "480p": "p4", "SD": "p4"},
    },
    "nvenc_multipass": {
        "movie": {
            "4K_HDR": "fullres",
            "4K_SDR": "qres",
            "1080p": "qres",
            "720p": "disabled",
            "480p": "disabled",
            "SD": "disabled",
        },
        "series": {
            "4K_HDR": "qres",
            "4K_SDR": "disabled",
            "1080p": "disabled",
            "720p": "disabled",
            "480p": "disabled",
            "SD": "disabled",
        },
    },
    "nvenc_lookahead": {
        "movie": {"4K_HDR": 32, "4K_SDR": 24, "1080p": 24, "720p": 16, "480p": 16, "SD": 16},
        "series": {"4K_HDR": 24, "4K_SDR": 16, "1080p": 16, "720p": 16, "480p": 16, "SD": 16},
    },
    "nvenc_maxrate": {
        "movie": {"4K_HDR": "40M", "4K_SDR": "20M", "1080p": "20M", "720p": None, "480p": None, "SD": None},
        "series": {"4K_HDR": "20M", "4K_SDR": None, "1080p": None, "720p": None, "480p": None, "SD": None},
    },
    "nvenc_bufsize": {
        "movie": {"4K_HDR": "80M", "4K_SDR": "40M", "1080p": "40M", "720p": None, "480p": None, "SD": None},
        "series": {"4K_HDR": "40M", "4K_SDR": None, "1080p": None, "720p": None, "480p": None, "SD": None},
    },
    "pixel_format_hdr": "p010le",  # 10-bit for HDR (mandatory) — NVENC uses p010le
    "pixel_format_sdr": "p010le",  # 10-bit for SDR too (better banding resistance)
    # Audio: smart mode — bulky→EAC3, efficient lossy→copy
    "audio_mode": "smart",  # "copy" = passthrough, "smart" = bulky→EAC3/efficient→copy
    "audio_eac3_surround_bitrate": "640k",  # EAC3 for surround (>2 channels)
    "audio_eac3_stereo_bitrate": "256k",  # EAC3 for stereo/mono
    "audio_loudnorm": False,  # EBU R128 loudness normalisation on transcoded audio
    # Audio codecs to transcode to EAC3 (lossless + wasteful lossy like DTS)
    "lossless_audio_codecs": {
        "truehd",
        "dts-hd ma",
        "dts-hd.ma",
        "dts",
        "flac",
        "pcm_s16le",
        "pcm_s24le",
        "pcm_s32le",
        "pcm_f32le",
        "pcm_s16be",
        "pcm_s24be",
        "pcm_s32be",
        "pcm_f32be",
        "alac",
    },
    # Audio re-encoding: codecs/bitrates considered "bulky" (transcode to EAC-3)
    # - lossless codecs: always transcode (TrueHD, FLAC, PCM, DTS-HD MA, ALAC)
    # - DTS core: transcode if >700kbps (typical 1536kbps → 640kbps EAC-3)
    # - AC-3: transcode if >400kbps (640kbps AC-3 → ~448kbps EAC-3)
    # - Everything else (AAC, Opus, EAC-3, MP3, low-bitrate AC-3): copy
    "audio_bulky_threshold_kbps": {
        "dts": 700,  # DTS core at 1536kbps is wasteful
        "ac3": 400,  # AC-3 at 640kbps can be trimmed
        "ac-3": 400,  # alternate name
    },
    # Stream stripping — drop non-English tracks during encode
    "strip_non_english_subs": True,  # subtitle streams (keeps English, und, forced)
    "strip_non_english_audio": True,  # audio streams (keeps stream 0 as original language + English/und)
    # Behaviour
    "overwrite_existing": False,
    "replace_original": True,  # Replace original on NAS after verify
    "verify_duration_tolerance_secs": 2.0,
    # Priority tiers (order matters — biggest savings first)
    "priority_tiers": [
        {"name": "H.264 1080p", "codec": "H.264", "resolution": "1080p", "min_bitrate_kbps": 0},
        {"name": "Bloated HEVC 1080p", "codec": "HEVC (H.265)", "resolution": "1080p", "min_bitrate_kbps": 15000},
        {"name": "Bloated HEVC 4K", "codec": "HEVC (H.265)", "resolution": "4K", "min_bitrate_kbps": 25000},
        {"name": "H.264 720p/other", "codec": "H.264", "resolution": None, "min_bitrate_kbps": 0},
        {
            "name": "HEVC 1080p",
            "codec": "HEVC (H.265)",
            "resolution": "1080p",
            "min_bitrate_kbps": 0,
            "max_bitrate_kbps": 15000,
        },
        {
            "name": "HEVC 4K >20Mbps",
            "codec": "HEVC (H.265)",
            "resolution": "4K",
            "min_bitrate_kbps": 20000,
            "max_bitrate_kbps": 25000,
        },
        {
            "name": "HEVC 4K <=20Mbps",
            "codec": "HEVC (H.265)",
            "resolution": "4K",
            "min_bitrate_kbps": 0,
            "max_bitrate_kbps": 20000,
        },
        {"name": "HEVC 720p/SD + other", "codec": "HEVC (H.265)", "resolution": None, "min_bitrate_kbps": 0},
        {"name": "Other codecs", "codec": None, "resolution": None, "min_bitrate_kbps": 0},
    ],
}

# Containers that can cause NVENC failures — remux to .mkv before encoding
REMUX_EXTENSIONS = {".m2ts", ".avi", ".wmv", ".ts", ".m2v", ".vob", ".mpg", ".mpeg", ".mp4"}


def build_config(overrides: dict | None = None) -> dict:
    """Build a config dict by merging DEFAULT_CONFIG with optional overrides.

    Overrides can contain top-level keys (e.g. "max_staging_bytes") or nested
    dicts (e.g. "cq": {"movie": {"1080p": 26}}) which are deep-merged.
    """
    import copy

    config = copy.deepcopy(DEFAULT_CONFIG)
    if not overrides:
        return config
    for key, value in overrides.items():
        if key in config and isinstance(config[key], dict) and isinstance(value, dict):
            # Deep merge one level (e.g. cq.movie.1080p)
            for subkey, subval in value.items():
                if subkey in config[key] and isinstance(config[key][subkey], dict) and isinstance(subval, dict):
                    config[key][subkey].update(subval)
                else:
                    config[key][subkey] = subval
        else:
            config[key] = value
    return config


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


def resolve_encode_params(config: dict, item: dict, profile_name: str = "baseline") -> dict:
    """Resolve NVENC encode parameters based on content type, resolution, and quality profile.

    Returns dict with keys: cq, preset, multipass, lookahead, maxrate, bufsize, profile.
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

    base_cq = config["cq"].get(content_type, {}).get(res_key, 30)
    base_preset = config["nvenc_preset"].get(content_type, {}).get(res_key, "p4")
    base_multipass = config["nvenc_multipass"].get(content_type, {}).get(res_key, "disabled")
    base_lookahead = config["nvenc_lookahead"].get(content_type, {}).get(res_key, 16)

    # Apply quality profile adjustments
    profile = QUALITY_PROFILES.get(profile_name, QUALITY_PROFILES["baseline"])
    cq = max(1, base_cq + profile["cq_offset"])
    preset = profile["preset_override"] or base_preset
    multipass = profile["multipass_override"] or base_multipass
    lookahead = profile["lookahead_override"] or base_lookahead

    return {
        "cq": cq,
        "preset": preset,
        "multipass": multipass,
        "lookahead": lookahead,
        "maxrate": config["nvenc_maxrate"].get(content_type, {}).get(res_key, None),
        "bufsize": config["nvenc_bufsize"].get(content_type, {}).get(res_key, None),
        "content_type": content_type,
        "res_key": res_key,
        "profile": profile_name,
        "temporal_aq": profile.get("temporal_aq"),
    }
