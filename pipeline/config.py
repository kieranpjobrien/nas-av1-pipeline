"""Pipeline configuration defaults and constants."""

# Language sets used across the pipeline for track filtering
# Language codes treated as "non-foreign" everywhere — track strip leaves
# them alone, compliance doesn't flag them.
#
# - "eng"/"en"/"english": English audio/subs (the user's primary)
# - "und"/"": untagged or unknown — held back from strip until language
#   detection resolves them (rule 2026-04-29 inviolate)
# - "zxx": ISO 639-2 "no linguistic content / not applicable" — used for
#   audio tracks that are dialogue-free (orchestral score only,
#   instrumental shorts like Paperman / The Lost Thing / Inner Workings).
#   These tracks are deliberately tagged this way; they're not "foreign"
#   in the strip sense and they're not "undetected" in the detect sense.
#   Whisper correctly returns unresolved on them because there's no speech.
#   Added 2026-05-02.
KEEP_LANGS: set[str] = {"eng", "en", "english", "und", "", "zxx"}
ENG_LANGS: set[str] = {"eng", "en", "english"}


# Default configuration — all values can be overridden via CLI args
DEFAULT_CONFIG = {
    # Staging limits (bytes)
    "max_staging_bytes": 2_500_000_000_000,  # 2.5 TB total local staging
    "max_fetch_buffer_bytes": 200_000_000_000,  # 200 GB fetch buffer
    "min_free_space_bytes": 50_000_000_000,  # 50 GB minimum free on staging drive
    # Concurrent NVENC sessions. RTX 40-series has 2 NVENC chips so 2 is the practical cap
    # with zero perf penalty. Set to 1 on older Turing/Ampere cards with one chip.
    # IMPORTANT: keep gpu_concurrency at 1. The RTX 4080 has dual NVENC chips
    # but running two concurrent NVENC encodes triggered system BSODs in
    # production. Treat this as the same severity class as rule 9a (no
    # whisper+NVENC). One encode at a time is the safe operating envelope —
    # the gain from a second concurrent encode isn't worth the crash risk.
    "gpu_concurrency": 1,
    # CPU prep workers — run filename clean, language detect (whisper),
    # qualify gate, external sub scan, and container remux AHEAD of the
    # GPU. Multiple workers can prep multiple files simultaneously so the
    # GPU never waits on CPU work. Tuneable; CPU-only so no GPU contention.
    "prep_concurrency": 2,
    # Cap on prepped-and-waiting-for-GPU files. Prep workers pause when
    # this many files already sit in the "prepped, awaiting encode" state —
    # avoids burning CPU producing more than the single GPU can consume.
    "prep_buffer_max": 3,
    # Upload workers — run finalize_upload (NAS upload, verify, atomic
    # replace, mkvpropedit tags, Plex scan) AFTER the GPU encode. Splitting
    # this off the GPU thread lets the GPU dive straight into the next
    # encode while bytes ship back over SMB. Default 1 = single concurrent
    # upload (SMB is already saturated by one transfer). Set to 0 to
    # restore inline-upload behaviour (e.g. for unit tests).
    "upload_concurrency": 1,
    # Concurrent SMB fetches. One transfer already saturates the SMB link; more threads
    # just add contention. Keep the loop, just drop the second thread.
    "fetch_concurrency": 1,
    # Tier-3 telemetry opt-ins — both significantly slow encodes, leave off by default.
    # history_source_hash: sha256 the source before replace. Adds ~60-90s/2GB over SMB.
    # history_vmaf:        run libvmaf on a 10s sample vs output. Adds ~10-30s per encode.
    "history_source_hash": False,
    "history_vmaf": False,
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
    # - lossless codecs: always transcode (FLAC, PCM, DTS-HD MA, ALAC) — TrueHD
    #   stays passthrough because it carries Atmos object data the Sonos Arc decodes
    # - DTS core: transcode if >700kbps (typical 1536kbps → 640kbps EAC-3)
    # - AC-3: transcode if >400kbps (640kbps AC-3 → ~448kbps EAC-3)
    # - Opus: ALWAYS transcode (Sonos Arc has no native Opus decode → Plex would
    #   transcode on every play; pre-transcoding once eliminates that overhead)
    # - Everything else (AAC, EAC-3, MP3, low-bitrate AC-3): copy
    "audio_bulky_threshold_kbps": {
        "dts": 700,  # DTS core at 1536kbps is wasteful
        "ac3": 400,  # AC-3 at 640kbps can be trimmed
        "ac-3": 400,  # alternate name
    },
    # Stream stripping — drop non-English tracks during encode
    "strip_non_english_subs": True,  # subtitle streams (keeps English, und, forced)
    "strip_non_english_audio": True,  # audio streams: master switch (False = keep all)
    # Audio keep policy:
    #   "original_language" — keep tracks matching TMDb original_language; strip
    #                         foreign dubs (incl. English dubs of foreign-origin
    #                         films). Falls back to "english_und" when no TMDb
    #                         data is available. NEVER strips `und` tracks
    #                         whisper hasn't resolved (conservative).
    #   "english_und"       — legacy: keep stream 0 + any English/und track.
    "audio_keep_policy": "original_language",
    # When policy is "original_language", set this True to ALSO keep English
    # tracks alongside the original language (e.g. for dual-language watching).
    # Default False matches the "strip non-original including English" policy.
    "audio_keep_english_with_original": False,
    # Behaviour
    "overwrite_existing": False,
    "replace_original": True,  # Replace original on NAS after verify
    "verify_duration_tolerance_secs": 2.0,
    # Mid-session queue refresh: re-read media_report.json on this cadence
    # (seconds) and merge any new files into the live queues. Lets a Sonarr/
    # Radarr drop-in jump to top of queue (smallest-first sort) without a
    # pipeline restart. 30-min default — scanner runs aren't frequent enough
    # to justify tighter polling, and mtime hasn't moved 99% of the time so
    # the check is essentially free either way. Set to 0 to disable.
    "queue_refresh_interval_secs": 1800.0,
    # Gap filler drain-and-rescan: between passes, wait this many seconds
    # before re-scanning the queue for new gap-fill work. The refresh worker
    # can add files mid-session; this lets gap filler pick them up without
    # a pipeline restart. Idle pause; busy pause is hardcoded short (5s)
    # so consecutive batches drain back-to-back.
    "gap_filler_rescan_interval_secs": 60.0,
    # Gap filler mux backend: "local" runs mkvmerge.exe on this machine
    # against UNC paths (NAS-only does SMB I/O — no NAS CPU stress, no SSH
    # required). "remote" SSHes to NAS and runs mkvmerge inside the
    # mkvworker Docker container (faster, ~10s/file vs ~2-3min, but adds
    # NAS load). User chose local 2026-04-29 after we hit OOM-kill cascades
    # running concurrent SSH+Docker+mkvmerge on the Synology.
    "gap_filler_mux_backend": "local",

    # Files at or above this size get staged to local SSD before mkvmerge
    # rather than running with both INPUT and OUTPUT on UNC. The 2026-05-01
    # House of the Dragon S01E06 incident (9.17 GB, 5+h ETA at 520 KB/s)
    # and the 2026-05-01 21:25 House S01E17 incident (1.7 GB, hung at
    # 140 MB written for 16+ minutes) both showed UNC-in-place mkvmerge
    # degrades unpredictably even on smaller files when SMB is contended
    # or the file's track structure makes mkvmerge do heavy seeking.
    #
    # Sequential bulk copy to local SSD finishes in seconds-to-minutes
    # depending on file size; mkvmerge then runs against local I/O with no
    # SMB contention. The cost of staging a small file (a few hundred MB
    # in 1-3 seconds) is negligible compared to the variance we see on the
    # UNC path. Threshold dropped from 2 GB → 256 MB so the staging path
    # is the default for almost everything. Files <256 MB still go direct
    # — they're typically small enough that even a stalled UNC mkvmerge
    # finishes in reasonable time.
    "gap_filler_local_stage_threshold_bytes": 256 * 1024**2,

    # Per-mkvmerge progress watchdog: if the .gapfill_tmp.mkv file's size
    # doesn't grow for this many seconds, kill the mkvmerge process. The
    # 2026-05-01 House S01E17 case had mkvmerge writing 140 MB then
    # producing zero bytes for 16 minutes before we manually killed it.
    # Without a watchdog the same case stalls forever and blocks the
    # single-flight gap_filler queue. 90s default.
    "gap_filler_mkvmerge_stall_secs": 90,

    # Order in which the full-gamut encode queue is processed.
    #   "largest_first" (default): big files first, ETA shrinks visibly
    #   "smallest_first":          quick wins first, larger files at the tail
    # User flipped this to largest_first on 2026-05-02 to stop the ETA
    # from growing every time the queue refresh discovered another 30 GB
    # 4K HDR title.
    "encode_queue_order": "largest_first",
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


def resolve_encode_params(config: dict, item: dict) -> dict:
    """Resolve NVENC encode parameters based on content type + resolution.

    Returns dict with keys: cq, preset, multipass, lookahead, maxrate, bufsize,
    content_type, res_key, content_grade, cq_offset, base_cq.

    The base CQ comes from the resolution × library matrix. On top of that we
    apply a content-grade offset (sitcom +5, classic_film +1, etc.) so older
    sitcoms get the harsher quantization their static talking-head content
    can absorb without a visible quality hit. See pipeline.content_grade for
    the matrix.
    """
    from pipeline.content_grade import target_cq

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
    final_cq, content_grade, applied_offset = target_cq(base_cq, item)

    # Per-file CQ override (set via /api/file/cq-override from the dashboard).
    # Replaces the grade-derived target so the user can dial a specific film
    # up or down without modifying the grade rules. Override is bounds-checked
    # at the API; here we just trust whatever's in extras.
    override_applied: int | None = None
    filepath = item.get("filepath")
    if filepath:
        try:
            from pipeline.cq_override import get_override  # noqa: PLC0415
            from paths import PIPELINE_STATE_DB  # noqa: PLC0415
            override = get_override(PIPELINE_STATE_DB, filepath)
            if override is not None:
                override_applied = override
                final_cq = override
        except Exception:
            # Override is best-effort; if anything explodes, use the grade target.
            pass

    return {
        "cq": final_cq,
        "base_cq": base_cq,
        "cq_offset": applied_offset,
        "cq_override": override_applied,
        "content_grade": content_grade,
        "preset": config["nvenc_preset"].get(content_type, {}).get(res_key, "p4"),
        "multipass": config["nvenc_multipass"].get(content_type, {}).get(res_key, "disabled"),
        "lookahead": config["nvenc_lookahead"].get(content_type, {}).get(res_key, 16),
        "maxrate": config["nvenc_maxrate"].get(content_type, {}).get(res_key, None),
        "bufsize": config["nvenc_bufsize"].get(content_type, {}).get(res_key, None),
        "content_type": content_type,
        "res_key": res_key,
    }
