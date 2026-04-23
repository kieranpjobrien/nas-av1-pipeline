"""Filename cleaning — strips scene tags, resolution/codec markers, and normalises formatting.

Canonical implementation. Consolidates what was previously split across
pipeline/filename.py and tools/strip_tags.py. The two diverged over time:
this module takes the UNION of both rule sets, preserving case-sensitive
language handling (so "An Italian Dream" keeps the title) while also picking
up the extra tokens that were only in strip_tags (BDMux, DVDR, UpScaled,
ALL4, TOD, VC-1, 3-letter ISO language codes, trailing H1/H2 residue, ...).

Exports:
  - SCENE_TAG_RE: canonical scene-tag detector used by compliance checks.
  - EPISODE_RE, MOVIE_YEAR_RE: anchor patterns.
  - clean_series_name, clean_movie_name, clean_filename: main API.
"""

import json
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Regex for SxxExx (case-insensitive). Captures season+episode marker.
# Also matches "Season.01.Episode.01" long-form format.
EPISODE_RE = re.compile(
    r"(S\d{1,4}\s?E\d{1,2}(?:\s?E\d{1,2})?)"
    r"|(S\d{1,4})(?=[\s.\-](?:1080|720|480|2160|4K|UHD|WEB|BluRay|HDTV|DSNP|AMZN|NF|ATVP|HMAX))"
    r"|Season[\s.]?(\d{1,4})[\s.]?Episode[\s.]?(\d{1,2})",
    re.IGNORECASE,
)

# Base tag parts — tokens that signal the end of an episode title.
# UNION of the rule sets from the old pipeline/filename.py and tools/strip_tags.py.
_BASE_TAG_PARTS = (
    r"(?:19[2-9]\d|20[0-2]\d)(?=[\s.)\-]|$)"  # bare year (Fargo.S02E04.2015.)
    # Resolution / quality
    r"|1080[pi]|720[pi]|480[pi]|2160[pi]|4K|UHD|DS4K"
    # Source (UNION: BDMux, DVDR, UpScaled added from strip_tags)
    r"|WEB[-.]?DL|WEBRip|BluRay|Blu[-.]?Ray|BDRip|BDRemux|BDMux|HDTV|DVDRip|DVDR|REMUX|WEB|UpScaled"
    # Streaming services (UNION: ALL4, TOD added from strip_tags)
    r"|NF|AMZN|DSNP|HULU|MAX|HBO|ATVP|PCOK|PMTP|STAN|CRAV|Netflix|ALL4|TOD"
    r"|BINGE|ROKU|(?-i:iT)|MA|CRITERION|MUBI|TUBI|SHUDDER|PMNP|SHO|STRP"
    # Video codecs (UNION: H\s?264, H\s?265, VC-1 added from strip_tags)
    r"|x264|x265|H\s?264|H\s?265|H\.?264|H\.?265|HEVC|AVC|AV1|XviD|DivX|VP9|VP8|VC[-.]?1|MPEG[24]"
    # Audio codecs / channels
    r"|TrueHD\d*\.?\d*|AAC\d*\.?\d*|DDP?\d*\.?\d*|DD\+?\d*\.?\d*"
    r"|Atmos|DTS(?:[-.]?HD(?:[\s.]?MA)?)?|FLAC|AC3|EAC3|LPCM|Opus"
    r"|5[\s.]1|7[\s.]1|2[\s.]0|51"
    # HDR / color / bit depth
    r"|SDR|HDR\d*|HDR10\+?|DV|DoVi|Dolby[\s.]?Vision|HLG"
    r"|10bit|8bit|12bit"
    # Language tags — case-sensitive via (?-i:) to avoid matching real words like "Italian Dream".
    # The old pipeline/filename.py enforced this; strip_tags did not (causing false positives).
    r"|(?-i:DUAL|MULTi|ENGLISH|GERMAN|POLISH|iTALiAN|FRENCH|SPANISH"
    r"|NORDiC|DUTCH|SWEDISH|FINNISH|DANISH|NORWEGIAN|CZECH"
    r"|HUNGARIAN|TURKISH|ARABIC|DL"
    r"|PORTUGUESE|RUSSIAN|JAPANESE|KOREAN|CHINESE|HINDI|THAI"
    r"|ROMANIAN|GREEK|BULGARIAN|CROATIAN|SERBIAN|UKRAINIAN)"
    # ISO 639-2 three-letter language codes (from strip_tags) — also case-sensitive
    # to avoid stripping "ITA" inside legitimate words.
    r"|(?-i:ITA|ENG|GER|FRE|FRA|SPA|JPN|CHI|RUS|POR|DUT|KOR|ARA|HIN|THA"
    r"|POL|RUM|ROM|SWE|NOR|DAN|FIN|CZE|HUN|TUR|GRE|BUL|CRO|SRP|UKR)"
    # Release tags
    r"|REPACK\d*|INTERNAL|PROPER|HYBRID|Hybrid"
    r"|EXTENDED|UNRATED|THEATRICAL|IMAX|OPEN[\s.]?MATTE"
    # Scene/tracker markers + subtitle markers (from strip_tags)
    r"|xpost|subs?|N[-.]?Z[-.]?B"
    # Lone resolution "p" (from stripped "1080p") — lowercase only, as standalone token
    # Uses inline (?-i:p) to match only lowercase despite global IGNORECASE flag
    r"|(?<=[\s.])(?-i:p)(?=[\s.]|$|[A-Z])"
)

# Year pattern for movies: (YYYY) or .YYYY. or space-YYYY-space, range 1920-2029.
MOVIE_YEAR_RE = re.compile(r"[\s.(]*((?:19[2-9]\d|20[0-2]\d))[\s.)]*")

# Edition tags to preserve after movie year (these are part of the title identity)
_EDITION_RE = re.compile(
    r"(?:Director'?s?[\s.]?Cut|Extended[\s.]?(?:Edition|Cut)?|Unrated[\s.]?(?:Edition|Cut)?"
    r"|Theatrical[\s.]?(?:Cut)?|IMAX[\s.]?(?:Edition)?|Open[\s.]?Matte"
    r"|Remastered|Criterion[\s.]?(?:Edition)?|Special[\s.]?Edition"
    r"|Ultimate[\s.]?(?:Edition|Cut)?)",
    re.IGNORECASE,
)

# Canonical scene-tag detector used by compliance checks + full_gamut post-replace
# verify. Any filename containing one of these tokens is considered dirty.
# Streaming-service codes (NF, AMZN, MAX, etc.) require dot-or-dash context anchors
# to avoid matching "MAX" inside "Mad Max" or "NF" inside a title word.
SCENE_TAG_RE = re.compile(
    # Primary technical scene tags — these are unambiguous, word-boundary only.
    r"\b(?:1080p|720p|480p|2160p|UHD|BluRay|BDRip|BRRip|WEB-?DL|WEBRip|HDTV|HDRip|"
    r"DVDRip|REMUX|x264|x265|HEVC|AAC|DDP?\d|AC3|EAC3|DTS|TrueHD|Atmos|"
    r"REPACK|MULTi|PROPER)\b"
    # Streaming services — only flag when dot/dash surrounded (scene format)
    r"|(?<=[.-])(?:NF|AMZN|DSNP|HULU|MAX|ATVP|PCOK|PMTP|STAN)(?=[.-])"
    # Scene release-group suffix — trailing "-GROUP" all-caps after a dot cluster
    r"|\.[A-Z]{2,4}\d?-[A-Z0-9][A-Za-z0-9]{2,}$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Custom keyword loading (from strip_tags)
# ---------------------------------------------------------------------------


def _custom_tags_path() -> Path:
    """Return the path to the custom tags control file.

    Imported lazily to avoid circular / env-var timing issues at module load.
    """
    from paths import STAGING_DIR

    return STAGING_DIR / "control" / "custom_tags.json"


def _load_custom_keywords() -> list[str]:
    """Read custom keywords from control/custom_tags.json if it exists."""
    path = _custom_tags_path()
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return [k for k in data.get("keywords", []) if isinstance(k, str) and k.strip()]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _dots_to_spaces(s: str) -> str:
    """Replace dots/underscores with spaces, preserving decimal numbers (e.g. 2.5).

    The decimal preservation is important so "Naked Gun 2.5" doesn't become
    "Naked Gun 2 5". The old strip_tags.py version did NOT preserve decimals;
    we adopt the pipeline/filename.py behaviour since it's strictly more correct.
    """
    # Protect decimal numbers: "2.5" -> "2DECPT5" then restore after
    s = re.sub(r"(\d)\.(\d)", r"\1DECPT\2", s)
    s = re.sub(r"[._]+", " ", s)
    s = s.replace("DECPT", ".")
    return " ".join(s.split())


def _build_tag_regex(extra_keywords: list[str] | None = None) -> re.Pattern:
    """Compile tag boundary regex, optionally including extra keywords."""
    parts = _BASE_TAG_PARTS
    if extra_keywords:
        escaped = "|".join(re.escape(k) for k in extra_keywords)
        parts = f"{parts}|{escaped}"
    return re.compile(
        rf"(?:\b|(?<=\[))({parts})(?:\b|(?=[\]\-]))",
        re.IGNORECASE,
    )


# Default regex (no custom keywords) for backward compat / direct imports
_TAG_BOUNDARY_RE = _build_tag_regex()


# ---------------------------------------------------------------------------
# Series filename cleaning
# ---------------------------------------------------------------------------


def clean_series_name(stem: str, tag_re: re.Pattern = _TAG_BOUNDARY_RE) -> str | None:
    """Find SxxExx anchor, keep title + episode title, strip tags.

    Returns cleaned name or None if no SxxExx anchor found.
    """
    m = EPISODE_RE.search(stem)
    if not m:
        return None

    # Title portion: everything before SxxExx
    title = stem[: m.start()]
    # Strip trailing year — bare or parenthesized (e.g. "Show.2019.", "Show (2019) -")
    title = re.sub(r"[\s.]*\(?(19[2-9]\d|20[0-2]\d)\)?[\s.\-]*$", "", title)
    # Normalize episode marker: group(1) is SxxExx, group(2) is season-only,
    # groups 3+4 are Season/Episode long form
    if m.group(1):
        episode_marker = re.sub(r"\s+", "", m.group(1)).upper()
    elif m.group(2):
        episode_marker = m.group(2).upper()
    else:
        episode_marker = f"S{int(m.group(3)):02d}E{int(m.group(4)):02d}"

    # After SxxExx: might contain episode title then tags
    after = stem[m.end() :]

    # Strip leading resolution "p" blob: "pHybridDDPAtmos..." or "pH264..." or "p10..."
    # (from filenames like "ShowS01E01pHybrid..." where "1080" was stripped leaving "p")
    after = re.sub(r"^[\s.]*p(?=[A-Z\d])", " ", after)

    # Strip parenthesized metadata blocks: (1080p AMZN WEB-DL ...) or (p H SDR ...)
    # These contain technical info, not episode titles
    after = re.sub(
        r"\s*\([^)]*(?:1080|720|480|2160|WEB|Blu|DDP|AAC|SDR|HDR|Hybrid|HONE|TheSickle|DarQ|Webrip|Goki)[^)]*\)",
        "",
        after,
    )

    # Strip trailing bracket fragments: [WEBDL...], [h264-WEBDL-720p AAC-2 0], etc.
    after = re.sub(
        r"\s*\[[^\]]*(?:1080|720|480|2160|WEB|Blu|DDP|AAC|h\.?264|h\.?265|HEVC|AVC|HDTV|DVDRip)[^\]]*\]",
        "",
        after,
        flags=re.IGNORECASE,
    )

    # Strip leading absolute episode number (e.g. " - 095 - " after SxxExx)
    after = re.sub(r"^[\s\-]*\d{2,4}[\s\-]+", " ", after)

    # Normalize separators before tag search so concatenated blobs get boundaries
    after = _dots_to_spaces(after)

    # Strip trailing resolution+service junk concatenated to episode title:
    # "ArrivalspNOWAtmosHLG" -> "Arrivals", "RecenteringpNOWAtmos" -> "Recentering"
    after = re.sub(
        r"p(?:NOW|AMZN|DSNP|HULU|HBO|ATVP|PCOK|PMTP|MAX)"
        r"(?:Atmos|HLG|HDR|DDP|DD|AC3|H264|H265|HEVC|AVC|WEB)*\s*$",
        "",
        after,
        flags=re.IGNORECASE,
    )

    # Find where tags begin
    tag_match = tag_re.search(after)
    if tag_match:
        episode_title = after[: tag_match.start()]
    else:
        # No recognizable tags -- keep everything (rare)
        episode_title = after

    # Clean up each part
    title = _dots_to_spaces(title).strip().rstrip(" -")
    # Title-case all-lowercase titles: "mythbusters" -> "Mythbusters"
    if title and title == title.lower():
        title = title.title()
    # CamelCase split title: "TheSopranos" -> "The Sopranos"
    if re.search(r"[a-z][A-Z]", title):
        title = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", title)
        # Rejoin name prefixes that got split: "Mc Beal" -> "McBeal", "Bo Jack" -> "BoJack"
        title = re.sub(r"\b(Mc|Mac|De|Le|La|Bo|Myth) (?=[A-Z])", r"\1", title)
    episode_title = episode_title.strip()
    # Title-case all-lowercase episode titles: "electrified escape" -> "Electrified Escape"
    if episode_title and episode_title == episode_title.lower():
        episode_title = episode_title.title()
    # Strip leading hyphens/spaces (but preserve trailing parens for part numbers)
    episode_title = episode_title.lstrip("- ")

    # Strip concatenated junk BEFORE release group (so "Helenp-CRFW" -> "Helen"):
    # - "p-GROUP" combos where p is from resolution
    episode_title = re.sub(r"p-[A-Z][A-Za-z0-9]*$", "", episode_title)  # "p-CRFW"
    episode_title = re.sub(r"p\d+[\s.\d]*$", "", episode_title)  # "p51", "p10+..."
    episode_title = re.sub(r"(\d)p$", r"\1", episode_title)  # "1080p" residue

    # Strip trailing release group: "-GROUP" at end of episode title.
    # Match ALL-CAPS groups (-CRFW, -XEBEC, -FLAME), camelCase groups
    # (-playWEB, -ViETNAM, -PiR8), and known mixed-case groups (-NTb, -FuN).
    # Require 3+ chars to avoid stripping real hyphenated words like "Break-In".
    # Also strip space-separated trailing groups (no hyphen): "EzzRips", "CRFW".
    episode_title = re.sub(
        r"\s*-("
        r"[A-Z][A-Z0-9]{2,12}"  # ALL CAPS 3+ chars: -CRFW, -XEBEC
        r"|[a-z]+[A-Z][A-Za-z0-9]*"  # camelCase: -playWEB, -edge2020
        r"|[A-Z][a-z][A-Z][A-Za-z0-9]*"  # mixed: -NTb, -FuN, -PiR8, -DarQ
        r")(?:\s*-xpost)?$",
        "",
        episode_title,
    )
    # Space-separated release group at end (no hyphen): "EzzRips", "BONE"
    # Only match specific patterns to avoid stripping real words (XXX, III, CUT, etc.)
    episode_title = re.sub(
        r"\s+("
        r"[A-Z][a-z]+(?:Rips?|DL|HD)"  # EzzRips, NtbRip, etc.
        r"|BONE|FLUX|NOGRP"  # known groups that appear without hyphen
        r")$",
        "",
        episode_title,
    )
    # Strip remaining trailing channel/resolution junk
    episode_title = re.sub(r"\s*(?:51|5 1)$", "", episode_title)
    # Strip trailing lone junk tokens
    episode_title = re.sub(r"\s+(?:mkv|xpost)$", "", episode_title, flags=re.IGNORECASE)
    # Strip trailing/sole lone "p" (resolution remnant from "1080p")
    episode_title = re.sub(r"(?:^|\s+)p$", "", episode_title)
    # Strip trailing unclosed paren with tech junk: "( ATVP5 1" or "(p"
    # But NOT part numbers like "(1)" or "(2)" which are legitimate
    episode_title = re.sub(r"\s*\(\s*(?:p\b|[A-Z]{2,}|\d{2,}).*$", "", episode_title)
    # Strip trailing unclosed bracket or dangling " - [" fragments
    episode_title = re.sub(r"\s*-?\s*\[$", "", episode_title)
    # Strip trailing service/channel junk that wasn't in a paren: "ATVP5 1", "DS4K ATVP5 1"
    episode_title = re.sub(
        r"\s+(?:ATVP|DSNP|NF|AMZN|HULU|MAX|HBO|PCOK|PMTP|STAN|CRAV|PBS)\d*[\s.\d]*$",
        "",
        episode_title,
        flags=re.IGNORECASE,
    )
    episode_title = episode_title.rstrip(" -")

    # Insert spaces in CamelCase words (e.g. "AlligatorMan" -> "Alligator Man",
    # "AHit Is AHit" -> "A Hit Is A Hit", "46Long" -> "46 Long")
    # Applied per-word so titles with spaces still get CamelCase splits.
    # Skip words with scene-style alternating case (lowercase i): "FiNAL", "REJECTiON"
    if episode_title and re.search(r"[a-z][A-Z]|\d[A-Z][a-z]|\b[A-Z][A-Z][a-z]{2,}", episode_title):

        def _split_camel(word: str) -> str:
            if len(word) <= 3:
                return word
            # Skip scene-style words with isolated lowercase i: "FiNAL", "NiXON"
            if re.search(r"[A-Z]i[A-Z]", word):
                return word
            # "AlligatorMan" -> "Alligator Man"
            w = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", word)
            # Rejoin name prefixes: "Mc Beal" -> "McBeal", "De Lorean" -> "DeLorean"
            w = re.sub(r"\b(Mc|Mac|De|Le|La|Bo|Myth) (?=[A-Z])", r"\1", w)
            # Single letter + CamelCase word: "AHit" -> "A Hit", "AGoing" -> "A Going"
            w = re.sub(r"^([A-Z])(?=[A-Z][a-z]{2,})", r"\1 ", w)
            # Digit-to-letter boundary: "46Long" -> "46 Long"
            w = re.sub(r"(?<=\d)(?=[A-Z][a-z])", " ", w)
            return w

        episode_title = " ".join(_split_camel(w) for w in episode_title.split())

    # Strip trailing tech tokens (after CamelCase split, these may now be separated)
    episode_title = re.sub(r"\s+(?:WEB|H\s*1|H\s*0|0)$", "", episode_title)
    # Strip concatenated trailing codec remnants: "710NH1" -> "710N", "titleH264" -> "title".
    # UNION of both versions: broader regex from strip_tags (matches H0/H1/H2/H264/H265,
    # predecessor can be alphanumeric) is more aggressive than the old pipeline/filename
    # version (lowercase-only predecessor, H264/H265 only). strip_tags' version wins
    # because it strips more junk without known false positives.
    episode_title = re.sub(r"(?<=[A-Za-z0-9])H[012]\d*$", "", episode_title)
    episode_title = episode_title.rstrip(" -")

    # Quality gate: if the cleaned episode title still contains obvious tag junk,
    # the source was too mangled to clean reliably — skip rather than produce garbage.
    _JUNK_WORDS = re.compile(
        r"\b(Hybrid|DDP|AAC|AC3|TrueHD|Atmos|BluRay|Bluray|HDTV|Dtsa|AVC"
        r"|WebHD|DLWeb|DLAudio|WEBRip|Webrip|REMUX|REPACK|HLG|WEBh264"
        r"|(?-i:iTALiAN|MULTi|NORDiC)|LPCM|Opus|VP9|MPEG[24]"
        r"|(?-i:PORTUGUESE|RUSSIAN|JAPANESE|KOREAN|CHINESE|HINDI))\b",
        re.IGNORECASE,
    )
    # Catch concatenated junk like "Hybrid1English", "SDR1English", "10+DDP...",
    # "AC3DLWeb", "German AC3", or entire episode title is just junk + language
    _JUNK_CONCAT = re.compile(
        r"(Hybrid|DDP|AAC|AC3|SDR|HDR|AVC|Atmos|DoVi?|blurayd)\d"
        r"|AC3DL|DLWeb|bluraydd|DD\+\d|\d+Bluray"
        # Removed: ^\d+[A-Z]\w{2,}$ was too aggressive (matched real titles like "710NH1")
        r"|(?-i:^GERMAN\b|^ENGLISH\b)"  # leading language tag = no real title (ALL CAPS only)
        r"|(?-i:i\s*TALi|MULTi)"  # space-split iTALiAN/MULTi (case-sensitive)
        r"|^Do Vi?\d"  # DoVi/DV remnant: "Do Vi10Atmos"
        r"|\bp\s*DD"  # "p DD+5.1" — resolution+audio junk
        r"|^p\s+\w{1,3}$"  # lone "p H" or "p H1" — pure junk
        r"|p\s*NOW"  # "p NOWAtmos" — resolution+service junk
        r"|AVCREMUX|REMUX[A-Z]"  # concatenated remux junk
        r"|p\s+NOW|p\s+Atmos"  # trailing "p Atmos", "p NOW..."
        r"|(?-i:^FiNAL$)",  # scene-style "FiNAL" (case-sensitive, not "Final")
        re.IGNORECASE,
    )
    # Also catch trailing concatenated tech tokens: words ending with H1, WEB, H264, etc.
    # Uses (?-i:) for the leading char so only actual lowercase triggers (not "N" in "710NH1")
    _TRAILING_TECH = re.compile(
        r"(?-i:[a-z])(H\d|WEB|AVC|H264|H265|HEVC)$",
        re.IGNORECASE,
    )
    if episode_title and (
        _JUNK_WORDS.search(episode_title) or _JUNK_CONCAT.search(episode_title) or _TRAILING_TECH.search(episode_title)
    ):
        # Episode title is junk, but show title + marker are still valid
        # Return without episode title rather than skipping entirely
        if title:
            return f"{title} {episode_marker}"
        return None  # no show title either — truly mangled

    if episode_title:
        return f"{title} {episode_marker} {episode_title}"
    return f"{title} {episode_marker}"


# ---------------------------------------------------------------------------
# Movie filename cleaning
# ---------------------------------------------------------------------------


def clean_movie_name(stem: str, tag_re: re.Pattern = _TAG_BOUNDARY_RE) -> str | None:
    """Find year anchor, keep title + (year) + edition tag, strip everything after.

    Returns cleaned name or None if no year anchor found.
    """
    # Find all year candidates; pick the last one that looks like a movie year
    # (sometimes a year appears in the title itself, e.g. "2001 A Space Odyssey")
    matches = list(MOVIE_YEAR_RE.finditer(stem))
    if not matches:
        return None

    # Use the first year that's followed by tags or end-of-string.
    # For most filenames, the first year IS the release year.
    for m in matches:
        year = m.group(1)
        title = stem[: m.start()]
        title = _dots_to_spaces(title).strip()
        if not title:
            continue

        # Check for edition tag after the year
        after_year = stem[m.end() :]
        after_year_clean = _dots_to_spaces(after_year).strip()
        edition_match = _EDITION_RE.match(after_year_clean)
        if edition_match:
            edition = edition_match.group(0).strip()
            return f"{title} ({year}) {edition}"

        return f"{title} ({year})"

    return None


# ---------------------------------------------------------------------------
# High-level convenience function
# ---------------------------------------------------------------------------


def clean_filename(filepath: str, library_type: str) -> str | None:
    """Clean a filename by stripping scene tags, resolution tags, codec tags etc.

    Args:
        filepath: Full path to the file.
        library_type: "movie" or "series".

    Returns:
        Clean filename (just the name, no path) or None if no cleaning needed.
    """
    p = Path(filepath)
    stem = p.stem
    ext = p.suffix

    # Build the tag regex including any user-configured custom keywords from
    # control/custom_tags.json. If the file doesn't exist / isn't readable we
    # fall back to the base regex — the lookup is deliberately best-effort.
    tag_re = _build_tag_regex(_load_custom_keywords() or None)

    if library_type == "series":
        clean_stem = clean_series_name(stem, tag_re)
    elif library_type == "movie":
        clean_stem = clean_movie_name(stem, tag_re)
    else:
        return None

    if clean_stem is None:
        return None

    # Strip trailing lone brackets/hyphens left by aggressive cleaning
    clean_stem = re.sub(r"\s*[\(\[\-]+\s*$", "", clean_stem).rstrip()

    if clean_stem == stem:
        return None

    return f"{clean_stem}{ext}"
