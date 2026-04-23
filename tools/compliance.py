"""Library-wide standards compliance audit.

For each file in media_report.json, check whether it meets the library's target
standards and emit a report (stdout + CSV) of violations.

Standards enforced:
  - Video codec == AV1
  - Every audio codec in {EAC-3, Opus, configured lossless passthrough list}
  - Every audio language in KEEP_LANGS (English + undetermined variants)
  - Every sub language in KEEP_LANGS
  - Filename has no scene tags
  - TMDb tags present in the report entry (proxy for MKV-level tag)

Usage:
    python -m tools.compliance                       # dry-run audit, print to stdout
    python -m tools.compliance --csv out.csv         # also write CSV
    python -m tools.compliance --queue reencode      # write non-compliant paths into
                                                     # control/reencode.json so the
                                                     # pipeline picks them up
"""

import argparse
import csv
import json
import re
import sys
from pathlib import Path

from paths import MEDIA_REPORT, STAGING_DIR
from pipeline.config import DEFAULT_CONFIG, KEEP_LANGS
from pipeline.streams import is_hi_external, is_hi_internal

CONTROL_DIR = STAGING_DIR / "control"

# Scene-tag detector — matches common scene markers; a filename containing
# even one is considered dirty. Streaming-service codes (NF, AMZN, MAX, etc.)
# require context anchors (dot-or-dash on at least one side) to avoid matching
# "MAX" inside "Mad Max" or "NF" inside a title word.
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

TARGET_VIDEO = {"av1"}  # post-encode codec_name — "av1_nvenc" is the encoder, not the codec
TARGET_AUDIO = {"eac3", "opus"}  # after hyphen-strip normalisation
ENG_LANGS = {"en", "eng", "english"}
UND_LANGS = {"und", "unk", ""}

# Per-language equivalence map for "original language" audio rule.
# TMDb original_language is ISO 639-1; ffprobe usually emits ISO 639-2/639-3 codes.
ORIG_LANG_EQUIVS: dict[str, set[str]] = {
    "en":  {"en", "eng"},
    "ja":  {"ja", "jpn"},
    "ko":  {"ko", "kor"},
    "zh":  {"zh", "chi", "zho", "cmn", "yue"},
    "cn":  {"zh", "chi", "zho", "cmn", "yue"},
    "fr":  {"fr", "fre", "fra"},
    "de":  {"de", "ger", "deu"},
    "es":  {"es", "spa", "esp"},
    "it":  {"it", "ita"},
    "pt":  {"pt", "por", "pt-br", "pt-pt"},
    "ru":  {"ru", "rus"},
    "sv":  {"sv", "swe"},
    "no":  {"no", "nor", "nob", "nno"},
    "da":  {"da", "dan"},
    "fi":  {"fi", "fin"},
    "nl":  {"nl", "dut", "nld"},
    "pl":  {"pl", "pol"},
    "cs":  {"cs", "cze", "ces"},
    "hu":  {"hu", "hun"},
    "tr":  {"tr", "tur"},
    "ar":  {"ar", "ara"},
    "hi":  {"hi", "hin"},
    "th":  {"th", "tha"},
    "he":  {"he", "heb", "iw"},
    "el":  {"el", "gre", "ell"},
    "fa":  {"fa", "per", "fas"},
    "xx":  set(),  # no linguistic content — skip original-language check
    "zxx": set(),
}


# HI detection delegated to pipeline.streams; thin wrappers preserve call sites.
def _is_hi(stream: dict) -> bool:
    """Detect HI/SDH/captions sub streams (delegates to pipeline.streams.is_hi_internal)."""
    return is_hi_internal(stream)


def _is_hi_external(s: dict) -> bool:
    """Detect HI/SDH/CC external sidecar (delegates to pipeline.streams.is_hi_external)."""
    return is_hi_external(s.get("filename") or "")


def check_file(entry: dict, config: dict) -> list[str]:
    """Return a list of violation strings (empty if compliant).

    Rules (matching dashboard):
      - Video codec == AV1
      - Every audio codec in {EAC-3, Opus} (or configured lossless passthrough)
      - Every audio language matches TMDb original_language (plus und/"")
      - Exactly ONE non-HI English sub (internal or external), zero HI/SDH,
        zero foreign subs
      - Filename has no scene tags
      - TMDb metadata present (movies only; series episode has no per-episode
        TMDb — show-level tags are sufficient but not modelled here)
    """
    violations: list[str] = []
    lossless = {c.lower() for c in config.get("lossless_audio_codecs") or []}

    # Video
    v = entry.get("video") or {}
    vcodec = (v.get("codec_raw") or v.get("codec") or "").lower()
    if vcodec and vcodec not in TARGET_VIDEO:
        violations.append(f"video codec {vcodec} (target: av1)")

    # Audio codec + original-language check
    tmdb = entry.get("tmdb") or {}
    orig_lang = (tmdb.get("original_language") or "").lower().strip()
    keeper_langs = ORIG_LANG_EQUIVS.get(orig_lang, {orig_lang} if orig_lang else set())
    keeper_langs = keeper_langs | UND_LANGS
    enforce_orig = bool(orig_lang) and orig_lang not in ("xx", "zxx")

    for i, a in enumerate(entry.get("audio_streams") or []):
        codec = (a.get("codec_raw") or a.get("codec", "")).lower().replace("-", "")
        lang = (a.get("language") or a.get("detected_language") or "").lower().strip()
        if codec and codec not in TARGET_AUDIO and codec not in lossless:
            violations.append(f"audio[{i}] codec {a.get('codec_raw') or a.get('codec')}")
        if enforce_orig and lang and lang not in keeper_langs:
            violations.append(f"audio[{i}] language {lang} (original: {orig_lang})")

    # Sub language check — exactly ONE non-HI English sub, zero HI, zero foreign
    int_subs = entry.get("subtitle_streams") or []
    ext_subs = entry.get("external_subtitles") or []
    regular_eng = sum(
        1 for s in int_subs
        if (s.get("language") or s.get("detected_language") or "").lower().strip() in ENG_LANGS
        and not _is_hi(s)
    )
    regular_eng += sum(
        1 for s in ext_subs
        if (s.get("language") or "").lower().strip() in ENG_LANGS and not _is_hi_external(s)
    )
    hi_count = sum(1 for s in int_subs if _is_hi(s)) + sum(1 for s in ext_subs if _is_hi_external(s))
    foreign_internal = sum(
        1 for s in int_subs
        if (s.get("language") or s.get("detected_language") or "und").lower().strip() not in (ENG_LANGS | UND_LANGS)
    )
    foreign_external = sum(
        1 for s in ext_subs
        if (s.get("language") or "und").lower().strip() not in (ENG_LANGS | UND_LANGS)
    )
    if regular_eng == 0:
        violations.append("sub: missing non-HI English sub")
    elif regular_eng > 1:
        violations.append(f"sub: {regular_eng} English subs (want exactly 1)")
    if hi_count > 0:
        violations.append(f"sub: {hi_count} HI/SDH sub(s) present")
    if foreign_internal + foreign_external > 0:
        violations.append(f"sub: {foreign_internal + foreign_external} foreign sub(s)")

    # Filename
    fname = entry.get("filename") or ""
    if SCENE_TAG_RE.search(fname):
        violations.append(f"filename has scene tags: {fname}")

    # TMDb — only enforce on movies (series episodes don't have per-episode TMDb)
    if entry.get("library_type") == "movie":
        if not (entry.get("tmdb") and entry["tmdb"].get("tmdb_id")):
            violations.append("no tmdb metadata")

    return violations


def main() -> None:
    parser = argparse.ArgumentParser(description="Library standards compliance audit")
    parser.add_argument("--report", type=str, default=str(MEDIA_REPORT))
    parser.add_argument("--csv", type=str, default=None, help="Write per-file CSV of violations")
    parser.add_argument(
        "--queue",
        choices=["reencode", "print"],
        default="print",
        help="'reencode' writes non-compliant paths into control/reencode.json",
    )
    parser.add_argument("--limit", type=int, default=0, help="Stop after N non-compliant files (0 = all)")
    args = parser.parse_args()

    with open(args.report, encoding="utf-8") as f:
        report = json.load(f)

    files = report.get("files", [])
    print(f"Auditing {len(files)} files against library standards...")

    non_compliant: list[tuple[str, list[str]]] = []
    from collections import Counter

    violation_counter: Counter = Counter()

    for entry in files:
        vs = check_file(entry, DEFAULT_CONFIG)
        if not vs:
            continue
        non_compliant.append((entry["filepath"], vs))
        for v in vs:
            # Normalise for counting — take the leading tag before the colon
            key = v.split(":")[0].split(" ", 2)[0:2]
            violation_counter[" ".join(key)] += 1
        if args.limit and len(non_compliant) >= args.limit:
            break

    total = len(files)
    compliant = total - len(non_compliant)
    pct = (compliant / total * 100) if total else 0
    print()
    print(f"Compliant:     {compliant:>5} / {total} ({pct:.1f}%)")
    print(f"Non-compliant: {len(non_compliant):>5}")
    print()
    print("Top violation types:")
    for v, n in violation_counter.most_common(10):
        print(f"  {n:>5}  {v}")
    print()

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["filepath", "n_violations", "violations"])
            for path, vs in non_compliant:
                w.writerow([path, len(vs), "; ".join(vs)])
        print(f"CSV: {args.csv}")

    if args.queue == "reencode":
        # reencode.json format: {"files": {path: override_dict, ...}, "patterns": {pattern: override}}.
        # An empty override dict just means "re-queue with default params".
        out = CONTROL_DIR / "reencode.json"
        try:
            existing = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
        except Exception:
            existing = {}
        # Normalise legacy list-form if we previously wrote it wrong
        existing_files = existing.get("files", {})
        if isinstance(existing_files, list):
            existing_files = {p: {} for p in existing_files}
        for path, _vs in non_compliant:
            if path not in existing_files:
                existing_files[path] = {}
        out.write_text(
            json.dumps({"files": existing_files, "patterns": existing.get("patterns", {})}, indent=2),
            encoding="utf-8",
        )
        print(f"Queued {len(non_compliant)} files (total now {len(existing_files)} in reencode.json)")


if __name__ == "__main__":
    main()
