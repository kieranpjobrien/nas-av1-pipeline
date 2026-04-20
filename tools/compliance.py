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

CONTROL_DIR = STAGING_DIR / "control"

# Scene-tag detector — matches any of the common scene markers; a filename containing
# even one is considered dirty.
SCENE_TAG_RE = re.compile(
    r"\b(?:1080p|720p|480p|2160p|UHD|BluRay|BDRip|BRRip|WEB-?DL|WEBRip|HDTV|HDRip|"
    r"DVDRip|REMUX|x264|x265|HEVC|AAC|DDP?\d|AC3|EAC3|DTS|TrueHD|Atmos|"
    r"NF|AMZN|DSNP|HULU|MAX|ATVP|REPACK|MULTi|PROPER)\b",
    re.IGNORECASE,
)

TARGET_VIDEO = {"av1", "av1_nvenc"}
TARGET_AUDIO = {"eac3", "opus", "e-ac-3"}


def check_file(entry: dict, config: dict) -> list[str]:
    """Return a list of violation strings (empty if compliant)."""
    violations: list[str] = []
    lossless = {c.lower() for c in config.get("lossless_audio_codecs") or []}

    # Video
    v = entry.get("video") or {}
    vcodec = (v.get("codec") or v.get("codec_raw") or "").lower()
    if vcodec and vcodec not in TARGET_VIDEO:
        violations.append(f"video codec {vcodec} (target: av1)")

    # Audio: every track must be target codec + language in KEEP_LANGS
    for i, a in enumerate(entry.get("audio_streams") or []):
        codec = (a.get("codec") or a.get("codec_raw") or "").lower().replace("-", "")
        lang = (a.get("language") or "").lower().strip()
        if codec and codec not in TARGET_AUDIO and codec not in lossless:
            violations.append(f"audio[{i}] codec {a.get('codec')}")
        if lang and lang not in KEEP_LANGS:
            violations.append(f"audio[{i}] language {lang}")

    # Subs
    for i, s in enumerate(entry.get("subtitle_streams") or []):
        lang = (s.get("language") or "").lower().strip()
        if lang and lang not in KEEP_LANGS:
            violations.append(f"sub[{i}] language {lang}")

    # Filename
    fname = entry.get("filename") or ""
    if SCENE_TAG_RE.search(fname):
        violations.append(f"filename has scene tags: {fname}")

    # TMDb (proxy — if the report has no tmdb block, MKV probably has no tags either)
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
        out = CONTROL_DIR / "reencode.json"
        paths = [p for p, _ in non_compliant]
        try:
            existing = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
        except Exception:
            existing = {}
        merged = list({*existing.get("files", []), *paths})
        out.write_text(json.dumps({"files": merged, "patterns": existing.get("patterns", {})}, indent=2), encoding="utf-8")
        print(f"Queued {len(paths)} files (total now {len(merged)} in reencode.json)")


if __name__ == "__main__":
    main()
