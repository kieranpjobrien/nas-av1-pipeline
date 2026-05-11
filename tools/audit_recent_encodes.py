"""Audit recent encodes against pipeline.compliance.check_compliance — the
SAME function the encoder's verify gate runs pre-replace. If a shipped
file fails this check, either:
  * the verify gate had a bug (file shipped despite a violation), or
  * something modified the file post-replace (manual rename, external
    tool re-muxed it, etc.).

Either way, this tool is the post-shipping reconciliation. Run it
periodically; clean output means the verify gate is doing its job.

Run: ``uv run python -m tools.audit_recent_encodes [--n 20]``
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.compliance import check_compliance  # noqa: E402
from pipeline.config import build_config  # noqa: E402

MKVEXTRACT = r"C:/Program Files/MKVToolNix/mkvextract.exe"
BACKSLASH = chr(92)


def ffprobe(fp: str) -> dict:
    """Mirror pipeline.full_gamut._probe_full's output shape — the compliance
    function expects ``{video, audio, subs, format, error}``."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-print_format", "json",
             "-show_format", "-show_streams", fp],
            capture_output=True, timeout=60,
        )
        if result.returncode != 0:
            return {"error": (result.stderr.decode("utf-8", "replace").strip().splitlines() or ["probe failed"])[-1]}
        data = json.loads(result.stdout.decode("utf-8", "replace"))
    except Exception as e:
        return {"error": str(e)}

    fmt = data.get("format", {})
    streams = data.get("streams", [])
    out = {
        "format": {
            "name": fmt.get("format_name", ""),
            "duration_secs": float(fmt.get("duration", 0) or 0),
            "size_bytes": int(fmt.get("size", 0) or 0),
            "bit_rate_kbps": int((int(fmt.get("bit_rate", 0) or 0)) / 1000) if fmt.get("bit_rate") else None,
        },
        "video": {},
        "audio": [],
        "subs": [],
    }
    for s in streams:
        ct = s.get("codec_type")
        if ct == "video" and not out["video"]:
            out["video"] = {
                "codec": s.get("codec_name", ""),
                "width": s.get("width"),
                "height": s.get("height"),
                "pix_fmt": s.get("pix_fmt"),
                "bit_rate_kbps": int(int(s.get("bit_rate", 0)) / 1000) if s.get("bit_rate") else None,
                "r_frame_rate": s.get("r_frame_rate"),
            }
        elif ct == "audio":
            tags = s.get("tags") or {}
            out["audio"].append({
                "codec": s.get("codec_name", ""),
                "channels": s.get("channels"),
                "channel_layout": s.get("channel_layout"),
                "bit_rate_kbps": int(int(s.get("bit_rate", 0)) / 1000) if s.get("bit_rate") else None,
                "language": tags.get("language", ""),
                "title": tags.get("title", ""),
            })
        elif ct == "subtitle":
            tags = s.get("tags") or {}
            out["subs"].append({
                "codec": s.get("codec_name", ""),
                "language": tags.get("language", ""),
                "title": tags.get("title", ""),
            })
    return out


def mkv_tags(fp: str) -> dict[str, str]:
    out = subprocess.run([MKVEXTRACT, "tags", fp], capture_output=True, timeout=60)
    if out.returncode != 0:
        return {}
    xml = out.stdout.decode("utf-8", "replace")
    return {
        m.group(1).upper(): m.group(2)
        for m in re.finditer(r"<Simple>\s*<Name>([^<]+)</Name>\s*<String>([^<]*)</String>", xml)
    }


def audit_file(history_entry: dict, cfg: dict, media_report: dict) -> dict:
    fp = history_entry.get("filepath", "")
    fp_fwd = fp.replace(BACKSLASH, "/")
    if not fp_fwd.startswith("//"):
        fp_fwd = "//" + fp_fwd.lstrip("/")
    name = os.path.basename(fp_fwd)

    rec: dict = {
        "filename": name,
        "filepath": fp,
        "ts": history_entry.get("timestamp", "")[:19],
        "violations": [],
    }
    if not os.path.exists(fp_fwd):
        rec["violations"].append("file no longer exists at the path the encoder used")
        return rec

    rec["disk_size_gb"] = round(os.path.getsize(fp_fwd) / 1024**3, 3)

    # Find the matching media_report entry (for tmdb + library_type).
    rep_entry = next((e for e in media_report.get("files", []) if e.get("filepath") == fp), None)
    item = {
        "tmdb": (rep_entry or {}).get("tmdb") or {},
        "library_type": (rep_entry or {}).get("library_type", ""),
        "filename": name,
        "final_name": name,  # by definition — file is on NAS at this name
        "resolution": ((rep_entry or {}).get("video") or {}).get("resolution_class", ""),
        "hdr": ((rep_entry or {}).get("video") or {}).get("hdr", False),
    }

    encode_params = history_entry.get("encode_params") or {}
    src_codec = (((history_entry.get("source") or {}).get("video") or {}).get("codec") or "").upper()
    source_was_av1 = src_codec == "AV1"

    output_probe = ffprobe(fp_fwd)
    tags = mkv_tags(fp_fwd)
    input_size = history_entry.get("input_bytes", 0)
    output_size = history_entry.get("output_bytes", 0) or os.path.getsize(fp_fwd)

    rec["video_codec"] = (output_probe.get("video") or {}).get("codec", "?")
    rec["audio_count"] = len(output_probe.get("audio", []))
    rec["sub_count"] = len(output_probe.get("subs", []))
    rec["mkv_cq"] = tags.get("CQ")
    rec["mkv_grade"] = tags.get("CONTENT_GRADE")

    violations = check_compliance(
        filepath=fp,
        item=item,
        encode_params=encode_params,
        output_probe=output_probe,
        mkv_tags=tags,
        input_size_bytes=input_size,
        output_size_bytes=output_size,
        source_was_av1=source_was_av1,
        config=cfg,
    )
    rec["violations"] = [(v.tag, v.category.value, v.message) for v in violations]
    return rec


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=20)
    parser.add_argument("--since", default="2026-05-09T00:00")
    args = parser.parse_args()

    history_path = "F:/AV1_Staging/encode_history.jsonl"
    entries: list[dict] = []
    with open(history_path, encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if r.get("timestamp", "") >= args.since:
                entries.append(r)
    entries.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
    entries = entries[: args.n]
    print(f"Auditing {len(entries)} encodes since {args.since}")
    print()

    cfg = build_config()
    with open("F:/AV1_Staging/media_report.json", encoding="utf-8") as f:
        report = json.load(f)

    n_clean = 0
    n_violation = 0
    for h in entries:
        rec = audit_file(h, cfg, report)
        viols = rec["violations"]
        marker = "OK   " if not viols else "FAIL "
        print(f"{marker} {rec['ts']}  {rec['filename'][:55]}")
        if rec.get("video_codec"):
            print(
                f"        codec={rec.get('video_codec','?')} "
                f"audio={rec.get('audio_count','?')}x sub={rec.get('sub_count','?')}x "
                f"cq={rec.get('mkv_cq','?')} grade={rec.get('mkv_grade','?')!r} "
                f"size={rec.get('disk_size_gb','?')} GB"
            )
        for tag, cat, msg in viols:
            print(f"        [{cat}] {tag}: {msg}")
        if not viols:
            n_clean += 1
        else:
            n_violation += 1

    print()
    print(
        f"Summary: {n_clean} clean, {n_violation} with violations (out of {len(entries)})"
    )
    return 0 if n_violation == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
