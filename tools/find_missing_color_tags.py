"""Find AV1 files in media_report.json with missing colour-space tags.

Pre-2026-05-26 the encoder didn't emit ``-color_primaries`` /
``-color_trc`` / ``-colorspace`` flags for SDR sources. The AV1
output stream was tagged "unspecified" for all three, and players
default-guessed the matrix. On 10-bit SDR content the guess often
landed on BT.2020 (because of the bit depth), producing the
green / purple tint Op observed on 1917 and The Drama.

This sweep ffprobes every AV1 file in media_report and lists ones
where any of {color_primaries, color_transfer, color_space} is
missing or "unknown" / "reserved". Outputs grouped by risk:

  * HIGH risk: 10-bit SDR (yuv420p10le) with all three tags missing
    — the The-Drama / 1917 pattern; visible tint highly likely.
  * MEDIUM:    8-bit SDR with all three tags missing — less likely
    to tint because 8-bit defaults are more predictable, but
    possible on certain players.
  * LOW:       Some tags present, some missing — partial coverage,
    behaviour player-dependent.

Files where all three tags ARE present pass the check and aren't
listed.

Run: ``uv run python -m tools.find_missing_color_tags``
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from paths import MEDIA_REPORT


def _probe(filepath: str, timeout: int = 15) -> tuple[str, str, str, str] | None:
    """Return (color_primaries, color_transfer, color_space, pix_fmt)
    or None if probe failed. Each colour field is the raw ffprobe
    value (could be "unknown" / "reserved" / "" / None / a real name)."""
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries",
         "stream=color_primaries,color_transfer,color_space,pix_fmt",
         "-of", "json", filepath],
        capture_output=True, text=True, timeout=timeout,
    )
    if out.returncode != 0:
        return None
    try:
        info = json.loads(out.stdout)
        s = (info.get("streams") or [{}])[0]
    except (json.JSONDecodeError, IndexError):
        return None
    return (
        s.get("color_primaries"),
        s.get("color_transfer"),
        s.get("color_space"),
        s.get("pix_fmt"),
    )


def _is_missing(v) -> bool:
    return v in (None, "", "unknown", "reserved")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--limit", type=int, default=0,
                        help="Stop after N AV1 files (0 = all)")
    parser.add_argument("--out", type=str, default=None,
                        help="Write a CSV of affected paths to this file")
    args = parser.parse_args()

    rep = json.loads(Path(MEDIA_REPORT).read_text(encoding="utf-8"))
    av1_files = [
        f for f in rep.get("files", [])
        if ((f.get("video") or {}).get("codec_raw") or "").lower() == "av1"
    ]
    print(f"AV1 files in media_report: {len(av1_files)}")
    if args.limit:
        av1_files = av1_files[: args.limit]
        print(f"  ...limited to first {args.limit} for this run")

    high: list[tuple[str, str]] = []   # (path, pix_fmt)
    medium: list[tuple[str, str]] = []
    low: list[tuple[str, str, str, str, str]] = []  # (path, pix_fmt, p, t, s)
    probe_failed: list[str] = []
    ok = 0

    for i, f in enumerate(av1_files, 1):
        fp = f.get("filepath")
        if not fp or not os.path.exists(fp):
            continue
        result = _probe(fp)
        if result is None:
            probe_failed.append(fp)
            continue
        prim, trc, sp, pix = result
        missing = [_is_missing(v) for v in (prim, trc, sp)]
        n_missing = sum(missing)
        if n_missing == 0:
            ok += 1
        elif n_missing == 3:
            # All three missing — the smoking-gun case
            if pix == "yuv420p10le":
                high.append((fp, pix))
            else:
                medium.append((fp, pix))
        else:
            low.append((fp, pix, str(prim), str(trc), str(sp)))
        if i % 200 == 0:
            print(f"  ...{i}/{len(av1_files)} probed", file=sys.stderr)

    print()
    print(f"=== Sweep complete ===")
    print(f"  AV1 files probed:                {len(av1_files)}")
    print(f"  Probe failed:                    {len(probe_failed)}")
    print(f"  Fully tagged (OK):               {ok}")
    print(f"  HIGH risk (10-bit, all missing): {len(high)}")
    print(f"  MEDIUM (8-bit, all missing):     {len(medium)}")
    print(f"  LOW (partial tags):              {len(low)}")
    print()

    if high:
        print(f"=== HIGH RISK — {len(high)} files ===")
        print("(10-bit SDR AV1 with no colour tags — visible tint likely)")
        for fp, pix in sorted(high, key=lambda x: x[0]):
            print(f"  {os.path.basename(fp)}")
    if medium:
        print()
        print(f"=== MEDIUM RISK — {len(medium)} files ===")
        print("(8-bit SDR AV1 with no colour tags — tint possible)")
        for fp, pix in sorted(medium, key=lambda x: x[0])[:30]:
            print(f"  {os.path.basename(fp)}")
        if len(medium) > 30:
            print(f"  ... ({len(medium) - 30} more — full list in --out)")
    if low:
        print()
        print(f"=== LOW RISK — {len(low)} files ===")
        print("(partial colour tags — behaviour player-dependent)")
        for fp, pix, p, t, sp in sorted(low, key=lambda x: x[0])[:10]:
            print(f"  pix={pix:14s} p={p:10s} t={t:10s} s={sp:10s}  {os.path.basename(fp)}")
        if len(low) > 10:
            print(f"  ... ({len(low) - 10} more — full list in --out)")

    if args.out:
        rows = []
        for fp, pix in high:
            rows.append(("HIGH", fp, pix, "missing", "missing", "missing"))
        for fp, pix in medium:
            rows.append(("MEDIUM", fp, pix, "missing", "missing", "missing"))
        for fp, pix, p, t, s in low:
            rows.append(("LOW", fp, pix, p, t, s))
        Path(args.out).write_text(
            "risk,filepath,pix_fmt,color_primaries,color_transfer,color_space\n"
            + "\n".join(
                ",".join(f'"{c}"' if "," in str(c) else str(c) for c in r) for r in rows
            ),
            encoding="utf-8",
        )
        print(f"\nWrote CSV: {args.out}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
