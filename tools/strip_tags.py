"""Strip release group tags from series filenames on NAS."""

import argparse
import os
import re
from pathlib import Path

from paths import NAS_SERIES

# Tags to strip -- add more as needed
STRIP_TAGS = [
    " ( SDR DDP 5 1 English - HONE)",
    " p mkv",
    " Webrip ",
    " WebRip 5 1",
    " REPACK 1",
    " NORDiC REPACK 1",
    " NORDiC",
    " -ViGoR",
    " -YAWNiX",
    " p -Vyndros",
    "INTERNAL",
    "H1-SLiGNOME",
    " 0 -BS",
    " DD 2 0 -monkee",
    " 1080p Bluray x265-HiQVE",
    " 5 1 Bluray",
    " 5 1",
    "-STORIES",
    " bluray h264-reactant",
    " POLISH WEB -A4O",
    " p ESRGAN Upscale 5 1 X264-PoF",
    "- -oxidizer",
    " -PHOENiX",
    " iTALiAN WEB -NTROPiC",
    " DD+5 1",
    " GERMAN DL WEB h264-SAUERKRAUT",
    " FINAL MULTI WEB -HiggsBoson",
    " MULTi WEB -UKDTV",
    " WEB -SuccessfulCrab",
    " -SiGMA",
    " WEB h264-NOMA",
    "AVC-HDMA51REMUX-FraMeSToR",
    "DTS",
    "REPACK20-FLUX",
    ".1999.Disney+.WEB-DL...AAC-HDCTV",
    " 0 -OldT",
    "..NF.WEB-DL..H.264-NTb",
    ".1998.Disney+.WEB-DL...AAC-HDCTV",
    "..web.h264-sundry",
    ".Disney+.WEB-DL...AAC-HDCTV",
    "pSDR1English",
    "pAtmos-HHWEB",
    " iTALiAN MULTi WEB -NTROPiC",
    ".p.WEB-DL..H.265-WADU",
    ".REPACK.p.DSNP.WEB-DL..Atmos..H.265-BLOOM",
    ".p.10bit.HDR.DV.WEBRip..Atmos.X265.HEVC-PSA",
    "-HONE",
    "[TAoE]",
]


def strip_multiple_and_collect(root, strip_tags):
    all_names = []

    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            old_path = os.path.join(dirpath, fn)
            name, ext = os.path.splitext(fn)

            # Remove tags
            new_name = name
            for tag in strip_tags:
                new_name = new_name.replace(tag, "")

            # Replace any remaining dots with spaces, collapse multi-spaces
            new_name = re.sub(r"\.+", " ", new_name)
            new_name = new_name.strip()
            new_name = " ".join(new_name.split())

            new_fn = f"{new_name}{ext}"
            new_path = os.path.join(dirpath, new_fn)

            if new_path != old_path:
                try:
                    os.rename(old_path, new_path)
                except OSError as e:
                    print(f"SKIP rename (error): {old_path!r} -> {new_path!r}: {e}")
                    new_path = old_path

            all_names.append(os.path.relpath(new_path, root))

    return all_names


def main():
    parser = argparse.ArgumentParser(description="Strip release group tags from series filenames")
    parser.add_argument("--root", type=str, default=str(NAS_SERIES),
                        help="Root series directory")
    parser.add_argument("--report", type=str, default=None,
                        help="Output report path (default: series_filenames.txt next to script)")
    args = parser.parse_args()

    report_path = args.report or str(Path(__file__).with_name("series_filenames.txt"))
    names = strip_multiple_and_collect(args.root, STRIP_TAGS)

    with open(report_path, "w", encoding="utf-8") as report:
        for name in sorted(names):
            report.write(name + "\n")

    print(f"Done: stripped {len(STRIP_TAGS)} tags and wrote {len(names)} filenames to {report_path}")


if __name__ == "__main__":
    main()
