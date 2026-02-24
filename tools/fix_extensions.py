"""Fix missing .mkv extensions on series files."""

import argparse
import os
from pathlib import Path

from paths import NAS_SERIES

# Extensions to fix (without leading dot)
EXTENSIONS = ["mkv", "mp4", "avi", "mov"]


def fix_extensions(root, exts):
    actions = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            old_path = os.path.join(dirpath, fn)
            name, ext = os.path.splitext(fn)
            lower_ext = ext.lower().lstrip(".")

            # If file already has a correct dot-prefixed extension, skip
            if lower_ext in exts and ext.startswith("."):
                continue

            # If no proper extension but name ends with an extension string
            if not ext:
                for e in exts:
                    if name.lower().endswith(e):
                        base = name[:-len(e)]
                        new_fn = f"{base}.{e}"
                        new_path = os.path.join(dirpath, new_fn)
                        try:
                            os.rename(old_path, new_path)
                            actions.append(f"RENAMED: {old_path} -> {new_path}")
                        except OSError as err:
                            actions.append(f"FAILED: {old_path} -> {new_path}: {err}")
                        break
    return actions


def main():
    parser = argparse.ArgumentParser(description="Fix missing file extensions on series files")
    parser.add_argument("--root", type=str, default=str(NAS_SERIES),
                        help="Root series directory")
    parser.add_argument("--report", type=str, default=None,
                        help="Output report path (default: series_fix_ext_report.txt next to script)")
    args = parser.parse_args()

    report_path = args.report or str(Path(__file__).with_name("series_fix_ext_report.txt"))
    results = fix_extensions(args.root, EXTENSIONS)

    with open(report_path, "w", encoding="utf-8") as rpt:
        rpt.write("Series extension fix report\n")
        for line in results:
            rpt.write(line + "\n")

    print(f"Done: fixed {len(results)} files, report at {report_path}")


if __name__ == "__main__":
    main()
