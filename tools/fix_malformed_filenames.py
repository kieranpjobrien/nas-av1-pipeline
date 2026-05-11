"""One-off: fix the 2 malformed Bad Batch filenames where the original
rename pipeline mangled the extension boundary.

  S02E04 MP4.mkv             → S02E04 Faster.mkv         (TMDb episode title)
  S03E10 Identity Crisismkv.mkv → S03E10 Identity Crisis.mkv  (lost dot)

Renames on NAS, updates state DB filepath, refreshes media_report.

Run: ``uv run python -m tools.fix_malformed_filenames``
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BACKSLASH = chr(92)


def main() -> int:
    renames = [
        (
            "//KieranNAS/Media/Series/Star Wars - The Bad Batch/Season 2/Star Wars - The Bad Batch S02E04 MP4.mkv",
            "//KieranNAS/Media/Series/Star Wars - The Bad Batch/Season 2/Star Wars - The Bad Batch S02E04 Faster.mkv",
        ),
        (
            "//KieranNAS/Media/Series/Star Wars - The Bad Batch/Season 3/Star Wars - The Bad Batch S03E10 Identity Crisismkv.mkv",
            "//KieranNAS/Media/Series/Star Wars - The Bad Batch/Season 3/Star Wars - The Bad Batch S03E10 Identity Crisis.mkv",
        ),
    ]

    SIDECAR_EXTS = [".en.srt", ".eng.srt", ".srt", ".original.bak", ".nfo"]

    renamed_count = 0
    for src, dst in renames:
        src_stem = src[:-4]
        dst_stem = dst[:-4]

        if os.path.exists(src):
            os.rename(src, dst)
            print(f"RENAMED: {os.path.basename(src)} -> {os.path.basename(dst)}")
            renamed_count += 1
        else:
            print(f"MISSING: {src}")
            continue

        for ext in SIDECAR_EXTS:
            sc_src = src_stem + ext
            sc_dst = dst_stem + ext
            if os.path.exists(sc_src):
                os.rename(sc_src, sc_dst)
                print(f"  sidecar: {os.path.basename(sc_src)} -> {os.path.basename(sc_dst)}")

        tmp_src = src + ".av1.tmp"
        tmp_dst = dst + ".av1.tmp"
        if os.path.exists(tmp_src):
            os.rename(tmp_src, tmp_dst)
            print(f"  tmp: {os.path.basename(tmp_src)} -> renamed")

    print(f"\n{renamed_count} files renamed on NAS")

    # State DB updates
    con = sqlite3.connect("F:/AV1_Staging/pipeline_state.db")
    cur = con.cursor()
    db_updates = 0
    for src, dst in renames:
        src_back = BACKSLASH * 2 + src.lstrip("/").replace("/", BACKSLASH)
        dst_back = BACKSLASH * 2 + dst.lstrip("/").replace("/", BACKSLASH)
        cur.execute(
            "UPDATE pipeline_files SET filepath = ?, status='pending', stage=NULL, "
            "error=NULL, local_path=NULL, output_path=NULL WHERE filepath = ?",
            (dst_back, src_back),
        )
        if cur.rowcount:
            db_updates += cur.rowcount
            print(f"state DB updated: {os.path.basename(dst)}")
    con.commit()
    con.close()
    print(f"\n{db_updates} state DB rows updated")

    # Media report update
    from tools.report_lock import patch_report

    def patch(report: dict) -> None:
        files = report.get("files", [])
        updated = 0
        for f in files:
            for src, dst in renames:
                src_back = BACKSLASH * 2 + src.lstrip("/").replace("/", BACKSLASH)
                dst_back = BACKSLASH * 2 + dst.lstrip("/").replace("/", BACKSLASH)
                if f.get("filepath") == src_back:
                    f["filepath"] = dst_back
                    f["filename"] = os.path.basename(dst)
                    updated += 1
        print(f"media_report.json: updated {updated} entries")

    patch_report(patch)
    return 0


if __name__ == "__main__":
    sys.exit(main())
