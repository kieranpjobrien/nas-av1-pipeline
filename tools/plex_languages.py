"""Find movies without English audio by querying a Plex database backup."""

import argparse
import csv
import json
import os
import sqlite3
import sys
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory

from paths import STAGING_DIR

HERE = Path(__file__).parent.parent


def open_sqlite(db_path: Path):
    con = sqlite3.connect(str(db_path))
    con.execute("SELECT name FROM sqlite_master LIMIT 1").fetchone()
    return con


def extract_sqlite_from_backup(backup_path: Path) -> Path:
    with zipfile.ZipFile(backup_path, "r") as zf, TemporaryDirectory(dir=HERE) as tdir:
        tdirp = Path(tdir)
        candidates = sorted(
            (n for n in zf.namelist() if n.lower().endswith(".db") or "com.plexapp.plugins.library.db" in n.lower()),
            key=lambda s: (0 if "com.plexapp.plugins.library.db" in s.lower() else 1, len(s))
        )
        if not candidates:
            raise RuntimeError("Backup looks like a ZIP, but no .db file found inside.")
        member = candidates[0]
        zf.extract(member, path=tdirp)
        extracted = (tdirp / member).resolve()
        final = HERE / "extracted_plex_library.db"
        if final.exists():
            final.unlink()
        extracted.replace(final)
        return final


def get_db_connection(backup_path: Path):
    try:
        return open_sqlite(backup_path)
    except Exception:
        pass
    try:
        extracted = extract_sqlite_from_backup(backup_path)
        return open_sqlite(extracted)
    except zipfile.BadZipFile:
        raise SystemExit(f"Not a SQLite DB or ZIP: {backup_path}")
    except Exception as e:
        raise SystemExit(f"Failed to open DB from backup: {e}")


def cols(con, table):
    return {r[1].lower() for r in con.execute(f"PRAGMA table_info({table})").fetchall()}


def tables(con):
    return {r[0].lower() for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}


def build_sql(con):
    t = tables(con)
    ms_cols = cols(con, "media_streams")

    # Determine how to pick the audio rows
    if "stream_type" in ms_cols:
        audio_where = "ms.stream_type = 2"
    elif "stream_type_id" in ms_cols:
        audio_where = "ms.stream_type_id = 2"
    elif "codec_type" in ms_cols:
        audio_where = "LOWER(ms.codec_type)='audio'"
    elif "type" in ms_cols:
        audio_where = "LOWER(ms.type)='audio'"
    else:
        raise SystemExit("Cannot determine how audio streams are identified.")

    # Find which language-related columns actually exist
    lang_candidates = [c for c in ("language_code", "language", "lang", "language_tag") if c in ms_cols]
    if not lang_candidates:
        lang_expr = "''"
    elif len(lang_candidates) == 1:
        lang_expr = f"LOWER(ms.{lang_candidates[0]})"
    else:
        joined = ",".join(f"ms.{c}" for c in lang_candidates)
        lang_expr = f"LOWER(COALESCE({joined}))"

    sql = f"""
WITH movie_parts AS (
  SELECT mi.id AS media_item_id,
         md.id AS metadata_id,
         md.title AS title,
         md.year  AS year,
         mp.file  AS file
  FROM metadata_items md
  JOIN media_items mi ON mi.metadata_item_id = md.id
  JOIN media_parts  mp ON mp.media_item_id   = mi.id
  WHERE md.metadata_type = 1
),
audio_streams AS (
  SELECT mp.media_item_id,
         {lang_expr} AS lang
  FROM media_streams ms
  JOIN media_parts   mp ON mp.id = ms.media_part_id
  WHERE {audio_where}
),
english_audio AS (
  SELECT DISTINCT media_item_id
  FROM audio_streams
  WHERE lang IN ('eng','en','english')
),
audio_langs AS (
  SELECT media_item_id,
         GROUP_CONCAT(DISTINCT NULLIF(lang,'')) AS langs
  FROM audio_streams
  GROUP BY media_item_id
)
SELECT mp.title, mp.year, mp.file, COALESCE(al.langs,'') AS audio_languages
FROM movie_parts mp
LEFT JOIN english_audio ea ON ea.media_item_id = mp.media_item_id
LEFT JOIN audio_langs   al ON al.media_item_id = mp.media_item_id
WHERE ea.media_item_id IS NULL
ORDER BY mp.title COLLATE NOCASE;
"""
    return sql


def main():
    parser = argparse.ArgumentParser(description="Find non-English movies from Plex database backup")
    parser.add_argument("--backup", type=str, default=None,
                        help="Path to Plex database backup file (auto-detects databaseBackup* in project dir)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output CSV path (default: plex_non_english.csv)")
    args = parser.parse_args()

    if args.backup:
        backup_path = Path(args.backup)
    else:
        # Auto-detect backup file in project directory
        candidates = list(HERE.glob("databaseBackup*"))
        if not candidates:
            print(f"No databaseBackup* file found in {HERE}")
            sys.exit(1)
        backup_path = candidates[0]

    csv_out = Path(args.output) if args.output else HERE / "plex_non_english.csv"

    if not backup_path.exists():
        print(f"Backup not found: {backup_path}")
        sys.exit(1)

    con = get_db_connection(backup_path)
    try:
        sql = build_sql(con)
        rows = con.execute(sql).fetchall()
    finally:
        con.close()

    with open(csv_out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["title", "year", "path", "audio_languages"])
        w.writerows(rows)

    print(f"Found {len(rows)} movies with NO English audio (from Plex DB).")
    print(f"Wrote: {csv_out}")


if __name__ == "__main__":
    main()
