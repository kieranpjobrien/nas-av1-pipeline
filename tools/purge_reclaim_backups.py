"""Purge _reclaim_backup originals for verified-reclaimed films (banks the space).

Safe + idempotent. Deletes a backup ONLY when the new (smaller) file is present
at the original path, probes as video, and is smaller than the backup -- so the
originals never go while anything is wrong. Does NOT write the shared
reclaim_ledger.json (the reclaim batch may still be running -> avoid the
read-modify-write race); records to a separate purge log instead.

Usage: python -m tools.purge_reclaim_backups
"""
import json
import os
import time

from tools.reclaim_debloat import LEDGER, probe_counts

PURGE_LOG = "F:/AV1_Staging/reclaim_purge.log"


def main() -> None:
    led = json.load(open(LEDGER, encoding="utf-8"))
    reclaimed = [(k, v) for k, v in led.items() if v.get("status") == "reclaimed" and v.get("backup")]
    freed = 0
    purged, skipped = [], []
    for k, v in reclaimed:
        backup = v["backup"]
        if not os.path.exists(backup):
            continue  # already purged
        if not os.path.exists(k):
            skipped.append((os.path.basename(k), "NEW FILE MISSING -> keeping backup"))
            continue
        if probe_counts(k)[0] < 1:
            skipped.append((os.path.basename(k), "new file has no video -> keeping backup"))
            continue
        new_sz, bk_sz = os.path.getsize(k), os.path.getsize(backup)
        if new_sz >= bk_sz:
            skipped.append((os.path.basename(k), f"new not smaller ({new_sz/1e9:.1f}>={bk_sz/1e9:.1f}GB) -> keeping"))
            continue
        os.remove(backup)
        try:
            os.rmdir(os.path.dirname(backup))  # tidy the now-empty film folder
        except OSError:
            pass
        freed += bk_sz
        purged.append(os.path.basename(k))

    line = f"{time.strftime('%Y-%m-%d %H:%M')} purged {len(purged)} backups, freed {freed/1e9:.0f}GB"
    with open(PURGE_LOG, "a", encoding="utf-8") as fh:
        fh.write(line + "\n" + "".join(f"  + {p}\n" for p in purged))
    print(line)
    for p in purged:
        print("  purged:", p)
    for n, r in skipped:
        print("  SKIPPED:", n, "->", r)


if __name__ == "__main__":
    main()
