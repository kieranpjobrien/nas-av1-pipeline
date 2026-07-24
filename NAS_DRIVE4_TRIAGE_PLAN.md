# NAS Drive 4 — Triage Plan (drafted overnight 2026-06-19)

## TL;DR
- Drive 4 dropped out of the storage pool. Pool is **DEGRADED but still running** (RAID10 survives one
  drive) and **/volume1 is still mounted read-write and readable** — the backup window is OPEN.
- **There is no prior NAS incident in any chat log** (searched every project back to mid-March). The
  "reseat / degraded / disk health" history you're half-remembering was the **PC workstation** (the 13700K
  instability in June), not the NAS. If a NAS drive ever blipped before, you fixed it in DSM off-chat. So
  there's no old playbook to copy — this *is* the playbook.
- Two **independent** questions:
  - **(A) Is drive 4 actually dead, or just disconnected?**  ← your "false alarm" theory lives here, and it
    has a real shot (see below).
  - **(B) How damaged is the filesystem?**  ← this one is *not* looking like a false alarm.

## What we already know (as of tonight)
| Check | Result | Read |
|---|---|---|
| Pool / RAID (`/proc/mdstat`) | drive 4 missing from all 3 arrays — `md2 … [6/5] [UUU_UU]` | degraded, still active |
| Drive 4 on the bus? (`synodisk --enum`) | **not listed**, no `/dev/sata4` | electrically absent |
| 5 present drives — SMART (`synodisk --smart_info_get`) | **all "Status: OK"** (reallocated / pending / power-on all OK) | no second failure brewing ✅ |
| Drive 4 — SMART | no response | can't query a drive that isn't there |
| Filesystem (`dmesg`, BTRFS on dm-2) | `parent transid verify failed` ×many + `failed to repair … mirror=1` | **metadata corruption present** ⚠️ |
| Volume mounted / readable | `/volume1` btrfs **rw**, 53 TB (44 used) | **backup window open** |

**Why (A) is genuinely hopeful:** a drive that *vanishes completely*, with **healthy neighbours** and **no
SMART error trail**, is more consistent with a **cable / backplane / power connection** fault than a dying
disk (dying disks usually stay visible-but-sick, throwing reallocated/pending sectors, before they go). That
is the recoverable, "you were right" case — and it's a 15-minute test.

**Why (B) is not:** RAID10 losing one drive should *not* corrupt the filesystem. The BTRFS transid failures +
"failed to repair" mean on-disk metadata is inconsistent and couldn't self-heal from its mirror. That is real
and independent of the drive — and it's why we must **not** just "convince it the drive's fine" and kick off a
rebuild (hours of heavy I/O on top of corrupt metadata is the worst-case move).

---

## The triage ladder — fastest signal first; stop the moment hope dies
Rule: **hope survives** a rung only if a benign story (soft-drop / cable / healthy hardware / intact fs) still
fits what you saw. **Hope dies** the moment a rung shows a hardware or fs failure no benign story explains.

### Rung 1 — Is the drive even on the bus?  (~30 s, zero risk)  ✅ DONE
`cat /proc/mdstat` · `synodisk --enum` · `ls -l /dev/sata4`
- ✅ hope-alive: drive 4 *is* listed but kicked from the array → soft drop, most recoverable.
- ❌ hope-dims: not listed at all → electrically absent (cable-or-dead-drive, not "NAS imagining it").
- **Result: not listed → past "pure false alarm," now cable-vs-dead-drive.**

### Rung 2 — Why did it drop? (the kernel's story)  (~1 min, zero risk)  ⏳ needs root SSH
`dmesg -T | grep -iE "ata4|sata4"` · `/var/log/messages`
- ✅ hope-alive: one-off "hard resetting link" / "link down" then silence, or a clean removal → connection glitch.
- ❌ hope-dims: repeated "failed command" / "medium error" / ATA timeouts before it vanished → drive was dying.

### Rung 3 — Is the hardware healthy? (SMART)  (~2 min, zero risk)  ✅ DONE for present drives
`synodisk --smart_info_get /dev/sataN` (no root needed) · or DSM → Storage Manager → HDD/SSD → Health Info
- ✅ hope-alive: every present drive PASSES, 0 reallocated/pending/uncorrectable.
- ❌ hope-dies: any present drive failing/rising counts → second sick drive = a rebuild could kill the pool.
- **Result: all 5 present drives OK ✅. Drive 4 itself can only be SMART-tested once it's back (Rung 5).**

### Rung 4 — How damaged is the filesystem?  (~2 min read)  ⏳ partial; needs root for full extent
`btrfs scrub status /volume1` · DSM → Storage Manager → file-system status · (dmesg transid errors already seen)
- ✅ hope-alive: scrub shows 0 unrecoverable errors (or repairs from good copies) → fs intact.
- ❌ hope-dies (for "just rebuild"): persistent uncorrectable metadata errors / "failed to repair" → fs damage;
  back up first, this is scrub + possibly Synology-support / recovery territory.
- **Result: already flashing red (transid failures + failed-to-repair). This is the real problem.**

### Rung 5 — The reversible physical test  (~15 min, low risk) — ONLY if Rungs 1–4 still leave hope
**Back up irreplaceable data first (Rung 6).** Then: clean shutdown → reseat drive 4 + reseat/replace its SATA
cable → ideally move it to a **known-good bay/port** to isolate drive-vs-cable/backplane → power on → re-run
Rung 1 + SMART (Rung 3) on drive 4.
- ✅ hope-confirmed: drive 4 re-appears, enumerates, SMART-clean → it **was** the connection (your false alarm).
- ❌ hope-dies: still absent after reseat *and* a cable/port swap → the drive (or backplane) is dead → replace.

### Rung 6 — The gate before ANY rebuild  (non-negotiable)
Only "Repair"/re-add (→ hours of heavy rebuild I/O) **after**: (1) irreplaceable data backed up — the volume is
readable *now*, that window is precious; (2) SMART on all drives green (Rung 3); (3) fs damage assessed (Rung 4).
A rebuild on a degraded array + corrupt fs + a possibly-marginal neighbour is the single highest-risk moment.

---

## The morning's first ~15 minutes — do in this order
1. **(2 min, GUI, no SSH) DSM → Storage Manager.** Confirm: Storage Pool status, which drive shows
   missing/failed, and **Health Info → SMART on all drives** (re-confirms Rungs 1 + 3 visually).
2. **(2 min, if you enable root SSH) the kernel + fs story** — paste me root access (or run these):
   ```
   sudo dmesg -T | grep -iE "ata4|sata4" | tail -40        # Rung 2 — why #4 dropped
   sudo btrfs scrub status /volume1                          # Rung 4 — fs damage extent
   sudo smartctl -a /dev/sata4 2>/dev/null || echo absent    # drive 4 SMART if it ever answers
   ```
3. **Decision point:**
   - Rungs 1–4 still leave hope (drive cleanly absent, neighbours healthy) → **back up irreplaceables**
     (family photos/videos, configs — not the re-downloadable media), **then** do the **Rung 5 reseat/cable test**.
   - Drive 4 comes back SMART-clean → it was the cable (your call). Re-add/repair **after** the fs is checked.
   - Drive 4 stays gone after reseat + cable swap → it's dead → replace the drive, then repair.
4. **The filesystem, regardless of the drive:** run a `btrfs scrub` (DSM file-system check). If it can't clear
   the transid errors, treat the pool as suspect and raise a **Synology support** ticket before trusting it.

## Current state (left safe for you)
- **All work is paused** — pipeline, encoders, de-bloat, and the dashboard are all stopped; nothing of ours is
  touching the pool (reads included). It stays down until you decide.
- Nothing has been written to or changed on the NAS. All checks above were read-only.

## Bottom line
Your instinct that **the drive itself may be fine** is defensible — the evidence (clean vanish, healthy
neighbours) fits a connection fault, and the reseat test will confirm or kill it in 15 minutes. But "it's a
false alarm, just re-add it" is **not** safe here, because the **filesystem corruption is real and separate**.
So: test the drive cheaply (reseat), but back up first and check the filesystem before any rebuild.
