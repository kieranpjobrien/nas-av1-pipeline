# NASCleanup

Personal AV1 re-encoding pipeline for a home NAS. Converts H.264/HEVC media to AV1 using NVENC hardware encoding (RTX 4000+ GPU).

## Essentials
- Package manager: **uv** (not Poetry). Run with `uv run python -m <module>`
- `paths.py` is the single source of truth for all env-var-backed paths
- Pipeline (`pipeline/`) uses stdlib only — no pip dependencies
- Server (`server/`) uses FastAPI + uvicorn; frontend is Vite + React in `frontend/`
- Single quality profile ("baseline") — the multi-profile mechanism was dropped after the 2026-04-23/24 cleanup

## Key Paths
- `F:\AV1_Staging\` — Local staging (state, control files, temp encodes)
- `\\KieranNAS\Media\Movies\`, `\\KieranNAS\Media\Series\` — NAS libraries

## Audio policy (user setup: Sonos Arc + 2× Sonos One rears = 5.1 + Atmos)

- **Target codec: EAC-3.** 640 kbps surround, 256 kbps stereo. Netflix-grade.
- **Passthrough (never transcode)**:
  - EAC-3 (already target; EAC-3-JOC carries Atmos — preserved bit-exact)
  - **TrueHD** (primary Dolby Atmos carrier — Sonos Arc decodes it natively)
- **Transcode to EAC-3**: everything else (DTS, DTS-HD MA, FLAC, PCM, AC-3, AAC, MP3, **Opus**).
  Opus was previously passthrough but Sonos Arc has no native Opus decode — Plex
  transcoded on every play. Pre-transcoding once removes that overhead.
- Channel count + layout preserved through transcode (no `-ac N` flag).

---

# DISCIPLINE CONTRACT (post-incident, do not delete)

This repo has a history of the model lying to the user by framing failures
as progress. Read and obey before doing anything else.

## Hard rules — violating any of these is a bug

1. **Never mark a file DONE on failure.** If a remote mkvmerge / ffmpeg /
   mkvpropedit call returns non-zero or the subprocess raises, the file
   status MUST be `ERROR` or `PENDING`, never `DONE`. Reason strings like
   "deferred", "skipped", "local ops done (strip deferred)" paired with
   `status=DONE` are a lie. Runtime guard in `state.set_file` rejects this.

2. **Never downplay a recurring error.** If the same error class fires
   3+ times in a row (e.g. rc=137, rc=255, banner timeout), circuit
   breaker opens. "Transient" is only legitimate for a single isolated
   event.

3. **Evidence before claim.** Before saying "X is working", "X is
   progressing", "X will climb to Y%", or "the fix is deployed":
     - Run a concrete check that proves it.
     - Paste the command and its output verbatim in the response.
     - If you cannot provide evidence, do not make the claim.

4. **Never convert "process alive" into "work happening".** A python
   process at 0% CPU and stable memory is hung, not working.

5. **Enumerate and reconcile at session start.** Use
   `pipeline.process_registry.ProcessRegistry` via `reconcile()` before
   any destructive action. Ghost processes are reaped, not ignored.

6. **Every fix gets a test.** If a bug existed, a regression test must
   be added in the same commit. Invariants live in `tools/invariants.py`
   and are enforceable via `/api/health-deep`.

7. **No background process without a registry entry.** Every long-running
   `uv run` must register via `ProcessRegistry.register(role, cmd)`.

8. **Before touching media files, probe them first.** Pre-probe source
   (video >= 1, audio >= 1). Post-probe staging (counts match expected).
   Atomic replace only on all checks passing.

9. **No `-err_detect ignore_err` globally.** Ever. Scope to video only
   (`-err_detect:v ignore_err`) or not at all. Audio decode errors
   should fail the encode, not silently produce a zero-audio output.
   Pre-commit hook blocks the global form.

9a. **TrueHD passthrough is mandatory.** TrueHD is the primary Dolby
    Atmos carrier. Transcoding it to EAC-3 drops the object layer.
    User has a Sonos Arc which decodes TrueHD-Atmos natively.
    `_should_transcode_audio` returns False for codec == "truehd".

9b. **One NVENC encode at a time.** RTX 4080 has dual NVENC physically,
    but running two concurrent encodes caused system BSODs in production.
    `gpu_concurrency` MUST stay at 1. Same severity class as 9a — single
    encode is the safe envelope; the throughput of a second concurrent
    encode isn't worth the crash risk.

9c. **One CUDA inference at a time on this GPU.** Same RTX 4080, same
    crash class. The 2026-04-30 18:27 BSOD (bugcheck 0x135, access
    violation in nvlddmkm) was triggered by 4 concurrent whisper worker
    threads each holding a CUDA context. CPU whisper survives the same
    code because the GIL serialises the inference calls in practice;
    GPU whisper hits real parallelism and overruns the driver. When
    `WHISPER_FORCE_CPU` is unset (GPU mode), `_run_text_whisper_strategy`
    must clamp `workers` to 1 — and any future GPU-inference path must
    follow the same rule. Multiple concurrent CUDA contexts on this
    machine = scheduled BSOD. Single context is the only safe envelope.

10. **No `-map 0:a?` (optional audio map).** Ever. Use `-map 0:a` hard
    or explicit `-map 0:a:N`. Optional maps silently drop audio.
    Pre-commit hook blocks this pattern.

11. **When a metric doesn't move, the metric is the source of truth,
    not the log.** The metric reflects on-disk reality. If the metric
    is flat or dropping, work isn't happening, regardless of log output.

12. **Never silently substitute an empty result for a corrupt one.**
    A read failure (corrupt JSON, partial truncation, schema mismatch)
    must NOT return an empty placeholder that the next write commits
    back to disk. That's the cascade-of-loss pattern that wiped 8,679
    file entries from media_report.json overnight 2026-04-29 — one bad
    write, then every subsequent reader saw "empty" and every patch
    cycle wrote that emptiness back. Recovery paths: try a rolling
    backup first; if that's also bad, raise loud — never silently substitute.
    Validate shape before writing (a dict with the expected keys) so
    obviously-bad data never reaches disk. See `tools/report_lock.py`
    for the canonical implementation.

13. **Never run two writers on the same shared state file.** Scanner +
    language detection + pipeline encoder all wrote to media_report.json
    concurrently on 2026-04-29 — even with a file lock, the race window
    around `os.replace` exposed corruption to the cascade in rule 12.
    Either coordinate via a single writer or partition the writes by
    file (one report-shard per worker class, merged offline).

14. **Health questions get liveness checks, not aggregate metrics.**
    When the user asks "how's the pipeline going", "anything to be
    concerned about", "is it working", or any variant: ALWAYS run
    both of these BEFORE answering, paste the raw output, and only
    THEN form an opinion:
      a. `tasklist` (Windows) / `ps -ef` (Linux) — is ffmpeg /
         mkvmerge / the supervisor python process actually running
         right now? Empty result with "in flight" rows in state =
         DEAD.
      b. `tail -1 F:/AV1_Staging/pipeline.log` vs `date` — is the
         last log line within the last 2 minutes? Gap > 5 min with
         "in flight" rows = DEAD. (The gap filler heartbeats every
         62s; absence of those for > 2 minutes is conclusive.)

    Aggregate metrics ("X done since restart") are LAGGING — they
    prove past activity, never present. The 2026-05-19 incident: the
    supervisor died at 10:34 silently; row counters kept saying "165
    done", in-flight rows showed `age=460min` (7.5h stale), and I
    answered "no concerns" anyway because I rationalised the stale
    ages as "long 4K HDR encodes" without running either check
    above. The user found out via the dashboard's "stale 7h 52m"
    indicator hours later.

    The rule's bite: even one row with age > 30 min in
    processing/uploading/fetching combined with NO live encoder
    child = the answer is "yes, something is wrong" regardless of
    how many files completed earlier. Aggregate up-counts cannot
    save a dead supervisor.

## Required pre-flight before running anything that touches NAS

1. `ssh nas "uptime; free -m"` — confirm load < 10 and memory > 500MB
   free. If either fails, do NOT run bulk operations.
2. `ssh nas "ps -eo pid,etime,comm | grep mkvmerge | wc -l"` — zero
   zombie mkvmerge processes. If non-zero, investigate first.
3. `uv run python -m tools.invariants` — all CRITICAL + HIGH must pass.
4. Enumerate local python processes. Any pipeline / scanner process
   from a previous session must be reaped before launching new.

## When the user says "why is this not working"

DO NOT:
- Guess at a cause (e.g. "Synology Auto-Block") without verifying
- Restart a process and hope
- Call the symptom "transient"
- Claim X should climb without measuring prior slope

DO:
- Run `uv run python -m tools.invariants` first
- Probe the state DB for files marked DONE with suspicious reasons
- Probe the NAS for zombie / old remote processes
- Re-read the exact function(s) that would do the work

## Audit agent discipline

Agent summaries are not an audit. They are research. Every bug an
agent reports at HIGH or CRITICAL severity MUST be either (a) patched
in the same session or (b) explicitly acknowledged to the user with
an issue link.

Before committing any fix based on agent findings, re-read the
actual file and line being changed. Do not trust the summary.

## When you can't verify something

Say so explicitly. "I have not verified this" / "I cannot check this
from here". Never invent confidence.

---

# Architecture quickref

The four files below get touched constantly. This summary saves a
read on every session — only re-open the file when the line ranges
or invariants below are insufficient. (Generated 2026-05-25 after
the harness flagged 35×Library.jsx + 32×orchestrator.py + 24×__main__.py
+ 15×state.py reads in 7 days.)

## `pipeline/__main__.py` (~574 lines)

CLI entry. `python -m pipeline --resume` boots here.

Top of file (lines 1-30): faulthandler wiring + a defensive
`json.encoder.JSONEncoder.key_separator = ": "` reset (works around
the 5× runtime corruption events from 2026-05-22/23).

Key functions:
- `_build_full_gamut_item(entry)` (line 75) — flattens a media_report
  entry into the queue-item dict shape the workers expect.
- `_stamp_force_reencode(state, filepath, existing, *, reason)`
  (line 114) — stamps `force_reencode=true` so the AV1-source guard
  in `full_gamut.py:689` lets the encode proceed. Called from the
  AV1 branches of `categorise_entry`.
- `categorise_entry(entry, config, state, control, priority_paths=None)`
  (line 149) — returns `("full_gamut", item)`, `("gap_filler", entry)`,
  or `("skip", None)`. Auto-resets `flagged_*` rows on file_mtime
  advance, and auto-resets DONE rows when on-disk codec isn't AV1.
  Priority override fires for AV1 paths in `priority_paths` (also
  stamps force_reencode). Source of policy decisions; ALWAYS re-read
  before changing routing behaviour.
- `_prune_done_from_priority` (line 372) — strips terminal entries
  out of priority.json on each build. Writes atomically with the
  read-back-parse guard pattern.
- `_sort_full_gamut(queue, config, priority_paths)` (line 456) —
  priority bucket smallest-first, the rest by `encode_queue_order`
  (default largest-first).
- `build_queues(report_path, config, state, control)` (line 488) —
  startup queue build. Iterates `report['files']`, calls
  `categorise_entry` per row, sorts. **Does NOT add orphan
  priority paths** that aren't in media_report — only iterates
  report.

## `pipeline/state.py` (~700 lines)

State DB wrapper. SQLite at `F:/AV1_Staging/pipeline_state.db`.

Enum (line 19-60): `FileStatus` — PENDING/QUALIFYING/FETCHING/
PROCESSING/UPLOADING/DONE/ERROR/FLAGGED_FOREIGN_AUDIO/
FLAGGED_UNDETERMINED/FLAGGED_MANUAL/FLAGGED_CORRUPT. **No REPLACED
enum** — "replaced" appears in some raw SQL but isn't in the enum.

Groupings: `TERMINAL_STATUSES` (DONE + all FLAGGED_*),
`ACTIVE_STATUSES` (QUALIFYING/FETCHING/PROCESSING/UPLOADING).

Schema: `pipeline_files` table, `filepath` is PRIMARY KEY. Direct
columns + `extras` JSON column. `idx_files_status` for state filtering.

Key class:
- `PipelineState` (line 247) — `set_file(filepath, status, **kwargs)`
  is the canonical write. Guards inside `set_file`:
  - Rejects DONE with deferred/skipped reason (rule 1 enforcement).
  - On DONE transition, scrubs failure-flavoured reasons via
    `__scrub_stale_reason` sentinel.
  - Read-back-parse on the `extras` JSON before commit (catches the
    `JSONEncoder.key_separator` corruption class).
  - Uses `separators=(",", ": ")` explicitly to survive that
    corruption when it fires.

## `pipeline/orchestrator.py` (~1700 lines)

`Orchestrator` class (line 38). Threading model: GPU worker(s) +
fetch worker + prep worker(s) + upload worker + gap_filler +
refresh worker. Shared state via `_dispatched`, `_gpu_wants_set`,
`_prepping`, `_prep_skip` + dedicated locks.

Worker entry points:
- `_gpu_worker` (616) — picks via `_pick_next_locked`, runs
  `full_gamut(...)` per file.
- `_fetch_worker` (721) — two-pass loop: Priority 1 = whatever
  GPU is blocked on (`_get_gpu_wants`), Priority 2 = pre-fetch
  next queue items. Handles `SOURCE_MISSING` sentinel from
  `fetch_file` via `_remove_missing_source` (line 813) which
  drops the queue entry + flags state flagged_corrupt.
- `_post_fetch` (855) — eager CPU work (language detect, sidecar
  scan) so the GPU worker doesn't pay the cost.
- `_gap_filler_worker` (938) — strip / mux / metadata fixes that
  don't need full re-encode.
- `_pick_next_locked(queue)` (1302) — first picks already-fetched
  items (status=PROCESSING + local file exists), then pending.
  Skips ACTIVE/ERROR/terminal.
- `_upload_worker` (1351) + `_pick_for_upload` (1397).
- `_prep_worker` (1423) — circuit-breaker counts consecutive prep
  crashes per filepath. Three failures → flagged_manual or
  in-memory `_prep_skip`.
- `_refresh_worker` (1581) — periodically re-reads media_report
  (1800s default) and priority.json (10s — `_apply_priority_resort`
  at line 1638).
- `_merge_new_files(full_queue, gap_queue, report_path)` (1675) —
  iterates report, appends new entries to queues. Skips known paths
  and terminal-state rows.

## `frontend/src/pages/dashboard/Library.jsx` (~1800 lines)

The Library browser + Inspector. Reads `data.files` (from
`/api/media-report`) + `data.codecs` + `data.resolutions` +
`pipelineData.files` (state DB summary).

Top-level module exports + predicates:
- `drillFailures` (line 73) — predicate-per-drill-key for the
  Glance drill-ins. `grade_optimal` (121) matches `too_low |
  too_high | unknown` (deliberately excludes `inferred_uncertain`
  to align with the KPI denominator).
- `parseCodecResDrill(key)` (156) — parses `codec:av1` / `res:4k`.
- `_needsEncode(f)` (174) — MODULE SCOPE (was inside the component
  before, TDZ crash 2026-05-22). Predicate for the "Needs encode"
  Status chip: non-AV1 OR AV1 with cur != tgt.

Component (line 185 `export function Library`):
- Filter state at ~167: `{codec, res, hdr, atmos, foreignSubs,
  status, library, hideDone}`.
- `bucketCounts` useMemo (~338): counts files by `cq_audit_bucket`.
- `rows` useMemo (~348): the filtered + sorted list. Filter chain:
  drillFn → cqBucket → query → codec → res → hdr → atmos →
  foreignSubs → status (needs_encode / errored / flagged_corrupt)
  → hideDone → library. Hide set for grade-optimal in-flight
  EXCLUDES "pending" (see commit 6626f40).
- `toggleCodecChip` + `toggleResChip` (~415-422): chip-click clears
  the matching drill so chip + drill don't conflict.
- Bucket chip row at ~625 (grade_optimal drill only).
- Codec / Res / Status chip rows ~720-840.
- Action panel + "Queue re-encode" button ~1700: text becomes
  "Queue encode" for non-AV1 sources.

---

# Cost / model selection

Default to **Sonnet** for routine turns (status checks, dashboard
glance answers, single-edit acknowledgements, "is X done?"
questions, log-grep follow-ups, restart-and-reap dances). Reserve
**Opus** for: multi-file refactors, complex debugging across
modules, designing new abstractions, or anything spanning >3 files.

Background: harness telemetry on 2026-05-25 flagged 437 short Opus
turns in 7 days at ~$40 versus the same workload on Sonnet at ~$8.
Short turns dominate this project (lots of "supervisor died → reap
+ restart" + "checked X, fine" patterns) — those don't need the
extra model capability.

Switch with `/model sonnet` in Claude Code. Stays per-session, so
re-set at session start when working on this repo.
