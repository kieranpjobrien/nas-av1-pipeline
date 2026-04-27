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

10. **No `-map 0:a?` (optional audio map).** Ever. Use `-map 0:a` hard
    or explicit `-map 0:a:N`. Optional maps silently drop audio.
    Pre-commit hook blocks this pattern.

11. **When a metric doesn't move, the metric is the source of truth,
    not the log.** The metric reflects on-disk reality. If the metric
    is flat or dropping, work isn't happening, regardless of log output.

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
