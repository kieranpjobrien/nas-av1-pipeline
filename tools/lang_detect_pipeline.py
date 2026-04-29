"""Three-phase chained language detection pipeline.

Runs the watchdog'd detect_languages tool in three sequential phases:
  1. text/OCR pass at default 0.85 confidence (high-quality wins only)
  2. whisper audio pass, single-threaded for stability
  3. metadata-soft retry at 0.65 confidence (lets the soft fallback layers
     — sole-audio inference at 0.80 and TMDb at 0.70 — actually persist
     detected_language, not just the method)

Replaces a bash version that kept spawning duplicate processes under
Git Bash's nohup wrapper. Pure Python = one process per phase, period.

Each phase is logged inline. WHISPER_FORCE_CPU=1 is set for the whole
pipeline so we never compete with the GPU encoder for VRAM.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime


def _stamp() -> str:
    return datetime.now().isoformat(timespec="seconds")


def run_phase(name: str, args: list[str], stall_secs: int = 600) -> int:
    """Run one watchdog'd detect_languages phase. Streams output to stdout."""
    cmd = [
        "uv", "run", "python", "-m", "tools.lang_detect_watchdog",
        "--stall-secs", str(stall_secs),
        "--",
        *args,
    ]
    print(f"[{_stamp()}] {name} starting: {' '.join(args)}", flush=True)
    env = os.environ.copy()
    env.setdefault("WHISPER_FORCE_CPU", "1")
    rc = subprocess.run(cmd, env=env).returncode
    print(f"[{_stamp()}] {name} finished rc={rc}", flush=True)
    return rc


def _clear_attempted_but_unresolved() -> int:
    """Clear ``detection_method`` from streams where it's set but ``detected_language``
    is empty. Lets the next pass re-attempt these tracks instead of filtering
    them out as already-handled. Returns total streams cleared.

    Without this, phase 3's softer threshold can't help — enrich_report's
    queue filter skips files whose tracks have any detection_method set,
    even when the prior pass produced no actual language signal.
    """
    from tools.report_lock import patch_report

    cleared = [0]

    def _patch(report: dict) -> None:
        for entry in report.get("files", []) or []:
            for stream_key in ("subtitle_streams", "audio_streams"):
                for s in entry.get(stream_key) or []:
                    method = s.get("detection_method") or ""
                    det = s.get("detected_language")
                    # Clear when method exists but no detected_language was kept.
                    # This catches: text_extraction with low conf, bitmap_no_match,
                    # metadata_fallback (which only sets method when conf < threshold),
                    # whisper_exhausted, heuristic with no result, etc.
                    if method and not det:
                        s.pop("detection_method", None)
                        s.pop("detection_confidence", None)
                        cleared[0] += 1

    patch_report(_patch)
    return cleared[0]


def main() -> int:
    print(f"[{_stamp()}] lang_detect_pipeline starting", flush=True)

    rc1 = run_phase("PHASE 1 (text/OCR @ 0.85)", ["--apply"], stall_secs=600)
    rc2 = run_phase(
        "PHASE 2 (whisper audio, workers=1)",
        ["--whisper", "--apply", "--workers", "1"],
        stall_secs=1200,
    )

    # Pre-phase-3: re-arm the tracks that earlier passes touched but
    # couldn't resolve. Without this, the softer threshold has nothing
    # to bite on — the queue filter would skip them as already-attempted.
    n_cleared = _clear_attempted_but_unresolved()
    print(
        f"[{_stamp()}] pre-phase-3: cleared detection_method on {n_cleared} "
        f"attempted-but-unresolved streams",
        flush=True,
    )

    rc3 = run_phase(
        "PHASE 3 (metadata-soft retry @ 0.65)",
        ["--apply", "--min-confidence", "0.65"],
        stall_secs=600,
    )

    print(
        f"[{_stamp()}] pipeline complete: phase1={rc1} phase2={rc2} phase3={rc3}",
        flush=True,
    )
    return max(rc1, rc2, rc3)


if __name__ == "__main__":
    sys.exit(main())
