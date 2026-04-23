"""Thin CLI shim — logic lives in ``pipeline.language``.

Kept so the existing ``uv run python -m tools.detect_languages ...`` invocations
(server/process_manager.py, scripts, scheduled tasks) keep working unchanged.
Re-exports the safety-critical apply helpers and ``_find_mkvpropedit`` that
external callers (tests, server/admin) import.
"""

from pipeline.language import (  # noqa: F401
    _apply_file_ffmpeg,
    _find_mkvpropedit,
    _probe_stream_counts,
    main,
)

if __name__ == "__main__":
    main()
