"""Thin CLI shim for TMDb enrichment. Logic lives in ``pipeline.metadata``.

Kept for backward compatibility with shell scripts and process_manager
entries that invoke ``python -m tools.tmdb``. New callers should import
from ``pipeline.metadata`` directly.
"""

from pipeline.metadata import main

if __name__ == "__main__":
    main()
