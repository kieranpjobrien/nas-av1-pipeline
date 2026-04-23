"""Minimal, idempotent, single-writer NAS media pipeline.

This package replaces the sprawling stitched-together ``pipeline/`` + ``tools/``
modules with a tightly scoped five-file architecture:

* :mod:`naslib.inventory` — SQLite schema and single-writer helpers.
* :mod:`naslib.scan` — walk the NAS, ffprobe, write ``inventory.files`` rows.
* :mod:`naslib.plan` — diff inventory against standards, emit plan rows.
* :mod:`naslib.run` — execute one plan row as a pure, idempotent function.
* :mod:`naslib.verify` — re-probe a file, return a compliance report.

The design is deliberately narrow: no threading primitives, no shared JSON
control files, no background workers. The only mutable state is the SQLite
database and the files on disk, and every MKV-modifying action is guarded by
matched pre- and post-ffprobe invariants so the pipeline cannot silently
destroy audio tracks (as the previous pipeline repeatedly did).
"""

from __future__ import annotations

__all__ = [
    "SCHEMA_VERSION",
]

# Schema version. Bump whenever ``inventory.SCHEMA_SQL`` changes in a way that
# requires migration. The scanner writes this value into every ``files`` row so
# we can recognise stale rows from an older schema on the next scan.
SCHEMA_VERSION: int = 1
