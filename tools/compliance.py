"""Deprecated alias — use ``python -m tools.maintain audit``.

Historical CLI preserved: the legacy ``--queue reencode`` flag is forwarded
unchanged to the audit subcommand, which handles writing to
``control/reencode.json`` itself.

The per-file :func:`check_file` function is re-exported so callers that
imported it (tests, dashboard routers) keep working unchanged.
"""
from __future__ import annotations

import sys

from tools.maintain import check_file, main  # re-exported

__all__ = ["check_file", "main"]


if __name__ == "__main__":
    raise SystemExit(main(["audit", *sys.argv[1:]]))
