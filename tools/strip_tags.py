"""Deprecated alias — use ``python -m tools.maintain clean-names``.

Forwards to the unified maintain CLI. Historical behaviour preserved:
default scans series only; ``--movies`` expands to both.
"""
from __future__ import annotations

import sys

from tools.maintain import main

if __name__ == "__main__":
    raise SystemExit(main(["clean-names", *sys.argv[1:]]))
