"""Deprecated alias — use ``python -m tools.maintain relocate``."""
from __future__ import annotations

import sys

from tools.maintain import main

if __name__ == "__main__":
    raise SystemExit(main(["relocate", *sys.argv[1:]]))
