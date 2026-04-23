"""Deprecated alias — use ``python -m tools.maintain repair-sidecars``."""
from __future__ import annotations

import sys

from tools.maintain import main

if __name__ == "__main__":
    raise SystemExit(main(["repair-sidecars", *sys.argv[1:]]))
