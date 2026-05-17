"""Pin the 2026-05-18 pause semantics: pause = GPU only.

User-facing model: clicking the "Pause" button on the dashboard sets
``control/pause_all.json`` (type ``"all"``). The intent is to free the
RTX 4080 for gaming, NOT to halt the whole pipeline. Fetch / prep /
upload should keep running in the background so the moment the user
resumes, the GPU has a stack of already-prepped files ready to encode.

Pre-2026-05-18 ``is_fetch_paused()`` returned True for both "all" and
"fetch_only" — so clicking Pause halted fetch + prep work too, leaving
the pipeline cold-start every resume. Post-fix: "all" pauses ONLY
encode. "fetch_only" is still available as an explicit type for the
rare case of needing to halt SMB activity (e.g. NAS maintenance).

The pause-type table after the change:

  | Type            | Encode  | Fetch  |
  | --------------- | ------- | ------ |
  | (none)          | run     | run    |
  | "all"           | PAUSED  | run    |  ← the dashboard button
  | "encode_only"   | PAUSED  | run    |
  | "fetch_only"    | run     | PAUSED |
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.control import PipelineControl


def _make_control(tmp_path: Path, pause_payload: dict | None = None) -> PipelineControl:
    """Create a fresh PipelineControl with an optional pause flag.
    The control dir is ``tmp_path / "control"``."""
    control_dir = tmp_path / "control"
    control_dir.mkdir(exist_ok=True)
    if pause_payload is not None:
        (control_dir / "pause.json").write_text(json.dumps(pause_payload))
    return PipelineControl(str(tmp_path))


def test_no_pause_neither_blocked(tmp_path):
    c = _make_control(tmp_path)
    assert c.is_encode_paused() is False
    assert c.is_fetch_paused() is False


def test_pause_all_blocks_encode_only(tmp_path):
    """The user's primary case: dashboard Pause button → pause_all.
    Encode blocks. Fetch must NOT block — that's the whole point of
    the 2026-05-18 change."""
    c = _make_control(tmp_path, {"type": "all"})
    assert c.is_encode_paused() is True
    assert c.is_fetch_paused() is False, (
        "pause_all must NOT halt fetch — prep/fetch/upload need to keep "
        "draining so a resume has prepped files ready for the GPU"
    )


def test_pause_all_via_alias_file(tmp_path):
    """The pause_all.json alias (legacy filename) must resolve to the
    same {"type": "all"} payload and hit the encode-only behaviour."""
    control_dir = tmp_path / "control"
    control_dir.mkdir(exist_ok=True)
    (control_dir / "pause_all.json").write_text("{}")
    c = PipelineControl(str(tmp_path))
    assert c.is_encode_paused() is True
    assert c.is_fetch_paused() is False


def test_pause_encode_only_blocks_encode_only(tmp_path):
    c = _make_control(tmp_path, {"type": "encode_only"})
    assert c.is_encode_paused() is True
    assert c.is_fetch_paused() is False


def test_pause_fetch_only_blocks_fetch_only(tmp_path):
    """Explicit "fetch_only" type — the rare NAS-maintenance case.
    Halts SMB activity, leaves the GPU free to drain existing
    prepped files."""
    c = _make_control(tmp_path, {"type": "fetch_only"})
    assert c.is_encode_paused() is False
    assert c.is_fetch_paused() is True


def test_pause_flag_file_at_repo_root(tmp_path):
    """The legacy ``PAUSE`` flag (no JSON, just a file) implies type "all"
    — same encode-only semantics as the alias."""
    (tmp_path / "PAUSE").write_text("")
    (tmp_path / "control").mkdir(exist_ok=True)
    c = PipelineControl(str(tmp_path))
    assert c.is_encode_paused() is True
    assert c.is_fetch_paused() is False
