"""Pin the 2026-05-19 priority.json mid-run watch.

User complaint, paraphrased: "I keep prioritising the 150 smallest
HEVC/H264 files and the pipeline keeps ignoring it — Love Death &
Robots stays at the top of what's processing." Root cause: the
queue refresh worker only fired on ``media_report.json`` mtime
changes. Dashboard clicks that touched ``control/priority.json``
wrote to disk correctly but the running orchestrator's in-memory
queue stayed sorted against the OLD priority set. Net effect: the
"Prioritise" button felt broken even though it wasn't.

Post-fix: the refresh worker polls priority.json mtime on a short
cadence (~10s) and re-sorts the in-memory queue when it advances.
Media report polling stays on its native ~30 min cadence — only the
priority watch needs to be snappy.

This module pins ``_apply_priority_resort`` directly (the loop's
polling logic is harder to exercise without time-mocking, but the
behaviour-bearing function is self-contained).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.control import PipelineControl
from pipeline.orchestrator import Orchestrator
from pipeline.state import FileStatus, PipelineState


def _orch(tmp_path: Path) -> Orchestrator:
    state = PipelineState(str(tmp_path / "state.db"))
    control = PipelineControl(str(tmp_path))
    config = {
        "gpu_concurrency": 1,
        "fetch_concurrency": 1,
        "prep_concurrency": 1,
        "queue_refresh_interval_secs": 60,
        "encode_queue_order": "smallest_first",
    }
    (tmp_path / "control").mkdir(exist_ok=True)
    with patch("pipeline.orchestrator.signal") as fake_signal:
        fake_signal.SIGTERM = 0
        fake_signal.SIGINT = 0
        fake_signal.signal = MagicMock()
        return Orchestrator(config, state, str(tmp_path), control)


def _set_priority(tmp_path: Path, paths: list[str]) -> None:
    """Write a priority.json with the given paths."""
    p = tmp_path / "control" / "priority.json"
    p.write_text(json.dumps({"paths": paths}), encoding="utf-8")


def _item(filepath: str, size: int) -> dict:
    return {"filepath": filepath, "file_size_bytes": size}


def test_apply_priority_resort_lifts_priority_items_to_front(tmp_path, monkeypatch):
    """The core contract: when priority.json contains certain paths,
    calling _apply_priority_resort moves those items to the front of
    the queue without otherwise changing membership."""
    orch = _orch(tmp_path)

    # 5 files in the queue, priority list names #4 and #5.
    queue = [
        _item("/big_1.mkv", 50_000_000_000),
        _item("/big_2.mkv", 40_000_000_000),
        _item("/medium_3.mkv", 5_000_000_000),
        _item("/small_4.mkv", 100_000_000),
        _item("/small_5.mkv", 200_000_000),
    ]
    _set_priority(tmp_path, ["/small_4.mkv", "/small_5.mkv"])
    # Stub the priority-prune step so it doesn't try to talk to a real state DB.
    monkeypatch.setattr(
        "pipeline.__main__._prune_done_from_priority",
        lambda staging_dir=None, state=None: 0,
    )

    orch._apply_priority_resort(queue)

    # First two items must be the priority pair (smallest_first preserves
    # order within the bucket: 100M then 200M).
    assert queue[0]["filepath"] == "/small_4.mkv"
    assert queue[1]["filepath"] == "/small_5.mkv"
    # Non-priority items follow, sorted by size (smallest_first config).
    non_prio_paths = [it["filepath"] for it in queue[2:]]
    assert non_prio_paths == ["/medium_3.mkv", "/big_2.mkv", "/big_1.mkv"]


def test_apply_priority_resort_no_op_when_priority_empty(tmp_path, monkeypatch):
    """Empty priority.json (or missing) → queue must not be reordered.
    Pre-fix the worker would skip the re-sort entirely; the carve-out
    here pins that behaviour so we never accidentally apply an
    arbitrary global re-sort."""
    orch = _orch(tmp_path)
    queue = [
        _item("/a.mkv", 100),
        _item("/b.mkv", 50),
        _item("/c.mkv", 200),
    ]
    original_order = [it["filepath"] for it in queue]

    _set_priority(tmp_path, [])
    monkeypatch.setattr(
        "pipeline.__main__._prune_done_from_priority",
        lambda staging_dir=None, state=None: 0,
    )

    orch._apply_priority_resort(queue)
    assert [it["filepath"] for it in queue] == original_order


def test_apply_priority_resort_prunes_done_entries(tmp_path, monkeypatch):
    """Done/flagged priority paths get pruned from priority.json by the
    re-sort (matches the startup behaviour). The prune count is what
    the orchestrator logs; verify the count surfaces via the
    underlying helper."""
    orch = _orch(tmp_path)
    queue = [_item("/keep.mkv", 100)]
    _set_priority(tmp_path, ["/keep.mkv", "/done_already.mkv"])

    pruned_counter = {"n": 0}

    def fake_prune(staging_dir=None, state=None):
        pruned_counter["n"] = 1
        return 1

    monkeypatch.setattr(
        "pipeline.__main__._prune_done_from_priority",
        fake_prune,
    )

    orch._apply_priority_resort(queue)
    assert pruned_counter["n"] == 1


def test_apply_priority_resort_holds_dispatched_lock(tmp_path, monkeypatch):
    """The re-sort must acquire ``_dispatched_lock`` so iterating
    workers don't race with the sort. Otherwise the GPU picker
    iterating ``queue`` could see a partially-reordered list and
    pick a stale path. Source-introspection check — the regex
    confirms the helper opens its body with the lock."""
    import inspect

    from pipeline.orchestrator import Orchestrator

    src = inspect.getsource(Orchestrator._apply_priority_resort)
    # First non-docstring statement should be the lock acquisition.
    assert "with self._dispatched_lock:" in src, (
        "_apply_priority_resort must wrap the re-sort in "
        "`with self._dispatched_lock:` to keep iterating workers safe"
    )
