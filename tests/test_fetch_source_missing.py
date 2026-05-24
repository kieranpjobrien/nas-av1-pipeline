"""Pin the 2026-05-24 self-cleaning fetch behaviour.

Background: when a queue item's source file isn't on disk (renamed,
deleted, Sonarr restore wiped it), the pre-fix fetch_file would
INSERT an ERROR state row and return None. The picker would then
skip the now-errored row, but the IN-MEMORY queue still held the
item — and the dashboard surfaced the error row indefinitely. On
2026-05-24 this fired 14 times in 60 seconds when supervisor started
with stale dashed-format paths in its queue snapshot.

Post-fix: fetch_file returns a SOURCE_MISSING sentinel (no state
write). The caller (orchestrator's fetch worker) removes the item
from the in-memory queue and stamps the state row flagged_corrupt
with a clear 'source missing' reason. Queue self-cleans across
rename events without needing a supervisor restart.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.state import FileStatus, PipelineState
from pipeline.transfer import SOURCE_MISSING, fetch_file


def _state(tmp_path) -> PipelineState:
    return PipelineState(str(tmp_path / "state.db"))


def _missing_file_item(filepath: str) -> dict:
    return {
        "filepath": filepath,
        "filename": filepath.split("\\")[-1].split("/")[-1],
        "file_size_bytes": 1_000_000_000,
    }


def test_fetch_file_returns_sentinel_when_source_missing(tmp_path):
    """The contract: source absent on disk → return SOURCE_MISSING,
    no state row created. Previous behaviour was state.set_file(ERROR)
    which produced rename-ghost rows in the dashboard."""
    state = _state(tmp_path)
    item = _missing_file_item(r"\\NAS\Series\Severance\Season 2\Severance - S02E03 - Who Is Alive.mkv")
    config = {
        "max_staging_bytes": 1_000_000_000_000,
        "min_free_space_bytes": 100_000_000,
        "max_fetch_buffer_bytes": 100_000_000_000,
    }

    # Patch os.path.exists to return False for the source path (simulating
    # a renamed/deleted file). Also patch time.sleep to skip the 2s
    # re-probe wait.
    with patch("pipeline.transfer.os.path.exists", return_value=False), \
         patch("pipeline.transfer.time.sleep"):
        result = fetch_file(item, str(tmp_path), config, state)

    assert result is SOURCE_MISSING, (
        f"missing source must return SOURCE_MISSING sentinel; got {result!r}"
    )
    # No state row should have been created — the caller's job to flag.
    assert state.get_file(item["filepath"]) is None, (
        "fetch_file must NOT INSERT a state row when source is missing — "
        "that produced the rename-ghost dashboard pollution on 2026-05-24"
    )


def test_remove_missing_source_drops_queue_entry_and_flags(tmp_path):
    """Integration: the orchestrator helper called when SOURCE_MISSING
    is returned. Must (a) drop the item from the in-memory queue, (b)
    clear gpu_wants if pointing at this path, (c) stamp state row
    flagged_corrupt with the 'source missing' reason that the
    auto-reset on mtime advance recognises."""
    from pipeline.control import PipelineControl
    from pipeline.orchestrator import Orchestrator

    state = _state(tmp_path)
    control = PipelineControl(str(tmp_path))
    orch = Orchestrator(
        config={},
        state=state,
        staging_dir=str(tmp_path),
        control=control,
    )

    fp = r"\\NAS\Series\Severance\Season 2\Severance - S02E03 - Who Is Alive.mkv"
    fp_other = r"\\NAS\Series\Severance\Season 2\Severance - S02E04 - Woe's Hollow.mkv"
    queue = [
        {"filepath": fp, "filename": "Severance - S02E03 - Who Is Alive.mkv"},
        {"filepath": fp_other, "filename": "other.mkv"},
    ]
    orch._set_gpu_wants(fp)
    orch._dispatched.add(fp)

    orch._remove_missing_source(queue, fp)

    # Queue entry gone.
    assert all(it["filepath"] != fp for it in queue), (
        f"missing-source item must be removed from queue; queue={queue!r}"
    )
    # Other entry stays.
    assert any(it["filepath"] == fp_other for it in queue)
    # gpu_wants + dispatched cleared.
    assert fp not in orch._get_gpu_wants()
    assert fp not in orch._dispatched
    # State row flagged with the right reason.
    row = state.get_file(fp)
    assert row is not None
    assert row["status"] == FileStatus.FLAGGED_CORRUPT.value
    assert "source missing" in (row.get("reason") or "")
