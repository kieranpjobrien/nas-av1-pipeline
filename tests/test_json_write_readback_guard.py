"""Pin the 2026-05-18 read-back-parse guards on JSON writers.

That morning's incident silently corrupted four JSON files
simultaneously — media_report.json, priority.json,
agents.registry.json, heavy_worker_state.json. The JSON key/value
separator ``: `` was replaced with arbitrary words (``utf-8``, ``frame``,
``search``) consistent within each file. Source unconfirmed (no
in-tree ``json.dump(..., separators=...)`` call exists); the most
plausible cause is an external editor's botched find-and-replace.

The in-memory shape check at the top of ``_atomic_write_with_backup``
didn't catch it because the corruption only manifested AFTER the bytes
hit disk. Defense-in-depth: re-open the just-written .tmp and parse
it. If parse fails, abort the os.replace and raise; the destination +
.last_good stay intact.

These tests pin the contract for all four writers we touched:

  * tools.report_lock._atomic_write_with_backup → raises ReportCorruptError
  * pipeline.process_registry._write_entries → raises OSError
  * server.helpers.write_json_safe → raises OSError

The orchestrator's heavy_worker_state path swallows the malformed-JSON
case at log-error level (it's a status file, not a critical path).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest


def test_report_lock_rejects_corrupt_writeback(tmp_path, monkeypatch):
    """Force ``json.dump`` to write garbage, verify
    ``_atomic_write_with_backup`` refuses to replace the destination."""
    import tools.report_lock as rl

    target = tmp_path / "media_report.json"
    # Seed a healthy baseline so we can prove it wasn't overwritten.
    healthy = {"files": [{"filepath": "/baseline"}]}
    target.write_text(json.dumps(healthy, indent=2))

    # Make json.dump write the corruption pattern instead of valid JSON.
    real_dump = json.dump

    def corrupt_dump(obj, fp, **kw):
        # Mimic the 2026-05-18 corruption: separators replaced with `utf-8`.
        fp.write('{"files"utf-8[{"filepath"utf-8"/garbage"}]}')

    monkeypatch.setattr("tools.report_lock.json.dump", corrupt_dump)

    with pytest.raises(rl.ReportCorruptError, match="malformed JSON"):
        rl._atomic_write_with_backup(target, {"files": [{"filepath": "/new"}]})

    # Destination must be the original baseline, unchanged.
    parsed = json.loads(target.read_text())
    assert parsed == healthy, "corrupt write must not overwrite the destination"
    # .tmp must be cleaned up.
    assert not (tmp_path / "media_report.json.tmp").exists()


def test_report_lock_happy_path_unaffected(tmp_path):
    """Sanity: normal writes still succeed."""
    import tools.report_lock as rl

    target = tmp_path / "media_report.json"
    payload = {"files": [{"filepath": "/x", "filename": "x.mkv"}], "summary": {}}
    rl._atomic_write_with_backup(target, payload)

    parsed = json.loads(target.read_text(encoding="utf-8"))
    assert parsed["files"][0]["filepath"] == "/x"


def test_process_registry_rejects_corrupt_writeback(tmp_path, monkeypatch):
    """``_write_entries`` raises and leaves no .tmp behind when the
    just-written bytes don't parse."""
    from pipeline import process_registry as pr

    target = tmp_path / "agents.registry.json"

    def corrupt_dump(obj, fp, **kw):
        fp.write('[{"role"utf-8"pipeline"}]')

    monkeypatch.setattr("pipeline.process_registry.json.dump", corrupt_dump)

    with pytest.raises(OSError, match="malformed JSON"):
        pr._write_entries(target, [{"role": "pipeline"}])

    assert not target.exists(), "destination must not be created"
    assert not (tmp_path / "agents.registry.json.tmp").exists()


def test_write_json_safe_rejects_corrupt_writeback(tmp_path, monkeypatch):
    """``server.helpers.write_json_safe`` raises and leaves the
    destination untouched when read-back parse fails."""
    from server import helpers

    target = tmp_path / "thing.json"
    target.write_text('{"baseline": true}')

    real_dumps = json.dumps

    def corrupt_dumps(obj, **kw):
        return '{"baseline"utf-8true}'

    monkeypatch.setattr("server.helpers.json.dumps", corrupt_dumps)

    with pytest.raises(OSError, match="malformed JSON"):
        helpers.write_json_safe(target, {"baseline": False})

    # Original survived.
    assert json.loads(target.read_text())["baseline"] is True
    # Tmp gone.
    assert not target.with_suffix(".tmp").exists()
