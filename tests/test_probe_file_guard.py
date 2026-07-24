"""Regression: probe_file must return None (not crash) when ffprobe reports
success (rc=0) but yields no JSON.

The bare ``json.loads(result.stdout)`` raised ``TypeError: the JSON object must
be str, bytes or bytearray, not NoneType`` 11x in a single session (2026-07-24),
propagating out of quick_worker -> update_entry -> probe_file. update_entry
already handles a None probe by skipping (never writing an empty entry, rule
12), so returning None is the correct, safe behaviour.
"""

import pipeline.report as report


def _fake_run(returncode, stdout):
    class _R:
        pass

    r = _R()
    r.returncode = returncode
    r.stdout = stdout
    return lambda *a, **k: r


def test_probe_file_none_stdout_returns_none(monkeypatch):
    # rc=0 but stdout is None (the crash case) -> None, no TypeError.
    monkeypatch.setattr(report.subprocess, "run", _fake_run(0, None))
    assert report.probe_file("whatever.mkv") is None


def test_probe_file_empty_stdout_returns_none(monkeypatch):
    monkeypatch.setattr(report.subprocess, "run", _fake_run(0, ""))
    assert report.probe_file("whatever.mkv") is None


def test_probe_file_nonzero_returncode_returns_none(monkeypatch):
    monkeypatch.setattr(report.subprocess, "run", _fake_run(1, '{"streams": []}'))
    assert report.probe_file("whatever.mkv") is None


def test_probe_file_valid_json_parses(monkeypatch):
    monkeypatch.setattr(report.subprocess, "run", _fake_run(0, '{"streams": [], "format": {}}'))
    assert report.probe_file("whatever.mkv") == {"streams": [], "format": {}}
