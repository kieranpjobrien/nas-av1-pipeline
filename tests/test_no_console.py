"""Console subprocesses must not open a window when the parent has no console.

The dashboard and watchdog run under pythonw so no console Ctrl+C can reach
them. Windows then gives every console child they spawn its own console - and
with Windows Terminal as the default host, that is a visible terminal window
per ffmpeg/ffprobe/powershell call. The operator was being spammed.

server/ has eight subprocess call sites and only one set creationflags. The one
that got missed (`ffmpeg -version` in routers/admin.py) kept the spam going
through two rounds of "fixed", which is why this is patched centrally.
"""

import subprocess
import sys

import pytest

from pipeline import no_console


@pytest.fixture(autouse=True)
def _restore():
    """Leave Popen exactly as found - other tests spawn real processes."""
    original = subprocess.Popen.__init__
    installed = getattr(subprocess.Popen, "_no_console_installed", False)
    yield
    subprocess.Popen.__init__ = original
    if not installed and hasattr(subprocess.Popen, "_no_console_installed"):
        del subprocess.Popen._no_console_installed


class Recorder:
    """Stands in for the real Popen.__init__ so nothing is actually spawned."""

    def __init__(self):
        self.kwargs = None
        self.args = None

    def __call__(self, inst, *args, **kwargs):
        self.args, self.kwargs = args, kwargs


def _patch(monkeypatch):
    rec = Recorder()
    monkeypatch.setattr(subprocess.Popen, "__init__", rec, raising=False)
    monkeypatch.setattr(subprocess.Popen, "_no_console_installed", False, raising=False)
    monkeypatch.setattr(sys, "platform", "win32")
    no_console.install()
    return rec


class TestFlagIsInjected:
    def test_creationflags_added_when_absent(self, monkeypatch):
        rec = _patch(monkeypatch)
        subprocess.Popen(["ffmpeg", "-version"])
        assert rec.kwargs["creationflags"] & subprocess.CREATE_NO_WINDOW

    def test_existing_creationflags_are_preserved(self, monkeypatch):
        rec = _patch(monkeypatch)
        subprocess.Popen(["x"], creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
        flags = rec.kwargs["creationflags"]
        assert flags & subprocess.CREATE_NEW_PROCESS_GROUP
        assert flags & subprocess.CREATE_NO_WINDOW

    def test_other_kwargs_untouched(self, monkeypatch):
        rec = _patch(monkeypatch)
        subprocess.Popen(["x"], stdout=subprocess.PIPE, text=True, timeout=5)
        assert rec.kwargs["stdout"] is subprocess.PIPE
        assert rec.kwargs["text"] is True
        assert rec.kwargs["timeout"] == 5


class TestSafety:
    def test_install_is_idempotent(self, monkeypatch):
        _patch(monkeypatch)
        first = subprocess.Popen.__init__
        no_console.install()
        assert subprocess.Popen.__init__ is first, "double-wrapped"

    def test_positional_creationflags_caller_is_left_alone(self, monkeypatch):
        """Someone passing it positionally knows what they want."""
        rec = _patch(monkeypatch)
        args = [None] * (no_console._CREATIONFLAGS_POS + 1)
        subprocess.Popen(["x"], *args)
        assert "creationflags" not in (rec.kwargs or {})

    def test_non_windows_is_a_no_op(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setattr(subprocess.Popen, "_no_console_installed", False, raising=False)
        before = subprocess.Popen.__init__
        no_console.install()
        assert subprocess.Popen.__init__ is before


class TestItIsActuallyWiredUp:
    def test_server_installs_it(self):
        src = (__import__("pathlib").Path("server/__init__.py")).read_text(encoding="utf-8")
        assert "no_console" in src and "_install_no_console()" in src

    def test_watchdog_installs_it(self):
        src = (__import__("pathlib").Path("tools/dashboard_watchdog.py")).read_text(encoding="utf-8")
        assert "no_console" in src and "_install_no_console()" in src
