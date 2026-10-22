"""Managed children must never be launched with a windowless interpreter.

2026-10-23: the dashboard moved to pythonw.exe so no console Ctrl+C could reach
it. That made sys.executable pythonw.exe, and every managed child inherited it.
pythonw is GUI-subsystem and has NO console, so CREATE_NO_WINDOW buys nothing -
every console grandchild (ffprobe/ffmpeg/mkvmerge, thousands per scan) had to
allocate its own console, each flashing a window. Roughly five popups a second.
"""

import os
import subprocess
import sys

import server.process_manager as pm


class TestConsolePython:
    def test_pythonw_is_mapped_to_python(self, tmp_path, monkeypatch):
        (tmp_path / "python.exe").write_bytes(b"")
        pw = tmp_path / "pythonw.exe"
        pw.write_bytes(b"")
        monkeypatch.setattr(sys, "executable", str(pw))
        monkeypatch.setattr(sys, "platform", "win32")
        assert pm._console_python() == str(tmp_path / "python.exe")

    def test_versioned_pythonw_is_mapped(self, tmp_path, monkeypatch):
        (tmp_path / "python3.13.exe").write_bytes(b"")
        pw = tmp_path / "pythonw3.13.exe"
        pw.write_bytes(b"")
        monkeypatch.setattr(sys, "executable", str(pw))
        monkeypatch.setattr(sys, "platform", "win32")
        assert pm._console_python() == str(tmp_path / "python3.13.exe")

    def test_plain_python_is_left_alone(self, tmp_path, monkeypatch):
        py = tmp_path / "python.exe"
        py.write_bytes(b"")
        monkeypatch.setattr(sys, "executable", str(py))
        monkeypatch.setattr(sys, "platform", "win32")
        assert pm._console_python() == str(py)

    def test_falls_back_when_no_console_python_exists(self, tmp_path, monkeypatch):
        """Better the windowless interpreter than a path that cannot run."""
        pw = tmp_path / "pythonw.exe"
        pw.write_bytes(b"")
        monkeypatch.setattr(sys, "executable", str(pw))
        monkeypatch.setattr(sys, "platform", "win32")
        assert pm._console_python() == str(pw)

    def test_non_windows_is_untouched(self, monkeypatch):
        monkeypatch.setattr(sys, "executable", "/usr/bin/python3")
        monkeypatch.setattr(sys, "platform", "linux")
        assert pm._console_python() == "/usr/bin/python3"


class TestProcessConfigs:
    def test_no_config_uses_a_windowless_interpreter(self):
        """The actual bug: PROCESS_CONFIGS captured sys.executable at import."""
        for name, cfg in pm.PROCESS_CONFIGS.items():
            for arg in cfg["cmd"]:
                assert "pythonw" not in str(arg).lower(), f"{name} would spawn via pythonw"

    def test_every_config_has_a_command_and_cwd(self):
        for name, cfg in pm.PROCESS_CONFIGS.items():
            assert cfg.get("cmd"), f"{name} has no cmd"
            assert cfg.get("cwd"), f"{name} has no cwd"

    def test_create_no_window_is_still_requested(self):
        """It only works for a console-subsystem parent - but it must stay."""
        import inspect

        src = inspect.getsource(pm.ProcessManager.start)
        assert "CREATE_NO_WINDOW" in src
        assert "CREATE_NEW_PROCESS_GROUP" in src

    def test_windows_flags_exist(self):
        if sys.platform == "win32":
            assert hasattr(subprocess, "CREATE_NO_WINDOW")

    def test_resolved_py_is_a_real_path_on_this_machine(self):
        assert os.path.exists(pm.PY), f"resolved interpreter missing: {pm.PY}"
