"""Regression (2026-07-11): managed subprocesses must be launched with UTF-8
stdio. Without it a child's stdout defaults to the Windows ANSI codepage
(cp1252); a single log line containing a non-Latin1 filename character (a macron
'ā', U+0101) then raises UnicodeEncodeError and kills the whole process. reclaim
died exit 1 this way after 8 clean reclaims — the 9th candidate had 'ā' in its
name."""
from unittest.mock import MagicMock, patch

from server.process_manager import ProcessManager


def test_managed_process_launched_with_utf8_stdio():
    pm = ProcessManager()
    fake = MagicMock()
    fake.pid = 4321
    with patch("server.process_manager.subprocess.Popen", return_value=fake) as popen, \
            patch("server.process_manager.threading.Thread"):
        result = pm.start("reclaim")

    assert result["ok"] is True
    kwargs = popen.call_args.kwargs
    assert kwargs.get("encoding") == "utf-8", "parent must decode child stdout as UTF-8"
    assert kwargs.get("errors") == "replace", "a stray byte must not crash the reader thread"
    assert kwargs["env"]["PYTHONIOENCODING"] == "utf-8", "child must be told to encode stdio as UTF-8"
