"""Stop console subprocesses opening a window when the parent has no console.

Windows decides where a console child draws itself by looking at the PARENT's
console. A GUI-subsystem parent (pythonw.exe) has none, so every console child
it spawns - ffmpeg, ffprobe, mkvmerge, powershell - allocates its OWN, and the
user sees a window appear for each one. With Windows Terminal set as the
default console host, that is a terminal window rather than a brief flash.

The dashboard and the watchdog both run under pythonw deliberately, so that no
console Ctrl+C can reach them (six silent deaths, 2026-10-03..10-07). That
choice is right and stays. This makes it survivable for their subprocesses.

Patching Popen centrally rather than each call site is deliberate: server/ alone
has eight subprocess calls and only one of them set creationflags. The one that
got missed - ``ffmpeg -version`` in routers/admin.py - was enough to keep
spamming windows after two rounds of "fixed". A per-call-site fix is a fix that
has to be remembered every time someone adds a call.

CREATE_NO_WINDOW gives the child a console that is simply never shown, so its
own children inherit it silently too.
"""

from __future__ import annotations

import subprocess
import sys

# Position of `creationflags` in the Popen signature, after self. Guarded so a
# caller passing it positionally is left completely alone.
_CREATIONFLAGS_POS = 13


def install() -> None:
    """Make every subprocess from this interpreter windowless. Idempotent."""
    if sys.platform != "win32":
        return
    if getattr(subprocess.Popen, "_no_console_installed", False):
        return

    flag = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
    original = subprocess.Popen.__init__

    def __init__(self, *args, **kwargs):  # noqa: N807
        if len(args) <= _CREATIONFLAGS_POS:
            kwargs["creationflags"] = kwargs.get("creationflags", 0) | flag
        return original(self, *args, **kwargs)

    subprocess.Popen.__init__ = __init__
    subprocess.Popen._no_console_installed = True
