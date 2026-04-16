"""Remote execution on NAS and Media Server via SSH + Docker.

Runs mkvmerge/mkvpropedit/ffprobe on remote machines where the media
files are local (NAS) or NFS-mounted (media server). Avoids SMB
network transfer entirely -- 100x faster than running locally on PC.

Machines:
  NAS: Synology, Docker via sudo, /volume1/Media -> /media
  Media Server: Ubuntu, Docker native, /mnt/nas/media -> /media
"""

import json
import os
import subprocess
from typing import Optional

from paths import NAS_DOCKER_PREFIX, NAS_SSH_HOST, SERVER_DOCKER_PREFIX, SERVER_SSH_HOST

# Machine configs
NAS = {
    "host": NAS_SSH_HOST,
    "docker_prefix": NAS_DOCKER_PREFIX,
    "label": "NAS",
}

SERVER = {
    "host": SERVER_SSH_HOST,
    "docker_prefix": SERVER_DOCKER_PREFIX,
    "label": "SRV",
}


def unc_to_container_path(unc_path: str) -> str:
    """Convert UNC path to container-internal path.

    \\\\KieranNAS\\Media\\Movies\\X.mkv -> /media/Movies/X.mkv
    Works with both Python repr (\\\\) and actual path (\\).
    """
    # Normalise: os.sep on Windows is \, UNC starts with \\
    path = unc_path.replace(os.sep, "/")
    # Strip the UNC server/share prefix
    for prefix in ("//KieranNAS/Media", "//kierannas/Media", "//kierannas/media"):
        if path.startswith(prefix):
            path = "/media" + path[len(prefix) :]
            break
    return path


def _ssh_docker(machine: dict, tool: str, args: list[str], timeout: int = 900) -> subprocess.CompletedProcess:
    """Run a tool inside the persistent mkvworker container via SSH + docker exec.

    Uses a pre-started container (docker exec, not docker run) to avoid
    the 5-7s startup overhead per operation. Container stays running.
    """
    prefix = machine["docker_prefix"]

    # Shell-quote the tool name and every argument individually so the
    # remote shell cannot interpret special characters in any position.
    safe_parts = [_shell_quote(tool)] + [_shell_quote(a) for a in args]
    docker_cmd = f"{prefix} {' '.join(safe_parts)}"

    ssh_cmd = ["ssh", "-o", "ConnectTimeout=10", "-o", "BatchMode=yes", machine["host"], docker_cmd]

    return subprocess.run(
        ssh_cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        encoding="utf-8",
        errors="replace",
    )


def remote_mkvmerge(machine: dict, args: list[str], timeout: int = 900) -> subprocess.CompletedProcess:
    """Run mkvmerge on a remote machine.

    All file paths in args must be container paths (/media/...).
    """
    return _ssh_docker(machine, "mkvmerge", args, timeout)


def remote_mkvpropedit(
    machine: dict, filepath: str, edit_args: list[str], timeout: int = 60
) -> subprocess.CompletedProcess:
    """Run mkvpropedit on a remote machine.

    filepath: container path (/media/...)
    edit_args: e.g. ['--edit', 'track:s1', '--set', 'language=eng']
    """
    args = [filepath] + edit_args
    return _ssh_docker(machine, "mkvpropedit", args, timeout)


def remote_identify(machine: dict, filepath: str, timeout: int = 60) -> Optional[dict]:
    """Run mkvmerge --identify on a remote machine, return parsed JSON."""
    args = ["--identify", "--identification-format", "json", filepath]
    result = _ssh_docker(machine, "mkvmerge", args, timeout)
    if result.returncode <= 1 and result.stdout:
        try:
            return json.loads(result.stdout)
        except (json.JSONDecodeError, TypeError):
            pass
    return None


def remote_strip_and_mux(
    machine: dict,
    input_path: str,
    output_path: str,
    audio_keep_ids: list[int] | None = None,
    sub_keep_ids: list[int] | None = None,
    no_subs: bool = False,
    external_sub_paths: list[tuple[str, str]] | None = None,
    timeout: int = 900,
) -> subprocess.CompletedProcess:
    """Run mkvmerge strip + mux on a remote machine.

    Args:
        input_path: container path to input file
        output_path: container path for output
        audio_keep_ids: absolute track IDs to keep (None = keep all)
        sub_keep_ids: absolute track IDs to keep (None = keep all)
        no_subs: if True, strip all subtitles
        external_sub_paths: list of (container_path, language) for external subs
        timeout: seconds
    """
    args = ["-o", output_path]

    if audio_keep_ids is not None:
        args.extend(["--audio-tracks", ",".join(str(i) for i in audio_keep_ids)])

    if no_subs:
        args.append("--no-subtitles")
    elif sub_keep_ids is not None:
        args.extend(["--subtitle-tracks", ",".join(str(i) for i in sub_keep_ids)])

    args.append(input_path)

    # External subtitle files
    if external_sub_paths:
        for sub_path, lang in external_sub_paths:
            args.extend(["--language", f"0:{lang}", sub_path])

    return remote_mkvmerge(machine, args, timeout)


def _shell_quote(s: str) -> str:
    """Quote a string for shell use over SSH."""
    return "'" + s.replace("'", "'\\''") + "'"
