"""Pipeline state management — persistent JSON state that survives crashes."""

import json
import logging
import os
import threading
import time
from datetime import datetime
from enum import Enum
from typing import Optional


class FileStatus(str, Enum):
    PENDING = "pending"
    FETCHING = "fetching"
    FETCHED = "fetched"
    ENCODING = "encoding"
    ENCODED = "encoded"
    UPLOADING = "uploading"
    UPLOADED = "uploaded"
    VERIFIED = "verified"
    REPLACING = "replacing"
    REPLACED = "replaced"
    SKIPPED = "skipped"
    ERROR = "error"


class PipelineState:
    """Persistent state tracker — survives Ctrl+C and resumes."""

    def __init__(self, state_file: str):
        self.state_file = state_file
        self._lock = threading.RLock()
        self.data = {
            "created": datetime.now().isoformat(),
            "last_updated": None,
            "config": {},
            "stats": {
                "total_files": 0,
                "completed": 0,
                "skipped": 0,
                "errors": 0,
                "bytes_saved": 0,
                "total_encode_time_secs": 0,
            },
            "files": {},  # keyed by source filepath
        }
        self._load()

    def _load(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, "r", encoding="utf-8") as f:
                self.data = json.load(f)
            logging.info(f"Loaded state: {len(self.data['files'])} files tracked")

    def save(self):
        with self._lock:
            # Merge external changes (e.g. server's reset-errors endpoint)
            # before saving. If the file on disk has entries we modified
            # externally (error→pending), adopt those changes.
            self._merge_external_resets()
            self.data["last_updated"] = datetime.now().isoformat()
            tmp = self.state_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
            for attempt in range(5):
                try:
                    os.replace(tmp, self.state_file)
                    return
                except PermissionError:
                    if attempt < 4:
                        time.sleep(0.1 * (attempt + 1))
                    else:
                        raise

    def _merge_external_resets(self):
        """Check if the state file on disk has items reset from error→pending
        by the server's reset-errors endpoint, and adopt those changes."""
        if not os.path.exists(self.state_file):
            return
        try:
            mtime = os.path.getmtime(self.state_file)
            # Only check if file was modified externally (not by us)
            if not hasattr(self, '_last_save_mtime') or mtime <= self._last_save_mtime:
                return
            with open(self.state_file, "r", encoding="utf-8") as f:
                disk_data = json.load(f)
            for fp, disk_info in disk_data.get("files", {}).items():
                mem_info = self.data["files"].get(fp)
                if mem_info and mem_info.get("status") == "error" and disk_info.get("status") == "pending":
                    mem_info["status"] = "pending"
                    mem_info.pop("error", None)
                    mem_info.pop("stage", None)
                    mem_info["last_updated"] = disk_info.get("last_updated", "")
        except Exception:
            pass  # don't break saves if merge fails
        finally:
            self._last_save_mtime = time.time()

    def get_file(self, filepath: str) -> Optional[dict]:
        with self._lock:
            entry = self.data["files"].get(filepath)
            return dict(entry) if entry else None

    def set_file(self, filepath: str, status: FileStatus, **kwargs):
        with self._lock:
            if filepath not in self.data["files"]:
                self.data["files"][filepath] = {
                    "status": status.value,
                    "added": datetime.now().isoformat(),
                }
            entry = self.data["files"][filepath]
            entry["status"] = status.value
            entry["last_updated"] = datetime.now().isoformat()
            entry.update(kwargs)
        # Save after every status change (save has its own lock)
        self.save()

    def get_files_by_status(self, status: FileStatus) -> list[str]:
        with self._lock:
            return [fp for fp, info in self.data["files"].items() if info["status"] == status.value]

    @property
    def stats(self):
        return self.data["stats"]

    def compact(self) -> int:
        """Remove REPLACED and SKIPPED entries from state to reduce file size.

        Stats are already tracked separately in stats dict and encode_history.jsonl,
        so these entries are no longer needed.
        """
        with self._lock:
            terminal = {FileStatus.REPLACED.value, FileStatus.SKIPPED.value}
            to_remove = [fp for fp, info in self.data["files"].items()
                         if info.get("status") in terminal]
            for fp in to_remove:
                del self.data["files"][fp]

            if to_remove:
                self.data["stats"]["archived_count"] = (
                    self.data["stats"].get("archived_count", 0) + len(to_remove)
                )
                logging.info(f"Compacted state: removed {len(to_remove)} terminal entries "
                             f"({len(self.data['files'])} remaining)")

        if to_remove:
            self.save()
        return len(to_remove)
