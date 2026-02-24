"""Pipeline state management — persistent JSON state that survives crashes."""

import json
import logging
import os
import threading
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
            self.data["last_updated"] = datetime.now().isoformat()
            # Write to temp file then rename for atomicity
            tmp = self.state_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.state_file)

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
