"""File-based control system for real-time pipeline management.

Monitors a control directory for JSON command files. Supports both canonical
names and friendly aliases — drop whichever is easier:

Pause:    pause.json, pause_all.json, pause_fetch.json, pause_encode.json, or just PAUSE
Gentle:   gentle.json
Skip:     skip.json
Priority: priority.json
"""

import fnmatch
import json
import logging
import os
import time
from typing import Optional


class PipelineControl:
    """File-based control system for real-time pipeline management."""

    # Friendly filename → (canonical name, implicit data to merge)
    _ALIASES = {
        "pause_all.json":    ("pause.json", {"type": "all"}),
        "pause_fetch.json":  ("pause.json", {"type": "fetch_only"}),
        "pause_encode.json": ("pause.json", {"type": "encode_only"}),
    }

    # Files that should always exist in control/ (edited in-place, not deleted)
    _PERSISTENT_FILES = {
        "gentle.json": {
            "paths": {},
            "patterns": {},
            "default_offset": 0,
        },
        "skip.json": {
            "paths": [],
        },
        "priority.json": {
            "paths": [],
        },
    }

    def __init__(self, staging_dir: str):
        self.control_dir = os.path.join(staging_dir, "control")
        os.makedirs(self.control_dir, exist_ok=True)
        self._last_read = {}
        self._seed_persistent_files()

    def _seed_persistent_files(self):
        """Create persistent control files with empty defaults if they don't exist yet."""
        for name, default_data in self._PERSISTENT_FILES.items():
            path = os.path.join(self.control_dir, name)
            if not os.path.exists(path):
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(default_data, f, indent=4, ensure_ascii=False)
                logging.info(f"Created control/{name} (edit to configure)")

    def _find_control_file(self, canonical_name: str) -> Optional[tuple[str, dict]]:
        """Find a control file by canonical name or any alias.

        Returns (filepath, implicit_data) or None. Aliases carry implicit data
        so the file can be empty or minimal.
        """
        # Check canonical name first
        path = os.path.join(self.control_dir, canonical_name)
        if os.path.exists(path):
            return (path, {})

        # Check aliases
        for alias, (canon, implicit) in self._ALIASES.items():
            if canon == canonical_name:
                alias_path = os.path.join(self.control_dir, alias)
                if os.path.exists(alias_path):
                    return (alias_path, implicit)

        return None

    def _read_control_file(self, canonical_name: str) -> Optional[dict]:
        """Read and parse a control JSON file (or alias). Returns None if missing."""
        found = self._find_control_file(canonical_name)
        if not found:
            return None

        path, implicit_data = found
        try:
            mtime = os.path.getmtime(path)
            cache_key = path
            if self._last_read.get(cache_key, {}).get("mtime") == mtime:
                return self._last_read[cache_key].get("data")

            # Allow empty files — just the presence is enough
            size = os.path.getsize(path)
            if size == 0:
                data = {}
            else:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)

            # Merge implicit data from alias (alias values take priority for type fields)
            merged = {**data, **implicit_data}
            self._last_read[cache_key] = {"mtime": mtime, "data": merged}
            return merged
        except (json.JSONDecodeError, OSError) as e:
            logging.warning(f"Control file {os.path.basename(path)} unreadable: {e}")
            # If the alias carries implicit data, still honour it (file might just be empty/malformed)
            if implicit_data:
                return implicit_data
            return None

    def _any_pause_file_exists(self) -> bool:
        """Check if any pause-related file exists (including simple PAUSE)."""
        simple = os.path.exists(os.path.join(os.path.dirname(self.control_dir), "PAUSE"))
        return simple or self._find_control_file("pause.json") is not None

    def _get_pause_type(self) -> Optional[str]:
        """Get the current pause type, or None if not paused."""
        if os.path.exists(os.path.join(os.path.dirname(self.control_dir), "PAUSE")):
            return "all"
        pause = self._read_control_file("pause.json")
        if pause is None:
            return None
        return pause.get("type", "all")

    def check_pause(self, shutdown_flag: callable) -> None:
        """Block if any pause file exists. Delete the file to resume."""
        pause_type = self._get_pause_type()
        if pause_type is None:
            return

        logging.info(f"PAUSED ({pause_type}). Delete the pause file from control/ to resume.")
        while not shutdown_flag():
            if self._get_pause_type() is None:
                break
            time.sleep(5)
        if not shutdown_flag():
            logging.info("Resumed.")

    def is_fetch_paused(self) -> bool:
        """Check if fetching specifically is paused."""
        pt = self._get_pause_type()
        return pt in ("all", "fetch_only")

    def is_encode_paused(self) -> bool:
        """Check if encoding specifically is paused."""
        pt = self._get_pause_type()
        return pt in ("all", "encode_only")

    def should_skip(self, filepath: str) -> bool:
        """Check if a file is in the skip list."""
        skip = self._read_control_file("skip.json")
        if not skip:
            return False
        skip_paths = skip.get("paths", [])
        # Normalize for comparison
        norm = os.path.normpath(filepath).lower()
        return any(os.path.normpath(p).lower() == norm for p in skip_paths)

    def get_gentle_override(self, filepath: str) -> Optional[dict]:
        """Get CQ/preset overrides for a specific file.

        gentle.json format:
        {
            "paths": {
                "Z:\\Movies\\Interstellar\\...mkv": {"cq_offset": 4, "preset": "p4"},
            },
            "default_offset": 0
        }
        Also supports pattern-based matching:
        {
            "patterns": {
                "*Interstellar*": {"cq_offset": 4, "preset": "p7"},
                "*anime*": {"cq_offset": -2}
            }
        }
        """
        gentle = self._read_control_file("gentle.json")
        if not gentle:
            return None

        norm = os.path.normpath(filepath).lower()

        # Exact path match
        paths = gentle.get("paths", {})
        for p, overrides in paths.items():
            if os.path.normpath(p).lower() == norm:
                return overrides

        # Pattern match (simple glob-style)
        patterns = gentle.get("patterns", {})
        for pattern, overrides in patterns.items():
            if fnmatch.fnmatch(norm, pattern.lower()):
                return overrides

        # Default offset
        default_offset = gentle.get("default_offset", 0)
        if default_offset != 0:
            return {"cq_offset": default_offset}

        return None

    def get_priority_bumps(self) -> list[str]:
        """Get list of filepaths that should be bumped to front of queue."""
        priority = self._read_control_file("priority.json")
        if not priority:
            return []
        return priority.get("paths", [])

    def apply_queue_overrides(self, queue: list[dict]) -> list[dict]:
        """Apply skip and priority overrides to the queue."""
        # Remove skipped files
        filtered = [item for item in queue if not self.should_skip(item["filepath"])]

        # Bump priority files to front
        bumps = self.get_priority_bumps()
        if bumps:
            bump_set = {os.path.normpath(p).lower() for p in bumps}
            priority_items = []
            rest = []
            for item in filtered:
                if os.path.normpath(item["filepath"]).lower() in bump_set:
                    priority_items.append(item)
                else:
                    rest.append(item)
            if priority_items:
                logging.info(f"Priority bumped {len(priority_items)} files to front of queue")
            filtered = priority_items + rest

        return filtered
