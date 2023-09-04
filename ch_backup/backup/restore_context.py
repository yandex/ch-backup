"""
Backup restoring context.
"""

import json
from collections import defaultdict
from os.path import exists
from typing import Any, Dict, List, Mapping

from ch_backup.backup.metadata import PartMetadata


class RestoreContext:
    """
    Backup restore context. Allows continue restore process after errors.
    """

    def __init__(self, config: Dict):
        self._state_file = config["restore_context_path"]
        self._databases: Dict[str, Dict[str, List]] = {}
        self._failed: Mapping[str, Any] = defaultdict(
            lambda: defaultdict(
                lambda: {
                    "failed_paths": [],
                    "failed_parts": {},
                }
            )
        )
        if exists(self._state_file):
            self._load_state()

    def add_table(self, database: str, table: str) -> None:
        """
        Add table to restore metadata.
        """
        if database not in self._databases:
            self._databases[database] = {}

        if table not in self._databases[database]:
            self._databases[database][table] = []

    def add_part(self, part: PartMetadata) -> None:
        """
        Marks that data part was restored.
        """
        self._databases[part.database][part.table].append(part.name)

    def part_restored(self, part: PartMetadata) -> bool:
        """
        Checks if data part was restored.
        """
        return part.name in self._databases[part.database][part.table]

    def add_failed_chown(self, database: str, table: str, path: str) -> None:
        """
        Save information about failed detached dir chown in context
        """
        self._failed[database][table]["failed_paths"].append(path)

    def add_failed_part(self, part: PartMetadata, e: Exception) -> None:
        """
        Save information about failed to restore part in context
        """
        self._failed[part.database][part.table]["failed_parts"][part.name] = repr(e)

    def has_failed_parts(self) -> bool:
        """
        Returns whether some parts failed during restore.
        """
        return len(self._failed) > 0

    def dump_state(self) -> None:
        """
        Dumps restore state to file of disk.
        """
        with open(self._state_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "databases": self._databases,
                    "failed": self._failed,
                },
                f,
            )

    def _load_state(self) -> None:
        with open(self._state_file, "r", encoding="utf-8") as f:
            state: Dict[str, Any] = json.load(f)
            self._databases = state["databases"]
