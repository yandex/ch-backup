"""
Backup restoring context.
"""

import json
from collections import defaultdict
from enum import Enum
from os.path import exists
from typing import Any, Dict, Mapping

from ch_backup.backup.metadata import PartMetadata


class PartState(str, Enum):
    """
    Represents status of the data part, during restore.
    """

    INVALID = "invalid"
    DOWNLOADED = "downloaded"
    RESTORED = "restored"


class RestoreContext:
    """
    Backup restore context. Allows continue restore process after errors.
    """

    def __init__(self, config: Dict):
        self._state_file = config["restore_context_path"]
        self._databases_dict: Dict[str, Dict[str, Dict[str, PartState]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: PartState.INVALID))
        )
        self._failed: Mapping[str, Any] = defaultdict(
            lambda: defaultdict(
                lambda: {
                    "failed_paths": [],
                    "failed_parts": {},
                }
            )
        )

    @property
    def _databases(self) -> Dict[str, Dict[str, Dict[str, PartState]]]:
        """
        Databases property with lazy load.
        """
        if not self._databases_dict:
            if exists(self._state_file):
                self._load_state()
        return self._databases_dict

    @_databases.setter
    def _databases(self, databases: Dict[str, Dict[str, Dict[str, PartState]]]) -> None:
        """
        Databases property setter.
        """
        self._databases_dict = databases

    def add_part(self, state: PartState, part: PartMetadata) -> None:
        """
        Marks that data part was restored.
        """
        self._databases[part.database][part.table][part.name] = state

    def _part(self, part: PartMetadata) -> PartState:
        return self._databases[part.database][part.table][part.name]

    def part_downloaded(self, part: PartMetadata) -> bool:
        """
        Checks if data part was downloaded.
        """
        return self._part(part) == PartState.DOWNLOADED

    def part_restored(self, part: PartMetadata) -> bool:
        """
        Checks if data part was restored.
        """
        return self._part(part) == PartState.RESTORED

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
        # Using _databases property with empty dict might cause loading of the state file
        # So use it here before file is opened
        json_dict = {
            "databases": self._databases,
            "failed": self._failed,
        }
        with open(self._state_file, "w", encoding="utf-8") as f:
            json.dump(
                json_dict,
                f,
            )

    def _load_state(self) -> None:
        with open(self._state_file, "r", encoding="utf-8") as f:
            state: Dict[str, Any] = json.load(f)
            databases: Dict[str, Dict[str, Dict[str, PartState]]] = defaultdict(
                lambda: defaultdict(lambda: defaultdict(lambda: PartState.INVALID))
            )
            for db, tables in state.get("databases", {}).items():
                for table, parts in tables.items():
                    for part_name, part_state in parts.items():
                        databases[db][table][part_name] = part_state
            self._databases = databases
