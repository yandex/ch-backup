"""
Backup restoring context.
"""

import json
from collections import defaultdict
from enum import Enum
from os.path import exists
from typing import Any, Callable

from ch_backup.backup.metadata import PartMetadata


class PartState(str, Enum):
    """
    Represents status of the data part, during restore.
    """

    NOT_DOWNLOADED = "not_downloaded"
    INVALID = "invalid"
    DOWNLOADED = "downloaded"
    RESTORED = "restored"


class RestoreContext:
    """
    Backup restore context. Allows continue restore process after errors.
    """

    def __init__(self, config: dict):
        self._state_file = config["restore_context_path"]
        self._state_file_dump_threshold = config["restore_context_sync_threshold_ops"]
        self._state_updates_cnt = 0
        self._state_loaded = False
        # Persisted diagnostics must not affect the current invocation's exit code.
        self._has_failed_parts = False

        self._databases_dict: dict[str, dict[str, dict[str, PartState]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: PartState.NOT_DOWNLOADED))
        )
        self._failed: dict[str, Any] = defaultdict(
            lambda: defaultdict(
                lambda: {
                    "failed_paths": [],
                    "failed_parts": {},
                }
            )
        )

    @staticmethod
    def _dump_state_if_need(function: Callable) -> Any:
        def wrapper(self, *args, **kwargs):
            # pylint: disable=protected-access
            # Load before mutations, including errors recorded before any part
            # state is read. Otherwise loading can overwrite the new diagnostics.
            self._load_state()
            result = function(self, *args, **kwargs)
            self._state_updates_cnt += 1
            if self._state_updates_cnt >= self._state_file_dump_threshold:
                self.dump_state()
                self._state_updates_cnt = 0
            return result

        return wrapper

    @property
    def _databases(self) -> dict[str, dict[str, dict[str, PartState]]]:
        """
        Databases property with lazy load.
        """
        self._load_state()
        return self._databases_dict

    def _part(self, identity: tuple[str, str, str]) -> PartState:
        database, table, name = identity
        return self._databases[database][table][name]

    @_dump_state_if_need
    def change_part_state(self, state: PartState, part: PartMetadata) -> None:
        """
        Changes the state of the restoring part.
        """
        database, table, name = part.identity
        self._databases[database][table][name] = state
        if state == PartState.RESTORED:
            tables = self._failed.get(database, {})
            errors = tables.get(table)
            if errors is not None:
                errors["failed_parts"].pop(name, None)
                if not errors["failed_parts"] and not errors["failed_paths"]:
                    del tables[table]
                    if not tables:
                        del self._failed[database]

    @_dump_state_if_need
    def add_failed_chown(self, database: str, table: str, path: str) -> None:
        """
        Save information about failed detached dir chown in context
        """
        self._failed[database][table]["failed_paths"].append(path)
        self._has_failed_parts = True

    @_dump_state_if_need
    def add_failed_part(self, part: PartMetadata, e: Exception) -> None:
        """
        Save information about failed to restore part in context
        """
        database, table, name = part.identity
        self._failed[database][table]["failed_parts"][name] = repr(e)
        self._has_failed_parts = True

    def has_failed_parts(self) -> bool:
        """
        Returns whether some parts failed during this restore invocation.
        """
        return self._has_failed_parts

    def part_downloaded(self, part: PartMetadata) -> bool:
        """
        Checks if data part was downloaded.
        """
        return self._part(part.identity) == PartState.DOWNLOADED

    def part_restored(self, part: PartMetadata) -> bool:
        """
        Checks if data part was restored.
        """
        return self._part(part.identity) == PartState.RESTORED

    def dump_state(self) -> None:
        """
        Dumps restore state of file to disk.
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
        if self._state_loaded:
            return
        if not exists(self._state_file):
            self._state_loaded = True
            return
        with open(self._state_file, "r", encoding="utf-8") as f:
            state: dict[str, Any] = json.load(f)
            for db, tables in state.get("databases", {}).items():
                for table, parts in tables.items():
                    for part_name, part_state in parts.items():
                        self._databases_dict[db][table][part_name] = part_state
            for db, tables in state.get("failed", {}).items():
                for table, errors in tables.items():
                    self._failed[db][table].update(errors)
        self._state_loaded = True
