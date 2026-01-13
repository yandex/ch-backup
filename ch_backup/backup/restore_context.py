"""
Backup restoring context.
"""

import json
from collections import defaultdict
from enum import Enum
from multiprocessing import Lock
from os.path import exists
from typing import Any, Callable, Dict, Mapping

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

    def __init__(self, config: Dict):
        self._state_file = config["restore_context_path"]
        self._state_file_dump_threshold = config[
            "restore_context_sync_on_disk_operation_threshold"
        ]

        self._state_updates_cnt = 0
        self._state_lock = Lock()

        self._databases_dict: Dict[str, Dict[str, Dict[str, PartState]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: PartState.NOT_DOWNLOADED))
        )
        self._failed: Mapping[str, Any] = defaultdict(
            lambda: defaultdict(
                lambda: {
                    "failed_paths": [],
                    "failed_parts": {},
                }
            )
        )

    ### Note: In most of cases we don't need lock for operations, because operations aren't concurrent.
    ### But sometimes we pass updates of the restore context to process pools where the tasks are async.
    ### That's why lock is required.
    def _method_with_lock_decorator_factory(respect_update_counter: bool) -> Any:
        def decorator(function: Callable) -> Any:
            def wrapper(self, *args, **kwargs):
                # pylint: disable=protected-access
                with self._state_lock:
                    result = function(self, *args, **kwargs)
                    if respect_update_counter:
                        # pylint: disable=protected-access
                        self._process_update_counter_locked()
                    return result

            return wrapper

        return decorator

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

    def _part(self, part: PartMetadata) -> PartState:
        return self._databases[part.database][part.table][part.name]

    @_method_with_lock_decorator_factory(True)
    def change_part_state(self, state: PartState, part: PartMetadata) -> None:
        """
        Changes the state of the restoring part.
        """
        self._databases[part.database][part.table][part.name] = state

    @_method_with_lock_decorator_factory(True)
    def add_failed_chown(self, database: str, table: str, path: str) -> None:
        """
        Save information about failed detached dir chown in context
        """
        self._failed[database][table]["failed_paths"].append(path)

    @_method_with_lock_decorator_factory(True)
    def add_failed_part(self, part: PartMetadata, e: Exception) -> None:
        """
        Save information about failed to restore part in context
        """
        self._failed[part.database][part.table]["failed_parts"][part.name] = repr(e)

    @_method_with_lock_decorator_factory(False)
    def has_failed_parts(self) -> bool:
        """
        Returns whether some parts failed during restore.
        """
        return len(self._failed) > 0

    @_method_with_lock_decorator_factory(False)
    def dump_state(self) -> None:
        """
        Dumps restore state to file of disk.
        """
        self._dump_state_locked()

    @_method_with_lock_decorator_factory(False)
    def part_downloaded(self, part: PartMetadata) -> bool:
        """
        Checks if data part was downloaded.
        """
        return self._part(part) == PartState.DOWNLOADED

    @_method_with_lock_decorator_factory(False)
    def part_restored(self, part: PartMetadata) -> bool:
        """
        Checks if data part was restored.
        """
        return self._part(part) == PartState.RESTORED

    def _process_update_counter_locked(self):
        """
        Process update counter. Increment and dump state to disk if needed.
        """
        self._state_updates_cnt += 1
        if self._state_updates_cnt == self._state_file_dump_threshold:
            self._dump_state_locked()
            self._state_updates_cnt = 0

    def _dump_state_locked(self) -> None:
        """
        The Internal realization of dump_state.
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
                lambda: defaultdict(
                    lambda: defaultdict(lambda: PartState.NOT_DOWNLOADED)
                )
            )
            for db, tables in state.get("databases", {}).items():
                for table, parts in tables.items():
                    for part_name, part_state in parts.items():
                        databases[db][table][part_name] = part_state
            self._databases = databases
