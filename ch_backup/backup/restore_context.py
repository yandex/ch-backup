"""
Backup restoring context.
"""

import json
from os.path import exists
from typing import Dict, List

from ch_backup.backup.metadata import PartMetadata, TableMetadata


class RestoreContext:
    """
    Backup restore context. Allows continue restore process after errors.
    """
    def __init__(self, config: Dict):
        self._state_file = config['restore_context_path']
        self._databases: Dict[str, Dict[str, List]] = {}
        if exists(self._state_file):
            self._load_state()

    def add_table(self, table: TableMetadata) -> None:
        """
        Add table to restore metadata.
        """
        if table.database not in self._databases:
            self._databases[table.database] = {}

        if table.name not in self._databases[table.database]:
            self._databases[table.database][table.name] = []

    def add_part(self, part: PartMetadata) -> None:
        """
        Marks that data part was restored.
        """
        self._databases[part.database][part.table].append(part.name)

    def part_restored(self, part: PartMetadata) -> bool:
        """
        Check is data part was restored.
        """
        return part.name in self._databases[part.database][part.table]

    def dump_state(self) -> None:
        """
        Dumps restore state to file of disk.
        """
        with open(self._state_file, 'w', encoding='utf-8') as f:
            json.dump({
                'databases': self._databases,
            }, f)

    def _load_state(self) -> None:
        with open(self._state_file, 'r', encoding='utf-8') as f:
            state = json.load(f)
            self._databases = state['databases']
