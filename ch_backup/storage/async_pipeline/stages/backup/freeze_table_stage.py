"""
Compressing stage.
"""

import os
from typing import Any, Iterable, Optional, Tuple

from ch_backup import logging
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.models import Database, FrozenPart, Table
from ch_backup.storage.async_pipeline.base_pipeline.handler import InputHandler
from ch_backup.storage.async_pipeline.stages.types import StageType


class FreezeTableStage(InputHandler):
    """
    Freeze table and return it's frozen parts
    """

    stype = StageType.BACKUP

    def __init__(
        self, ch_ctl: Any, db: Database, table: Table, backup_name: str, mtimes
    ) -> None:
        self.ch_ctl = ch_ctl
        self.table = table
        self.db = db
        self.backup_name = backup_name
        self.mtimes = mtimes

    def on_start(self) -> None:
        logging.debug('Trying to freeze "{}"."{}"', self.db.name, self.table.name)
        try:
            self.ch_ctl.freeze_table(self.backup_name, self.table)
        except ClickhouseError:
            if self.ch_ctl.does_table_exist(self.table.database, self.table.name):
                raise

            logging.warning(
                'Table "{}"."{}" was removed by a user during backup',
                self.table.database,
                self.table.name,
            )
            # TODO: error
            raise

    def __call__(self) -> Iterable[Tuple[str, FrozenPart]]:
        # Check if table metadata was updated
        new_mtime = self._get_mtime(self.table.metadata_path)
        if new_mtime is None or self.mtimes[self.table.name].mtime != new_mtime:
            logging.warning(
                'Skipping table backup for "{}"."{}". The metadata file was updated or removed during backup',
                self.table.database,
                self.table.name,
            )
            self.ch_ctl.remove_freezed_data(self.backup_name, self.table)
            # TODO: error
            raise ValueError(1488)

        for data_path, disk in self.table.paths_with_disks:
            for fpart in self.ch_ctl.scan_frozen_parts(
                self.table, disk, data_path, self.backup_name
            ):
                # TODO: add disk or disk type to metadata?
                yield disk.type, fpart

    @staticmethod
    def _get_mtime(file_name: str) -> Optional[float]:
        """
        Fetch last modification time of the file safely.
        """
        try:
            return os.path.getmtime(file_name)
        except OSError as e:
            logging.debug(f"Failed to get mtime of {file_name}: {str(e)}")
            return None
