"""
Compressing stage.
"""

import os
from threading import Condition
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

from ch_backup import logging
from ch_backup.backup.metadata.part_metadata import PartMetadata
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.clickhouse.models import Database, FrozenPart, Table
from ch_backup.storage.async_pipeline.base_pipeline.handler import IterableHandler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.storage.async_pipeline.stages.backup.stage_communication import PartPipelineInfo


class UploadPartStage(IterableHandler):
    """ """

    stype = StageType.BACKUP

    def __init__(
        self,
        ch_ctl: Any,
        db: Database,
        table: Table,
        backup_path: str,
        calc_estimated_part_size: Callable,
    ) -> None:
        self.ch_ctl = ch_ctl
        self.db = db
        self.table = table
        self.backup_path = backup_path
        self.calc_estimated_part_size = calc_estimated_part_size

    def __call__(self, value: FrozenPart, index: int) -> Iterable[Tuple[List[str], PartPipelineInfo]]:
        part_metadata = PartMetadata.from_frozen_part(value)
        part_path = self._part_path(value.name)
        remote_path = os.path.join(part_path, value.name + ".tar")
        estimated_size = self.calc_estimated_part_size(value.path, value.files)
        yield (value.files, PartPipelineInfo(part_metadata, self.table.name, value.path, remote_path, estimated_size))

    def _part_path(
        self,
        part_name: str,
        escape_names: bool = True,
    ) -> str:
        """
        Return S3 path to data part.
        """
        if escape_names:
            return os.path.join(
                self.backup_path,
                "data",
                _quote(self.db.name),
                _quote(self.table.name),
                part_name,
            )
        return os.path.join(
            self.backup_path, "data", self.db.name, self.table.name, part_name
        )


def _quote(value: str) -> str:
    return quote(value, safe="").translate(
        {
            ord("."): "%2E",
            ord("-"): "%2D",
        }
    )
