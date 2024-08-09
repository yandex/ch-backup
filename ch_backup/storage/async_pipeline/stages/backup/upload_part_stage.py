"""
Compressing stage.
"""

from queue import Queue
from typing import Any, Dict, Iterable, Optional, Tuple

from ch_backup import logging
from ch_backup.backup.metadata.part_metadata import PartMetadata
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.clickhouse.models import Database, FrozenPart, Table
from ch_backup.storage.async_pipeline.base_pipeline.handler import IterableHandler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.storage.async_pipeline.stages.backup.queues import part_metadata_queue


class UploadPartStage(IterableHandler):
    """
    """

    stype = StageType.BACKUP

    def __init__(self, ch_ctl: Any, db: Database, table: Table, part_metadata_pipeline_queue: Queue) -> None:
        self.ch_ctl = ch_ctl
        self.db = db
        self.table = table
        self.part_metadata_pipeline_queue = part_metadata_pipeline_queue

    def __call__(self, value: PartMetadata, index: int) -> None:
        self.part_metadata_pipeline_queue.put()