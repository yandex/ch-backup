"""
Compressing stage.
"""

import os
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

from ch_backup import logging
from ch_backup.backup.metadata.part_metadata import PartMetadata
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.models import Database, FrozenPart, Table
from ch_backup.storage.async_pipeline.base_pipeline.handler import IterableHandler
from ch_backup.storage.async_pipeline.stages.types import StageType


class DeduplicateStage(IterableHandler):
    """ """

    stype = StageType.BACKUP

    def __init__(self, config: Dict, ch_ctl: Any, db: Database, table: Table) -> None:
        self.ch_ctl = ch_ctl
        self.db = db
        self.table = table
        self.frozen_parts_batch: Dict[str, PartMetadata] = {}
        self.dedup_batch_size = config["deduplication_batch_size"]

    def __call__(self, value: Tuple[str, PartMetadata], index: int) -> Optional[Iterable[PartMetadata]]:
        disk_type, part_metadata = value
        if disk_type == "s3":
            yield part_metadata
        else:
            self.frozen_parts_batch[part_metadata.name] = part_metadata
            if len(self.frozen_parts_batch) >= self.dedup_batch_size:
                for part in self.deduplicate_batch():
                    yield part
                self.frozen_parts_batch.clear()

    def on_done(self) -> Optional[Iterable[PartMetadata]]:
        if self.frozen_parts_batch:
            for part in self.deduplicate_batch():
                yield part

    def deduplicate_batch(self): #-> Dict[str, PartMetadata]:
        return self.frozen_parts_batch.values()