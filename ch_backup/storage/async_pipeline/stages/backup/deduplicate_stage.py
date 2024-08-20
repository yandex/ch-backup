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

import ch_backup.storage.async_pipeline.stages.backup.stage_communication as stage_communication


class DeduplicateStage(IterableHandler):
    """ """

    stype = StageType.BACKUP

    def __init__(self, config: Dict, ch_ctl: Any, db: Database, table: Table) -> None:
        self.ch_ctl = ch_ctl
        self.db = db
        self.table = table
        self.frozen_parts_batch: Dict[str, FrozenPart] = {}
        self.dedup_batch_size = config["deduplication_batch_size"]

    def __call__(
        self, value: Tuple[str, FrozenPart], index: int
    ) -> Optional[Iterable[FrozenPart]]:
        disk_type, frozen_part = value
        if disk_type == "s3":
            stage_communication.part_metadata_queue.put(
                PartMetadata.from_frozen_part(frozen_part)
            )
        else:
            self.frozen_parts_batch[frozen_part.name] = frozen_part
            if len(self.frozen_parts_batch) >= self.dedup_batch_size:
                for fpart in self.deduplicate_batch():
                    yield fpart
                self.frozen_parts_batch.clear()

    def on_done(self) -> Optional[Iterable[PartMetadata]]:
        if self.frozen_parts_batch:
            for fpart in self.deduplicate_batch():
                yield fpart

    def deduplicate_batch(self):  # -> Dict[str, PartMetadata]:
        return self.frozen_parts_batch.values()
