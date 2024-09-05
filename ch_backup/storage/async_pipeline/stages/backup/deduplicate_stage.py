"""
Compressing stage.
"""

import os
from typing import Any, Dict, Tuple

import ch_backup.storage.async_pipeline.stages.backup.part_queue as part_queue
from ch_backup import logging
from ch_backup.backup.layout_utils import check_data_part
from ch_backup.backup.metadata.part_metadata import PartMetadata
from ch_backup.clickhouse.models import Database, FrozenPart, Table
from ch_backup.encryption import get_encryption
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.stages.types import StageType
from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine
from ch_backup.storage.engine.s3.s3_retry import S3RetryingError


class DeduplicateStage(Handler):
    """
    Deduplicate frozen parts and put them in the queue
    """

    stype = StageType.BACKUP

    def __init__(
        self,
        config: Dict,
        ch_ctl: Any,
        loader: PipeLineCompatibleStorageEngine,
        db: Database,
        table: Table,
    ) -> None:
        self.ch_ctl = ch_ctl
        self.loader = loader
        self.db = db
        self.table = table
        self.frozen_parts_batch: Dict[str, FrozenPart] = {}
        self.dedup_batch_size = config["backup"]["deduplication_batch_size"]
        self._encryption_chunk_size = config["encryption"]["chunk_size"]
        self._encryption_metadata_size = get_encryption(
            config["encryption"]["type"], config["encryption"]
        ).metadata_size()

    def __call__(self, value: Tuple[str, FrozenPart], index: int) -> None:
        disk_type, frozen_part = value
        if disk_type == "s3":
            part_queue.part_metadata_queue.put(
                part_queue.FrozenPartInfo(
                    frozen_part,
                    table=self.table.name,
                    s3_part=True,
                )
            )
        else:
            self.frozen_parts_batch[frozen_part.name] = frozen_part
            if len(self.frozen_parts_batch) >= self.dedup_batch_size:
                deduplicated_parts = self.deduplicate_parts()
                for part_name in self.frozen_parts_batch.keys():
                    part_queue.part_metadata_queue.put(
                        part_queue.FrozenPartInfo(
                            self.frozen_parts_batch[part_name],
                            table=self.table.name,
                            deduplicated_metadata=deduplicated_parts.get(
                                part_name, None
                            ),
                        )
                    )
                self.frozen_parts_batch.clear()

    def on_done(self) -> None:
        if self.frozen_parts_batch:
            deduplicated_parts = self.deduplicate_parts()
            for part_name in self.frozen_parts_batch.keys():
                part_queue.part_metadata_queue.put(
                    part_queue.FrozenPartInfo(
                        self.frozen_parts_batch[part_name],
                        table=self.table.name,
                        deduplicated_metadata=deduplicated_parts.get(part_name, None),
                    )
                )
        part_queue.part_metadata_queue.put(
            part_queue.FrozenPartInfo(
                None, table=self.table.name, all_parts_done=True
            )
        )

    def deduplicate_parts(self) -> Dict[str, PartMetadata]:
        """
        Deduplicate part if it's possible.
        """
        # layout = context.backup_layout

        existing_parts = self.ch_ctl.get_deduplication_info(
            self.db.name, self.table.name, self.frozen_parts_batch
        )
        deduplicated_parts: Dict[str, PartMetadata] = {}

        for existing_part in existing_parts:
            part = PartMetadata(
                database=self.db.name,
                table=self.table.name,
                name=existing_part["name"],
                checksum=existing_part["checksum"],
                size=int(existing_part["size"]),
                link=existing_part["backup_path"],
                files=existing_part["files"],
                tarball=existing_part["tarball"],
                disk_name=existing_part["disk_name"],
            )

            if not existing_part["verified"]:
                if not check_data_part(self.loader, existing_part["backup_path"], part, self._encryption_chunk_size, self._encryption_metadata_size):
                    logging.debug(
                        'Part "{}" found in "{}", but it\'s invalid, skipping',
                        part.name,
                        existing_part["backup_path"],
                    )
                    continue

            deduplicated_parts[part.name] = part

            logging.debug(
                'Part "{}" found in "{}", reusing',
                part.name,
                existing_part["backup_path"],
            )

        return deduplicated_parts
