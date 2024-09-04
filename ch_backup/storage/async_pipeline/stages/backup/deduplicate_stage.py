"""
Compressing stage.
"""

import os
from typing import Any, Callable, Dict, Tuple

import ch_backup.storage.async_pipeline.stages.backup.stage_communication as stage_communication
from ch_backup import logging
from ch_backup.backup.layout_utils import part_path, target_part_size
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
            stage_communication.part_metadata_queue.put(
                stage_communication.FrozenPartInfo(
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
                    stage_communication.part_metadata_queue.put(
                        stage_communication.FrozenPartInfo(
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
                stage_communication.part_metadata_queue.put(
                    stage_communication.FrozenPartInfo(
                        self.frozen_parts_batch[part_name],
                        table=self.table.name,
                        deduplicated_metadata=deduplicated_parts.get(part_name, None),
                    )
                )
        stage_communication.part_metadata_queue.put(
            stage_communication.FrozenPartInfo(
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
                if not self.check_data_part(existing_part["backup_path"], part):
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

    def check_data_part(self, backup_path: str, part: PartMetadata) -> bool:
        """
        Check availability of part data in storage.
        """
        try:
            remote_dir_path = self._get_escaped_if_exists(
                part_path,
                part.link or backup_path,
                part.database,
                part.table,
                part.name,
            )
            remote_files = self.loader.list_dir(remote_dir_path)

            if remote_files == [f"{part.name}.tar"]:
                actual_size = self.loader.get_object_size(
                    os.path.join(remote_dir_path, f"{part.name}.tar")
                )
                target_size = target_part_size(
                    part, self._encryption_chunk_size, self._encryption_metadata_size
                )
                if target_size != actual_size:
                    logging.warning(
                        f"Part {part.name} files stored in tar, size not match {target_size} != {actual_size}"
                    )
                    return False
                return True

            notfound_files = set(part.files) - set(remote_files)
            if notfound_files:
                logging.warning(
                    "Some part files were not found in {}: {}",
                    remote_dir_path,
                    ", ".join(notfound_files),
                )
                return False

            return True

        except S3RetryingError:
            logging.warning(
                f"Failed to check data part {part.name}, consider it's broken",
                exc_info=True,
            )
            return False

    def _get_escaped_if_exists(
        self, path_function: Callable, *args: Any, **kwargs: Any
    ) -> str:
        """
        Return escaped path if it exists. Otherwise return regular path.
        """
        path = path_function(*args, escape_names=True, **kwargs)
        if self.loader.path_exists(path, is_dir=True):
            return path
        return path_function(*args, escape_names=False, **kwargs)
