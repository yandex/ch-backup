"""Uploading part observer."""
import time
from typing import List, Optional

from ch_backup.backup.metadata.part_metadata import PartMetadata
from ch_backup.backup_context import BackupContext


class UploadPartObserver:
    """
    Observe uploading parts.

    Update backup metadata with specified interval after completion of
    uploading every part to object storage.
    """

    def __init__(self, context: BackupContext) -> None:
        self._context = context
        self._last_time = time.time()
        self._uploaded_parts: List[PartMetadata] = []
        self._interval = self._context.config["update_metadata_interval"]

    def __call__(
        self, part: PartMetadata, exception: Optional[Exception] = None
    ) -> None:
        if exception:
            return

        self._uploaded_parts.append(part)
        self._context.backup_meta.add_part(part)

        now = time.time()
        if now - self._last_time >= self._interval:
            self._context.backup_layout.upload_backup_metadata(
                self._context.backup_meta
            )
            self._last_time = now

    @property
    def uploaded_parts(self) -> List[PartMetadata]:
        """
        Return uploaded parts metadata.
        """
        return self._uploaded_parts
