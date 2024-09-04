from dataclasses import dataclass
from multiprocessing import Queue
from typing import Optional

from ch_backup.backup.metadata.part_metadata import PartMetadata
from ch_backup.clickhouse.models import FrozenPart


@dataclass
class FrozenPartInfo:
    frozen_part: Optional[FrozenPart]
    table: str
    deduplicated_metadata: Optional[PartMetadata] = None
    s3_part: bool = False
    all_parts_done: bool = False


# This queue will be inherited by child processes
# Used to communicate with the main process
part_metadata_queue = Queue()
