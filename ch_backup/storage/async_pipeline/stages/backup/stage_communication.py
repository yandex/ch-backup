from dataclasses import dataclass
from multiprocessing import Queue
from threading import Condition
from typing import Optional

from ch_backup.backup.metadata.part_metadata import PartMetadata

@dataclass
class PartPipelineInfo:
    part_metadata: Optional[PartMetadata]
    table: Optional[str]
    part_path: Optional[str]
    remote_path: Optional[str]
    estimated_size: Optional[int]
    all_parts_done: bool = False

# This queue will be inherited by child processes
# Used to communicate with the main process
part_metadata_queue = Queue()
