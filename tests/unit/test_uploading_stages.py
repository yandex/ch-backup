"""
Tests for chunked and multipart uploading to storage.
"""

import copy
import threading
from typing import Any
from unittest.mock import patch

import pytest

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.storage.async_pipeline.pipeline_builder import PipelineBuilder
from ch_backup.storage.async_pipeline.pipelines import run

CHUNK_SIZE = 16
REMOTE_PATH = "ch_backup/backup/data/db/table/part/part.tar"
UPLOAD_ID = "upload-id"


class FakeStorageEngine:
    """
    Records the storage calls made by the uploading stages.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.objects: dict[str, bytes] = {}
        self.single_uploads = 0
        self.created_uploads = 0
        self.completed_uploads = 0
        self.parts: dict[int, bytes] = {}

    def upload_data(self, data: bytes, remote_path: str) -> str:
        with self._lock:
            self.single_uploads += 1
            self.objects[remote_path] = data
        return remote_path

    def create_multipart_upload(self, remote_path: str) -> str:
        assert remote_path == REMOTE_PATH
        with self._lock:
            self.created_uploads += 1
        return UPLOAD_ID

    def upload_part(
        self,
        data: bytes,
        remote_path: str,
        upload_id: str,
        part_num: int,
    ) -> None:
        assert (remote_path, upload_id) == (REMOTE_PATH, UPLOAD_ID)
        with self._lock:
            assert part_num not in self.parts, f"Part {part_num} uploaded twice"
            self.parts[part_num] = data

    def complete_multipart_upload(self, remote_path: str, upload_id: str) -> None:
        assert (remote_path, upload_id) == (REMOTE_PATH, UPLOAD_ID)
        with self._lock:
            self.completed_uploads += 1
            self.objects[remote_path] = b"".join(
                self.parts[num] for num in sorted(self.parts)
            )


def _upload(data: bytes, piece_size: int) -> FakeStorageEngine:
    config: dict[str, Any] = copy.deepcopy(DEFAULT_CONFIG)
    config["storage"]["chunk_size"] = CHUNK_SIZE
    config["storage"]["buffer_size"] = 4 * CHUNK_SIZE

    engine = FakeStorageEngine()
    pieces = [data[i : i + piece_size] for i in range(0, len(data), piece_size)]
    with patch(
        "ch_backup.storage.async_pipeline.pipeline_builder.get_storage_engine",
        return_value=engine,
    ):
        builder = PipelineBuilder(config)
        builder.build_iterable_stage(pieces)
        builder.build_uploading_stage(REMOTE_PATH, len(data))
        run(builder.pipeline())

    return engine


@pytest.mark.parametrize("piece_size", [5, 1024], ids=["small-pieces", "one-piece"])
@pytest.mark.parametrize(
    "data_size, parts",
    [
        pytest.param(CHUNK_SIZE - 1, 0, id="less-than-chunk"),
        pytest.param(CHUNK_SIZE, 1, id="one-chunk"),
        pytest.param(CHUNK_SIZE + 1, 2, id="one-chunk-plus-byte"),
        pytest.param(2 * CHUNK_SIZE, 2, id="two-chunks"),
        pytest.param(2 * CHUNK_SIZE + 1, 3, id="two-chunks-plus-byte"),
    ],
)
def test_uploading_stages(data_size: int, parts: int, piece_size: int) -> None:
    data = bytes(i % 251 for i in range(data_size))

    engine = _upload(data, piece_size)

    assert engine.objects == {REMOTE_PATH: data}
    if parts:
        assert engine.single_uploads == 0
        assert engine.created_uploads == engine.completed_uploads == 1
        assert sorted(engine.parts) == list(range(1, parts + 1))
        assert all(len(engine.parts[num]) == CHUNK_SIZE for num in range(1, parts))
    else:
        assert engine.single_uploads == 1
        assert engine.created_uploads == engine.completed_uploads == 0
        assert not engine.parts
