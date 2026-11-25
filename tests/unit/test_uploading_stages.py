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
    engine.objects[REMOTE_PATH] = b"previous data"
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
        pytest.param(0, 0, id="empty"),
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


@pytest.fixture(name="upload_target")
def fixture_upload_target(monkeypatch):
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["storage"].update(chunk_size=CHUNK_SIZE, buffer_size=4 * CHUNK_SIZE)
    engine = FakeStorageEngine()
    engine.objects[REMOTE_PATH] = b"previous data"
    monkeypatch.setattr(
        "ch_backup.storage.async_pipeline.pipeline_builder.get_storage_engine",
        lambda _config: engine,
    )
    return PipelineBuilder(config), engine


@pytest.mark.parametrize("prefix_size", [0, CHUNK_SIZE - 1, CHUNK_SIZE])
def test_failed_source_does_not_replace_existing_object(upload_target, prefix_size):
    """A failed source must not be treated as a successfully completed stream.

    Empty-upload completion now writes an object even when no parts arrive.
    Exercise failure with no data, a buffered partial chunk, and a full chunk
    that can start multipart upload. Neither an empty write nor multipart
    completion may replace the previous object with incomplete data.
    """
    builder, engine = upload_target

    def source():
        yield b"x" * prefix_size
        raise RuntimeError("source failed")

    builder.build_iterable_stage(source()).build_uploading_stage(
        REMOTE_PATH, CHUNK_SIZE
    )

    with pytest.raises(RuntimeError, match="source failed"):
        run(builder.pipeline())

    assert engine.objects == {REMOTE_PATH: b"previous data"}
    assert engine.completed_uploads == engine.single_uploads == 0


def test_failed_empty_upload_keeps_source_file(upload_target, tmp_path):
    """An empty source still requires a successful write before local deletion.

    Its upload happens during completion because the stream produces no parts.
    If that write fails, the error must reach the caller and prevent the
    downstream deletion stage from removing the source needed for a retry.
    The previous remote object must also remain intact.
    """
    builder, engine = upload_target
    source = tmp_path / "empty"
    source.write_bytes(b"")
    builder.build_read_file_stage(source).build_uploading_stage(
        REMOTE_PATH, 0
    ).build_delete_files_stage([source])

    with patch.object(engine, "upload_data", side_effect=RuntimeError("upload failed")):
        with pytest.raises(RuntimeError, match="upload failed"):
            run(builder.pipeline())

    assert source.exists()
    assert engine.objects == {REMOTE_PATH: b"previous data"}
