"""Bound archives collected in memory before accepting their file contents."""

import gzip
from tarfile import BLOCKSIZE, GNU_FORMAT, GNUTYPE_LONGNAME, TarInfo

import pytest

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ConfigurationError, StorageError
from ch_backup.storage.async_pipeline.pipeline_builder import PipelineBuilder
from ch_backup.storage.async_pipeline.pipelines import run_and_collect_all
from ch_backup.storage.async_pipeline.stages.filesystem.write_files_stage import (
    UnpackTarballStage,
)


@pytest.fixture(name="make_stage")
def fixture_make_stage():
    def make(limit):
        config = {
            **DEFAULT_CONFIG["filesystem"],
            "max_in_memory_tarball_size": limit,
        }
        return UnpackTarballStage(config, buffer_size=4096)

    return make


@pytest.fixture(name="make_header")
def fixture_make_header():
    def make(name, size, member_type=None):
        info = TarInfo(name)
        info.size = size
        if member_type is not None:
            info.type = member_type
        return info.tobuf(format=GNU_FORMAT)

    return make


@pytest.fixture(name="make_compressed_pipeline")
def fixture_make_compressed_pipeline(make_header):
    def make(limit):
        member = make_header("data.bin", 1) + b"x" + bytes(BLOCKSIZE - 1)
        config = {
            "filesystem": {
                **DEFAULT_CONFIG["filesystem"],
                "max_in_memory_tarball_size": limit,
            },
            "compression": DEFAULT_CONFIG["compression"],
        }
        builder = PipelineBuilder(config)
        builder.build_iterable_stage([gzip.compress(member)])
        builder.build_decompress_stage().build_unpack_data_tarball_stage()
        return builder.pipeline()

    return make


def test_unpack_accepts_archive_at_limit(make_stage, make_header):
    stage = make_stage(1024)
    member = make_header("data.bin", 1) + b"x" + bytes(BLOCKSIZE - 1)

    for index, offset in enumerate(range(0, len(member), 128)):
        stage(member[offset : offset + 128], index)

    assert list(stage.on_done()) == [("data.bin", b"x")]


@pytest.mark.parametrize("member_type", [None, GNUTYPE_LONGNAME])
def test_unpack_rejects_oversized_member_before_body(
    make_stage, make_header, member_type
):
    stage = make_stage(1024)

    with pytest.raises(StorageError, match="max_in_memory_tarball_size"):
        stage(make_header("data.bin", 1025, member_type), 0)


def test_unpack_counts_total_archive_size(make_stage, make_header):
    stage = make_stage(1536)
    stage(make_header("first.bin", 1) + b"x" + bytes(BLOCKSIZE - 1), 0)

    with pytest.raises(StorageError, match="max_in_memory_tarball_size"):
        stage(make_header("second.bin", 1), 1)


def test_unpack_counts_empty_file_headers(make_stage, make_header):
    stage = make_stage(512)
    stage(make_header("first.bin", 0), 0)

    with pytest.raises(StorageError, match="max_in_memory_tarball_size"):
        stage(make_header("second.bin", 0), 1)


def test_unpack_rejects_negative_member_size(make_stage, make_header):
    stage = make_stage(1024)

    with pytest.raises(StorageError, match="negative"):
        stage(make_header("data.bin", -1), 0)


@pytest.mark.parametrize("limit", [0, -1, True, "1024"])
def test_unpack_rejects_invalid_limit(make_stage, limit):
    with pytest.raises(ConfigurationError, match="positive integer"):
        make_stage(limit)


def test_compressed_archive_at_limit(make_compressed_pipeline):
    assert run_and_collect_all(make_compressed_pipeline(1024)) == [("data.bin", b"x")]


def test_compressed_archive_limit_uses_uncompressed_size(make_compressed_pipeline):
    with pytest.raises(StorageError, match="max_in_memory_tarball_size"):
        run_and_collect_all(make_compressed_pipeline(512))
