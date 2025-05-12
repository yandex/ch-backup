"""
Pipeline and PipelineCMD unit tests
"""

import hashlib
import io
import os
import shutil
import tempfile
from functools import partial

import pytest
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

from ch_backup.compression import get_compression
from ch_backup.storage.async_pipeline.stages.compression.compress_stage import (
    CompressStage,
)
from ch_backup.storage.async_pipeline.stages.compression.decompress_stage import (
    DecompressStage,
)
from ch_backup.storage.pipeline import Pipeline
from ch_backup.storage.stages.encryption import DecryptStage, EncryptStage
from ch_backup.storage.stages.filesystem import ReadFileStage, WriteFileStage

WRITE_FILE_CMD_TEST_COUNT = 100
PIPELINE_TEST_COUNT = 100
ENCRYPT_DECRYPT_TEST_COUNT = 100
COMPRESS_DECOMPRESS_TEST_COUNT = ENCRYPT_DECRYPT_TEST_COUNT

SECRET_KEY = "a" * 32

DEFAULT_TMP_DIR_PATH = "staging/tmp_test_data"

# pylint: disable=redefined-outer-name


def get_test_stream(total_size):
    """
    Generates test stream
    """

    data = io.BytesIO()
    line_num = 0
    while True:
        data.write(f"{line_num}\n".encode())
        if data.tell() >= total_size:
            break
        line_num += 1
    data.seek(0)
    return data


class StreamInter:
    """
    Iterate stream
    """

    def __init__(self, chunk_size):
        self._chunk_size = chunk_size

    def __call__(self, incoming_data, src_key=None, dst_key=None):
        while True:
            chunk = incoming_data.read(self._chunk_size)
            if not chunk:
                break
            yield chunk


@pytest.fixture(scope="module")
def tmp_dir_path(dir_path=None):
    """
    Create-delete tmp dir
    """

    if dir_path is None:
        dir_path = DEFAULT_TMP_DIR_PATH
    os.makedirs(dir_path, exist_ok=True)
    yield dir_path
    shutil.rmtree(dir_path)


@settings(max_examples=PIPELINE_TEST_COUNT, deadline=None)
@given(
    file_size=st.integers(1, 1024),
    read_conf=st.fixed_dictionaries(
        {
            "chunk_size": st.integers(1, 1024),
        }
    ),
    encrypt_conf=st.fixed_dictionaries(
        {
            "enabled": st.booleans(),
            "buffer_size": st.integers(1, 1024),
            "chunk_size": st.integers(1, 1024),
            "type": st.just("nacl"),
            "key": st.just(SECRET_KEY),
        }  # type: ignore
    ),
    write_conf=st.fixed_dictionaries(
        {key: st.integers(1, 1024) for key in ("buffer_size", "chunk_size")}
    ),
)
@example(
    file_size=1024,
    read_conf={"chunk_size": 128},
    encrypt_conf={
        "buffer_size": 512,
        "chunk_size": 256,
        "type": "nacl",
        "key": SECRET_KEY,
    },
    write_conf={
        "buffer_size": 512,
        "chunk_size": 256,
    },
)
@example(
    file_size=1024,
    read_conf={"chunk_size": 128},
    encrypt_conf={
        "enabled": True,
        "buffer_size": 512,
        "chunk_size": 256,
        "type": "nacl",
        "key": SECRET_KEY,
    },
    write_conf={
        "buffer_size": 512,
        "chunk_size": 256,
    },
)
@example(
    file_size=1024,
    read_conf={"chunk_size": 128},
    encrypt_conf={},
    write_conf={
        "buffer_size": 512,
        "chunk_size": 256,
    },
)
def test_pipeline_roundtrip(
    tmp_dir_path, file_size, read_conf, encrypt_conf, write_conf
):
    """
    Pipeline
    """
    with tempfile.NamedTemporaryFile(
        mode="wb", prefix="test_file_", dir=tmp_dir_path, delete=False
    ) as orig_fobj:
        test_stream = get_test_stream(file_size)
        test_stream.seek(0)
        orig_fobj.write(test_stream.read())
        original_file_path = orig_fobj.name

    forward_file_path = original_file_path + "-forward"
    backward_file_name = original_file_path + "-backward"

    run_forward_pl(
        original_file_path, forward_file_path, read_conf, encrypt_conf, write_conf
    )
    run_backward_pl(
        forward_file_path, backward_file_name, read_conf, encrypt_conf, write_conf
    )

    with (
        open(original_file_path, "rb") as orig_fobj,
        open(backward_file_name, "rb") as res_fobj,
    ):
        orig_contents = orig_fobj.read()
        res_contents = res_fobj.read()

        assert (
            orig_contents.decode() == res_contents.decode()
        ), "Equal contents expected"

        orig_md5sum = hashlib.md5(orig_contents).digest()
        res_md5sum = hashlib.md5(res_contents).digest()

        assert orig_md5sum == res_md5sum, "Equal md5sum of contents expected"


def run_forward_pl(in_file_name, out_file_name, read_conf, encrypt_conf, write_conf):
    """
    Creates and executes forward pipeline
    """

    pipeline = Pipeline()
    pipeline.append(ReadFileStage(read_conf, {}))
    if encrypt_conf:
        pipeline.append(EncryptStage(encrypt_conf, {}))
    pipeline.append(WriteFileStage(write_conf, {}))

    return pipeline(in_file_name, out_file_name)


def run_backward_pl(in_file_name, out_file_name, read_conf, encrypt_conf, write_conf):
    """
    Creates and executes backward pipeline
    """

    pipeline = Pipeline()
    pipeline.append(ReadFileStage(read_conf, {}))
    if encrypt_conf:
        pipeline.append(DecryptStage(encrypt_conf, {}))
    pipeline.append(WriteFileStage(write_conf, {}))

    return pipeline(in_file_name, out_file_name)


@settings(max_examples=ENCRYPT_DECRYPT_TEST_COUNT, deadline=None)
@example(791, 28, {"buffer_size": 562, "chunk_size": 211})
@given(
    incoming_stream_size=st.integers(1, 1024),
    incoming_chunk_size=st.integers(1, 1024),
    conf=st.fixed_dictionaries(
        {
            "enabled": st.booleans(),
            "buffer_size": st.integers(1, 1024),
            "chunk_size": st.integers(1, 1024),
            "type": st.just("nacl"),
            "key": st.just(SECRET_KEY),
        }  # type: ignore
    ),
)
def test_nacl_ecrypt_decrypt(incoming_stream_size, incoming_chunk_size, conf):
    """
    Tests encryption stage
    """

    encrypted_stream = io.BytesIO()
    decrypted_stream = io.BytesIO()

    conf["key"] = SECRET_KEY
    conf["type"] = "nacl"

    encrypt_cmd = EncryptStage(conf, {})
    decrypt_cmd = DecryptStage(conf, {})

    test_stream = get_test_stream(incoming_stream_size)
    stream_iter = StreamInter(chunk_size=incoming_chunk_size)

    for chunk in encrypt_cmd(partial(stream_iter, test_stream), None, None):
        encrypted_stream.write(chunk)

    encrypted_stream.seek(0)
    for chunk in decrypt_cmd(partial(stream_iter, encrypted_stream), None, None):
        decrypted_stream.write(chunk)

    test_stream.seek(0)
    decrypted_stream.seek(0)

    assert test_stream.read().decode() == decrypted_stream.read().decode()


@settings(max_examples=COMPRESS_DECOMPRESS_TEST_COUNT, deadline=None)
@example(791, 28)
@given(
    incoming_stream_size=st.integers(1, 1024),
    incoming_chunk_size=st.integers(1, 1024),
)
def test_gzip_compress_decompress(incoming_stream_size, incoming_chunk_size):
    """
    Tests compression stage
    """

    conf = {"type": "gzip"}

    compressed_stream = io.BytesIO()
    decompressed_stream = io.BytesIO()

    compressor = get_compression(conf["type"])

    compress_cmd = CompressStage(compressor)
    decompress_cmd = DecompressStage(compressor)

    test_stream = get_test_stream(incoming_stream_size)
    stream_iter = StreamInter(chunk_size=incoming_chunk_size)

    for chunk in stream_iter(test_stream):
        data = compress_cmd(chunk, -1)
        if data:
            compressed_stream.write(data)
    data = compress_cmd.on_done()
    if data:
        compressed_stream.write(data)

    compressed_stream.seek(0)
    for chunk in stream_iter(compressed_stream):
        data = decompress_cmd(chunk, -1)
        if data:
            decompressed_stream.write(data)
    data = compress_cmd.on_done()
    if data:
        decompressed_stream.write(data)

    test_stream.seek(0)
    decompressed_stream.seek(0)

    assert test_stream.read().decode() == decompressed_stream.read().decode()


@settings(
    max_examples=WRITE_FILE_CMD_TEST_COUNT,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    deadline=None,
)
@example(791, 28, {"buffer_size": 562, "chunk_size": 211})
@given(
    incoming_stream_size=st.integers(1, 1024),
    incoming_chunk_size=st.integers(1, 1024),
    conf=st.fixed_dictionaries(
        {key: st.integers(1, 1024) for key in ("buffer_size", "chunk_size")}
    ),
)
def test_write_file_cmd(monkeypatch, incoming_stream_size, incoming_chunk_size, conf):
    """
    Tests write file stage
    """

    result_stream = io.BytesIO()

    monkeypatch.setattr(WriteFileStage, "_pre_process", lambda x, y, z: True)
    monkeypatch.setattr(WriteFileStage, "_post_process", lambda x: None)
    write_file_cmd = WriteFileStage(conf, {})
    monkeypatch.setattr(write_file_cmd, "_fobj", result_stream)

    test_stream = get_test_stream(incoming_stream_size)
    stream_iter = StreamInter(chunk_size=incoming_chunk_size)
    for _ in write_file_cmd(partial(stream_iter, test_stream), None, None):
        pass

    result_stream.seek(0)
    test_stream.seek(0)

    assert test_stream.read().decode() == result_stream.read().decode()
