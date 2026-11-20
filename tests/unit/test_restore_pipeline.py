"""Local restore byte equivalence, bounded buffering and ordered decryption."""

import copy
import io
import threading
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from hashlib import md5
from pathlib import Path
from tarfile import GNU_FORMAT, TarInfo
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError, ResponseStreamingError
from botocore.response import StreamingBody
from nacl.bindings import crypto_secretbox, crypto_secretbox_open
from nacl.exceptions import CryptoError
from pypeln.thread import Worker
from pypeln.utils import Element

from ch_backup.backup.metadata import PartMetadata
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.encryption import get_encryption
from ch_backup.exceptions import StorageError
from ch_backup.logic.part_restore import LocalPartRestorer
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.base_pipeline.map import OrderedMap, thread_map
from ch_backup.storage.async_pipeline.pipeline_builder import PipelineBuilder
from ch_backup.storage.async_pipeline.pipelines import (
    download_files_pipeline,
    run_and_collect_all,
)
from ch_backup.storage.async_pipeline.stages.filesystem.chunking_stage import (
    ChunkingStage,
)
from ch_backup.storage.async_pipeline.stages.filesystem.write_files_stage import (
    WriteFilesStage,
)
from ch_backup.storage.async_pipeline.stages.storage.download_storage_stage import (
    DownloadStorageStage,
)
from ch_backup.storage.engine.s3.s3_engine import S3StorageEngine
from ch_backup.storage.engine.s3.s3_retry import S3RetryingError


class DownloadBody(io.BytesIO):
    """Serve real bytes and optionally interrupt the first response."""

    def __init__(self, data: bytes, fail_after: int | None = None) -> None:
        """Record read sizes without collecting payload copies."""
        super().__init__(data)
        self.fail_after = fail_after
        self.read_sizes: list[int | None] = []
        self.error_read_size = 0
        self.discarded = 0

    def read(self, size: int | None = -1) -> bytes:
        """Raise a transport failure after the selected delivered prefix."""
        self.read_sizes.append(size)
        if self.fail_after is not None and self.tell() >= self.fail_after:
            self.discarded += len(super().read(self.error_read_size))
            raise ResponseStreamingError(error="interrupted test response")
        return super().read(size)


def make_storage(
    data: bytes, fail_after: int | None = None
) -> tuple[S3StorageEngine, Mock, list[DownloadBody]]:
    """Use the real S3 engine with an in-memory HTTP response body."""
    config = copy.deepcopy(DEFAULT_CONFIG["storage"])
    config["credentials"]["bucket"] = "test-bucket"
    config.update(retry_max_attempts=2, retry_max_interval=0)
    client = Mock()
    client.head_object.return_value = {"ContentLength": len(data), "ETag": '"version"'}
    bodies: list[DownloadBody] = []

    def get_object(**kwargs):
        """Serve the requested range and enforce the pinned object identity."""
        assert kwargs.get("IfMatch", '"version"') == '"version"'
        start, end = map(int, kwargs["Range"].removeprefix("bytes=").split("-"))
        body = DownloadBody(data[start : end + 1], fail_after if not bodies else None)
        bodies.append(body)
        return {
            "Body": StreamingBody(body, end - start + 1),
            "ETag": '"version"',
            "ContentLength": end - start + 1,
            "ContentRange": f"bytes {start}-{end}/{len(data)}",
        }

    client.get_object.side_effect = get_object
    engine = S3StorageEngine(
        config, client_factory=Mock(create_s3_client=Mock(return_value=client))
    )
    return engine, client, bodies


def tar_files(files: dict[str, bytes]) -> bytes:
    """Create the GNU TAR stream used by backup without final zero blocks."""
    stream = bytearray()
    for name, data in files.items():
        header = TarInfo(name)
        header.size = len(data)
        stream.extend(header.tobuf(format=GNU_FORMAT))
        stream.extend(data)
        stream.extend(b"\0" * (-len(data) % 512))
    return bytes(stream)


@pytest.mark.parametrize("direct", [False, True])
@pytest.mark.parametrize("read_size", [1, 13, 64, 77, 257])
@pytest.mark.parametrize("chunk_size", [1, 16, 65])
def test_chunking_preserves_every_byte(
    direct: bool, read_size: int, chunk_size: int
) -> None:
    """Exercise partial chunks, wraparound, empty inputs and the final short block."""
    data = bytes(range(256)) * 3 + b"end"
    stage = ChunkingStage(chunk_size, chunk_size + 7, direct_chunks=direct)
    chunks: list[bytes] = []
    for offset in range(0, len(data), read_size):
        chunks.extend(stage(data[offset : offset + read_size], offset))
        chunks.extend(stage(b"", offset))
    chunks.extend(stage.on_done())
    assert chunks == [
        data[offset : offset + chunk_size] for offset in range(0, len(data), chunk_size)
    ]


def test_direct_chunking_keeps_only_one_chunk_of_buffering() -> None:
    """An oversized input and legacy buffer setting do not allocate that buffer."""
    data = bytes(range(256)) * 16384
    tracemalloc.start()
    try:
        stage = ChunkingStage(64 * 1024, 16 * 1024 * 1024, direct_chunks=True)
        offset = 0
        for chunk in stage(data, 0):
            assert chunk == data[offset : offset + len(chunk)]
            offset += len(chunk)
        assert not list(stage.on_done())
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert offset == len(data)
    assert peak < 1024 * 1024


@pytest.mark.parametrize("global_limit", [-1, 0, 1, 50])
def test_restore_queue_caps_preserve_lower_global_limits(global_limit: int) -> None:
    """Keep existing low-memory limits and bound otherwise unbounded queues."""
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["encryption"].update(key="k" * 32, queue_size=global_limit)
    config["filesystem"]["queue_size"] = global_limit
    before = copy.deepcopy(config)
    builder = PipelineBuilder(config, local_restore=True).build_iterable_stage([])
    builder.build_decrypt_stage().build_write_files_stage(Path("unused"))
    with patch.object(builder, "_iterate", return_value=iter(())) as iterate:
        builder.pipeline()
    stage = iterate.call_args.args[0]
    limits = []
    for _ in range(4):
        limits.append(stage.maxsize)
        stage = stage.dependencies[0]
    expected = [
        min(global_limit, cap) if global_limit > 0 else cap for cap in (4, 4, 2, 2)
    ]
    assert limits == expected
    assert config == before


@pytest.mark.parametrize(
    "direct_options", [(False, False), (False, True), (True, False), (True, True)]
)
@pytest.mark.parametrize("threads", [1, 2, 3])
@pytest.mark.parametrize("range_size", [0, 8192])
@pytest.mark.parametrize("encrypted", [False, True])
def test_local_tar_pipeline_restores_files(
    tmp_path: Path,
    direct_options: tuple[bool, bool],
    threads: int,
    range_size: int,
    encrypted: bool,
) -> None:
    """Restore actual payload, empty files and GNU long names through all stages."""
    direct_chunks, direct_write = direct_options
    files = {
        "checksums.txt": b"checksums",
        "empty.bin": b"",
        "data.bin": bytes(range(256)) * 39,
        "long-directory/" * 20 + "column.bin": b"column" * 533,
    }
    plaintext = tar_files(files)
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["encryption"].update(key="k" * 32, chunk_size=2048, buffer_size=8192)
    config["filesystem"].update(chunk_size=1024, buffer_size=4096)
    config["storage"]["chunk_size"] = 713
    config["restore"]["local_data"].update(
        direct_chunks=direct_chunks,
        direct_write=direct_write,
        decrypt_threads=threads,
        download_range_size=range_size,
        encryption_queue_size=1,
        filesystem_queue_size=1,
    )
    crypto = get_encryption("nacl", config["encryption"])
    data = (
        b"".join(
            crypto.encrypt(plaintext[pos : pos + 2048])
            for pos in range(0, len(plaintext), 2048)
        )
        if encrypted
        else plaintext
    )
    storage, _, bodies = make_storage(data, fail_after=1426 if range_size else None)
    with patch(
        "ch_backup.storage.async_pipeline.pipeline_builder.get_storage_engine",
        return_value=storage,
    ):
        download_files_pipeline(config, "part.tar", tmp_path, encrypted, False)
    restored = {
        str(path.relative_to(tmp_path)): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    assert restored == files
    if range_size:
        assert all(body.closed for body in bodies)
        assert all(
            size is not None and 0 < size <= 713
            for body in bodies
            for size in body.read_sizes
        )


@pytest.mark.parametrize("size", [0, 1, 63, 64, 65, 8192])
def test_nacl_reads_legacy_frames_and_writes_compatible_frames(size: int) -> None:
    """Check byte-format compatibility with the padded NaCl API used by PyNaCl 1.5."""
    key, nonce = b"k" * 32, b"n" * 24
    plaintext = bytes(i % 251 for i in range(size))
    crypto = get_encryption("nacl", {"key": key.decode()})
    legacy = nonce + crypto_secretbox(plaintext, nonce, key)
    assert crypto.decrypt(legacy) == plaintext
    new = crypto.encrypt(plaintext)
    assert crypto_secretbox_open(new[24:], new[:24], key) == plaintext
    for broken in (legacy[:-1], legacy[:24] + bytes([legacy[24] ^ 1]) + legacy[25:]):
        with pytest.raises(CryptoError):
            crypto.decrypt(broken)


@pytest.mark.parametrize("fail_after", [None, 4, 8])
def test_streaming_ranges_retry_without_repeating_bytes(fail_after: int | None) -> None:
    """Retry a partially read HTTP response from its first undelivered byte."""
    data = bytes(range(53))
    storage, client, bodies = make_storage(data, fail_after)
    stage = DownloadStorageStage({"chunk_size": 4}, storage, "part.tar", range_size=16)
    stage.on_start()
    restored = b"".join(stage())
    stage.on_done()
    assert restored == data
    assert all(body.closed for body in bodies)
    assert all(
        0 < size <= 4 for body in bodies for size in body.read_sizes if size is not None
    )
    if fail_after is not None:
        assert (
            client.get_object.call_args_list[1]
            .kwargs["Range"]
            .startswith(f"bytes={fail_after}-")
        )
    assert len(client.get_object.call_args_list) < len(data) // 4


@pytest.mark.parametrize(
    "field,value",
    [("ETag", '"changed"'), ("ContentRange", "bytes 0-15/999"), ("ContentLength", 99)],
)
def test_streaming_rejects_changed_or_mismatched_ranges(
    field: str, value: str | int
) -> None:
    """Do not return bytes from a response with the wrong identity or offsets."""
    storage, client, bodies = make_storage(bytes(range(64)))
    serve = client.get_object.side_effect

    def changed(**kwargs):
        """Corrupt metadata only after the first complete range."""
        response = serve(**kwargs)
        if len(bodies) > 1:
            response[field] = value
        return response

    client.get_object.side_effect = changed
    stage = DownloadStorageStage({"chunk_size": 4}, storage, "part.tar", range_size=16)
    stage.on_start()
    restored = bytearray()
    with pytest.raises(StorageError, match="does not match the requested object range"):
        for chunk in stage():
            restored.extend(chunk)
    assert restored == bytes(range(16))
    assert all(body.closed for body in bodies)


def test_streaming_consumer_close_releases_response() -> None:
    """Closing an unfinished stream releases the retained HTTP body."""
    storage, _, bodies = make_storage(bytes(range(64)))
    stage = DownloadStorageStage({"chunk_size": 4}, storage, "part.tar", range_size=32)
    stage.on_start()
    stream = stage()
    assert next(stream) == bytes(range(4))
    stream.close()
    stage.on_done()
    assert bodies[0].closed


@pytest.mark.parametrize("length", [0, 6])
def test_streaming_short_response_resumes_at_delivered_offset(length: int) -> None:
    """An early EOF cannot silently truncate a part or repeat its prefix."""
    data = bytes(range(53))
    storage, client, bodies = make_storage(data)
    serve = client.get_object.side_effect

    def truncated(**kwargs):
        """Truncate the first body while preserving its advertised length."""
        response = serve(**kwargs)
        if len(bodies) == 1:
            bodies[-1].truncate(length)
        return response

    client.get_object.side_effect = truncated
    stage = DownloadStorageStage({"chunk_size": 4}, storage, "part.tar", range_size=16)
    stage.on_start()
    assert b"".join(stage()) == data
    assert (
        client.get_object.call_args_list[1]
        .kwargs["Range"]
        .startswith(f"bytes={length}-")
    )
    assert all(body.closed for body in bodies)


def test_streaming_retry_exhaustion_closes_all_responses() -> None:
    """A persistent failure returns only the valid prefix and drains resources."""
    storage, client, bodies = make_storage(bytes(range(53)), fail_after=4)
    serve = client.get_object.side_effect

    def unavailable(**kwargs):
        """Keep every subsequent response unreadable."""
        response = serve(**kwargs)
        if len(bodies) > 1:
            bodies[-1].fail_after = 0
        return response

    client.get_object.side_effect = unavailable
    stage = DownloadStorageStage({"chunk_size": 4}, storage, "part.tar", range_size=16)
    stage.on_start()
    stream = stage()
    assert next(stream) == bytes(range(4))
    with pytest.raises(S3RetryingError, match="interrupted test response"):
        next(stream)
    assert client.get_object.call_count == 2
    assert all(body.closed for body in bodies)


def test_streaming_read_failure_discards_unreturned_bytes() -> None:
    """Retry bytes consumed inside a failed read but never returned to the pipeline."""
    data = bytes(range(53))
    storage, client, bodies = make_storage(data, fail_after=4)
    serve = client.get_object.side_effect

    def interrupted_read(**kwargs):
        """Consume two extra bytes before the first body raises."""
        response = serve(**kwargs)
        bodies[-1].error_read_size = 2
        return response

    client.get_object.side_effect = interrupted_read
    stage = DownloadStorageStage({"chunk_size": 4}, storage, "part.tar", range_size=16)
    stage.on_start()
    assert b"".join(stage()) == data
    assert bodies[0].discarded == 2
    assert client.get_object.call_args_list[1].kwargs["Range"] == "bytes=4-19"
    assert all(body.closed for body in bodies)


def test_streaming_precondition_failure_is_not_retried() -> None:
    """Do not switch to a replacement object after sending its original ETag."""
    storage, client, _ = make_storage(bytes(range(53)))
    client.get_object.side_effect = ClientError(
        {
            "Error": {"Code": "PreconditionFailed"},
            "ResponseMetadata": {"HTTPStatusCode": 412},
        },
        "GetObject",
    )
    stage = DownloadStorageStage({"chunk_size": 4}, storage, "part.tar", range_size=16)
    stage.on_start()
    with pytest.raises(ClientError, match="PreconditionFailed"):
        list(stage())
    assert client.get_object.call_count == 1


def test_downstream_write_failure_closes_streaming_response(tmp_path: Path) -> None:
    """A failed consumer must release the producer's partially read HTTP range."""
    storage, _, bodies = make_storage(tar_files({"data.bin": b"x" * (256 * 1024)}))
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["storage"]["chunk_size"] = 1024
    config["filesystem"].update(chunk_size=1024, buffer_size=4096)
    config["restore"]["local_data"].update(
        download_range_size=65536, filesystem_queue_size=1
    )
    with (
        patch(
            "ch_backup.storage.async_pipeline.pipeline_builder.get_storage_engine",
            return_value=storage,
        ),
        patch.object(
            WriteFilesStage, "_write_data", side_effect=OSError("test disk full")
        ),
        pytest.raises(OSError, match="test disk full"),
    ):
        download_files_pipeline(config, "part.tar", tmp_path, False, False)
    assert bodies
    assert all(body.closed for body in bodies)


class OutOfOrderHandler(Handler):
    """Finish the second job first and retain evidence of real thread completion."""

    def __init__(self, fail: bool = False) -> None:
        """Coordinate completion with events instead of timing assumptions."""
        self.second = threading.Event()
        self.threads: list[threading.Thread] = []
        self.fail = fail

    def __call__(self, value: bytes, index: int) -> bytes:
        """Force reordering, optionally failing the first result."""
        self.threads.append(threading.current_thread())
        if index == 0:
            assert self.second.wait(5), "second decrypt job did not run concurrently"
            if self.fail:
                raise CryptoError("test authentication failed")
        elif index == 1:
            self.second.set()
        return value


@pytest.mark.parametrize("fail", [False, True])
def test_parallel_map_orders_results_and_joins_threads(fail: bool) -> None:
    """Preserve input order and leave no running jobs on success or failure."""
    handler = OutOfOrderHandler(fail)
    builder = PipelineBuilder(DEFAULT_CONFIG).build_iterable_stage(
        [b"first", b"second", b"third"]
    )
    builder.append(thread_map(handler, workers=2, ordered=True, maxsize=1))
    if fail:
        with pytest.raises(CryptoError, match="test authentication failed"):
            run_and_collect_all(builder.pipeline())
    else:
        assert run_and_collect_all(builder.pipeline()) == [
            b"first",
            b"second",
            b"third",
        ]
    assert handler.threads
    assert all(not thread.is_alive() for thread in handler.threads)


def test_parallel_map_bounds_read_ahead_and_joins_on_consumer_close() -> None:
    """A paused consumer retains at most one window and can close it safely."""
    handler = OutOfOrderHandler()
    submitted = []

    def source():
        """Record consumption of each distinct input frame."""
        for index in range(20):
            submitted.append(index)
            yield Element(index=(index,), value=bytes([index]))

    worker = Mock()
    worker.stage_params.input_queue = source()
    results = OrderedMap(handler, 2).process(worker)
    assert next(results) is None
    assert next(results) == b"\0"
    assert submitted == [0, 1]
    results.close()
    assert all(not thread.is_alive() for thread in handler.threads)


def test_parallel_map_drains_after_cancellation_interrupts_shutdown() -> None:
    """Simultaneous decrypt and writer failures cannot leave decrypt jobs running."""
    signals = SimpleNamespace(
        ready=threading.Barrier(3),
        shutdown=threading.Event(),
        join_started=threading.Event(),
        retrying=threading.Event(),
        progress=threading.Event(),
        completed=threading.Event(),
    )
    release: dict[threading.Thread, threading.Event] = {}
    threads: list[threading.Thread] = []
    first: list[threading.Thread] = []
    control_errors: list[str] = []
    original = SimpleNamespace(
        shutdown=ThreadPoolExecutor.shutdown,
        stop=Worker.stop,
        join=threading.Thread.join,
    )

    def observed_shutdown(executor, *args, **kwargs):
        """Expose actual shutdown entry without changing its join behavior."""
        if not signals.shutdown.is_set():
            signals.shutdown.set()
        else:
            signals.retrying.set()
            signals.progress.set()
        return original.shutdown(executor, *args, **kwargs)

    def observed_join(thread, *args, **kwargs):
        """Identify the live decrypt thread whose join will be interrupted."""
        if thread in release and not first:
            first.append(thread)
            signals.join_started.set()
        return original.join(thread, *args, **kwargs)

    def observed_stop(worker):
        """Let the interrupted first join finish after real pypeln cancellation."""
        original.stop(worker)
        if isinstance(worker.process_fn, OrderedMap):
            for thread in first:
                if thread in release:
                    release[thread].set()

    class Crypto(Handler):
        """Fail one job while two others retain their working memory."""

        def __call__(self, value: bytes, index: int) -> bytes:
            """Keep later jobs alive until the test allows them to finish."""
            thread = threading.current_thread()
            threads.append(thread)
            if index == 1:
                signals.ready.wait(5)
                raise CryptoError("test decrypt failed")
            if index >= 2:
                release[thread] = threading.Event()
                signals.ready.wait(5)
                assert release[thread].wait(10), "decrypt job was not released"
            return value

    class Writer(Handler):
        """Fail exactly while decryption is shutting down."""

        def __call__(self, value: bytes, index: int) -> None:
            """Trigger the real supervisor's asynchronous cancellation."""
            assert signals.join_started.wait(5), "decrypt join did not begin"
            raise OSError("test writer failed")

    def finish_jobs():
        """Release the remaining jobs only after interrupted shutdown is retried."""
        if not signals.progress.wait(5):
            control_errors.append("drain did not retry")
        if not signals.retrying.is_set() or signals.completed.is_set():
            control_errors.append("pipeline returned before drain")
        for event in release.values():
            event.set()

    builder = PipelineBuilder(DEFAULT_CONFIG).build_iterable_stage(
        [b"1", b"2", b"3", b"4"]
    )
    builder.append(thread_map(Crypto(), workers=3, ordered=True, maxsize=1))
    builder.append(thread_map(Writer(), maxsize=1))
    controller = threading.Thread(target=finish_jobs)
    controller.start()
    try:
        with (
            patch.object(ThreadPoolExecutor, "shutdown", observed_shutdown),
            patch.object(Worker, "stop", observed_stop),
            patch.object(threading.Thread, "join", observed_join),
            pytest.raises(OSError, match="test writer failed"),
        ):
            run_and_collect_all(builder.pipeline())
        live = [thread.name for thread in set(threads) if thread.is_alive()]
    finally:
        signals.completed.set()
        signals.progress.set()
        controller.join(10)
        for event in release.values():
            event.set()
        for thread in set(threads):
            thread.join(5)
    assert not control_errors and not live, (control_errors, live)


@pytest.mark.parametrize("cut", [1, 511, 512, 562, 613, 1023])
def test_direct_tar_write_preserves_truncation_checks(tmp_path: Path, cut: int) -> None:
    """The fast payload path must still reject missing headers, data and padding."""
    data = tar_files({"data.bin": b"x" * 100})
    stage = WriteFilesStage({}, tmp_path, 1024, direct_write=True)
    for offset in range(0, cut, 64):
        stage(data[offset : min(offset + 64, cut)], offset)
    with pytest.raises(ValueError, match="Truncated TAR stream"):
        stage.on_done()


@pytest.mark.parametrize("threads", [1, 2])
def test_corrupt_frame_never_replaces_existing_part(
    tmp_path: Path, threads: int
) -> None:
    """An authenticated-stream failure keeps old data and removes failed staging."""
    files = {"checksums.txt": b"checksums", "data.bin": b"payload" * 2000}
    plaintext = tar_files(files)
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["encryption"].update(key="k" * 32, chunk_size=1024, buffer_size=4096)
    config["filesystem"].update(chunk_size=512, buffer_size=2048)
    config["storage"]["chunk_size"] = 713
    config["restore"]["local_data"].update(
        decrypt_threads=threads, download_range_size=8192
    )
    crypto = get_encryption("nacl", config["encryption"])
    frames = [
        bytes(crypto.encrypt(plaintext[pos : pos + 1024]))
        for pos in range(0, len(plaintext), 1024)
    ]
    frames[5] = frames[5][:-1] + bytes([frames[5][-1] ^ 1])
    storage, _, bodies = make_storage(b"".join(frames))
    part = PartMetadata(
        "db",
        "table",
        "all_1_1_0",
        md5(b"checksums").hexdigest(),
        14848,
        list(files),
        True,
    )
    destination = tmp_path / part.name
    destination.mkdir()
    (destination / "original.bin").write_bytes(b"keep original data")
    layout = Mock()

    def download(_backup, downloaded_part, path, callback, **_kwargs):
        """Exercise the real pipeline before accepting a completed part."""
        download_files_pipeline(config, "part.tar", Path(path), True, False)
        callback(downloaded_part)

    layout.download_data_part.side_effect = download
    success = Mock()
    restorer = LocalPartRestorer(
        layout, Mock(), keep_going=False, on_success=success, on_error=Mock()
    )
    with (
        patch(
            "ch_backup.storage.async_pipeline.pipeline_builder.get_storage_engine",
            return_value=storage,
        ),
        pytest.raises(CryptoError) as raised,
    ):
        restorer.download(part, str(destination))
    restorer.wait()
    assert "k" * 32 not in str(raised.value)
    assert (destination / "original.bin").read_bytes() == b"keep original data"
    assert list(tmp_path.iterdir()) == [destination]
    assert list(destination.iterdir()) == [destination / "original.bin"]
    assert not success.called
    assert all(body.closed for body in bodies)


def test_direct_tar_write_accepts_legacy_final_padding(tmp_path: Path) -> None:
    """Keep compatibility with complete final members lacking all final padding."""
    header = TarInfo("data.bin")
    header.size = 1025
    data = b"x" * header.size
    stream = header.tobuf(format=GNU_FORMAT) + data
    stage = WriteFilesStage({}, tmp_path, 1024, direct_write=True)
    for offset in range(0, len(stream), 512):
        stage(stream[offset : offset + 512], offset)
    stage.on_done()
    assert (tmp_path / "data.bin").read_bytes() == data
