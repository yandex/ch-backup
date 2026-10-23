"""
Free functions that create and run pipelines. Can be started in multiprocessing pool.
"""

import sys
from pathlib import Path
from tarfile import BLOCKSIZE
from types import TracebackType
from typing import (
    Any,
    AnyStr,
    BinaryIO,
    Callable,
    Iterable,
    Iterator,
    Sequence,
)

from ch_backup import logging
from ch_backup.calculators import (
    calc_aligned_data_size,
    calc_aligned_files_size,
    calc_aligned_files_size_scan,
    calc_encrypted_size,
    calc_tarball_size,
    calc_tarball_size_scan,
)
from ch_backup.encryption import get_encryption
from ch_backup.storage.async_pipeline.pipeline_builder import PipelineBuilder
from ch_backup.storage.engine import get_storage_engine
from ch_backup.storage.object_copy import CopyEntry
from ch_backup.util import exhaust_iterator


def upload_data_pipeline(
    config: dict, data: AnyStr, remote_path: str, encrypt: bool
) -> None:
    """
    Entrypoint of upload data pipeline.
    """
    builder = PipelineBuilder(config)

    estimated_size = len(data)
    builder.build_iterable_stage([data])
    if encrypt:
        builder.build_encrypt_stage()
        estimated_size = _calc_encrypted_size(config, estimated_size)
    builder.build_uploading_stage(remote_path, estimated_size)

    run(builder.pipeline())


# pylint: disable=too-many-positional-arguments
def upload_data_tarball_pipeline(
    config: dict,
    file_names: list[str],
    data_list: list[bytes],
    remote_path: str,
    encrypt: bool,
    compress: bool,
) -> None:
    """
    Entrypoint of upload data tarball pipeline.
    Accepts list of file names and corresponding data.
    """
    assert len(file_names) == len(data_list)

    builder = PipelineBuilder(config)

    estimated_size = calc_aligned_data_size(data_list, alignment=BLOCKSIZE)
    estimated_size = calc_tarball_size(file_names, estimated_size)
    builder.build_read_data_tarball_stage(file_names, data_list)
    if compress:
        builder.build_compress_stage()
    if encrypt:
        builder.build_encrypt_stage()
        estimated_size = _calc_encrypted_size(config, estimated_size)
    # Assuming actual size after compression is not larger than estimated_size
    # If it is not, number of chunks may exceed the maximum allowed count and upload will fail
    builder.build_uploading_stage(remote_path, estimated_size)

    run(builder.pipeline())


def upload_file_pipeline(
    config: dict, local_path: Path, remote_path: str, encrypt: bool, delete_after: bool
) -> None:
    """
    Entrypoint of upload file pipeline.
    """
    builder = PipelineBuilder(config)

    estimated_size = calc_aligned_files_size([local_path])
    builder.build_read_file_stage(file_path=local_path)
    if encrypt:
        builder.build_encrypt_stage()
        estimated_size = _calc_encrypted_size(config, estimated_size)
    builder.build_uploading_stage(remote_path, estimated_size)
    if delete_after:
        builder.build_delete_files_stage([local_path])

    run(builder.pipeline())


# pylint: disable=too-many-positional-arguments
def upload_files_tarball_scan_pipeline(
    config: dict,
    base_path: Path,
    remote_path: str,
    encrypt: bool,
    delete_after: bool,
    compression: bool,
    tar_base_dir: str | None = None,
    exclude_file_names: list[str] | None = None,
    file_reader: Callable[[Path], bytes] | None = None,
) -> None:
    """
    Entrypoint of upload files tarball pipeline.
    """
    builder = PipelineBuilder(config)

    estimated_size = calc_aligned_files_size_scan(
        base_path, exclude_file_names, alignment=BLOCKSIZE
    )
    estimated_size = calc_tarball_size_scan(
        base_path, estimated_size, exclude_file_names
    )
    builder.build_read_files_tarball_scan_stage(
        base_path, tar_base_dir, exclude_file_names, file_reader
    )
    if compression:
        builder.build_compress_stage()
    if encrypt:
        builder.build_encrypt_stage()
        estimated_size = _calc_encrypted_size(config, estimated_size)
    # Assuming actual size after compression is not larger than estimated_size
    # If it is not, number of chunks may exceed the maximum allowed count and upload will fail
    builder.build_uploading_stage(remote_path, estimated_size)
    if delete_after:
        builder.build_delete_files_scan_stage(base_path, exclude_file_names)

    run(builder.pipeline())


# pylint: disable=too-many-positional-arguments
def upload_files_tarball_pipeline(
    config: dict,
    base_path: Path,
    file_relative_paths: list[Path],
    remote_path: str,
    encrypt: bool,
    delete_after: bool,
    compression: bool,
) -> None:
    """
    Entrypoint of upload files tarball pipeline.
    """
    builder = PipelineBuilder(config)
    file_absolute_paths = [base_path / rel_path for rel_path in file_relative_paths]

    estimated_size = calc_aligned_files_size(file_absolute_paths, alignment=BLOCKSIZE)
    estimated_size = calc_tarball_size(
        [str(f) for f in file_relative_paths], estimated_size
    )
    builder.build_read_files_tarball_stage(base_path, file_relative_paths)
    if compression:
        builder.build_compress_stage()
    if encrypt:
        builder.build_encrypt_stage()
        estimated_size = _calc_encrypted_size(config, estimated_size)
    # Assuming actual size after compression is not larger than estimated_size
    # If it is not, number of chunks may exceed the maximum allowed count and upload will fail
    builder.build_uploading_stage(remote_path, estimated_size)
    if delete_after:
        builder.build_delete_files_stage(file_absolute_paths)

    run(builder.pipeline())


def decrypt_data_pipeline(config: dict, data: bytes) -> bytes:
    """Decrypt bytes already in memory using the standard download stages."""
    builder = PipelineBuilder(config)
    builder.build_iterable_stage([data])
    builder.build_decrypt_stage()
    builder.build_collect_data_stage()
    return run_and_return_first(builder.pipeline())


def download_data_pipeline(config: dict, remote_path: str, decrypt: bool) -> bytes:
    """
    Entrypoint of download data pipeline.
    """
    builder = PipelineBuilder(config)

    builder.build_download_storage_stage(remote_path)
    if decrypt:
        builder.build_decrypt_stage()
    builder.build_collect_data_stage()

    return run_and_return_first(builder.pipeline())


def download_data_tarball_pipeline(
    config: dict,
    remote_path: str,
    decrypt: bool,
    decompress: bool,
) -> list[tuple[str, bytes]]:
    """
    Entrypoint of download data tarball pipeline.
    Downloads tarball and unpacks it into list of (filename, data) pairs.
    """
    builder = PipelineBuilder(config)

    builder.build_download_storage_stage(remote_path)
    if decrypt:
        builder.build_decrypt_stage()
    if decompress:
        builder.build_decompress_stage()
    builder.build_unpack_data_tarball_stage()

    return run_and_collect_all(builder.pipeline())


def download_file_pipeline(
    config: dict,
    remote_path: str,
    local_path: Path | BinaryIO,
    decrypt: bool,
    decompress: bool,
) -> None:
    """
    Entrypoint of download file pipeline.
    """
    builder = PipelineBuilder(config)

    builder.build_download_storage_stage(remote_path)
    if decrypt:
        builder.build_decrypt_stage()
    if decompress:
        builder.build_decompress_stage()
    builder.build_write_file_stage(local_path)

    run(builder.pipeline())


def download_files_pipeline(
    config: dict, remote_path: str, local_path: Path, decrypt: bool, decompress: bool
) -> None:
    """
    Entrypoint of download files pipeline.
    """
    builder = PipelineBuilder(config)

    builder.build_download_storage_stage(remote_path)
    if decrypt:
        builder.build_decrypt_stage()
    if decompress:
        builder.build_decompress_stage()
    builder.build_write_files_stage(local_path)

    run(builder.pipeline())


def copy_object_pipeline(config: dict, entry: CopyEntry) -> None:
    """
    Entrypoint of copy object pipeline. Object data is copied as is.
    """
    if entry.server_side:
        get_storage_engine(entry.dst_storage).copy_object(
            entry.src_storage["credentials"]["bucket"], entry.src_path, entry.dst_path
        )
        return

    if entry.size == 0:
        # Uploading stage doesn't create an object if there is no data to upload
        get_storage_engine(entry.dst_storage).upload_data(b"", entry.dst_path)
        return

    builder = PipelineBuilder(config)

    builder.build_download_storage_stage(
        entry.src_path, storage_config=entry.src_storage
    )
    builder.build_uploading_stage(
        entry.dst_path, entry.size, storage_config=entry.dst_storage
    )

    run(builder.pipeline())


def delete_multiple_storage_pipeline(config: dict, remote_paths: Sequence[str]) -> None:
    """
    Entrypoint of delete multiple objects pipeline.
    """
    builder = PipelineBuilder(config)

    builder.build_delete_multiple_storage_stage(remote_paths)

    run(builder.pipeline())


def run(pipeline: Iterable[Any]) -> None:
    """
    Run pipeline until it is complete.
    """
    _, outer_error, outer_traceback = sys.exc_info()
    try:
        itr = iter(pipeline)
        exhaust_iterator(itr)
    except ValueError as e:
        if not _ignore_invalid_thread_id_error(e, outer_error, outer_traceback):
            raise


def run_and_return_first(pipeline: Iterable[Any]) -> Any:
    """
    Run pipeline until it is complete and return first its result.
    """
    _, outer_error, outer_traceback = sys.exc_info()
    itr = iter(pipeline)
    result_fetched = False

    try:
        result = next(itr)  # Fetch and save first item
        result_fetched = True
        exhaust_iterator(itr)
    except ValueError as e:
        if (
            not _ignore_invalid_thread_id_error(e, outer_error, outer_traceback)
            or not result_fetched
        ):
            raise

    return result


def run_and_collect_all(pipeline: Iterable[Any]) -> list[Any]:
    """
    Run pipeline until it is complete and collect all results.
    """
    _, outer_error, outer_traceback = sys.exc_info()
    itr = iter(pipeline)
    results: list = []

    try:
        for item in itr:
            if isinstance(item, Iterator):
                results.extend(item)
            else:
                results.append(item)
    except ValueError as e:
        if not _ignore_invalid_thread_id_error(e, outer_error, outer_traceback):
            raise

    return results


def _ignore_invalid_thread_id_error(
    error: ValueError,
    outer_error: BaseException | None,
    outer_traceback: TracebackType | None,
) -> bool:
    # The library "stopit" (dependency of "pypeln") as of version 1.1.2
    # incorrectly handles a timeout exception sent to an already terminated
    # thread as an internal error.
    # https://github.com/glenfant/stopit/blob/dda4bd181d1d29ab1fb22314dc9bde0e3c931abc/src/stopit/threadstop.py#L37
    if "Invalid thread ID" not in str(error):
        return False

    # A cleanup error also inherits the caller's active exception when a pipeline
    # runs inside an except block (e.g. saving FAILED metadata). Only restore an
    # error raised by this pipeline; a re-raised outer error has a new traceback.
    pipeline_error = error.__context__
    if pipeline_error is not None and (
        pipeline_error is not outer_error
        or pipeline_error.__traceback__ is not outer_traceback
    ):
        raise pipeline_error from None

    logging.warning(
        "Thread ID error due to incorrect handling of thread termination in stopit library, skipping",
        exc_info=True,
    )
    return True


def _calc_encrypted_size(config: dict, data_size: int) -> int:
    enc_conf = config["encryption"]
    chunk_size = enc_conf["chunk_size"]
    metadata_size = get_encryption(enc_conf["type"], enc_conf).metadata_size()

    return calc_encrypted_size(data_size, chunk_size, metadata_size)
