"""
Free functions that create and run pipelines. Can be started in multiprocessing pool.
"""
from pathlib import Path
from typing import Any, AnyStr, List, Sequence

from ch_backup.storage.async_pipeline.pipeline_builder import (PipelineBuilder, PypelnStage)
from ch_backup.util import exhaust_iterator, total_files_size


def upload_data_pipeline(config: dict, data: AnyStr, remote_path: str, encrypt: bool) -> None:
    """
    Entrypoint of upload data pipeline.
    """
    builder = PipelineBuilder(config)

    builder.build_iterable_stage([data])
    if encrypt:
        builder.build_encrypt_stage()
    builder.build_uploading_stage(remote_path, len(data))

    run(builder.pipeline())


def upload_file_pipeline(config: dict, local_path: Path, remote_path: str, encrypt: bool, delete_after: bool) -> None:
    """
    Entrypoint of upload file pipeline.
    """
    builder = PipelineBuilder(config)

    builder.build_read_file_stage(file_path=local_path)
    if encrypt:
        builder.build_encrypt_stage()
    builder.build_uploading_stage(remote_path, total_files_size([local_path]))
    if delete_after:
        builder.build_delete_files_stage([local_path])

    run(builder.pipeline())


def upload_files_tarball_pipeline(config: dict, base_path: Path, file_relative_paths: List[Path], remote_path: str,
                                  encrypt: bool, delete_after: bool) -> None:
    """
    Entrypoint of upload files tarball pipeline.
    """
    builder = PipelineBuilder(config)
    file_absolute_paths = [base_path / rel_path for rel_path in file_relative_paths]

    builder.build_read_files_tarball_stage(base_path, file_relative_paths)
    if encrypt:
        builder.build_encrypt_stage()
    builder.build_uploading_stage(remote_path, total_files_size(file_absolute_paths))
    if delete_after:
        builder.build_delete_files_stage(file_absolute_paths)

    run(builder.pipeline())


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


def download_file_pipeline(config: dict, remote_path: str, local_path: Path, decrypt: bool) -> None:
    """
    Entrypoint of download file pipeline.
    """
    builder = PipelineBuilder(config)

    builder.build_download_storage_stage(remote_path)
    if decrypt:
        builder.build_decrypt_stage()
    builder.build_write_file_stage(local_path)

    run(builder.pipeline())


def download_files_pipeline(config: dict, remote_path: str, local_path: Path, decrypt: bool) -> None:
    """
    Entrypoint of download files pipeline.
    """
    builder = PipelineBuilder(config)

    builder.build_download_storage_stage(remote_path)
    if decrypt:
        builder.build_decrypt_stage()
    builder.build_write_files_stage(local_path)

    run(builder.pipeline())


def delete_multiple_storage_pipeline(config: dict, remote_paths: Sequence[str]) -> None:
    """
    Entrypoint of delete multiple objects pipeline.
    """
    builder = PipelineBuilder(config)

    builder.build_delete_multiple_storage_stage(remote_paths)

    run(builder.pipeline())


def run(pipeline: PypelnStage) -> None:
    """
    Run pipeline until it is complete.
    """
    itr = iter(pipeline)
    exhaust_iterator(itr)


def run_and_return_first(pipeline: PypelnStage) -> Any:
    """
    Run pipeline until it is complete and return first its result.
    """
    itr = iter(pipeline)

    result = next(itr)  # Fetch and save first item
    exhaust_iterator(itr)

    return result
