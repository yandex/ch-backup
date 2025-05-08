"""
Pipeline builder.
"""

from functools import reduce
from math import ceil
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Union

from pypeln import utils as pypeln_utils
from pypeln.thread.api.from_iterable import from_iterable

from ch_backup.compression import get_compression
from ch_backup.encryption import get_encryption
from ch_backup.storage.async_pipeline import thread_flat_map
from ch_backup.storage.async_pipeline.base_pipeline.input import thread_input
from ch_backup.storage.async_pipeline.base_pipeline.map import thread_map
from ch_backup.storage.async_pipeline.stages import (
    ChunkingStage,
    CollectDataStage,
    CompleteMultipartUploadStage,
    CompressStage,
    DecompressStage,
    DecryptStage,
    DeleteFilesScanStage,
    DeleteFilesStage,
    DeleteMultipleStorageStage,
    DownloadStorageStage,
    EncryptStage,
    RateLimiterStage,
    ReadFileStage,
    ReadFilesTarballScanStage,
    ReadFilesTarballStage,
    StartMultipartUploadStage,
    StorageUploadingStage,
    WriteFilesStage,
    WriteFileStage,
)
from ch_backup.storage.engine import get_storage_engine

# Union here is a workaround according to https://github.com/python/mypy/issues/7866
PypelnStage = Union[pypeln_utils.BaseStage]


class PipelineBuilder:
    """
    Build the whole pipeline stage by stage and return the final result.

    build_* functions can be chained.
    """

    def __init__(self, config: dict) -> None:
        self._config = config
        self._stages: List[PypelnStage] = []

    def build_iterable_stage(self, iterable: Iterable[Any]) -> "PipelineBuilder":
        """
        Build stage from arbitrary iterable.
        """
        self.append(from_iterable(iterable))
        return self

    def build_read_file_stage(self, file_path: Path) -> "PipelineBuilder":
        """
        Build reading file stage.
        """
        stage_config = self._config[ReadFileStage.stype]
        queue_size = stage_config["queue_size"]

        self.append(
            thread_input(ReadFileStage(stage_config, file_path), maxsize=queue_size)
        )

        return self

    def build_compress_stage(self) -> "PipelineBuilder":
        """
        Build compressing stage.
        """
        stage_config = self._config[CompressStage.stype]
        compressor = get_compression(stage_config["type"])
        queue_size = stage_config["queue_size"]

        self.append(
            thread_map(
                CompressStage(
                    compressor,
                ),
                maxsize=queue_size,
            )
        )

        return self

    def build_decompress_stage(self) -> "PipelineBuilder":
        """
        Build decompressing stage.
        """
        stage_config = self._config[CompressStage.stype]
        compressor = get_compression(stage_config["type"])
        queue_size = stage_config["queue_size"]

        self.append(
            thread_map(
                DecompressStage(
                    compressor,
                ),
                maxsize=queue_size,
            )
        )

        return self

    def build_read_files_tarball_scan_stage(
        self,
        dir_path: Path,
        tar_base_dir: Optional[str] = None,
        exclude_file_names: Optional[List[str]] = None,
    ) -> "PipelineBuilder":
        """
        Build reading files to tarball stage.
        """
        stage_config = self._config[ReadFilesTarballStage.stype]
        queue_size = stage_config["queue_size"]

        self.append(
            thread_input(
                ReadFilesTarballScanStage(
                    stage_config, dir_path, tar_base_dir, exclude_file_names
                ),
                maxsize=queue_size,
            )
        )

        return self

    def build_read_files_tarball_stage(
        self, dir_path: Path, file_relative_paths: List[Path]
    ) -> "PipelineBuilder":
        """
        Build reading files to tarball stage.
        """
        stage_config = self._config[ReadFilesTarballStage.stype]
        queue_size = stage_config["queue_size"]

        self.append(
            thread_input(
                ReadFilesTarballStage(stage_config, dir_path, file_relative_paths),
                maxsize=queue_size,
            )
        )

        return self

    def build_encrypt_stage(self) -> "PipelineBuilder":
        """
        Build encrypting stage.
        """
        stage_config = self._config[EncryptStage.stype]

        buffer_size = stage_config["buffer_size"]
        chunk_size = stage_config["chunk_size"]
        queue_size = stage_config["queue_size"]
        crypto = get_encryption(stage_config["type"], stage_config)

        self.append(
            thread_flat_map(ChunkingStage(chunk_size, buffer_size), maxsize=queue_size),
            thread_map(EncryptStage(crypto), maxsize=queue_size),
        )

        return self

    def build_decrypt_stage(self) -> "PipelineBuilder":
        """
        Build decrypting stage.
        """
        stage_config = self._config[EncryptStage.stype]

        buffer_size = stage_config["buffer_size"]
        chunk_size = stage_config["chunk_size"]
        queue_size = stage_config["queue_size"]

        crypto = get_encryption(stage_config["type"], stage_config)
        chunk_size += crypto.metadata_size()

        self.append(
            thread_flat_map(ChunkingStage(chunk_size, buffer_size), maxsize=queue_size),
            thread_map(DecryptStage(crypto), maxsize=queue_size),
        )

        return self

    def build_uploading_stage(
        self, remote_path: str, source_size: int
    ) -> "PipelineBuilder":
        """
        Build uploading stage.
        """
        stage_config = self._config[StorageUploadingStage.stype]

        max_chunk_count = stage_config["max_chunk_count"]
        buffer_size = stage_config["buffer_size"]
        chunk_size = stage_config["chunk_size"]
        queue_size = stage_config["queue_size"]

        rate_limiter_config = self._config["rate_limiter"]

        max_upload_rate = rate_limiter_config["max_upload_rate"]
        retry_interval = rate_limiter_config["retry_interval"]

        storage = get_storage_engine(stage_config)

        if source_size > chunk_size:
            # Adjust chunk size for multipart uploading if needed
            chunk_count = source_size / chunk_size
            if chunk_count > max_chunk_count:
                multiplier = ceil(chunk_count / max_chunk_count)
                buffer_size *= multiplier
                chunk_size *= multiplier

        stages = [
            thread_map(
                StartMultipartUploadStage(
                    stage_config, chunk_size, storage, remote_path
                ),
                maxsize=queue_size,
            ),
            thread_map(
                StorageUploadingStage(stage_config, storage, remote_path),
                maxsize=queue_size,
                workers=stage_config["uploading_threads"],
            ),
            thread_map(
                CompleteMultipartUploadStage(stage_config, storage, remote_path),
                maxsize=queue_size,
            ),
        ]

        self.append(
            thread_flat_map(
                RateLimiterStage(max_upload_rate, retry_interval),
                maxsize=queue_size,
            ),
            thread_flat_map(ChunkingStage(chunk_size, buffer_size), maxsize=queue_size),
            *stages,
        )
        return self

    def build_delete_files_scan_stage(
        self, base_path: Path, exclude_file_names: Optional[List[str]] = None
    ) -> "PipelineBuilder":
        """
        Build deleting files stage.
        """
        stage_config = self._config[DeleteFilesStage.stype]

        self.append(
            thread_map(
                DeleteFilesScanStage(stage_config, base_path, exclude_file_names)
            )
        )
        return self

    def build_delete_files_stage(self, files: List[Path]) -> "PipelineBuilder":
        """
        Build deleting files stage.
        """
        stage_config = self._config[DeleteFilesStage.stype]

        self.append(thread_map(DeleteFilesStage(stage_config, files)))
        return self

    def build_download_storage_stage(self, remote_path: str) -> "PipelineBuilder":
        """
        Build downloading from storage stage.
        """
        stage_config = self._config[DownloadStorageStage.stype]
        storage = get_storage_engine(stage_config)
        queue_size = stage_config["queue_size"]

        self.append(
            thread_input(
                DownloadStorageStage(stage_config, storage, remote_path),
                maxsize=queue_size,
            )
        )
        return self

    def build_write_files_stage(self, dir_path: Path) -> "PipelineBuilder":
        """
        Build writing files to local filesystem stage.
        """
        stage_config = self._config[WriteFilesStage.stype]

        buffer_size = stage_config["buffer_size"]
        chunk_size = stage_config["chunk_size"]
        queue_size = stage_config["queue_size"]

        self.append(
            thread_flat_map(ChunkingStage(chunk_size, buffer_size), maxsize=queue_size),
            thread_map(
                WriteFilesStage(stage_config, dir_path, buffer_size), maxsize=queue_size
            ),
        )
        return self

    def build_collect_data_stage(self) -> "PipelineBuilder":
        """
        Build collecting data stage.
        """
        self.append(thread_map(CollectDataStage()))
        return self

    def build_write_file_stage(self, file_path: Path) -> "PipelineBuilder":
        """
        Build writing single file stage.
        """
        self.append(thread_map(WriteFileStage(file_path)))
        return self

    def build_delete_multiple_storage_stage(
        self, remote_paths: Sequence[str]
    ) -> "PipelineBuilder":
        """
        Build deleting objects from storage stage.
        """
        stage_config = self._config[DeleteMultipleStorageStage.stype]
        storage = get_storage_engine(stage_config)

        self.append(
            thread_map(DeleteMultipleStorageStage(stage_config, remote_paths, storage))
        )
        return self

    def append(self, *stages: PypelnStage) -> None:
        """
        Append new stage to pipeline that is being built.
        """
        for stage in stages:
            self._stages.append(stage)

    def pipeline(self) -> PypelnStage:
        """
        Return built pipeline.
        """
        if len(self._stages) == 0:
            raise RuntimeError(
                "Pipeline must not be empty. Please build at least one stage"
            )

        # The pipeline must start with a pypeln iterable Stage object (pypeln library requirement),
        # but all helper functions, like flat_map, produce pypeln_utils.Partial wrapper.
        # Therefore, we add dummy_stage in that case
        if isinstance(self._stages[0], pypeln_utils.Partial):
            dummy_stage = from_iterable([])
            self._stages = [dummy_stage, *self._stages]

        # pipeline = stage1 | stage2 | ... | stageN
        return reduce(lambda pipeline, stage: pipeline | stage, self._stages)
