import copy
from collections.abc import Callable, Iterator
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.encryption import get_encryption
from ch_backup.storage import StorageLoader
from ch_backup.storage.async_pipeline.pipelines import (
    download_data_pipeline,
    run,
    run_and_collect_all,
    run_and_return_first,
)
from ch_backup.storage.engine import StorageOperation


@pytest.mark.parametrize("size", [0, 1, 63, 64, 65, 128, 129])
@pytest.mark.parametrize("download_chunk_size", [17, 104, 257])
def test_decrypt_downloaded_data_matches_download_pipeline(
    size: int, download_chunk_size: int
) -> None:
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["encryption"].update(key="a" * 32, chunk_size=64)
    crypto = get_encryption("nacl", config["encryption"])
    plaintext = b"x" * size
    ciphertext = b"".join(
        crypto.encrypt(plaintext[pos : pos + 64]) for pos in range(0, size, 64)
    )
    with (
        patch("ch_backup.storage.loader.get_storage_engine"),
        patch("ch_backup.storage.loader.PipelineExecutor"),
    ):
        loader = StorageLoader(config)
    storage = MagicMock()
    storage.download_part.side_effect = [
        ciphertext[pos : pos + download_chunk_size]
        for pos in range(0, len(ciphertext), download_chunk_size)
    ] + [b""]
    with patch(
        "ch_backup.storage.async_pipeline.pipeline_builder.get_storage_engine",
        return_value=storage,
    ) as get_engine:
        assert loader.decrypt_data(ciphertext) == plaintext
        get_engine.assert_not_called()
        assert download_data_pipeline(config, "metadata", decrypt=True) == plaintext
        get_engine.assert_called_once_with(
            config["storage"], operation=StorageOperation.READ
        )


def test_download_data_pipeline_passes_backup_listing_operation() -> None:
    config = copy.deepcopy(DEFAULT_CONFIG)
    storage = MagicMock()
    storage.download_part.side_effect = [b"metadata", b""]

    with patch(
        "ch_backup.storage.async_pipeline.pipeline_builder.get_storage_engine",
        return_value=storage,
    ) as get_engine:
        assert (
            download_data_pipeline(
                config,
                "metadata",
                decrypt=False,
                operation=StorageOperation.BACKUP_LISTING,
            )
            == b"metadata"
        )

    get_engine.assert_called_once_with(
        config["storage"], operation=StorageOperation.BACKUP_LISTING
    )


def test_storage_loader_caches_engines_per_retry_policy() -> None:
    config = copy.deepcopy(DEFAULT_CONFIG)
    listing_engine = MagicMock()
    listing_engine.list_dir.return_value = ["backup"]
    listing_engine.path_exists.return_value = True
    read_engine = MagicMock()
    read_engine.path_exists.return_value = True
    engines = {
        StorageOperation.BACKUP_LISTING: listing_engine,
        StorageOperation.READ: read_engine,
    }

    with (
        patch(
            "ch_backup.storage.loader.get_storage_engine",
            side_effect=lambda _config, operation: engines[operation],
        ) as get_engine,
        patch("ch_backup.storage.loader.PipelineExecutor"),
    ):
        loader = StorageLoader(config)
        assert loader.list_dir(
            "backups", operation=StorageOperation.BACKUP_LISTING
        ) == ["backup"]
        assert loader.path_exists(
            "backups/backup", operation=StorageOperation.BACKUP_LISTING
        )
        assert loader.path_exists("backups/backup/metadata")

    assert get_engine.call_args_list == [
        call(config["storage"], operation=StorageOperation.BACKUP_LISTING),
        call(config["storage"], operation=StorageOperation.READ),
    ]


def _pipeline_with_masked_error() -> Iterator[str]:
    yield from ()
    try:
        raise RuntimeError("pipeline failed")
    except RuntimeError:
        raise ValueError("Invalid thread ID 123")


def _pipeline_with_cleanup_error() -> Iterator[str]:
    yield "result"
    raise ValueError("Invalid thread ID 123")


@pytest.mark.parametrize("runner", [run, run_and_return_first, run_and_collect_all])
def test_run_restores_pipeline_error(runner: Callable[[Any], Any]) -> None:
    with pytest.raises(RuntimeError, match="pipeline failed"):
        runner(_pipeline_with_masked_error())


def test_run_ignores_cleanup_error() -> None:
    run(_pipeline_with_cleanup_error())


def test_run_and_return_first_ignores_cleanup_error() -> None:
    assert run_and_return_first(_pipeline_with_cleanup_error()) == "result"


def test_run_and_collect_all_ignores_cleanup_error() -> None:
    assert run_and_collect_all(_pipeline_with_cleanup_error()) == ["result"]


def test_run_and_return_first_does_not_ignore_error_before_result() -> None:
    def pipeline() -> Iterator[str]:
        yield from ()
        raise ValueError("Invalid thread ID 123")

    with pytest.raises(ValueError, match="Invalid thread ID"):
        run_and_return_first(pipeline())
