"""
Various auxiliary functions
"""

import os
from typing import Any, Callable
from urllib.parse import quote as _quote

from ch_backup import logging
from ch_backup.backup.metadata.part_metadata import PartMetadata
from ch_backup.calculators import calc_encrypted_size, calc_tarball_size


def quote(value: str) -> str:
    """
    Escape given string.
    """
    return _quote(value, safe="").translate(
        {
            ord("."): "%2E",
            ord("-"): "%2D",
        }
    )


def target_part_size(
    part: PartMetadata, encryption_chunk_size: int, encryption_metadata_size: int
) -> int:
    """
    Predicts tar archive size after encryption.
    """
    tar_size = calc_tarball_size(list(part.raw_metadata.files), part.size)
    return calc_encrypted_size(
        tar_size, encryption_chunk_size, encryption_metadata_size
    )


def part_path(
    backup_path: str,
    db_name: str,
    table_name: str,
    part_name: str,
    escape_names: bool = True,
) -> str:
    """
    Return S3 path to data part.
    """
    if escape_names:
        return os.path.join(
            backup_path, "data", quote(db_name), quote(table_name), part_name
        )
    return os.path.join(backup_path, "data", db_name, table_name, part_name)


def get_escaped_if_exists(
    loader: Any,
    path_function: Callable,
    *args: Any,
    **kwargs: Any,
) -> str:
    """
    Return escaped path if it exists. Otherwise return regular path.
    """
    path = path_function(*args, escape_names=True, **kwargs)
    if loader.path_exists(path, is_dir=True):
        return path
    return path_function(*args, escape_names=False, **kwargs)


def check_data_part(
    loader: Any,
    backup_path: str,
    part: PartMetadata,
    encryption_chunk_size: int,
    encryption_metadata_size: int,
) -> bool:
    """
    Check availability of part data in storage.
    """
    try:
        remote_dir_path = get_escaped_if_exists(
            loader,
            part_path,
            part.link or backup_path,
            part.database,
            part.table,
            part.name,
        )
        remote_files = loader.list_dir(remote_dir_path)

        if remote_files == [f"{part.name}.tar"]:
            actual_size = loader.get_object_size(
                os.path.join(remote_dir_path, f"{part.name}.tar")
            )
            target_size = target_part_size(
                part, encryption_chunk_size, encryption_metadata_size
            )
            if target_size != actual_size:
                logging.warning(
                    f"Part {part.name} files stored in tar, size not match {target_size} != {actual_size}"
                )
                return False
            return True

        notfound_files = set(part.files) - set(remote_files)
        if notfound_files:
            logging.warning(
                "Some part files were not found in {}: {}",
                remote_dir_path,
                ", ".join(notfound_files),
            )
            return False

        return True

    except Exception:  # TODO: fix circular import for S3RetryingError
        logging.warning(
            f"Failed to check data part {part.name}, consider it's broken",
            exc_info=True,
        )
        return False
