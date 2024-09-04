"""
Various auxiliary functions
"""

import os
from urllib.parse import quote as _quote

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
