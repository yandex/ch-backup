"""
Auxiliary functions for calculation sizes of backped data blocks.
"""

import math
from pathlib import Path
from tarfile import BLOCKSIZE, LENGTH_NAME
from typing import Iterable, List, Optional

from ch_backup.util import scan_dir_files


def calc_aligned_files_size_scan(
    base_path: Path, exclude_file_names: Optional[List[str]] = None, alignment: int = 1
) -> int:
    """
    Calculate total size of files on disk with padding added after each file.
    """
    size = 0
    for file in scan_dir_files(base_path, exclude_file_names):
        filepath = base_path / file
        filesize = filepath.stat().st_size
        remainder = filesize % alignment
        if remainder > 0:
            filesize += alignment - remainder
        size += filesize
    return size


def calc_aligned_files_size(files: List[Path], alignment: int = 1) -> int:
    """
    Calculate total size of files on disk with padding added after each file.
    """
    size = 0
    for file in files:
        filesize = file.stat().st_size
        remainder = filesize % alignment
        if remainder > 0:
            filesize += alignment - remainder
        size += filesize
    return size


def _calc_tar_blocks_size_for_names(
    names: Iterable[str], aligned_files_size: int
) -> int:
    result = aligned_files_size
    for name in names:
        if len(name) <= LENGTH_NAME:
            result += BLOCKSIZE  # file header
        else:
            result += (
                math.ceil(len(name) / BLOCKSIZE) + 2
            ) * BLOCKSIZE  # long name header + name data + file header
    return result


def calc_tarball_size_scan(
    dir_path: Path,
    aligned_files_size: int,
    exclude_file_names: Optional[List[str]] = None,
) -> int:
    """
    Calculate tarball (TAR archive) size.

    Args:
        dir_path: Directory with files archived in the tarball.
        aligned_files_size: Summed size of all files including padding on BLOCKSIZE boundary
            for each file.
        exclude_file_names: File names that will not be included in tarball.
    """
    return _calc_tar_blocks_size_for_names(
        scan_dir_files(dir_path, exclude_file_names), aligned_files_size
    )


def calc_tarball_size(file_names: List[str], aligned_files_size: int) -> int:
    """
    Calculate tarball (TAR archive) size.

    Args:
        file_names: Names of files archived in the tarball.
        aligned_files_size: Summed size of all files including padding on BLOCKSIZE boundary
            for each file.
    """
    return _calc_tar_blocks_size_for_names(file_names, aligned_files_size)


def calc_encrypted_size(
    data_size: int, encrypt_chunk_size: int, encrypt_metadata_size: int
) -> int:
    """
    Calculate size of encrypted data.
    """
    return data_size + math.ceil(data_size / encrypt_chunk_size) * encrypt_metadata_size
