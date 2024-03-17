"""
Auxiliary functions for calculation sizes of backped data blocks.
"""

import math
from pathlib import Path
from tarfile import BLOCKSIZE, LENGTH_NAME
from typing import Callable, List

from ch_backup.util import scan_dir_files


def file_filter(p: str) -> bool:
    """
    TODO: DO SMTH
    """
    return not p.endswith("/frozen_metadata.txt")


def calc_aligned_files_size(
    base_path: Path, file_filter_: Callable = None, alignment: int = 1
) -> int:
    """
    Calculate total size of files on disk with padding added after each file.
    """
    size = 0
    for file in scan_dir_files(base_path, file_filter_):
        filepath = base_path / file
        filesize = filepath.stat().st_size
        remainder = filesize % alignment
        if remainder > 0:
            filesize += alignment - remainder
        size += filesize
    return size


def calc_aligned_files_size_in_memory(files: List[Path], alignment: int = 1) -> int:
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


def calc_tarball_size(
    dir_path: Path, file_filter_: Callable, aligned_files_size: int
) -> int:
    """
    Calculate tarball (TAR archive) size.

    Args:
        file_names: Names of files archived in the tarball.
        aligned_files_size: Summed size of all files including padding on BLOCKSIZE boundary
            for each file.
    """
    result = aligned_files_size
    for name in scan_dir_files(dir_path):
        if file_filter_ is None or file_filter_(name):
            if len(name) < LENGTH_NAME:
                result += BLOCKSIZE  # file header
            else:
                result += (
                    math.ceil(len(name) / BLOCKSIZE) + 2
                ) * BLOCKSIZE  # long name header + name data + file header
    return result


def calc_tarball_size_in_memory(file_names: List[str], aligned_files_size: int) -> int:
    """
    Calculate tarball (TAR archive) size.

    Args:
        file_names: Names of files archived in the tarball.
        aligned_files_size: Summed size of all files including padding on BLOCKSIZE boundary
            for each file.
    """
    result = aligned_files_size
    for name in file_names:
        if len(name) < LENGTH_NAME:
            result += BLOCKSIZE  # file header
        else:
            result += (
                math.ceil(len(name) / BLOCKSIZE) + 2
            ) * BLOCKSIZE  # long name header + name data + file header
    return result


def calc_encrypted_size(
    data_size: int, encrypt_chunk_size: int, encrypt_metadata_size: int
) -> int:
    """
    Calculate size of encrypted data.
    """
    return data_size + math.ceil(data_size / encrypt_chunk_size) * encrypt_metadata_size
