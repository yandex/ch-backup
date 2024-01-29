"""
Auxiliary functions for calculation sizes of backped data blocks.
"""

import math
from pathlib import Path
from tarfile import BLOCKSIZE, LENGTH_NAME
from typing import List


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


def calc_tarball_size(file_names: List[str], aligned_files_size: int) -> int:
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
