from typing import List
from unittest.mock import Mock

import pytest

from ch_backup.calculators import (calc_aligned_files_size, calc_encrypted_size, calc_tarball_size)

LENGTH_NAME = 100


@pytest.mark.parametrize(
    'file_sizes, alignment, expected_size',
    [
        ([1], 1, 1),
        ([1, 1], 1, 2),
        ([1], 512, 512),
        ([512], 512, 512),
        ([1, 512], 1, 513),
        ([1, 512], 2, 514),
        ([1, 511], 2, 514),
        ([1, 512], 512, 1024),
        ([1, 512], 513, 1026),
        ([512, 512], 512, 1024),
        ([1, 1, 1], 1, 3),
    ],
)
def test_calc_aligned_file_size(file_sizes: List[int], alignment: int, expected_size: int) -> None:
    files = []
    for file_size in file_sizes:
        file = Mock()
        file.stat.return_value.st_size = file_size
        files.append(file)

    assert calc_aligned_files_size(files, alignment) == expected_size  # type: ignore[arg-type]


@pytest.mark.parametrize(
    'name_lens, data_size, expected_size',
    [
        ([1], 512, 1024),  # 1 header + 1 data block
        ([1, 1], 1024, 2048),  # 2 header + 2 data blocks
        ([LENGTH_NAME - 1], 512, 1024),  # 1 header + 1 data block
        ([LENGTH_NAME], 512, 2048),  # header + long name header + 1 name data block + 1 data block
        ([LENGTH_NAME + 413], 512, 2560),  # header + long name header + 2 name data blocks + 1 data block
        ([
            LENGTH_NAME,
            LENGTH_NAME + 1,
        ], 1024, 4096),  # 2 header + 2 long name header + 2 name data block + 2 data block
    ],
)
def test_calc_tarball_size(name_lens: List[int], data_size: int, expected_size: int) -> None:
    names = ['0' * name_len for name_len in name_lens]

    assert calc_tarball_size(names, data_size) == expected_size


@pytest.mark.parametrize(
    'data_size, chunk_size, metadata_size, expected_size',
    [
        (1, 1, 0, 1),
        (1, 1, 1, 2),
        (100, 1, 1, 200),
        (100, 10, 0, 100),
        (100, 10, 10, 200),
        (101, 10, 10, 211),
        (100, 100, 0, 100),
        (512, 100, 10, 572),
    ],
)
def test_calc_encrypted_size(data_size: int, chunk_size: int, metadata_size: int, expected_size: int) -> None:
    assert calc_encrypted_size(data_size, chunk_size, metadata_size) == expected_size
