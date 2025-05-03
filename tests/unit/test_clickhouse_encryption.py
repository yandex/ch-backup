"""
Unit tests clickhouse encryption.
"""

from itertools import chain

from ch_backup.clickhouse.encryption import (
    EncryptedFile,
    EncryptedFileAlgorithm,
    EncryptedFileHeader,
)
from tests.unit.utils import assert_equal, parametrize

PIPELINE_TEST_COUNT = 100


@parametrize(
    {
        "id": "AES_128_CTR",
        "args": {
            "binary_data": bytes(
                chain.from_iterable(
                    [  # fmt: skip
                        [0, 0, 0, 0, 0],
                        [0, 0],  # algorithm
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 1, 2, 3, 4, 5, 6, 7],
                        [8, 9, 10, 11, 12, 13, 14, 15],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                    ]
                )
            ),
            "expected_algorithm": EncryptedFileAlgorithm.AES_128_CTR,
            "expected_iv_hex": "000102030405060708090a0b0c0d0e0f",
        },
    },
    {
        "id": "AES_192_CTR",
        "args": {
            "binary_data": bytes(
                chain.from_iterable(
                    [  # fmt: skip
                        [0, 0, 0, 0, 0],
                        [1, 0],  # algorithm
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 1, 2, 3, 4, 5, 6, 7],
                        [8, 9, 10, 11, 12, 13, 14, 15],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                    ]
                )
            ),
            "expected_algorithm": EncryptedFileAlgorithm.AES_192_CTR,
            "expected_iv_hex": "000102030405060708090a0b0c0d0e0f",
        },
    },
    {
        "id": "AES_256_CTR",
        "args": {
            "binary_data": bytes(
                chain.from_iterable(
                    [  # fmt: skip
                        [0, 0, 0, 0, 0],
                        [2, 0],  # algorithm
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 1, 2, 3, 4, 5, 6, 7],
                        [8, 9, 10, 11, 12, 13, 14, 15],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                        [0, 0, 0, 0, 0, 0, 0, 0],
                    ]
                )
            ),
            "expected_algorithm": EncryptedFileAlgorithm.AES_256_CTR,
            "expected_iv_hex": "000102030405060708090a0b0c0d0e0f",
        },
    },
)
def test_encrypted_file_header_constructor(
    binary_data: bytes,
    expected_algorithm: EncryptedFileAlgorithm,
    expected_iv_hex: str,
) -> None:
    efh = EncryptedFileHeader(binary_data)

    assert_equal(efh.algorithm, expected_algorithm)
    assert_equal(efh.iv_hex, expected_iv_hex)


def test_encrypted_file_constructor() -> None:
    binary_data = bytes(
        chain.from_iterable(
            [  # fmt: skip
                [0, 0, 0, 0, 0, 0, 0, 0],
                [0, 0, 0, 0, 0, 0, 0, 0],
                [0, 0, 0, 0, 0, 0, 0, 0],
                [0, 0, 0, 0, 0, 0, 0, 0],
                [0, 0, 0, 0, 0, 0, 0, 0],
                [0, 0, 0, 0, 0, 0, 0, 0],
                [0, 0, 0, 0, 0, 0, 0, 0],
                [0, 0, 0, 0, 0, 0, 0, 0],
                [0, 1, 2, 3, 4, 5, 6, 7],
                [8, 9, 10, 11, 12, 13, 14, 15],
            ]
        )
    )

    ef = EncryptedFile(binary_data)

    assert_equal(ef.data_hex, "000102030405060708090a0b0c0d0e0f")
