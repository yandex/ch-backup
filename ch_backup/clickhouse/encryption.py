"""
Encryption methods, implemented in ClickHouse.
"""

import os
from enum import Enum
from types import SimpleNamespace
from typing import Callable

from ch_backup.clickhouse.control import ClickhouseCTL

ENCRYPTED_FILE_HEADER_SIZE = 64
ENCRYPTED_FILE_HEADER_ALGORITHM_START = 5
ENCRYPTED_FILE_HEADER_ALGORITHM_END = 7
ENCRYPTED_FILE_HEADER_IV_START = 23
ENCRYPTED_FILE_HEADER_IV_END = 39


class EncryptedFileAlgorithm(Enum):
    """
    Encrypted file algorithm.
    """

    AES_128_CTR = 0
    AES_192_CTR = 1
    AES_256_CTR = 2


class ClickHouseEncryption:
    """
    Class responsible for calling ClickHouse encryption methods.
    """

    def __init__(self, ctl: ClickhouseCTL) -> None:
        self._ctl = ctl

    def decrypt_directory_content(
        self,
        dir_path: str,
        key_hex: str,
    ) -> None:
        """
        Decrypt directory content.
        """
        for _, _, files in os.walk(dir_path):
            for file in files:
                decrypted_data = ""
                filename = os.path.join(dir_path, file)

                with open(filename, "rb", encoding=None) as f:
                    data = f.read()
                    encrypted_file = EncryptedFile(data)

                    decrypted_data = encrypted_file.get_decrypted_data(
                        self._ctl.decrypt_aes_ctr, key_hex
                    )

                with open(filename, "w", encoding="utf-8") as f:
                    f.write(decrypted_data)


class EncryptedFileHeader(SimpleNamespace):
    """
    Encrypted file header.

    https://github.com/ClickHouse/ClickHouse/blob/v24.9.3.128-stable/src/IO/FileEncryptionCommon.h#L120
    """

    def __init__(self, encrypted_file: bytes):
        if len(encrypted_file) < ENCRYPTED_FILE_HEADER_SIZE:
            raise AssertionError("encrypted file should be at least 64 bytes")

        self._algorithm = encrypted_file[
            ENCRYPTED_FILE_HEADER_ALGORITHM_START:ENCRYPTED_FILE_HEADER_ALGORITHM_END
        ]
        self._iv = encrypted_file[
            ENCRYPTED_FILE_HEADER_IV_START:ENCRYPTED_FILE_HEADER_IV_END
        ]

        assert self._iv, "could not set initial vector in encrypted file header"

    @property
    def algorithm(self) -> EncryptedFileAlgorithm:
        """
        Encrypted file header algorithm.
        """
        return EncryptedFileAlgorithm(int.from_bytes(self._algorithm, "little"))

    @property
    def iv_hex(self) -> str:
        """
        Encrypted file header initial vector.
        """
        return self._iv.hex()


class EncryptedFile(SimpleNamespace):
    """
    Encrypted file.
    """

    def __init__(self, encrypted_file: bytes):
        if len(encrypted_file) < ENCRYPTED_FILE_HEADER_SIZE:
            raise AssertionError("encrypted file should be at least 64 bytes")

        self._header = EncryptedFileHeader(encrypted_file)
        self._data = encrypted_file[ENCRYPTED_FILE_HEADER_SIZE:]

        assert self._header, "could not set header in encrypted file"
        assert self._data, "could not set data in encrypted file"

    @property
    def header(self) -> EncryptedFileHeader:
        """
        Encrypted file header.
        """
        return self._header

    @property
    def data_hex(self) -> str:
        """
        Encrypted file data.
        """
        return self._data.hex()

    def get_decrypted_data(
        self, decrypt: Callable[[str, str, int, str], str], key_hex: str
    ) -> str:
        """
        Decrypts encrypted file content with provided decrypt function.
        """
        key_size = 128

        if self.header.algorithm == EncryptedFileAlgorithm.AES_192_CTR:
            key_size = 192
        elif self.header.algorithm == EncryptedFileAlgorithm.AES_256_CTR:
            key_size = 256

        return decrypt(
            self.data_hex,
            key_hex,
            key_size,
            self.header.iv_hex,
        )
