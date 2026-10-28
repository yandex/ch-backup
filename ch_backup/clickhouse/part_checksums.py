"""
Check of data part files against their checksums.txt.
"""

import os
import struct
from io import BytesIO
from subprocess import PIPE, run
from typing import BinaryIO, NamedTuple

from clickhouse_cityhash.cityhash import CityHash128

from ch_backup.clickhouse.cityhash import city_hash128_with_seed
from ch_backup.util import read_by_chunks

CLICKHOUSE_BINARY = "/usr/bin/clickhouse"

_FORMAT_PREFIX = b"checksums format version: "
_HASHING_BLOCK_SIZE = 2048
_HASH = struct.Struct("<QQ")
_BLOCK_HEADER = struct.Struct("<BII")


class _FileChecksum(NamedTuple):
    """
    Size and hash of a file, and the size of data of a compressed one.
    """

    size: int
    hash: tuple[int, int]
    uncompressed_size: int | None


def check_part_files(part_path: str, *, verify_compressed_hashes: bool = False) -> None:
    """
    Check that the files of a part on a local disk match its checksums.txt.

    Compressed files are checked by the checksums of their blocks, as hashing them in
    Python is too slow. For reuse, verify_compressed_hashes also binds compressed
    data to the manifest, rather than only checking each block's integrity.
    Raises ValueError, OSError or CalledProcessError on a mismatch.
    """
    _check_part_files(part_path, _read_checksums(part_path), verify_compressed_hashes)


def _check_part_files(
    part_path: str,
    checksums: dict[str, _FileChecksum],
    verify_compressed_hashes: bool,
) -> None:
    for name, checksum in checksums.items():
        path = os.path.join(part_path, name)
        if os.path.isdir(path):
            projection = _read_checksums(path)
            if verify_compressed_hashes and (
                sum(file.size for file in projection.values()) != checksum.size
                or _hash_checksums(projection) != checksum.hash
            ):
                raise ValueError(f"{path} does not match its parent checksums.txt")
            _check_part_files(path, projection, verify_compressed_hashes)
            continue
        size = os.path.getsize(path)
        if size != checksum.size:
            raise ValueError(f"{path} has size {size} instead of {checksum.size}")
        if checksum.uncompressed_size is None or verify_compressed_hashes:
            if _hash_file(path) != checksum.hash:
                raise ValueError(f"{path} does not match its hash in checksums.txt")
        else:
            uncompressed_size = _check_compressed_file(path)
            if uncompressed_size != checksum.uncompressed_size:
                raise ValueError(
                    f"{path} has {uncompressed_size} bytes of data"
                    f" instead of {checksum.uncompressed_size}"
                )


def _hash_checksums(checksums: dict[str, _FileChecksum]) -> tuple[int, int]:
    """Match MergeTreeDataPartChecksums::getTotalChecksumUInt128 for a projection.

    The parent hashes sorted file names (with UInt64 byte lengths) and file hashes.
    This binds a projection's own manifest to the backup's root manifest.
    """
    data = b"".join(
        struct.pack("<Q", len(name.encode())) + name.encode() + _HASH.pack(*value.hash)
        for name, value in sorted(checksums.items())
    )
    # ClickHouse's SipHash128 uses SipHash-2-4 with zero keys and returns both
    # halves of the final state, unlike the reference SipHash128 variant.
    mask = (1 << 64) - 1
    v0, v1, v2, v3 = (
        0x736F6D6570736575,
        0x646F72616E646F6D,
        0x6C7967656E657261,
        0x7465646279746573,
    )

    def rotate(value: int, shift: int) -> int:
        return ((value << shift) | (value >> (64 - shift))) & mask

    def rounds(count: int) -> None:
        nonlocal v0, v1, v2, v3
        for _ in range(count):
            v0 = (v0 + v1) & mask
            v1 = rotate(v1, 13) ^ v0
            v0 = rotate(v0, 32)
            v2 = (v2 + v3) & mask
            v3 = rotate(v3, 16) ^ v2
            v0 = (v0 + v3) & mask
            v3 = rotate(v3, 21) ^ v0
            v2 = (v2 + v1) & mask
            v1 = rotate(v1, 17) ^ v2
            v2 = rotate(v2, 32)

    end = len(data) - len(data) % 8
    tail = int.from_bytes(data[end:], "little") | ((len(data) & 255) << 56)
    for word in (*struct.unpack(f"<{end // 8}Q", data[:end]), tail):
        v3 ^= word
        rounds(2)
        v0 ^= word
    v2 ^= 0xFF
    rounds(4)
    return v0 ^ v1, v2 ^ v3


def _read_checksums(part_path: str) -> dict[str, _FileChecksum]:
    """
    Read checksums.txt of a part; format 4 is decompressed with ClickHouse, as its codec
    depends on the version.
    """
    with open(os.path.join(part_path, "checksums.txt"), "rb") as f:
        header = f.readline()
        data = f.read()
    if not header.startswith(_FORMAT_PREFIX):
        raise ValueError(f"Unexpected header of {part_path}/checksums.txt: {header!r}")
    version = int(header[len(_FORMAT_PREFIX) :])
    if version == 4:
        data = run(  # noqa: S603
            [CLICKHOUSE_BINARY, "compressor", "--decompress"],
            input=data,
            stdout=PIPE,
            stderr=PIPE,
            check=True,
        ).stdout
    elif version != 3:
        raise ValueError(f"Unsupported format {version} of {part_path}/checksums.txt")
    return _parse_checksums(BytesIO(data))


def _parse_checksums(buf: BinaryIO) -> dict[str, _FileChecksum]:
    """
    Parse checksums.txt in format 3.
    """
    files = {}
    for _ in range(_read_varint(buf)):
        name = _read_exact(buf, _read_varint(buf)).decode()
        if os.path.isabs(name) or ".." in name.split("/"):
            raise ValueError(f"Unexpected file name {name!r} in checksums.txt")
        size = _read_varint(buf)
        file_hash = _read_hash(buf)
        uncompressed_size = None
        if _read_exact(buf, 1)[0]:
            uncompressed_size = _read_varint(buf)
            _read_hash(buf)
        files[name] = _FileChecksum(size, file_hash, uncompressed_size)
    return files


def _check_compressed_file(path: str) -> int:
    """
    Check the checksums of compressed blocks of a file and return the size of its data.
    """
    uncompressed_size = 0
    with open(path, "rb") as f:
        while checksum := f.read(_HASH.size):
            if len(checksum) != _HASH.size:
                raise ValueError(f"{path} is truncated")
            header = _read_exact(f, _BLOCK_HEADER.size, path)
            _, compressed_size, block_uncompressed_size = _BLOCK_HEADER.unpack(header)
            f.seek(-len(header), os.SEEK_CUR)
            block = _read_exact(f, compressed_size, path)
            if divmod(CityHash128(block), 1 << 64) != _HASH.unpack(checksum):
                raise ValueError(f"{path} has a compressed block with wrong checksum")
            uncompressed_size += block_uncompressed_size
    return uncompressed_size


def _hash_file(path: str) -> tuple[int, int]:
    """
    Hash a file as ClickHouse does: CityHash128 chained over blocks of 2048 bytes.
    """
    file_hash = (0, 0)
    with open(path, "rb") as f:
        for block in read_by_chunks(f, _HASHING_BLOCK_SIZE):
            file_hash = city_hash128_with_seed(block, file_hash)
    return file_hash


def _read_hash(buf: BinaryIO) -> tuple[int, int]:
    """
    Read a 128-bit hash written by ClickHouse: two 64-bit halves in little endian.
    """
    first, second = _HASH.unpack(_read_exact(buf, _HASH.size))
    return first, second


def _read_varint(buf: BinaryIO) -> int:
    """
    Read an unsigned LEB128 integer.
    """
    result = 0
    shift = 0
    while True:
        byte = _read_exact(buf, 1)[0]
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result
        shift += 7


def _read_exact(buf: BinaryIO, size: int, name: str = "checksums.txt") -> bytes:
    """
    Read the given number of bytes.
    """
    data = buf.read(size) if size >= 0 else b""
    if len(data) != size:
        raise ValueError(f"{name} is truncated")
    return data
