"""
Unit tests for the check of data part files against checksums.txt.
"""

import os
import random
import struct
from pathlib import Path
from typing import Callable
from unittest.mock import patch

import pytest
from clickhouse_cityhash.cityhash import CityHash128

from ch_backup.clickhouse import part_checksums
from ch_backup.clickhouse.cityhash import (
    _K0,
    _K1,
    _K3,
    _MASK,
    city_hash128_with_seed,
)
from ch_backup.clickhouse.part_checksums import check_part_files

_COUNT_TXT = b"10"
_COUNT_TXT_HASH = (0x95FC7BAFFF28F6B1, 0x924D3940D9DC0F2E)
_PRIMARY_IDX = struct.pack("<18751Q", *range(0, 300000, 16), 299999)
_PRIMARY_IDX_HASH = (0xFAB0DFFF4E96E104, 0x72BBF4E036AED372)


def _city_hash128(data: bytes) -> tuple[int, int]:
    """Helper: CityHash128 expressed through CityHash128WithSeed as in CityHash 1.0.2."""

    def fetch(offset: int) -> int:
        return int.from_bytes(data[offset : offset + 8], "little")

    if len(data) >= 16:
        return city_hash128_with_seed(data[16:], (fetch(0) ^ _K3, fetch(8)))
    if len(data) >= 8:
        return city_hash128_with_seed(
            b"", (fetch(0) ^ (len(data) * _K0 & _MASK), fetch(len(data) - 8) ^ _K1)
        )
    return city_hash128_with_seed(data, (_K0, _K1))


@pytest.mark.parametrize(
    "size",
    [
        0,
        1,
        3,
        4,
        7,
        8,
        9,
        15,
        16,
        17,
        31,
        32,
        33,
        64,
        100,
        143,
        144,
        145,
        200,
        271,
        1000,
        4113,
    ],
)
def test_city_hash128_with_seed_matches_clickhouse_cityhash(size: int) -> None:
    """
    The seeded hash gives CityHash128 of clickhouse-cityhash for every branch of lengths.
    """
    data = random.Random(size).randbytes(size)
    unseeded = CityHash128(data)

    assert _city_hash128(data) == divmod(unseeded, 1 << 64)


@pytest.mark.parametrize(
    "data,file_hash",
    [(_COUNT_TXT, _COUNT_TXT_HASH), (_PRIMARY_IDX, _PRIMARY_IDX_HASH)],
    ids=["one block", "chained blocks"],
)
def test_file_hash_matches_clickhouse(
    tmp_path: Path, data: bytes, file_hash: tuple[int, int]
) -> None:
    """
    A file is hashed as ClickHouse 26.9 hashed it in checksums.txt of a real part.
    """
    path = tmp_path / "file"
    path.write_bytes(data)

    file_hash_of_path = part_checksums._hash_file  # pylint: disable=protected-access
    assert file_hash_of_path(str(path)) == file_hash


def _varint(value: int) -> bytes:
    """Helper: encode an unsigned LEB128 integer."""
    result = bytearray()
    while value >= 0x80:
        result.append(value & 0x7F | 0x80)
        value >>= 7
    return bytes(result + bytes([value]))


def _hash_bytes(value: tuple[int, int]) -> bytes:
    """Helper: encode a 128-bit hash as ClickHouse writes it."""
    return value[0].to_bytes(8, "little") + value[1].to_bytes(8, "little")


def _compressed(data: bytes) -> bytes:
    """Helper: wrap data into compressed blocks of the NONE codec, 100 bytes each."""
    result = b""
    for offset in range(0, len(data), 100):
        chunk = data[offset : offset + 100]
        block = struct.pack("<BII", 0x02, 9 + len(chunk), len(chunk)) + chunk
        checksum = CityHash128(block)
        result += _hash_bytes(divmod(checksum, 1 << 64)) + block
    return result


def _write_part(
    path: Path,
    files: dict[str, bytes | None],
    hashes: dict[str, tuple[int, int]],
    uncompressed_sizes: dict[str, int] | None = None,
    sizes: dict[str, int] | None = None,
) -> None:
    """
    Helper: write a part with checksums.txt in format 3; .bin files are compressed,
    a file without data is a projection.
    """
    path.mkdir(exist_ok=True)
    entries = b""
    for name, data in files.items():
        content = b""
        if data is not None:
            content = _compressed(data) if name.endswith(".bin") else data
            (path / name).write_bytes(content)
        entries += (
            _varint(len(name.encode()))
            + name.encode()
            + _varint((sizes or {}).get(name, len(content)))
        )
        entries += _hash_bytes(hashes.get(name, (0, 0)))
        if data is not None and name.endswith(".bin"):
            size = (uncompressed_sizes or {}).get(name, len(data))
            entries += b"\x01" + _varint(size) + _hash_bytes((0, 0))
        else:
            entries += b"\x00"
    (path / "checksums.txt").write_bytes(
        b"checksums format version: 3\n" + _varint(len(files)) + entries
    )


def _good_part(path: Path) -> None:
    """Helper: write a part that matches its checksums."""
    _write_part(
        path,
        {
            "count.txt": _COUNT_TXT,
            "primary.idx": _PRIMARY_IDX,
            "n.bin": random.Random(0).randbytes(350),
        },
        {"count.txt": _COUNT_TXT_HASH, "primary.idx": _PRIMARY_IDX_HASH},
    )


def test_part_that_matches_its_checksums_passes(tmp_path: Path) -> None:
    """
    Uncompressed files are checked by hash and compressed ones by their blocks.
    """
    _good_part(tmp_path / "all_1_1_0")

    check_part_files(str(tmp_path / "all_1_1_0"))


@pytest.mark.parametrize("projection", [False, True])
def test_strict_reuse_checks_compressed_file_hash(
    tmp_path: Path, projection: bool
) -> None:
    """Valid compressed blocks of different data must not match the backup."""
    # pylint: disable=protected-access
    reference = tmp_path / "reference"
    reference.write_bytes(_compressed(b"a" * 250))
    expected = part_checksums._hash_file(
        str(reference)
    )  # pylint: disable=protected-access
    part = tmp_path / "part"
    data_part = part
    if projection:
        _write_part(part, {"projection.proj": None}, {})
        data_part = part / "projection.proj"
    _write_part(data_part, {"n.bin": b"a" * 250}, {"n.bin": expected})
    if projection:
        checksums = part_checksums._read_checksums(str(data_part))
        _write_part(
            part,
            {"projection.proj": None},
            {},
            sizes={"projection.proj": sum(file.size for file in checksums.values())},
        )
    check_part_files(str(part), verify_compressed_hashes=True)
    (data_part / "n.bin").write_bytes(_compressed(b"b" * 250))
    check_part_files(str(part))
    with pytest.raises(ValueError, match="does not match its hash"):
        check_part_files(str(part), verify_compressed_hashes=True)


def _flip(path: Path, offset: int) -> None:
    """Helper: flip a bit of a file keeping its size."""
    data = bytearray(path.read_bytes())
    data[offset] ^= 0x08
    path.write_bytes(bytes(data))


@pytest.mark.parametrize(
    "damage,error",
    [
        (lambda part: _flip(part / "count.txt", 0), ValueError),
        (lambda part: _flip(part / "primary.idx", 5000), ValueError),
        (lambda part: _flip(part / "n.bin", 0), ValueError),
        (lambda part: _flip(part / "n.bin", 200), ValueError),
        (lambda part: _flip(part / "checksums.txt", 0), ValueError),
        (lambda part: os.truncate(part / "n.bin", 100), ValueError),
        (lambda part: (part / "count.txt").unlink(), OSError),
        (lambda part: (part / "checksums.txt").unlink(), OSError),
    ],
    ids=[
        "uncompressed file",
        "block of a chained hash",
        "checksum of a compressed block",
        "data of a compressed block",
        "checksums.txt",
        "truncated file",
        "missing file",
        "missing checksums.txt",
    ],
)
def test_part_that_does_not_match_its_checksums_fails(
    tmp_path: Path, damage: Callable[[Path], None], error: type[Exception]
) -> None:
    """
    A file rotten with its size kept is caught as well as a file of another size.
    """
    part = tmp_path / "all_1_1_0"
    _good_part(part)
    damage(part)

    with pytest.raises(error):
        check_part_files(str(part))


def test_compressed_file_with_other_size_of_data_fails(tmp_path: Path) -> None:
    """
    Blocks with valid checksums must hold as much data as checksums.txt says.
    """
    _write_part(tmp_path / "all_1_1_0", {"n.bin": b"x" * 250}, {}, {"n.bin": 300})

    with pytest.raises(ValueError, match="bytes of data"):
        check_part_files(str(tmp_path / "all_1_1_0"))


def test_projection_is_checked_against_its_own_checksums(tmp_path: Path) -> None:
    """
    A projection is a directory listed in checksums.txt of the part.
    """
    part = tmp_path / "all_1_1_0"
    _write_part(
        part, {"count.txt": _COUNT_TXT, "p.proj": None}, {"count.txt": _COUNT_TXT_HASH}
    )
    _write_part(part / "p.proj", {"count.txt": b"20"}, {"count.txt": _COUNT_TXT_HASH})

    with pytest.raises(ValueError, match="p.proj/count.txt"):
        check_part_files(str(part))


def test_file_name_outside_of_part_fails(tmp_path: Path) -> None:
    """
    A file name from checksums.txt must stay inside the part.
    """
    _write_part(tmp_path / "all_1_1_0", {"../count.txt": _COUNT_TXT}, {})

    with pytest.raises(ValueError, match="Unexpected file name"):
        check_part_files(str(tmp_path / "all_1_1_0"))


def test_checksums_in_format_4_are_decompressed_with_clickhouse(
    tmp_path: Path,
) -> None:
    """
    The codec of format 4 depends on the version of ClickHouse.
    """
    part = tmp_path / "all_1_1_0"
    _write_part(part, {"count.txt": _COUNT_TXT}, {"count.txt": _COUNT_TXT_HASH})
    body = (part / "checksums.txt").read_bytes().split(b"\n", 1)[1]
    (part / "checksums.txt").write_bytes(b"checksums format version: 4\ncompressed")

    with patch.object(part_checksums, "run") as run:
        run.return_value.stdout = body
        check_part_files(str(part))

    assert run.call_args.args[0] == [
        "/usr/bin/clickhouse",
        "compressor",
        "--decompress",
    ]
    assert run.call_args.kwargs["input"] == b"compressed"
