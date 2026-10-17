"""
Unit tests for parser of ClickHouse object storage metadata files.
"""

import pytest

from ch_backup.clickhouse.object_storage_metadata import (
    ObjectStorageMetadata,
    ObjectStorageMetadataError,
    StorageObject,
    parse_object_storage_metadata,
    replace_object_keys,
)


@pytest.mark.parametrize(
    ["data", "expected"],
    [
        pytest.param(
            b"2\n1\t2048\n2048\tqwe/rtyuiopasdfghjkl\n0\n",
            ObjectStorageMetadata(
                version=2,
                objects=[StorageObject("qwe/rtyuiopasdfghjkl", 2048)],
                ref_count=0,
                read_only=False,
                inline_data="",
            ),
            id="version 2",
        ),
        pytest.param(
            b"3\n2\t300\n100\tabc/aaa\n200\tabc/bbb\n2\n1\n",
            ObjectStorageMetadata(
                version=3,
                objects=[StorageObject("abc/aaa", 100), StorageObject("abc/bbb", 200)],
                ref_count=2,
                read_only=True,
                inline_data="",
            ),
            id="version 3 with several objects",
        ),
        pytest.param(
            b"4\n0\t0\n0\n0\ninline\\tdata\\n\n",
            ObjectStorageMetadata(
                version=4,
                objects=[],
                ref_count=0,
                read_only=False,
                inline_data="inline\tdata\n",
            ),
            id="version 4 with inline data",
        ),
        pytest.param(
            b"5\n1\t304\n304\tdata/krd/tllxsdkllurqmqpfryodupnpcdnki\n0\n0\n\n",
            ObjectStorageMetadata(
                version=5,
                objects=[StorageObject("data/krd/tllxsdkllurqmqpfryodupnpcdnki", 304)],
                ref_count=0,
                read_only=False,
                inline_data="",
            ),
            id="version 5 with full object key",
        ),
        pytest.param(
            b"4\n1\t10\n10\tkey\\\\with\\tescapes\\x41\n1\n0\n\n",
            ObjectStorageMetadata(
                version=4,
                objects=[StorageObject("key\\with\tescapesA", 10)],
                ref_count=1,
                read_only=False,
                inline_data="",
            ),
            id="escaped key",
        ),
    ],
)
def test_parse(data: bytes, expected: ObjectStorageMetadata) -> None:
    assert parse_object_storage_metadata(data) == expected


@pytest.mark.parametrize(
    ["data", "error"],
    [
        pytest.param(
            b"1\n1\t10\n10\tdata/abc/def\n0\n",
            "version 1 is not supported",
            id="absolute paths",
        ),
        pytest.param(
            b"6\n1\t10\n10\tdata/abc/def\n0\n0\n\n",
            "Unknown object storage metadata version 6",
            id="unknown version",
        ),
    ],
)
def test_unsupported_versions(data: bytes, error: str) -> None:
    with pytest.raises(ObjectStorageMetadataError, match=error):
        parse_object_storage_metadata(data)


def test_replace_object_keys() -> None:
    data = b"5\n2\t300\n100\tdata/abc/aaa\n200\tdata/abc/bbb\n2\n1\ninline\n"

    result = replace_object_keys(data, ["copy/b1/abc/aaa", "copy/b1/a\tb"])

    assert result == (
        b"5\n2\t300\n100\tcopy/b1/abc/aaa\n200\tcopy/b1/a\\tb\n2\n1\ninline\n"
    )
    assert parse_object_storage_metadata(result).objects == [
        StorageObject("copy/b1/abc/aaa", 100),
        StorageObject("copy/b1/a\tb", 200),
    ]


def test_replace_object_keys_with_wrong_number_of_keys() -> None:
    with pytest.raises(ObjectStorageMetadataError):
        replace_object_keys(b"5\n1\t100\n100\tdata/abc\n0\n0\n\n", [])


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"not a metadata file", id="garbage"),
        pytest.param(b"3\n2\t20\n10\tabc\n", id="truncated"),
        pytest.param(b"3\n1\t10\n10\tabc\n0\nmaybe\n", id="invalid read-only flag"),
        pytest.param(b"2\n0\t0\n0\nTRAILING_SECRET\n", id="trailing content v2"),
        pytest.param(b"5\n0\t0\n0\n0\n\nTRAILING_SECRET\n", id="trailing content v5"),
    ],
)
def test_malformed(data: bytes) -> None:
    with pytest.raises(ObjectStorageMetadataError):
        parse_object_storage_metadata(data)
