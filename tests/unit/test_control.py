import pytest

from ch_backup.clickhouse.control import (
    _format_string_array,
    _get_cloud_part_checksum,
    _get_cloud_part_size,
    _parse_version,
)
from ch_backup.clickhouse.models import Disk
from ch_backup.exceptions import ClickhouseBackupError
from tests.unit.utils import parametrize


@parametrize(
    {
        "id": "empty list",
        "args": {
            "value": [],
            "result": "[]",
        },
    },
    {
        "id": "single-item list",
        "args": {
            "value": ["value"],
            "result": "['value']",
        },
    },
    {
        "id": "multi-item list",
        "args": {
            "value": ["value1", "value2"],
            "result": "['value1','value2']",
        },
    },
    {
        "id": "escaping",
        "args": {
            "value": ["`for`.bar"],
            "result": r"['\`for\`.bar']",
        },
    },
)
def test_format_string_array(value, result):
    assert _format_string_array(value) == result


@pytest.mark.parametrize(
    "version,expected",
    [
        ("25.10", [25, 10]),
        ("25.10.2.65", [25, 10, 2, 65]),
        ("25.10.2.65.dev", [25, 10, 2, 65]),
        ("25.10.2.65-dev.1", [25, 10, 2, 65]),
    ],
)
def test_parse_version(version: str, expected: list[int]) -> None:
    assert _parse_version(version) == expected


def _make_cloud_part(path, object_keys, ref_count=0):
    """Helper: create disk metadata files of a part on an object storage disk."""
    path.mkdir(parents=True)
    for file_name, keys in object_keys.items():
        objects = "".join(f"370\t{key}\n" for key in keys)
        (path / file_name).write_text(
            f"5\n{len(keys)}\t370\n{objects}{ref_count}\n0\n", encoding="utf-8"
        )
    return _get_cloud_part_checksum(str(path), list(object_keys))


def test_cloud_part_checksum_ignores_rewritten_metadata(tmp_path):
    keys = {"checksums.txt": ["abc/defg"], "count.txt": ["hij/klmn"]}

    first = _make_cloud_part(tmp_path / "first", keys, ref_count=1)
    second = _make_cloud_part(tmp_path / "second", keys, ref_count=2)

    assert first == second


def test_cloud_part_checksum_differs_for_other_objects(tmp_path):
    first = _make_cloud_part(tmp_path / "first", {"checksums.txt": ["abc/defg"]})
    second = _make_cloud_part(tmp_path / "second", {"checksums.txt": ["abc/defh"]})

    assert first != second


def test_cloud_part_checksum_differs_for_other_files(tmp_path):
    first = _make_cloud_part(tmp_path / "first", {"checksums.txt": ["abc/defg"]})
    second = _make_cloud_part(tmp_path / "second", {"count.txt": ["abc/defg"]})

    assert first != second


def test_cloud_part_checksum_of_an_empty_file(tmp_path):
    checksum = _make_cloud_part(tmp_path / "part", {"checksums.txt": []})

    assert checksum


def test_cloud_part_checksum_ignores_the_metadata_of_a_freeze(tmp_path):
    """
    Freezing a replicated table leaves a file naming the replica, which is not
    disk metadata and has no object keys in it.
    """
    path = tmp_path / "part"
    checksum = _make_cloud_part(path, {"checksums.txt": ["abc/defg"]})
    (path / "frozen_metadata.txt").write_text("1\nclickhouse01\n", encoding="utf-8")

    assert (
        _get_cloud_part_checksum(str(path), ["checksums.txt", "frozen_metadata.txt"])
        == checksum
    )


def test_cloud_part_size_is_the_size_of_its_objects(tmp_path):
    path = tmp_path / "part"
    _make_cloud_part(
        path, {"checksums.txt": ["abc/defg"], "count.txt": ["hij/klmn", "opq/rstu"]}
    )
    (path / "frozen_metadata.txt").write_text("1\nclickhouse01\n", encoding="utf-8")

    size = _get_cloud_part_size(
        str(path), ["checksums.txt", "count.txt", "frozen_metadata.txt"]
    )

    assert size == 3 * 370


def test_cloud_part_checksum_fails_on_unknown_metadata_format(tmp_path):
    path = tmp_path / "part"
    path.mkdir()
    (path / "checksums.txt").write_text("6\nabc/defg\n", encoding="utf-8")

    with pytest.raises(ClickhouseBackupError):
        _get_cloud_part_checksum(str(path), ["checksums.txt"])


@pytest.mark.parametrize(
    "disk,expected",
    [
        (Disk("s3", "/s3/", "ObjectStorage", "S3", "Local"), True),
        (Disk("s3", "/s3/", "s3"), True),
        (Disk("s3", "/s3/", "ObjectStorage", "S3", "Plain"), False),
        (Disk("s3", "/s3/", "ObjectStorage", "S3", "PlainRewritable"), False),
        (Disk("web", "/web/", "ObjectStorage", "Web", "StaticWeb"), False),
        (Disk("s3", "/s3/", "ObjectStorage", "S3", "Local", "/cache/"), False),
        (Disk("default", "/var/lib/clickhouse/", "Local"), False),
    ],
)
def test_only_disks_with_local_metadata_describe_objects(disk, expected):
    """
    Files of a plain disk are the data itself, so reading them as metadata of
    objects would break a backup that used to work.
    """
    assert disk.keeps_object_metadata is expected
