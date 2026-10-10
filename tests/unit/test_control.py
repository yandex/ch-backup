from unittest import mock

import pytest

from ch_backup.clickhouse.control import (
    ClickhouseCTL,
    _format_string_array,
    _get_cloud_part_checksum_and_size,
    _parse_version,
)
from ch_backup.clickhouse.models import Disk, Table
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
    return _get_cloud_part_checksum_and_size(str(path), list(object_keys))[0]


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
        _get_cloud_part_checksum_and_size(
            str(path), ["checksums.txt", "frozen_metadata.txt"]
        )[0]
        == checksum
    )


def test_cloud_part_size_is_the_size_of_its_objects(tmp_path):
    path = tmp_path / "part"
    _make_cloud_part(
        path, {"checksums.txt": ["abc/defg"], "count.txt": ["hij/klmn", "opq/rstu"]}
    )
    (path / "frozen_metadata.txt").write_text("1\nclickhouse01\n", encoding="utf-8")

    _, size = _get_cloud_part_checksum_and_size(
        str(path), ["checksums.txt", "count.txt", "frozen_metadata.txt"]
    )

    assert size == 3 * 370


def test_cloud_part_checksum_fails_on_unknown_metadata_format(tmp_path):
    path = tmp_path / "part"
    path.mkdir()
    (path / "checksums.txt").write_text("6\nabc/defg\n", encoding="utf-8")

    with pytest.raises(ClickhouseBackupError):
        _get_cloud_part_checksum_and_size(str(path), ["checksums.txt"])


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


class TestCreateTable:
    """
    Tests for the settings sent along with a create statement on restore.
    """

    _PLAIN_STATEMENT = (
        "CREATE TABLE db1.table1 (id UInt64) ENGINE = MergeTree ORDER BY id"
    )

    @staticmethod
    def _make_ctl() -> tuple[ClickhouseCTL, mock.Mock]:
        """Helper: build a control object with ClickHouse mocked."""
        config = {
            "data_path": "/tmp",
            "timeout": 10,
            "freeze_timeout": 10,
            "unfreeze_timeout": 10,
            "restore_replica_timeout": 10,
            "drop_replica_timeout": 10,
        }
        with (
            mock.patch("ch_backup.clickhouse.control.ClickhouseClient") as client_cls,
            mock.patch.object(ClickhouseCTL, "get_disks", return_value={}),
        ):
            client = client_cls.return_value
            client.query.return_value = "26.5.1.1"
            ch_ctl = ClickhouseCTL(config, {}, {})

        client.query.reset_mock()
        return ch_ctl, client

    @staticmethod
    def _table(create_statement: str) -> Table:
        return Table("db1", "table1", "MergeTree", [], [], "", create_statement, None)

    @pytest.mark.parametrize(
        "clause,settings",
        [
            (" UNIQUE KEY id", {"allow_experimental_unique_key": 1}),
            ("", None),
        ],
        ids=["unique key table", "plain table"],
    )
    def test_experimental_setting_is_sent_only_for_a_unique_key_table(
        self, clause: str, settings: dict | None
    ) -> None:
        """
        ClickHouse gates the clause on CREATE, which is how tables of a replicated
        database are restored. Other tables must not get the setting, as builds
        without the clause do not have it.
        """
        ch_ctl, client = self._make_ctl()
        statement = f"{self._PLAIN_STATEMENT}{clause}"

        ch_ctl.create_table(self._table(statement))

        client.query.assert_called_once_with(statement, settings=settings)


class TestGetPartsOfOtherReplicas:
    """
    Tests for picking the parts that every other replica of a table has.
    """

    @staticmethod
    def _parts(zookeeper: dict[str, list[str]]) -> set[str]:
        """Helper: run the lookup against the given ZooKeeper children."""
        ch_ctl, client = TestCreateTable._make_ctl()  # pylint: disable=protected-access

        def query(sql: str) -> dict:
            """Answer the replica and ZooKeeper queries from the given data."""
            if "system.replicas" in sql:
                return {"data": [{"zookeeper_path": "/zk/t1", "replica_name": "new"}]}
            path = sql.split("path = '")[1].split("'")[0]
            return {"data": [{"name": name} for name in zookeeper.get(path, [])]}

        client.query.side_effect = query
        table = Table("db1", "table1", "ReplicatedMergeTree", [], [], "", "", None)
        return ch_ctl.get_parts_of_other_replicas(table)

    def test_parts_are_intersected_over_other_replicas(self) -> None:
        """
        The own replica is ignored, a lagging replica narrows the result.
        """
        assert self._parts(
            {
                "/zk/t1/replicas": ["new", "r1", "r2"],
                "/zk/t1/replicas/new/parts": ["all_9_9_0"],
                "/zk/t1/replicas/r1/parts": ["all_1_1_0", "all_2_2_0", "all_3_3_0"],
                "/zk/t1/replicas/r2/parts": ["all_2_2_0", "all_3_3_0", "all_4_4_0"],
            }
        ) == {"all_2_2_0", "all_3_3_0"}

    def test_covered_parts_are_dropped_before_intersection(self) -> None:
        """
        ZooKeeper keeps the source parts of a merge or mutation for a while, and the
        clone takes only the active parts of the source replica.
        """
        assert self._parts(
            {
                "/zk/t1/replicas": ["new", "r1", "r2"],
                "/zk/t1/replicas/r1/parts": [
                    "1_0_9_1",
                    "all_0_0_0",
                    "all_1_1_0",
                    "all_0_1_1",
                    "all_2_2_0",
                    "all_2_2_0_3",
                ],
                "/zk/t1/replicas/r2/parts": [
                    "1_0_9_1",
                    "all_0_0_0",
                    "all_1_1_0",
                    "all_2_2_0_3",
                ],
            }
        ) == {"1_0_9_1", "all_2_2_0_3"}

    def test_parts_are_kept_if_a_name_does_not_parse(self) -> None:
        """
        The filter only saves downloads, so it must not fail the restore.
        """
        assert self._parts(
            {
                "/zk/t1/replicas": ["new", "r1"],
                "/zk/t1/replicas/r1/parts": ["all_0_0_0", "all_0_1_1", "bad"],
            }
        ) == {"all_0_0_0", "all_0_1_1", "bad"}

    def test_no_parts_without_other_replicas(self) -> None:
        """
        A replica that would be the first one gets nothing, as ClickHouse would
        attach everything from detached/ in that case.
        """
        assert not self._parts({"/zk/t1/replicas": ["new"]})
