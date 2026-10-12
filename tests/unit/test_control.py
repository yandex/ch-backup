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


class TestGetLocalAccessStatements:
    """
    Tests for reading access entities of local storages of a replica.
    """

    @staticmethod
    def _ctl() -> tuple[ClickhouseCTL, mock.Mock]:
        """Helper: a control object over a replica with one entity of each type."""
        ch_ctl, client = TestCreateTable._make_ctl()  # pylint: disable=protected-access
        names = {
            "system.roles": ["r`1"],
            "system.settings_profiles": ["p1"],
            "system.users": ["u1"],
            "system.quotas": [],
        }

        def query(sql: str) -> dict | str:
            """Answer the access queries of a replica."""
            for table, entities in names.items():
                if f"FROM {table} " in sql:
                    return {"data": [{"name": name} for name in entities]}
            if "FROM system.row_policies" in sql:
                return {
                    "data": [
                        {"short_name": "rp1", "database": "db1", "table": "t1"},
                        {"short_name": "rp2", "database": "db1", "table": ""},
                    ]
                }
            if sql.startswith("SHOW GRANTS"):
                return (
                    "GRANT SELECT ON db1.* TO u1\nGRANT r1 TO u1" if "u1" in sql else ""
                )
            if sql.startswith("SHOW CREATE"):
                return sql.split(" FORMAT")[0].replace("SHOW CREATE", "CREATE") + " X"
            return ""

        client.query.side_effect = query
        return ch_ctl, client

    def test_bare_roles_and_profiles_go_first_and_are_altered_after_users(self) -> None:
        """
        Roles and users may refer to profiles and profiles to roles and users.
        """
        ch_ctl, _ = self._ctl()

        assert ch_ctl.get_local_access_statements() == [
            "CREATE ROLE `r\\`1`",
            "CREATE SETTINGS PROFILE `p1`",
            "CREATE USER `u1` X",
            "ALTER ROLE `r\\`1` X",
            "ALTER SETTINGS PROFILE `p1` X",
            "CREATE ROW POLICY `rp1` ON `db1`.`t1` X",
            "CREATE ROW POLICY `rp2` ON `db1`.* X",
            "GRANT SELECT ON db1.* TO u1",
            "GRANT r1 TO u1",
        ]

    def test_only_local_storages_are_read_and_dropped(self) -> None:
        """
        Entities from users.xml or shared through ZooKeeper must stay as they are.
        """
        ch_ctl, client = self._ctl()

        ch_ctl.drop_local_access_entities()

        queries = [call.args[0] for call in client.query.call_args_list]
        assert all(
            "WHERE storage IN (SELECT name FROM system.user_directories "
            "WHERE type = 'local_directory')" in sql
            for sql in queries
            if sql.startswith("SELECT")
        )
        assert [sql for sql in queries if sql.startswith("DROP")] == [
            "DROP ROLE IF EXISTS `r\\`1`",
            "DROP SETTINGS PROFILE IF EXISTS `p1`",
            "DROP USER IF EXISTS `u1`",
            "DROP ROW POLICY IF EXISTS `rp1` ON `db1`.`t1`",
            "DROP ROW POLICY IF EXISTS `rp2` ON `db1`.*",
        ]


def test_only_named_collections_created_by_sql_are_read() -> None:
    """
    A collection from the server config created by SQL elsewhere would clash with the
    config of that host.
    """
    ch_ctl, client = TestCreateTable._make_ctl()  # pylint: disable=protected-access
    client.query.return_value = {
        "data": [{"name": "nc1", "create_query": "CREATE ..."}]
    }

    assert ch_ctl.get_named_collections_create_queries() == {"nc1": "CREATE ..."}
    assert "WHERE source = 'SQL'" in client.query.call_args.args[0]
