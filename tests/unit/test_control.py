import errno
from hashlib import md5
from pathlib import Path
from tarfile import BLOCKSIZE
from typing import Callable
from unittest import mock

import pytest

from ch_backup.clickhouse.control import (
    ClickhouseCTL,
    _format_string_array,
    _get_cloud_part_checksum_and_size,
    _parse_version,
)
from ch_backup.clickhouse.models import DETACH_REASONS, Disk, Table
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


def test_cloud_part_of_unknown_metadata_format_is_described_by_its_files(tmp_path):
    """Such a part must not fail the backup."""
    path = tmp_path / "part"
    path.mkdir()
    (path / "checksums.txt").write_text("6\nabc/defg\n", encoding="utf-8")

    checksum, size = _get_cloud_part_checksum_and_size(str(path), ["checksums.txt"])

    assert checksum == md5((path / "checksums.txt").read_bytes()).hexdigest()
    assert size == BLOCKSIZE


def test_cloud_part_of_unknown_metadata_format_fails_when_asked(tmp_path):
    """The fallback is a default, not the only option."""
    path = tmp_path / "part"
    path.mkdir()
    (path / "checksums.txt").write_text("6\nabc/defg\n", encoding="utf-8")

    with pytest.raises(ClickhouseBackupError):
        _get_cloud_part_checksum_and_size(str(path), ["checksums.txt"], True)


def test_cloud_part_with_metadata_broken_beyond_text_is_described_by_its_files(
    tmp_path,
):
    """Invalid UTF-8 fails the read, not the parsing, and one broken file is enough."""
    path = tmp_path / "part"
    path.mkdir()
    (path / "checksums.txt").write_bytes(b"\x80\x81")
    (path / "count.txt").write_text(
        "5\n1\t370\n370\tabc/defg\n0\n0\n", encoding="utf-8"
    )

    checksum, size = _get_cloud_part_checksum_and_size(
        str(path), ["checksums.txt", "count.txt"]
    )

    assert checksum == md5((path / "checksums.txt").read_bytes()).hexdigest()
    assert size == 2 * BLOCKSIZE


def test_cloud_part_with_an_unreadable_metadata_file_fails(tmp_path):
    """The fallback reads the same files, so hiding the error would only delay it."""
    path = tmp_path / "part"
    (path / "checksums.txt").mkdir(parents=True)

    with pytest.raises(OSError):
        _get_cloud_part_checksum_and_size(str(path), ["checksums.txt"])


def test_cloud_part_without_copy_is_described_by_its_files(tmp_path):
    """Object keys matter only for a copy of the data, so they are not read."""
    disk = Disk("s3", f"{tmp_path}/", "ObjectStorage", "S3", "Local")
    table = Table("db1", "table1", "MergeTree", [], [], "", "", None)
    path = tmp_path / "shadow" / "backup1" / "store" / "all_1_1_0"
    path.mkdir(parents=True)
    (path / "checksums.txt").write_text("6\nabc/defg\n", encoding="utf-8")

    (part,) = ClickhouseCTL.scan_frozen_parts(
        table, disk, str(tmp_path / "store"), "backup1", False, True
    )

    assert part.checksum == md5((path / "checksums.txt").read_bytes()).hexdigest()
    assert part.size == BLOCKSIZE


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
        client.query.return_value = {
            "data": [{"zookeeper_path": "/zk/t1", "replica_name": "new"}]
        }
        zk_ctl = mock.MagicMock()
        zk_ctl.get_children.side_effect = lambda path: zookeeper.get(path, [])
        table = Table("db1", "table1", "ReplicatedMergeTree", [], [], "", "", None)
        return ch_ctl.get_parts_of_other_replicas(table, zk_ctl)

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
        ZooKeeper keeps merged parts for a while, the clone takes only active ones.
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
        The first replica gets nothing, as ClickHouse attaches all of detached/ then.
        """
        assert not self._parts({"/zk/t1/replicas": ["new"]})


class TestScanFrozenParts:
    """
    Tests for ClickhouseCTL.scan_frozen_parts.
    """

    _BACKUP_NAME = "backup1"

    @classmethod
    def _scan(
        cls, root: Path, on_error: Callable[[str, Exception], None] | None = None
    ) -> list[str]:
        """Helper: scan a shadow directory with a readable and a broken part."""
        disk = Disk("default", f"{root}/", "Local")
        shadow = root / "shadow" / cls._BACKUP_NAME / "data" / "db1" / "table1"
        (shadow / "all_1_1_0").mkdir(parents=True)
        (shadow / "all_1_1_0" / "checksums.txt").write_bytes(b"checksums")
        (shadow / "all_2_2_0").mkdir()
        table = Table("db1", "table1", "MergeTree", [disk], [], "", "", None)

        parts = ClickhouseCTL.scan_frozen_parts(
            table,
            disk,
            str(root / "data" / "db1" / "table1"),
            cls._BACKUP_NAME,
            on_error=on_error,
        )
        return [part.name for part in parts]

    def test_unreadable_part_fails_the_scan_by_default(self, tmp_path: Path) -> None:
        """Without a handler a broken part is not silently left out."""
        with pytest.raises(FileNotFoundError):
            self._scan(tmp_path)

    def test_unreadable_part_is_passed_to_the_handler(self, tmp_path: Path) -> None:
        """With a handler the remaining parts are still scanned."""
        errors: list[str] = []

        names = self._scan(tmp_path, on_error=lambda name, _e: errors.append(name))

        assert names == ["all_1_1_0"]
        assert errors == ["all_2_2_0"]


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
        Roles and users may refer to profiles and profiles to roles and users, and users
        may refer to each other through GRANTEES.
        """
        ch_ctl, _ = self._ctl()

        assert ch_ctl.get_local_access_statements() == [
            "CREATE ROLE `r\\`1`",
            "CREATE SETTINGS PROFILE `p1`",
            "CREATE USER IF NOT EXISTS `u1` X",
            "ALTER ROLE `r\\`1` X",
            "ALTER SETTINGS PROFILE `p1` X",
            "ALTER USER `u1` X",
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


def test_scan_detached_parts_reads_reason_from_directory_name(
    tmp_path: Path,
) -> None:
    ch_ctl, _ = TestCreateTable._make_ctl()  # pylint: disable=protected-access
    disk = Disk("default", str(tmp_path), "Local")
    ch_ctl._disks = {disk.name: disk}  # pylint: disable=protected-access
    table_path = tmp_path / "store" / "table"
    detached_path = table_path / "detached"
    normal_part = detached_path / "all_1_1_0"
    broken_part = detached_path / "broken-on-start_all_2_2_0"
    normal_part.mkdir(parents=True)
    broken_part.mkdir()
    (normal_part / "data.bin").write_bytes(b"data")
    (broken_part / "data.bin").write_bytes(b"broken")
    table = Table(
        "db1",
        "table1",
        "MergeTree",
        [disk],
        [str(table_path)],
        "",
        "",
        None,
    )

    parts = list(
        ch_ctl.scan_detached_parts(table, disk_names=["default"], reasons=[""])
    )

    assert len(parts) == 1
    assert parts[0].name == "all_1_1_0"
    assert parts[0].reason == ""
    assert parts[0].disk_name == "default"
    assert parts[0].checksum == ""

    broken_parts = list(
        ch_ctl.scan_detached_parts(
            table, disk_names=["default"], reasons=["broken-on-start"]
        )
    )
    assert [(part.name, part.reason) for part in broken_parts] == [
        ("broken-on-start_all_2_2_0", "broken-on-start")
    ]


@pytest.mark.parametrize("reason", ["", *sorted(DETACH_REASONS)])
@pytest.mark.parametrize(
    "partition_id", ["all", "202401", "20240101", "1", "1-2", "-1"]
)
@pytest.mark.parametrize(
    "suffix",
    ["", "_5", "_try1", "_5_try12"],
    ids=["regular", "mutated", "try-suffix", "mutated-try-suffix"],
)
def test_scan_detached_parts_recognizes_clickhouse_reasons(
    tmp_path: Path, reason: str, partition_id: str, suffix: str
) -> None:
    ch_ctl, _ = TestCreateTable._make_ctl()  # pylint: disable=protected-access
    disk = Disk("default", str(tmp_path), "Local")
    ch_ctl._disks = {disk.name: disk}  # pylint: disable=protected-access
    table_path = tmp_path / "store" / "table"
    part_name = f"{partition_id}_1_1_0{suffix}"
    if reason:
        part_name = f"{reason}_{part_name}"
    part_path = table_path / "detached" / part_name
    part_path.mkdir(parents=True)
    (part_path / "data.bin").write_bytes(b"data")
    table = Table(
        "db1",
        "table1",
        "MergeTree",
        [disk],
        [str(table_path)],
        "",
        "",
        None,
    )

    parts = list(
        ch_ctl.scan_detached_parts(table, disk_names=["default"], reasons=[reason])
    )

    assert [(part.name, part.reason) for part in parts] == [(part_name, reason)]


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(PermissionError("permission denied"), id="permission"),
        pytest.param(OSError(errno.EIO, "I/O error"), id="io-error"),
    ],
)
def test_scan_detached_parts_skips_inaccessible_detached_directory(
    tmp_path: Path, error: OSError
) -> None:
    ch_ctl, _ = TestCreateTable._make_ctl()  # pylint: disable=protected-access
    disk = Disk("default", str(tmp_path), "Local")
    ch_ctl._disks = {disk.name: disk}  # pylint: disable=protected-access
    table = Table(
        "db1",
        "table1",
        "MergeTree",
        [disk],
        [str(tmp_path / "store" / "table")],
        "",
        "",
        None,
    )

    with (
        mock.patch(
            "ch_backup.clickhouse.control.os.scandir",
            side_effect=error,
        ),
        mock.patch("ch_backup.clickhouse.control.logging.warning") as warning,
    ):
        assert not list(ch_ctl.scan_detached_parts(table, disk_names=["default"]))

    warning.assert_called_once()
    assert "not accessible" in warning.call_args.args[0]


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(PermissionError("permission denied"), id="permission"),
        pytest.param(OSError(errno.EIO, "I/O error"), id="io-error"),
    ],
)
def test_scan_detached_parts_skips_inaccessible_part_directory(
    tmp_path: Path, error: OSError
) -> None:
    ch_ctl, _ = TestCreateTable._make_ctl()  # pylint: disable=protected-access
    disk = Disk("default", str(tmp_path), "Local")
    ch_ctl._disks = {disk.name: disk}  # pylint: disable=protected-access
    table_path = tmp_path / "store" / "table"
    (table_path / "detached" / "all_1_1_0").mkdir(parents=True)
    table = Table(
        "db1",
        "table1",
        "MergeTree",
        [disk],
        [str(table_path)],
        "",
        "",
        None,
    )

    with (
        mock.patch(
            "ch_backup.clickhouse.control.scan_dir_files",
            side_effect=error,
        ),
        mock.patch("ch_backup.clickhouse.control.logging.warning") as warning,
    ):
        assert not list(ch_ctl.scan_detached_parts(table, disk_names=["default"]))

    warning.assert_called_once()
    assert "not accessible" in warning.call_args.args[0]


def test_scan_detached_parts_uses_selected_backing_disk_directory(
    tmp_path: Path,
) -> None:
    ch_ctl, _ = TestCreateTable._make_ctl()  # pylint: disable=protected-access
    storage_disk = Disk("object_storage", str(tmp_path), "ObjectStorage", "S3")
    cache_disk = Disk(
        "object_storage_cache",
        str(tmp_path),
        "ObjectStorage",
        "S3",
        cache_path=str(tmp_path / "cache"),
    )
    ch_ctl._disks = {  # pylint: disable=protected-access
        storage_disk.name: storage_disk,
        cache_disk.name: cache_disk,
    }
    table_path = tmp_path / "store" / "table"
    part_path = table_path / "detached" / "all_1_1_0"
    expected_checksum = _make_cloud_part(
        part_path, {"checksums.txt": ["store/object-key"]}
    )
    table = Table(
        "db1",
        "table1",
        "MergeTree",
        [cache_disk],
        [str(table_path)],
        "",
        "",
        None,
    )

    part = next(iter(ch_ctl.scan_detached_parts(table, disk_names=["object_storage"])))

    assert part.disk_name == "object_storage"
    assert part.object_storage is True
    assert part.path == str(part_path)
    assert part.checksum == expected_checksum
    assert part.size == BLOCKSIZE
