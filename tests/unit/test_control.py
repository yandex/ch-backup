import errno
from hashlib import md5
from pathlib import Path
from tarfile import BLOCKSIZE
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


@pytest.fixture(name="clickhouse_ctl")
def fixture_clickhouse_ctl() -> tuple[ClickhouseCTL, mock.Mock]:
    """Build a control object with ClickHouse mocked."""
    with (
        mock.patch("ch_backup.clickhouse.control.ClickhouseClient") as client_cls,
        mock.patch.object(ClickhouseCTL, "get_disks") as get_disks,
    ):
        client = client_cls.return_value
        client.query.return_value = "26.3.12.3"
        get_disks.return_value = {
            "default": Disk("default", "/var/lib/clickhouse/", "local")
        }
        ctl = ClickhouseCTL(
            {
                "data_path": "/var/lib/clickhouse/",
                "timeout": 10,
                "freeze_timeout": 10,
                "unfreeze_timeout": 10,
                "restore_replica_timeout": 10,
                "drop_replica_timeout": 10,
            },
            {},
            {},
        )
    client.query.reset_mock()
    return ctl, client


@pytest.fixture(name="cold_table_record")
def fixture_cold_table_record() -> dict:
    """The first paths query can still report TableProxy for loaded storage."""
    return {
        "database": "db1",
        "name": "table1",
        "uuid": "12345678-1234-1234-1234-123456789012",
        "engine": "TableProxy",
        "engine_full": "ReplicatedMergeTree('/tables/table1', 'r1') ORDER BY id",
        "create_table_query": "CREATE TABLE db1.table1 (id UInt64) ENGINE = ReplicatedMergeTree('/tables/table1', 'r1') ORDER BY id",
        "data_paths": ["/var/lib/clickhouse/store/123/table1/"],
        "metadata_path": "metadata/db1/table1.sql",
    }


def test_first_discovery_uses_declared_engine_for_lazy_table(
    clickhouse_ctl: tuple[ClickhouseCTL, mock.Mock],
    cold_table_record: dict,
) -> None:
    ctl, client = clickhouse_ctl
    client.query.return_value = {"data": [cold_table_record]}

    table = ctl.get_table("db1", "table1")

    assert table is not None
    assert table.engine == "ReplicatedMergeTree"
    assert table.is_merge_tree()
    assert table.is_replicated()
    assert table.paths_with_disks


@pytest.mark.parametrize("engine", ["MergeTree", "StorageProxy", "PostgreSQL"])
def test_discovery_preserves_other_runtime_engines(
    clickhouse_ctl: tuple[ClickhouseCTL, mock.Mock],
    cold_table_record: dict,
    engine: str,
) -> None:
    ctl, client = clickhouse_ctl
    cold_table_record["engine"] = engine
    client.query.return_value = {"data": [cold_table_record]}

    table = ctl.get_table("db1", "table1")

    assert table is not None
    assert table.engine == engine
    assert bool(table.paths_with_disks) is (engine == "MergeTree")


@pytest.mark.parametrize("engine_full", [None, "(missing engine)"])
def test_lazy_table_with_unknown_declared_engine_fails_discovery(
    clickhouse_ctl: tuple[ClickhouseCTL, mock.Mock],
    cold_table_record: dict,
    engine_full: str | None,
) -> None:
    ctl, client = clickhouse_ctl
    cold_table_record["engine_full"] = engine_full
    client.query.return_value = {"data": [cold_table_record]}

    with pytest.raises(ClickhouseBackupError, match="db1.table1"):
        ctl.get_table("db1", "table1")


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
        self,
        clickhouse_ctl: tuple[ClickhouseCTL, mock.Mock],
        clause: str,
        settings: dict | None,
    ) -> None:
        """
        ClickHouse gates the clause on CREATE, which is how tables of a replicated
        database are restored. Other tables must not get the setting, as builds
        without the clause do not have it.
        """
        ch_ctl, client = clickhouse_ctl
        statement = f"{self._PLAIN_STATEMENT}{clause}"

        ch_ctl.create_table(self._table(statement))

        client.query.assert_called_once_with(statement, settings=settings)


class TestGetLocalAccessStatements:
    """
    Tests for reading access entities of local storages of a replica.
    """

    @pytest.fixture(name="local_access_ctl")
    def fixture_local_access_ctl(
        self, clickhouse_ctl: tuple[ClickhouseCTL, mock.Mock]
    ) -> tuple[ClickhouseCTL, mock.Mock]:
        """A control object over a replica with one entity of each type."""
        ch_ctl, client = clickhouse_ctl
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

    def test_bare_roles_and_profiles_go_first_and_are_altered_after_users(
        self, local_access_ctl: tuple[ClickhouseCTL, mock.Mock]
    ) -> None:
        """
        Roles and users may refer to profiles and profiles to roles and users, and users
        may refer to each other through GRANTEES.
        """
        ch_ctl, _ = local_access_ctl

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

    def test_only_local_storages_are_read_and_dropped(
        self, local_access_ctl: tuple[ClickhouseCTL, mock.Mock]
    ) -> None:
        """
        Entities from users.xml or shared through ZooKeeper must stay as they are.
        """
        ch_ctl, client = local_access_ctl

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


def test_only_named_collections_created_by_sql_are_read(
    clickhouse_ctl: tuple[ClickhouseCTL, mock.Mock],
) -> None:
    """
    A collection from the server config created by SQL elsewhere would clash with the
    config of that host.
    """
    ch_ctl, client = clickhouse_ctl
    client.query.return_value = {
        "data": [{"name": "nc1", "create_query": "CREATE ..."}]
    }

    assert ch_ctl.get_named_collections_create_queries() == {"nc1": "CREATE ..."}
    assert "WHERE source = 'SQL'" in client.query.call_args.args[0]


def test_scan_detached_parts_reads_reason_from_directory_name(
    tmp_path: Path,
    clickhouse_ctl: tuple[ClickhouseCTL, mock.Mock],
) -> None:
    ch_ctl, _ = clickhouse_ctl
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
    tmp_path: Path,
    reason: str,
    partition_id: str,
    suffix: str,
    clickhouse_ctl: tuple[ClickhouseCTL, mock.Mock],
) -> None:
    ch_ctl, _ = clickhouse_ctl
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
    tmp_path: Path,
    error: OSError,
    clickhouse_ctl: tuple[ClickhouseCTL, mock.Mock],
) -> None:
    ch_ctl, _ = clickhouse_ctl
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
    tmp_path: Path,
    error: OSError,
    clickhouse_ctl: tuple[ClickhouseCTL, mock.Mock],
) -> None:
    ch_ctl, _ = clickhouse_ctl
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
    clickhouse_ctl: tuple[ClickhouseCTL, mock.Mock],
) -> None:
    ch_ctl, _ = clickhouse_ctl
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
