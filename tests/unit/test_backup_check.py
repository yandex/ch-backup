"""
Unit tests for check of backups.

Logic of the checker is tested directly. Backups needing the real format of stored data, such as
encryption, tarballs and disk metadata, are created by the real backup layout on top of an
in-memory storage.
"""

import json
import os
from pathlib import Path
from typing import Iterator

import pytest

from ch_backup.backup.checker import (
    BackupChecker,
    _cloud_storage_dirs,
    _differing_files,
    _DiskCheckResult,
    _files_of_parts,
    _ObjectCheckResult,
    _skipped_data,
    _stored_objects,
    _table_paths,
)
from ch_backup.backup.metadata import BackupMetadata, TableMetadata
from ch_backup.backup.metadata.table_metadata import DataSkippedReason
from ch_backup.clickhouse.models import Database
from ch_backup.exceptions import BackupNotFound
from tests.unit.backup_check_storage import (
    DISK,
    PATH_ROOT,
    Storage,
    copy_part,
    in_memory_storage,
    object_metadata,
)


@pytest.fixture(name="storage")
def _storage(tmp_path: Path) -> Iterator[Storage]:
    with in_memory_storage(tmp_path) as storage:
        yield storage


def _check(storage: Storage, deep: bool, names: list[str] | None = None) -> dict:
    checker = BackupChecker(storage.config, deep=deep, workers=4)
    return {result.name: result for result in checker.check(names)}


def _broken(results: dict) -> set[str]:
    return {name for name, result in results.items() if result.broken}


@pytest.mark.parametrize("deep", [False, True])
def test_intact_backups(storage: Storage, deep: bool) -> None:
    storage.backup("b1", parts=["all_1_1_0", "all_2_2_0"])
    storage.backup("b2", parts=["all_3_3_0"], links={"all_1_1_0": "b1"})
    storage.cloud_backup("c1", objects={"abc/obj1": 10, "abc/obj2": 5})
    storage.cloud_backup("c2", link="c1")

    results = _check(storage, deep)

    assert set(results) == {"b1", "b2", "c1", "c2"}
    assert _broken(results) == set()
    assert all(r.state == "created" and r.unverified == 0 for r in results.values())


@pytest.mark.parametrize("deep", [False, True])
def test_missing_part_breaks_linking_backups(storage: Storage, deep: bool) -> None:
    storage.backup("b1", parts=["all_1_1_0", "all_2_2_0"])
    storage.backup("b2", links={"all_1_1_0": "b1"})
    storage.backup("b3", links={"all_2_2_0": "b1"})
    del storage.engine.objects[storage.part_path("b1", "all_1_1_0")]

    results = _check(storage, deep)

    assert _broken(results) == {"b1", "b2"}
    assert results["b2"].problems == [
        'part all_1_1_0 of "db"."t" in backup "b1": data is missing or has wrong size'
    ]


def test_corrupted_part_content_is_found_by_deep_check(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.backup("b2", links={"all_1_1_0": "b1"})
    key = storage.part_path("b1", "all_1_1_0")
    storage.engine.objects[key] = os.urandom(len(storage.engine.objects[key]))

    assert _broken(_check(storage, deep=False)) == set()
    results = _check(storage, deep=True)
    assert _broken(results) == {"b1", "b2"}
    assert "CryptoError" in results["b2"].problems[0]


@pytest.mark.parametrize(
    "fields, problem",
    [
        ({"checksum": "0" * 32}, "checksums.txt differs from metadata"),
        (
            {"files": ["checksums.txt", "columns.txt", "other.bin"]},
            "files differ from metadata: missing ['other.bin'], unexpected ['data.bin']",
        ),
    ],
)
def test_part_differing_from_metadata_is_found_by_deep_check(
    storage: Storage, fields: dict, problem: str
) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.adjust_part("b1", "all_1_1_0", **fields)

    assert _broken(_check(storage, deep=False)) == set()
    results = _check(storage, deep=True)
    assert results["b1"].problems == [
        f'part all_1_1_0 of "db"."t" in backup "b1": {problem}'
    ]


@pytest.mark.parametrize("deep", [False, True])
def test_metadata_of_each_linking_backup_is_checked(
    storage: Storage, deep: bool
) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.backup("b2", links={"all_1_1_0": "b1"})
    storage.backup("b3", links={"all_1_1_0": "b1"})
    storage.adjust_part("b3", "all_1_1_0", link="b1", size=512)

    results = _check(storage, deep)

    assert _broken(results) == {"b3"}
    assert results["b3"].problems == [
        'part all_1_1_0 of "db"."t" in backup "b1": data is missing or has wrong size'
    ]


def test_part_stored_in_other_format_than_in_metadata(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.adjust_part("b1", "all_1_1_0", tarball=False)

    assert _broken(_check(storage, deep=False)) == {"b1"}


def test_unencrypted_part_content_is_reported_as_unverified(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"], encrypted=False)

    results = _check(storage, deep=True)

    assert _broken(results) == set()
    assert results["b1"].unverified == 1


@pytest.mark.parametrize(
    "path, problem",
    [
        ("backup_struct.json", "metadata: "),
        ("backup_light_struct.json", "light metadata: "),
        ("metadata/databases.tar", "database schemas: "),
        ("metadata/db.tar", 'table schemas of "db": '),
    ],
)
def test_unreadable_metadata_and_schemas(
    storage: Storage, path: str, problem: str
) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.backup("b2", parts=["all_2_2_0"])
    storage.engine.objects[f"{PATH_ROOT}/b1/{path}"] = b"garbage"

    results = _check(storage, deep=False)

    assert _broken(results) == {"b1"}
    assert results["b1"].problems[0].startswith(problem)


def test_parts_linked_to_backup_with_unreadable_metadata_are_checked(
    storage: Storage,
) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.backup("b2", links={"all_1_1_0": "b1"})
    storage.engine.objects[f"{PATH_ROOT}/b1/backup_struct.json"] = b"garbage"

    assert _broken(_check(storage, deep=True)) == {"b1"}
    del storage.engine.objects[storage.part_path("b1", "all_1_1_0")]
    results = _check(storage, deep=True)
    assert _broken(results) == {"b1", "b2"}
    assert results["b2"].problems == [
        'part all_1_1_0 of "db"."t" in backup "b1": data is missing or has wrong size'
    ]


@pytest.mark.parametrize(
    "damage",
    ["no part checksum", "no table engine", "invalid part name", "null part files"],
)
def test_backup_with_malformed_metadata_does_not_stop_the_check(
    storage: Storage, damage: str
) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.backup("b2", parts=["all_2_2_0"])
    key = f"{PATH_ROOT}/b1/backup_struct.json"
    data = json.loads(storage.engine.objects[key])
    table = data["databases"]["db"]["tables"]["t"]
    if damage == "no part checksum":
        del table["parts"]["all_1_1_0"]["checksum"]
    elif damage == "no table engine":
        del table["engine"]
    elif damage == "null part files":
        table["parts"]["all_1_1_0"]["files"] = None
    else:
        table["parts"]["invalid"] = table["parts"].pop("all_1_1_0")
    storage.engine.objects[key] = json.dumps(data).encode()

    results = _check(storage, deep=False)

    assert set(results) == {"b1", "b2"}
    assert _broken(results) == {"b1"}
    assert results["b1"].problems[0].startswith("metadata: ")


@pytest.mark.parametrize(
    "damage, problem",
    [
        ("delete", "1 objects of copied data are missing or have wrong size"),
        ("resize", "1 objects of copied data are missing or have wrong size"),
        ("metadata", "nonce must be exactly 24 bytes long"),
    ],
)
def test_broken_cloud_storage_data(storage: Storage, damage: str, problem: str) -> None:
    storage.cloud_backup("c1", objects={"abc/obj1": 10, "abc/obj2": 5})
    storage.cloud_backup("c2", link="c1")
    data_key = f"{PATH_ROOT}/c1/cloud_storage/{DISK}/abc/obj2"
    if damage == "delete":
        del storage.engine.objects[data_key]
    elif damage == "resize":
        storage.engine.objects[data_key] = b"y"
    else:
        storage.engine.objects[f"{PATH_ROOT}/c1/disks/{DISK}/db/c.tar.gz"] = b"garbage"

    results = _check(storage, deep=False)

    assert _broken(results) == {"c1", "c2"}
    assert problem in results["c2"].problems[0]


def test_copied_data_of_a_disk_is_listed_once(storage: Storage) -> None:
    storage.cloud_backup("c1", objects={"abc/obj1": 10})
    storage.cloud_backup("c2", link="c1")
    listings = []
    list_dir_with_sizes = storage.engine.list_dir_with_sizes
    storage.engine.list_dir_with_sizes = lambda path: listings.append(  # type: ignore
        path
    ) or list_dir_with_sizes(path)

    assert _broken(_check(storage, deep=False)) == set()
    assert listings == [f"{PATH_ROOT}/c1/cloud_storage/{DISK}"]


def test_data_of_a_table_is_listed_once(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0", "all_2_2_0"])
    storage.backup("b2", parts=["all_3_3_0"], links={"all_1_1_0": "b1"})
    listings = []
    list_dir_with_sizes = storage.engine.list_dir_with_sizes
    storage.engine.list_dir_with_sizes = lambda path: listings.append(  # type: ignore
        path
    ) or list_dir_with_sizes(path)
    checker = BackupChecker(storage.config, deep=False, workers=4)

    assert not any(result.broken for result in checker.check())
    assert sorted(listings) == [
        f"{PATH_ROOT}/b1/data/db/t",
        f"{PATH_ROOT}/b2/data/db/t",
    ]
    assert all(
        listing.sizes is None
        for listing in checker._listings.values()  # pylint: disable=protected-access
    )


def test_missing_disk_metadata_of_linked_part(storage: Storage) -> None:
    storage.cloud_backup("c1", objects={"abc/obj1": 10})
    storage.cloud_backup("c2", link="c1")
    storage.cloud_backup("c3", objects=None)

    results = _check(storage, deep=False)

    assert _broken(results) == {"c3"}
    assert (
        "disk metadata is missing for parts ['all_1_1_0']" in results["c3"].problems[0]
    )


@pytest.mark.parametrize(
    "damage, problem",
    [
        ("object", "1 objects of copied data are missing or have wrong size"),
        ("metadata", "disk metadata of parts is unreadable: all_2_2_0: ValueError: "),
    ],
)
def test_linked_cloud_storage_data_is_attributed_by_parts(
    storage: Storage, damage: str, problem: str
) -> None:
    meta = storage.cloud_backup("c1", objects={"abc/obj1": 10, "abc/obj2": 5})
    part = meta.find_part("db", "c", "all_1_1_0")
    assert part
    meta.add_part(copy_part(part, name="all_2_2_0"))
    storage.layout.upload_backup_metadata(meta)
    storage.disk_metadata(
        meta,
        {
            "all_1_1_0/data.bin": object_metadata("abc/obj1", 10),
            "all_2_2_0/data.bin": (
                object_metadata("abc/obj2", 5) if damage == "object" else b"garbage"
            ),
        },
    )
    storage.cloud_backup("c2", link="c1")
    linking = storage.cloud_backup("c3", link="c1")
    linked = linking.find_part("db", "c", "all_1_1_0")
    assert linked
    linking.remove_parts(linking.get_table("db", "c"), [linked])
    linking.add_part(copy_part(linked, name="all_2_2_0", link="c1"))
    storage.layout.upload_backup_metadata(linking)
    data_path = storage.layout.get_cloud_storage_data_path("c1", DISK)
    del storage.engine.objects[os.path.join(data_path, "abc/obj2").lstrip("/")]

    results = _check(storage, deep=False)

    assert _broken(results) == {"c1", "c3"}
    assert len(results["c3"].problems) == 1
    assert (
        results["c3"]
        .problems[0]
        .startswith(f'disk s3 of "db"."c" in backup "c1": {problem}')
    )


def test_disk_metadata_of_all_tables_of_a_disk(storage: Storage) -> None:
    meta = storage.cloud_backup("c1", objects={"abc/obj1": 10})
    storage.disk_metadata(
        meta,
        {
            "data/db/c/all_1_1_0/data.bin": object_metadata("abc/obj1", 10),
            "data/db/other/all_1_1_0/data.bin": object_metadata("abc/obj2", 5),
            "data/db/other/all_1_1_0/columns.txt": object_metadata("abc/obj2", 5),
        },
        old_style=True,
    )

    assert _broken(_check(storage, deep=False)) == set()


def test_not_copied_cloud_storage_data_is_reported_as_unverified(
    storage: Storage,
) -> None:
    storage.cloud_backup(
        "c1", objects={"abc/obj1": 10, "abc/obj2": 5}, data_copied=False
    )

    results = _check(storage, deep=False)

    assert _broken(results) == set()
    assert results["c1"].unverified == 2


def test_malformed_disk_metadata_of_not_copied_data(storage: Storage) -> None:
    meta = storage.cloud_backup("c1", objects={}, data_copied=False)
    storage.disk_metadata(meta, {"all_1_1_0/data.bin": b"garbage"})

    results = _check(storage, deep=False)

    assert _broken(results) == {"c1"}
    assert (
        results["c1"]
        .problems[0]
        .startswith(
            'disk s3 of "db"."c" in backup "c1": disk metadata of parts is unreadable:'
            " all_1_1_0: ValueError: "
        )
    )


def test_only_specified_backups_are_checked(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.backup("b2", parts=["all_2_2_0"])
    del storage.engine.objects[storage.part_path("b2", "all_2_2_0")]

    assert list(_check(storage, deep=False, names=["b1"])) == ["b1"]
    with pytest.raises(BackupNotFound):
        _check(storage, deep=False, names=["b3"])


@pytest.mark.parametrize("tarball", [True, False])
@pytest.mark.parametrize("deep", [False, True])
def test_access_control(storage: Storage, tarball: bool, deep: bool) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.access_control("b1", ["u1", "u2"], tarball=tarball)
    storage.backup("b2", parts=["all_2_2_0"])
    storage.access_control("b2", ["u1", "u2"], tarball=tarball)
    file_name = "access_control.tar" if tarball else "u1.sql"
    del storage.engine.objects[f"{PATH_ROOT}/b2/access_control/{file_name}"]

    results = _check(storage, deep)

    assert _broken(results) == {"b2"}
    assert results["b2"].problems == [f"access control: missing ['{file_name}']"]


@pytest.mark.parametrize(
    "damage, problem",
    [
        ("entity", "access control: missing entities ['u2.sql']"),
        ("garbage", "access control: "),
    ],
)
def test_damaged_access_control_is_found_by_deep_check(
    storage: Storage, damage: str, problem: str
) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    key = f"{PATH_ROOT}/b1/access_control/access_control.tar"
    storage.access_control("b1", ["u1"])
    tarball = storage.engine.objects[key]
    storage.access_control("b1", ["u1", "u2"])
    storage.engine.objects[key] = tarball if damage == "entity" else b"garbage"

    assert _broken(_check(storage, deep=False)) == set()
    results = _check(storage, deep=True)
    assert _broken(results) == {"b1"}
    assert results["b1"].problems[0].startswith(problem)


def test_missing_light_metadata(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    del storage.engine.objects[f"{PATH_ROOT}/b1/backup_light_struct.json"]

    results = _check(storage, deep=False)

    assert results["b1"].problems == ["light metadata: missing"]
    assert results["b1"].state == "created"


@pytest.mark.parametrize("damage", ["not copied", "no light metadata"])
def test_linked_cloud_storage_data_must_be_copied(
    storage: Storage, damage: str
) -> None:
    storage.cloud_backup(
        "c1", objects={"abc/obj1": 10}, data_copied=damage != "not copied"
    )
    storage.cloud_backup("c2", link="c1")
    if damage == "no light metadata":
        del storage.engine.objects[f"{PATH_ROOT}/c1/backup_light_struct.json"]

    results = _check(storage, deep=False)

    assert _broken(results) == ({"c2"} if damage == "not copied" else {"c1", "c2"})
    assert results["c2"].problems == [
        'disk s3 of "db"."c" in backup "c1": '
        'backup "c1" holding the data is missing or holds no copied data'
    ]


def test_cloud_storage_data_linked_from_unchecked_backup(storage: Storage) -> None:
    storage.cloud_backup("c1", objects={"abc/obj1": 10})
    storage.cloud_backup("c2", link="c1")

    assert _broken(_check(storage, deep=False, names=["c2"])) == set()
    storage.engine.objects[f"{PATH_ROOT}/c1/cloud_storage/{DISK}/abc/obj1"] = b"y"
    assert _broken(_check(storage, deep=False, names=["c2"])) == {"c2"}


def test_create_statement_missing_in_schemas(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    storage.layout._storage_loader.upload_data_tarball(  # pylint: disable=protected-access
        ["other"],
        [b"CREATE TABLE db.other"],
        f"{PATH_ROOT}/b1/metadata/db.tar",
        encryption=True,
    )

    results = _check(storage, deep=False)

    assert results["b1"].problems == ["table schemas of \"db\": missing for ['t']"]


def test_tables_of_external_databases_are_not_checked(storage: Storage) -> None:
    storage.backup("b1", parts=["all_1_1_0"])
    meta = storage.meta("b1")
    meta.add_database(Database("ext", "MySQL", "/ext.sql", None, None))
    meta.add_table(TableMetadata("ext", "x", "MySQL", None))
    storage.layout.upload_backup_metadata(meta)
    storage.layout._storage_loader.upload_data_tarball(  # pylint: disable=protected-access
        ["db", "ext"],
        [b"CREATE DATABASE db", b"CREATE DATABASE ext ENGINE = MySQL"],
        f"{PATH_ROOT}/b1/metadata/databases.tar",
        encryption=True,
    )

    assert _broken(_check(storage, deep=False)) == set()


@pytest.mark.parametrize(
    "file_names, problem",
    [
        (
            [
                "all_1_1_0/columns.txt",
                "all_1_1_0/data.bin",
                "all_1_1_0/frozen_metadata.txt",
                "all_2_2_0/data.bin",
            ],
            None,
        ),
        (
            [
                "store/abc/uuid/all_1_1_0/columns.txt",
                "store/abc/uuid/all_1_1_0/data.bin",
            ],
            None,
        ),
        (
            ["all_1_1_0/frozen_metadata.txt"],
            "missing ['columns.txt', 'data.bin'], unexpected []",
        ),
        (["all_1_1_0/data.bin"], "missing ['columns.txt'], unexpected []"),
        (
            [
                "all_1_1_0/columns.txt",
                "all_1_1_0/data.bin",
                "all_1_1_0/p.proj/data.bin",
            ],
            "missing [], unexpected ['p.proj/data.bin']",
        ),
    ],
)
def test_files_of_part_in_disk_metadata(
    file_names: list[str], problem: str | None
) -> None:
    files = ("columns.txt", "data.bin", "frozen_metadata.txt")
    found = _files_of_parts([(name, b"") for name in file_names], {"all_1_1_0"}, set())

    differing = _differing_files(found, [{"all_1_1_0": files}])

    assert differing == (
        {("all_1_1_0", files): f"all_1_1_0: {problem}"} if problem else {}
    )


def test_files_of_part_are_compared_with_metadata_of_each_backup() -> None:
    found = _files_of_parts([("all_1_1_0/data.bin", b"")], {"all_1_1_0"}, set())

    differing = _differing_files(
        found,
        [{"all_1_1_0": ("data.bin",)}, {"all_1_1_0": ("columns.txt", "data.bin")}],
    )

    assert differing == {
        ("all_1_1_0", ("columns.txt", "data.bin")): (
            "all_1_1_0: missing ['columns.txt'], unexpected []"
        )
    }


def test_files_of_table_are_taken_from_disk_metadata_of_all_tables() -> None:
    files = [
        ("store/abc/abcdef/all_1_1_0/data.bin", b"own"),
        ("store/xyz/xyzuvw/all_1_1_0/data.bin", b"other"),
        ("store/xyz/xyzuvw/all_1_1_0/columns.txt", b"other"),
    ]

    found = _files_of_parts(files, {"all_1_1_0"}, {"store/abc/abcdef"})

    assert found == {"all_1_1_0": {"data.bin": b"own"}}
    assert _table_paths(TableMetadata("db", "t-1", "MergeTree", "abcdef")) == {
        "data/db/t%2D1",
        "store/abc/abcdef",
    }


def test_disk_check_result_is_given_by_parts() -> None:
    obj1, obj2 = ("p/obj1", 10), ("p/obj2", 5)
    files = ("data.bin",)
    result = _DiskCheckResult(
        missing_parts={"all_3_3_0"},
        unreadable_parts={"all_8_8_0": "all_8_8_0: ValueError: garbage"},
        differing_files={
            ("all_2_2_0", files): "all_2_2_0: missing ['data.bin'], unexpected []"
        },
        broken_objects={"all_1_1_0": {obj1}, "all_4_4_0": {obj1, obj2}},
        unverified_objects={"all_5_5_0": {obj1}, "all_6_6_0": {obj1, obj2}},
    )

    assert result.for_parts(
        {"all_2_2_0": files, "all_3_3_0": files}
    ) == _ObjectCheckResult("disk metadata is missing for parts ['all_3_3_0']")
    assert result.for_parts(
        {"all_2_2_0": files, "all_8_8_0": files}
    ) == _ObjectCheckResult(
        "disk metadata of parts is unreadable: all_8_8_0: ValueError: garbage"
    )
    assert result.for_parts(
        {"all_1_1_0": files, "all_2_2_0": files}
    ) == _ObjectCheckResult(
        "files of parts differ from disk metadata: all_2_2_0: missing ['data.bin'], unexpected []"
    )
    assert result.for_parts({"all_2_2_0": ()}) == _ObjectCheckResult()
    assert result.for_parts(
        {"all_1_1_0": files, "all_4_4_0": files}
    ) == _ObjectCheckResult("2 objects of copied data are missing or have wrong size")
    assert result.for_parts(
        {"all_5_5_0": files, "all_6_6_0": files}
    ) == _ObjectCheckResult(unverified=2)
    assert result.for_parts({"all_7_7_0": files}) == _ObjectCheckResult()


def test_tables_left_without_data_or_some_parts() -> None:
    backup = BackupMetadata("b1", "1", "24.8", "%Y-%m-%d %H:%M:%S %z")
    backup.add_database(Database("db", "Atomic", "/db.sql", None, None))
    backup.add_table(TableMetadata("db", "t1", "MergeTree", None))
    backup.add_table(
        TableMetadata("db", "t2", "MergeTree", None, DataSkippedReason.UNIQUE_KEY.value)
    )
    backup.add_table(TableMetadata("db", "t3", "MergeTree", None))
    backup.skip_part("db", "t3", "all_3_3_0")
    backup.skip_part("db", "t3", "all_2_2_0")
    backup.add_table(
        TableMetadata(
            "db", "t4", "MergeTree", None, DataSkippedReason.BACKUP_FAILED.value
        )
    )
    backup.skip_part("db", "t4", "all_1_1_0")

    assert _skipped_data(backup) == [
        '"db"."t2": unique_key',
        "\"db\".\"t3\": skipped parts ['all_2_2_0', 'all_3_3_0']",
        '"db"."t4": backup_failed',
    ]
    assert not _skipped_data(None)


def test_cloud_storage_directories_of_backups() -> None:
    assert _cloud_storage_dirs({"b1", "c-1", "c_1", "d-1"}) == {"c_1"}


def test_keys_of_stored_objects() -> None:
    relative = object_metadata("abc/obj1", 10)
    full = b"5\n1\t5\n5\tother/abc/obj2\n0\n0\n\n"

    assert _stored_objects([relative, full], "prefix") == {
        ("prefix/abc/obj1", 10),
        ("other/abc/obj2", 5),
    }
