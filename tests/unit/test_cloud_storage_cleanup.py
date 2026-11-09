"""Cloud cleanup path validation, endpoint matching, and retry safety."""

# pylint: disable=protected-access

import json
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock, call, patch

import pytest

from ch_backup.backup.deduplication import (
    collect_dedup_references_for_batch_backup_deletion,
)
from ch_backup.backup.layout import cloud_storage_metadata_archive_path
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupState,
    PartMetadata,
    TableMetadata,
)
from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.clickhouse.models import Database
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ConfigurationError, InvalidBackupStruct, StorageError
from ch_backup.storage.engine.s3 import S3StorageEngine
from tests.unit.test_backup_copy import _object_storage_metadata
from tests.unit.test_ch_backup import _backup_metadata
from tests.unit.test_layout import make_layout


def _cloud_backup():
    backup = _backup_metadata(
        copied_from={"bucket": "original"},
        cloud_storage_source={
            "endpoint_url": "https://s3.amazonaws.com",
            "bucket": "payload",
            "path": "copies/backup1",
        },
    )
    backup.add_database(Database("db", "Atomic", None, None, None))
    backup.add_table(TableMetadata("db", "table", "MergeTree", None))
    return backup


@pytest.mark.parametrize("disk_name", ["", ".", "..", "../other", "/other", "s3/other"])
def test_archive_listing_rejects_disk_path_components(disk_name):
    layout = make_layout()
    loader = cast(MagicMock, layout._storage_loader)
    with pytest.raises(ConfigurationError, match="disk name"):
        layout.list_cloud_storage_metadata_archives("backup1", disk_name, False, "all")
    loader.path_exists.assert_not_called()
    loader.list_dir.assert_not_called()


@pytest.mark.parametrize(
    "archive",
    [
        "ch_backup/other/disks/s3/table.tar",
        "ch_backup/backup1/disks/s3/../../other.tar",
    ],
)
def test_archive_listing_rejects_paths_outside_disk_directory(archive):
    layout = make_layout()
    loader = cast(MagicMock, layout._storage_loader)
    loader.path_exists.return_value = False
    loader.list_dir.return_value = [archive]
    with pytest.raises(ConfigurationError, match="archive.*outside"):
        layout.list_cloud_storage_metadata_archives("backup1", "s3", False, "all")


@pytest.mark.parametrize(
    "configured,recorded,accepted",
    [
        (None, "https://s3.amazonaws.com/", True),
        ("https://s3.amazonaws.com", None, True),
        ("https://s3.amazonaws.com", "https://untrusted.test", False),
    ],
)
def test_cleanup_requires_configured_endpoint(cleanup, configured, recorded, accepted):
    """Accept both AWS default forms, but never send credentials to another host."""
    cleanup.config["storage"]["credentials"]["endpoint_url"] = configured
    cleanup.meta.cloud_storage.source["endpoint_url"] = recorded
    if not accepted:
        with pytest.raises(ConfigurationError, match="endpoint"):
            cleanup.manager._choose_cloud_cleanup_action(cleanup.meta, {}, True)
        cleanup.engine_factory.assert_not_called()
        cleanup.loader.download_data_tarball.assert_not_called()
        return
    storage, prefix = cleanup.manager._resolve_external_cloud_deletion_target(
        cleanup.meta, True
    )
    assert storage["credentials"]["endpoint_url"] == configured
    assert prefix == "copies/backup1"


@pytest.mark.parametrize("legacy", [False, True])
def test_cleanup_keeps_only_referenced_table_with_same_part_name(cleanup, legacy):
    """Equal part names in two tables must not protect or delete the other's data."""
    c = cleanup
    c.archives.clear()
    c.objects.clear()
    for name, uuid in [("table-one", "uuid-one"), ("table.two", "uuid-two")]:
        table = TableMetadata("db", name, "MergeTree", uuid)
        table.add_part(
            PartMetadata("db", name, "all_1_1_0", "c", 1, [], False, disk_name="s3")
        )
        c.meta.add_table(table)
        archive = cloud_storage_metadata_archive_path(
            "ch_backup/backup1",
            None if legacy else "db",
            None if legacy else name,
            "s3",
            True,
        )
        entry = (
            f"store/abc/{uuid}/all_1_1_0/projection.proj/data.bin"
            if legacy
            else "all_1_1_0/data.bin"
        )
        c.archives.setdefault(archive, []).append(
            (entry, _object_storage_metadata((name, 1)))
        )
        c.objects.add(f"copies/backup1/{name}")

    c.layout.build_unreferenced_cloud_cleanup_action(
        c.meta, {("db", "table-one", "all_1_1_0")}, c.config["storage"]
    )()

    assert c.objects == {"copies/backup1/table-one"}
    c.engine.delete_files.assert_called_once_with(["copies/backup1/table.two"])


def test_whole_cleanup_retries_after_partial_cleanup_emptied_a_disk(cleanup):
    """An empty archive must remain distinguishable from a lost retry input."""
    c = cleanup
    for disk, name in [("s3", "all_1_1_0"), ("other_s3", "all_2_2_0")]:
        c.meta.cloud_storage.add_disk(disk)
        c.meta.add_part(
            PartMetadata("db", "table", name, "checksum", 1, [], False, disk_name=disk)
        )
    c.archives.clear()
    c.archives.update(
        {
            f"ch_backup/backup1/disks/{part.disk_name}/db/table.tar.gz": [
                (
                    f"db/table/{part.name}/data.bin",
                    _object_storage_metadata((part.name, 1)),
                )
            ]
            for part in c.meta.get_parts()
        }
    )
    with patch.object(
        c.layout,
        "_delete_files",
        side_effect=lambda paths, **_: [c.archives.pop(path) for path in paths],
    ):
        c.layout.build_unreferenced_cloud_cleanup_action(
            c.meta, {("db", "table", "all_1_1_0")}, c.config["storage"]
        )()
        c.engine.delete_files.assert_called_once_with(["copies/backup1/all_2_2_0"])
        # Retry can delete the whole backup after the former child disappears.
        assert c.layout.collect_cloud_object_keys_for_deletion(
            c.meta, "copies/backup1"
        ) == ["copies/backup1/all_1_1_0"]


def test_mutation_renamed_child_protects_owners_cloud_part(cleanup):
    """Deletion protects the stored name even when its child requests a later mutation."""
    c = cleanup
    c.meta.add_part(
        PartMetadata("db", "table", "all_1_1_0", "c", 1, [], False, disk_name="s3")
    )
    child = _cloud_backup()
    child.name = "child"
    child.add_part(
        PartMetadata(
            "db",
            "table",
            "all_1_1_0_7",
            "c",
            1,
            [],
            False,
            disk_name="s3",
            link=c.meta.name,
            link_part_name="all_1_1_0",
        )
    )
    c.records[child.name] = child.dump_json()
    references = collect_dedup_references_for_batch_backup_deletion(
        c.layout, [child], {c.meta.name}
    )
    assert references[c.meta.name] == {"db": {"table": {"all_1_1_0"}}}
    assert ClickhouseBackup._has_referenced_cloud_parts(c.meta, references[c.meta.name])
    c.layout.build_unreferenced_cloud_cleanup_action(
        c.meta, {("db", "table", "all_1_1_0")}, c.config["storage"]
    )()
    c.engine.delete_files.assert_not_called()
    c.loader.upload_data_tarball.assert_not_called()


def test_empty_archive_does_not_satisfy_retained_part_metadata(cleanup):
    """An empty cleanup marker cannot supply metadata for a still-referenced part."""
    c = cleanup
    c.meta.add_part(
        PartMetadata("db", "table", "all_1_1_0", "c", 1, [], False, disk_name="s3")
    )
    c.archives[next(iter(c.archives))] = []
    with pytest.raises(InvalidBackupStruct, match="retained cloud parts is missing"):
        c.layout.build_unreferenced_cloud_cleanup_action(
            c.meta, {("db", "table", "all_1_1_0")}, c.config["storage"]
        )


@pytest.fixture(name="cleanup")
def fixture_cleanup(monkeypatch):
    """Keep persisted metadata and archives separate from each deletion attempt."""
    config = deepcopy(DEFAULT_CONFIG)
    config["storage"]["credentials"].update(
        endpoint_url="https://s3.amazonaws.com", bucket="backups"
    )
    config["storage"]["bulk_delete_chunk_size"] = 2
    config["backup"].update(retain_count=0, retain_time={}, path_root="ch_backup")
    meta = _cloud_backup()
    meta.state = BackupState.CREATED
    records = {meta.name: meta.dump_json()}
    archives = {
        "ch_backup/backup1/disks/s3/db/table.tar.gz": [
            (
                "db/table/all_1_1_0/data.bin",
                _object_storage_metadata(("a", 1), ("b", 1), ("c", 1)),
            )
        ]
    }
    objects = {"copies/backup1/" + key for key in ("a", "b", "c", "foreign")}
    layout = make_layout()
    loader = cast(MagicMock, layout._storage_loader)
    loader.path_exists.side_effect = lambda path: path in archives
    loader.list_dir.side_effect = lambda path, **_: [
        key for key in archives if key.startswith(path + "/")
    ]
    loader.download_data_tarball.side_effect = lambda path, **_: list(archives[path])
    pending = {}
    loader.upload_data_tarball.side_effect = (
        lambda *, remote_path, file_names, data_list, **_: pending.update(
            {remote_path: list(zip(file_names, data_list))}
        )
    )

    def commit_archives(*_args, **_kwargs):
        archives.update(pending)
        pending.clear()

    loader.wait.side_effect = commit_archives
    for name, operation in {
        "get_backup": lambda name, **_: (
            BackupMetadata.load_json(records[name]) if name in records else None
        ),
        "get_backups": lambda **kw: [
            BackupMetadata.load_json(value)
            for name, value in records.items()
            if name not in kw.get("excluded_backup_names", ())
        ],
        "reload_backup": lambda backup, **_: layout.get_backup(backup.name),
        "upload_backup_metadata": lambda backup, **_: records.update(
            {backup.name: backup.dump_json()}
        ),
    }.items():
        monkeypatch.setattr(layout, name, MagicMock(side_effect=operation))

    def delete_backup(name, unfreeze):
        assert BackupMetadata.load_json(records[name]).cloud_storage.source is None
        records.pop(name)
        unfreeze()

    monkeypatch.setattr(layout, "delete_backup", MagicMock(side_effect=delete_backup))
    monkeypatch.setattr(layout, "delete_data_parts", MagicMock())
    monkeypatch.setattr(layout, "delete_detached_parts", MagicMock())
    monkeypatch.setattr(layout, "update_legacy_cloud_owner", MagicMock())
    context = MagicMock(backup_layout=layout)
    context.config = config["backup"]
    manager = ClickhouseBackup(config)  # type: ignore[arg-type]
    manager.__dict__["_context"] = context
    engine = MagicMock()
    engine.delete_files.side_effect = objects.difference_update
    engine_factory = MagicMock(return_value=engine)
    for module in ("ch_backup.ch_backup", "ch_backup.backup.layout"):
        monkeypatch.setattr(module + ".get_storage_engine", engine_factory)
    return SimpleNamespace(
        manager=manager,
        meta=meta,
        layout=layout,
        loader=loader,
        records=records,
        archives=archives,
        pending=pending,
        commit_archives=commit_archives,
        objects=objects,
        engine=engine,
        engine_factory=engine_factory,
        context=context,
        config=config,
    )


@pytest.mark.parametrize("operation", ["delete", "purge"])
@pytest.mark.parametrize(
    "invalid", ["endpoint_url", "bucket", "path", "archive", "referenced_owner"]
)
def test_late_cloud_validation_failure_leaves_entire_batch_unchanged(
    cleanup, operation, invalid
):
    """
    Validating backups only as they are deleted could destroy the first backup
    before discovering an unsafe later target. Both public deletion operations
    must validate the complete batch before any mutation.
    """
    c = cleanup
    later = deepcopy(c.meta)
    later.name = "backup2"
    later.start_time -= timedelta(days=1)
    later.state = BackupState.PARTIALLY_DELETED
    later.cloud_storage.source["path"] = "copies/backup2"
    c.config["copy"]["cloud_storage"]["target"]["bucket"] = "payload"
    if invalid in ("endpoint_url", "bucket", "path"):
        later.cloud_storage.source[invalid] = "invalid"
    if invalid == "referenced_owner":
        child = deepcopy(c.meta)
        child.name = "child"
        child.start_time += timedelta(days=1)
        child.cloud_storage.source = None
        child.get_table("db", "table").add_part(
            PartMetadata(
                "db",
                "table",
                "all_1_1_0",
                "c",
                1,
                [],
                False,
                disk_name="s3",
                link=later.name,
            )
        )
        later.get_table("db", "table").add_part(
            PartMetadata("db", "table", "all_1_1_0", "c", 1, [], False, disk_name="s3")
        )
        c.records[child.name] = child.dump_json()
        c.config["backup"]["retain_count"] = 1
        later.cloud_storage.source["endpoint_url"] = "https://untrusted.test"
    c.records[later.name] = later.dump_json()
    before = dict(c.records)
    with pytest.raises((ConfigurationError, InvalidBackupStruct)):
        if operation == "delete":
            c.manager.delete(
                "backup1", purge_partial=True, delete_cloud_storage_data=True
            )
        else:
            c.manager.purge(delete_cloud_storage_data=True)
    assert c.records == before
    c.engine.delete_files.assert_not_called()
    c.loader.upload_data_tarball.assert_not_called()
    c.layout.delete_backup.assert_not_called()
    c.layout.upload_backup_metadata.assert_not_called()
    c.context.ch_ctl.system_unfreeze.assert_not_called()


@pytest.mark.parametrize(
    "version,prefix",
    [
        (3, "copies/backup1"),
        (5, "copies/backup1"),
        (3, "ch_backup/backup1"),
        (5, "ch_backup/backup1"),
    ],
)
def test_cleanup_selects_recorded_keys_and_skips_link_only_disks(
    cleanup, version, prefix
):
    """
    A prefix can contain unrelated data, and a linked disk can have no local
    archive. Selecting recorded payload keys across both metadata formats
    avoids broad deletion and false missing-archive failures.
    """
    c = cleanup
    c.meta.cloud_storage.source["path"] = prefix
    c.meta.cloud_storage.add_disk("linked")
    c.meta.get_table("db", "table").add_part(
        PartMetadata(
            "db",
            "table",
            "all_2_2_0",
            "c",
            1,
            [],
            False,
            disk_name="linked",
            link="other",
        )
    )
    key = prefix + "/a" if version == 5 else "a"
    archive = next(iter(c.archives))
    c.archives[archive] = [
        ("data.bin", _object_storage_metadata((key, 1), (key, 1), version=version)),
        ("frozen_metadata.txt", b"not an object descriptor"),
    ]
    assert c.layout.collect_cloud_object_keys_for_deletion(c.meta, prefix) == [
        prefix + "/a"
    ]
    c.archives[archive][0] = (
        "data.bin",
        _object_storage_metadata(("outside/a", 1), version=5),
    )
    with pytest.raises(ConfigurationError, match="outside"):
        c.layout.collect_cloud_object_keys_for_deletion(c.meta, prefix)
    c.engine.delete_files.assert_not_called()


@pytest.mark.parametrize(
    "operation,pending_generation",
    [("delete", False), ("purge", False), ("delete", True)],
)
def test_whole_cleanup_reloads_retry_metadata_after_late_batch_failure(
    cleanup, operation, pending_generation
):
    """
    A later batch can fail after earlier objects are gone. Recreating the
    manager from saved metadata proves that retry does not depend on in-memory
    state or lose the archive or manifest needed to finish.
    """
    c = cleanup
    calls = []
    if pending_generation:
        c.meta.generation_of = "original"
        c.meta.state = BackupState.FAILED
        c.records[c.meta.name] = c.meta.dump_json()
        c.archives.clear()
        manifest_path = "ch_backup/backup1/cloud_objects.json"
        manifests = {manifest_path: json.dumps(["copies/backup1/" + k for k in "abc"])}
        c.loader.download_data.side_effect = manifests.__getitem__

    def delete(keys):
        calls.append(list(keys))
        if len(calls) == 2:
            raise StorageError("AccessDenied")
        c.objects.difference_update(keys)

    c.engine.delete_files.side_effect = delete
    with pytest.raises(StorageError, match="AccessDenied"):
        if operation == "delete":
            c.manager.delete(
                "backup1", purge_partial=False, delete_cloud_storage_data=True
            )
        else:
            c.manager.purge(delete_cloud_storage_data=True)
    failed = BackupMetadata.load_json(c.records["backup1"])
    assert failed.state == BackupState.FAILED
    assert failed.cloud_storage.source["path"] == "copies/backup1"
    assert c.objects == {"copies/backup1/c", "copies/backup1/foreign"}
    c.layout.delete_backup.assert_not_called()
    if pending_generation:
        assert failed.generation_of == "original"
        assert manifest_path in manifests
    retry = ClickhouseBackup(c.config)
    retry.__dict__["_context"] = c.context
    assert retry.delete(
        "backup1", purge_partial=False, delete_cloud_storage_data=True
    ) == ("backup1", None)
    assert calls == [["copies/backup1/a", "copies/backup1/b"], ["copies/backup1/c"]] * 2
    assert c.objects == {"copies/backup1/foreign"}
    assert not c.records
    if pending_generation:
        assert c.loader.download_data.call_args_list == [call(manifest_path)] * 2
    c.context.ch_ctl.system_unfreeze.assert_not_called()


@pytest.mark.parametrize("full_metadata", ["missing", "unreadable"])
def test_external_cleanup_can_finish_from_light_metadata(cleanup, full_metadata):
    """
    Losing full metadata should not strand an unreferenced copy when readable
    light metadata and disk archives still identify its payload. The retained
    foreign object checks that recovery stays selective.
    """
    c = cleanup

    def read(name, use_light_meta=False, **_):
        if use_light_meta:
            return BackupMetadata.load_json(c.records[name])
        if full_metadata == "unreadable":
            raise StorageError("unreadable full metadata")
        return None

    c.layout.get_backup.side_effect = read
    assert c.manager.delete(
        "backup1", purge_partial=False, delete_cloud_storage_data=True
    ) == ("backup1", None)
    assert not c.records
    assert c.objects == {"copies/backup1/foreign"}


def test_external_cleanup_requires_readable_light_metadata(cleanup):
    """
    Without readable light metadata, cleanup cannot establish the copied
    payload destination. Refusing deletion preserves data instead of guessing a
    location.
    """
    c = cleanup
    c.layout.get_backup.side_effect = StorageError("unreadable")
    with pytest.raises(InvalidBackupStruct, match="readable"):
        c.manager.delete("backup1", purge_partial=False, delete_cloud_storage_data=True)
    c.engine.delete_files.assert_not_called()
    c.layout.delete_backup.assert_not_called()


@pytest.mark.parametrize(
    "failure", ["payload", "archive", "archive_ack", "after_retain"]
)
def test_partial_cleanup_preserves_shared_objects_and_retries_archive_rewrites(
    cleanup, failure
):
    """
    Retained and discarded parts can share objects, while payload deletion,
    archive replacement and metadata retention can complete separately. Retry
    must preserve shared objects and local parts through each tested partial
    state.
    """
    c = cleanup
    table = c.meta.get_table("db", "table")
    for name, disk in [
        ("all_1_1_0", "s3"),
        ("all_2_2_0", "s3"),
        ("all_3_3_0", "default"),
    ]:
        c.meta.add_part(
            PartMetadata("db", "table", name, "c", 10, [], False, disk_name=disk)
        )
    archive = next(iter(c.archives))
    retained = ("db/table/all_1_1_0/data.bin", _object_storage_metadata(("shared", 1)))
    c.archives[archive] = [
        retained,
        (
            "db/table/all_2_2_0/data.bin",
            _object_storage_metadata(("shared", 1), ("unused", 1)),
        ),
        (
            "db/table/all_2_2_0/p.proj/data.bin",
            _object_storage_metadata(("copies/backup1/projection", 1), version=5),
        ),
    ]
    original_archive = list(c.archives[archive])
    c.objects.clear()
    c.objects.update(
        "copies/backup1/" + key for key in ("shared", "unused", "projection", "foreign")
    )
    refs = {"db": {"table": {"all_1_1_0", "all_3_3_0"}}}
    c.records[c.meta.name] = c.meta.dump_json()

    def execute():
        loaded, light, action = c.manager._prepare_deletion_batch(
            [c.layout.get_backup("backup1")], {"backup1": refs}, True
        )[0]
        return c.manager._execute_backup_deletion(loaded, refs, light, action)

    if failure == "after_retain":
        c.meta.remove_parts(table, [c.meta.find_part("db", "table", "all_2_2_0")])
        c.records[c.meta.name] = c.meta.dump_json()
    else:
        if failure == "payload":
            c.engine.delete_files.side_effect = StorageError("payload failed")
        elif failure == "archive_ack":

            def lose_archive_ack(*args, **kwargs):
                c.commit_archives(*args, **kwargs)
                raise StorageError("archive acknowledgment failed")

            c.loader.wait.side_effect = lose_archive_ack
        else:
            c.loader.wait.side_effect = StorageError("archive failed")
        with pytest.raises(StorageError, match="[Ff]ailed"):
            execute()
        assert (
            BackupMetadata.load_json(c.records["backup1"]).state == BackupState.FAILED
        )
        assert c.archives[archive] == (
            [retained] if failure == "archive_ack" else original_archive
        )
        c.pending.clear()
    c.engine.delete_files.side_effect = c.objects.difference_update
    c.loader.wait.side_effect = c.commit_archives
    assert execute()[0] is None
    assert c.objects == {"copies/backup1/shared", "copies/backup1/foreign"}
    assert c.archives[archive] == [retained]
    for deletion in c.layout.delete_data_parts.call_args_list:
        assert not deletion.args[1]
    result = BackupMetadata.load_json(c.records["backup1"])
    assert result.state == BackupState.PARTIALLY_DELETED
    assert [part.name for part in result.get_parts()] == ["all_1_1_0", "all_3_3_0"]
    assert result.real_size == 20
    assert execute()[0] is None
    assert c.archives[archive] == [retained]


@pytest.mark.parametrize(
    "field,value",
    [
        ("copied_from", None),
        ("bucket", ""),
        ("path", ""),
        ("path", "copies/other"),
        ("path", "copies/../backup1"),
        ("path", "copies/./backup1"),
    ],
)
def test_invalid_cleanup_destination_cannot_create_storage_client(
    cleanup, field, value
):
    """
    An invalid destination can expose credentials or select unrelated objects
    before deletion starts. Reject it before creating a client or reading its
    archives.
    """
    c = cleanup
    if field == "copied_from":
        c.meta.copied_from = value
    else:
        c.meta.cloud_storage.source[field] = value
    with pytest.raises(ConfigurationError):
        c.manager._choose_cloud_cleanup_action(c.meta, {}, True)
    c.engine.delete_files.assert_not_called()
    c.loader.download_data_tarball.assert_not_called()
    c.engine_factory.assert_not_called()


@pytest.mark.parametrize(
    "prefix,configured_endpoint,accepted",
    [
        ("ch_backup/backup1", "https://s3.amazonaws.com", True),
        ("ch_backup/backup1/nested/backup1", "https://s3.amazonaws.com", False),
        ("ch_backup/backup1/nested/backup1", None, False),
        ("copies//backup1", "https://s3.amazonaws.com", True),
    ],
)
def test_cleanup_overlap_and_s3_key_boundaries(
    cleanup, prefix, configured_endpoint, accepted
):
    """
    Metadata removal could erase payload in a nested backup directory. Exact
    co-location is supported, and repeated slashes are literal S3 key
    characters; blanket overlap rejection or path normalization would break
    those layouts.
    """
    c = cleanup
    c.config["storage"]["credentials"]["endpoint_url"] = configured_endpoint
    c.config["copy"]["cloud_storage"]["target"][
        "endpoint_url"
    ] = "https://s3.amazonaws.com"
    c.meta.cloud_storage.source.update(bucket="backups", path=prefix)
    if not accepted:
        with pytest.raises(ConfigurationError, match="overlaps"):
            c.manager._resolve_external_cloud_deletion_target(c.meta, True)
    else:
        assert (
            c.manager._resolve_external_cloud_deletion_target(c.meta, True)[1] == prefix
        )


def test_cleanup_uses_explicit_credentials_with_recorded_bucket(cleanup):
    """
    The copy destination can require different credentials from backup storage.
    Resolve configured credentials for the recorded endpoint while retaining
    the payload bucket recorded in metadata.
    """
    c = cleanup
    c.config["copy"]["cloud_storage"]["target"].update(
        endpoint_url="https://external.test",
        access_key_id="dummy-key",
        secret_access_key="dummy-secret",
    )
    c.meta.cloud_storage.source["endpoint_url"] = "https://external.test/"
    storage, _ = c.manager._resolve_external_cloud_deletion_target(c.meta, True)
    assert storage["credentials"] == {
        "endpoint_url": "https://external.test",
        "bucket": "payload",
        "access_key_id": "dummy-key",
        "secret_access_key": "dummy-secret",
    }


@pytest.mark.parametrize(
    "manifest",
    [
        None,
        {},
        [1],
        ["elsewhere/a"],
        ["copies/backup1/../a"],
        ["copies/backup1/./a"],
        "unreadable",
    ],
)
def test_interrupted_generation_rejects_unsafe_or_missing_manifest(cleanup, manifest):
    """
    Before disk archives exist, the generation manifest is the only deletion
    inventory. Missing or unsafe entries must stop cleanup rather than
    authorize deletion outside the generation.
    """
    c = cleanup
    c.meta.generation_of = "original"
    c.meta.state = BackupState.FAILED
    if manifest == "unreadable":
        c.loader.download_data.side_effect = StorageError("missing manifest")
    else:
        c.loader.download_data.return_value = json.dumps(manifest)
    with pytest.raises((ConfigurationError, StorageError)):
        c.manager._choose_cloud_cleanup_action(c.meta, {}, True)
    c.engine.delete_files.assert_not_called()
    c.layout.upload_backup_metadata.assert_not_called()


def test_s3_per_key_error_keeps_cleanup_metadata_for_retry(cleanup):
    """
    An S3 deletion response can contain per-key failures without raising a
    transport error. The real adapter must expose those failures so cleanup
    retains its retry metadata instead of reporting success.
    """
    c = cleanup
    storage = deepcopy(c.config["storage"])
    storage["credentials"]["bucket"] = "payload"
    storage["retry_max_attempts"] = 1
    engine = S3StorageEngine(storage)
    client = MagicMock()
    client.delete_objects.side_effect = [
        {"Errors": [{"Key": "copies/backup1/a", "Code": "AccessDenied"}]},
        {},
        {},
    ]
    engine._s3_client_factory = MagicMock(
        create_s3_client=MagicMock(return_value=client)
    )
    c.engine_factory.return_value = engine
    with pytest.raises(StorageError, match="AccessDenied"):
        c.manager.delete("backup1", purge_partial=False, delete_cloud_storage_data=True)
    failed = BackupMetadata.load_json(c.records["backup1"])
    assert failed.state == BackupState.FAILED and failed.cloud_storage.source
    assert c.archives
    c.manager.delete("backup1", purge_partial=False, delete_cloud_storage_data=True)
    assert not c.records
    assert [
        call.kwargs["Delete"]["Objects"]
        for call in client.delete_objects.call_args_list
    ] == [
        [{"Key": "copies/backup1/a"}, {"Key": "copies/backup1/b"}],
        [{"Key": "copies/backup1/a"}, {"Key": "copies/backup1/b"}],
        [{"Key": "copies/backup1/c"}],
    ]
