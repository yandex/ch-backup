"""Deletion of external cloud objects owned by copied backups."""

# These tests exercise internal deletion and install a stubbed S3 client.
# pylint: disable=protected-access

from collections import defaultdict
from copy import deepcopy
from unittest.mock import MagicMock

import pytest
from botocore.stub import Stubber

from ch_backup.backup.deduplication import DedupReferences
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupState, PartMetadata, TableMetadata
from ch_backup.ch_backup import ClickhouseBackup, DeleteSettings
from ch_backup.clickhouse.models import Database
from ch_backup.config import Config
from ch_backup.exceptions import ConfigurationError, InvalidBackupStruct, StorageError
from ch_backup.storage.engine.s3 import S3StorageEngine
from tests.unit.test_ch_backup import _backup_metadata


@pytest.fixture(name="cloud_copy")
def fixture_cloud_copy(monkeypatch, tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("", encoding="utf-8")
    config = Config(str(config_path))
    config["storage"]["credentials"] = {
        "endpoint_url": "https://s3.example.test",
        "bucket": "backups",
        "access_key_id": "backup-key",
        "secret_access_key": "backup-secret",
    }
    config["backup"]["path_root"] = "ch_backup"
    config["storage"]["retry_max_attempts"] = 1
    config["storage"]["bulk_delete_chunk_size"] = 2
    backup = ClickhouseBackup(config)
    meta = _backup_metadata(
        copied_from={
            "endpoint_url": "https://s3.example.test",
            "bucket": "original",
            "path_root": "backups",
        },
        cloud_storage_source={
            "endpoint_url": "https://s3.example.test",
            "bucket": "disk-copy",
            "path": "copies/backup1",
        },
    )
    context = MagicMock()
    context.backup_layout.get_cloud_storage_object_keys.side_effect = (
        lambda meta, prefix: BackupLayout.get_cloud_storage_object_keys(
            context.backup_layout, meta, prefix
        )
    )
    context.config = config["backup"]
    context.backup_layout.get_backups.return_value = [meta]
    context.backup_layout.get_backup.return_value = meta
    context.backup_layout.delete_backup.side_effect = lambda _name, unfreeze: unfreeze()
    backup.__dict__["_context"] = context
    storage_configs = []
    storage = deepcopy(config["storage"])
    storage["credentials"]["bucket"] = "disk-copy"
    engine = S3StorageEngine(storage)

    def make_engine(storage):
        storage_configs.append(storage)
        configured_engine = S3StorageEngine(storage)
        configured_engine._s3_client_factory = engine._s3_client_factory
        return configured_engine

    monkeypatch.setattr(
        "ch_backup.ch_backup.get_storage_engine", make_engine, raising=False
    )
    with Stubber(engine.get_client()) as s3:
        yield backup, meta, context, s3, storage_configs
        s3.assert_no_pending_responses()


def _saved_objects(context, keys):
    context.backup_layout.get_cloud_storage_metadata_remote_paths.return_value = [
        "disks/s3/table.tar"
    ]
    data = f"5\n{len(keys)}\t{len(keys)}\n"
    data += "".join(f"1\t{key}\n" for key in keys)
    data += "1\n0\n\n"
    context.backup_layout.get_cloud_storage_metadata_files.return_value = [
        ("data.bin", data.encode())
    ]


def _delete_objects(s3, keys):
    s3.add_response(
        "delete_objects",
        {},
        {"Bucket": "disk-copy", "Delete": {"Objects": [{"Key": key} for key in keys]}},
    )


@pytest.mark.parametrize(
    "data",
    [
        b"3\n1\t1\n1\tcluster-a\n1\n0\n",
        b"5\n1\t1\n1\tcopies/backup1/cluster-a\n1\n0\n\n",
    ],
)
def test_delete_external_copy_uses_only_recorded_objects(cloud_copy, data):
    backup, meta, context, s3, _configs = cloud_copy
    context.backup_layout.get_cloud_storage_metadata_remote_paths.return_value = [
        "disks/s3/table.tar"
    ]
    context.backup_layout.get_cloud_storage_metadata_files.return_value = [
        ("data.bin", data)
    ]
    _delete_objects(s3, ["copies/backup1/cluster-a"])

    assert backup._delete(meta, {}, DeleteSettings(delete_cloud_storage_data=True)) == (
        "backup1",
        None,
    )


def test_delete_cloud_copy_removes_only_its_prefix_in_batches(cloud_copy):
    backup, meta, context, s3, configs = cloud_copy
    keys = ["copies/backup1/a", "copies/backup1/b", "copies/backup1/c"]
    _saved_objects(context, keys)
    _delete_objects(s3, keys[:2])
    _delete_objects(s3, keys[2:])

    assert backup._delete(
        meta, {}, settings=DeleteSettings(delete_cloud_storage_data=True)
    ) == ("backup1", None)

    assert configs[0]["credentials"] == {
        "endpoint_url": "https://s3.example.test",
        "bucket": "disk-copy",
        "access_key_id": "backup-key",
        "secret_access_key": "backup-secret",
    }
    context.ch_ctl.system_unfreeze.assert_not_called()
    context.backup_layout.delete_backup.assert_called_once()


def test_delete_cloud_copy_failure_preserves_metadata_and_can_retry(cloud_copy):
    backup, meta, context, s3, _configs = cloud_copy
    key = "copies/backup1/a"
    _saved_objects(context, [key])
    s3.add_response(
        "delete_objects",
        {"Errors": [{"Key": key, "Code": "AccessDenied"}]},
        {"Bucket": "disk-copy", "Delete": {"Objects": [{"Key": key}]}},
    )

    with pytest.raises(StorageError, match="AccessDenied"):
        backup._delete(
            meta, {}, settings=DeleteSettings(delete_cloud_storage_data=True)
        )

    assert meta.state == BackupState.FAILED
    context.backup_layout.delete_backup.assert_not_called()
    assert "AccessDenied" in meta.exception
    _saved_objects(context, [key])
    _delete_objects(s3, [key])
    assert backup._delete(
        meta, {}, settings=DeleteSettings(delete_cloud_storage_data=True)
    ) == ("backup1", None)


@pytest.mark.parametrize(
    "path", ["", "/", "copies", "copies/another", "copies/../backup1"]
)
def test_delete_cloud_copy_rejects_unowned_prefix(cloud_copy, path):
    backup, meta, context, _s3, configs = cloud_copy
    meta.cloud_storage.source["path"] = path

    with pytest.raises(ConfigurationError):
        backup._delete(
            meta, {}, settings=DeleteSettings(delete_cloud_storage_data=True)
        )

    assert not configs
    context.backup_layout.delete_backup.assert_not_called()


def test_delete_cloud_copy_rejects_untrusted_endpoint(cloud_copy):
    backup, meta, context, _s3, configs = cloud_copy
    meta.cloud_storage.source["endpoint_url"] = "https://untrusted.example.test"

    with pytest.raises(ConfigurationError, match="endpoint"):
        backup._delete(
            meta, {}, settings=DeleteSettings(delete_cloud_storage_data=True)
        )

    assert not configs
    context.backup_layout.delete_backup.assert_not_called()


@pytest.mark.parametrize(
    ("backup_root", "cloud_path"),
    [
        ("ch_backup", "ch_backup/backup1/payload/backup1"),
        ("ch_backup/backup1", "ch_backup/backup1"),
    ],
)
def test_delete_cloud_copy_rejects_overlapping_backup_prefix(
    cloud_copy, backup_root, cloud_path
):
    backup, meta, _context, _s3, configs = cloud_copy
    backup.config["backup"]["path_root"] = backup_root
    meta.cloud_storage.source.update({"bucket": "backups", "path": cloud_path})

    with pytest.raises(ConfigurationError, match="overlaps backup metadata"):
        backup._get_external_cloud_storage(
            meta, DeleteSettings(delete_cloud_storage_data=True)
        )

    assert not configs


def test_delete_cloud_copy_uses_explicit_target_credentials(cloud_copy):
    backup, meta, context, _s3, configs = cloud_copy
    backup.config["copy"]["cloud_storage"]["target"].update(
        {
            "endpoint_url": "https://external.example.test",
            "bucket": "disk-copy",
            "access_key_id": "cloud-key",
            "secret_access_key": "cloud-secret",
        }
    )
    meta.cloud_storage.source["endpoint_url"] = "https://external.example.test/"
    _saved_objects(context, [])

    assert backup._delete(
        meta, {}, settings=DeleteSettings(delete_cloud_storage_data=True)
    ) == ("backup1", None)

    assert configs[0]["credentials"] == {
        "endpoint_url": "https://external.example.test",
        "bucket": "disk-copy",
        "access_key_id": "cloud-key",
        "secret_access_key": "cloud-secret",
    }


def test_delete_cloud_copy_requires_copy_marker(cloud_copy):
    backup, meta, context, _s3, configs = cloud_copy
    meta.copied_from = None

    with pytest.raises(ConfigurationError):
        backup._delete(
            meta, {}, settings=DeleteSettings(delete_cloud_storage_data=True)
        )

    assert not configs
    context.backup_layout.delete_backup.assert_not_called()


@pytest.mark.parametrize("operation", ["delete", "purge"])
def test_public_delete_operations_clean_external_objects(cloud_copy, operation):
    backup, meta, context, s3, _configs = cloud_copy
    backup.config["backup"].update({"retain_count": 0, "retain_time": {}})
    _saved_objects(context, ["copies/backup1/a"])
    _delete_objects(s3, ["copies/backup1/a"])

    if operation == "delete":
        assert backup.delete(
            meta.name, DeleteSettings(delete_cloud_storage_data=True)
        ) == (
            "backup1",
            None,
        )
    else:
        assert backup.purge(
            settings=DeleteSettings(delete_cloud_storage_data=True)
        ) == (["backup1"], None)


@pytest.mark.parametrize("shared_disk", ["s3", "default"])
def test_partial_delete_keeps_referenced_cloud_data(cloud_copy, shared_disk):
    backup, meta, context, s3, configs = cloud_copy
    meta.add_database(Database("db", "Atomic", None, None, None))
    table = TableMetadata("db", "table", "MergeTree", None)
    table.add_part(
        PartMetadata(
            "db", "table", "all_1_1_0", "checksum", 1, [], False, disk_name=shared_disk
        )
    )
    meta.add_table(table)
    refs: DedupReferences = defaultdict(lambda: defaultdict(set))
    refs["db"]["table"].add("all_1_1_0")
    if shared_disk != "s3":
        _saved_objects(context, ["copies/backup1/a"])
        _delete_objects(s3, ["copies/backup1/a"])

    deleted, message = backup._delete(
        meta, refs, settings=DeleteSettings(delete_cloud_storage_data=True)
    )

    assert deleted is None
    assert message
    assert meta.state == BackupState.PARTIALLY_DELETED
    assert bool(configs) == (shared_disk != "s3")


@pytest.mark.parametrize(
    "data",
    [
        b"3\n1\t5\n5\txyz/data\n1\n0\n",
        b"5\n1\t5\n5\tch_backup/backup1/xyz/data\n1\n0\n\n",
    ],
)
def test_delete_cloud_copy_inside_backup_preserves_metadata(cloud_copy, data):
    backup, meta, context, s3, configs = cloud_copy
    meta.cloud_storage.source.update({"bucket": "backups", "path": "ch_backup/backup1"})
    cloud_object = "ch_backup/backup1/xyz/data"
    archive = "ch_backup/backup1/disks/s3/db/table.tar"
    context.backup_layout.get_cloud_storage_metadata_remote_paths.return_value = [
        archive
    ]
    context.backup_layout.get_cloud_storage_metadata_files.return_value = [
        ("db/table/all_2_2_0/data.bin", data),
        ("db/table/frozen_metadata.txt", b"not object metadata"),
    ]
    s3.add_response(
        "delete_objects",
        {},
        {"Bucket": "backups", "Delete": {"Objects": [{"Key": cloud_object}]}},
    )

    assert backup._delete(meta, {}, DeleteSettings(delete_cloud_storage_data=True)) == (
        "backup1",
        None,
    )

    assert configs[0]["credentials"]["bucket"] == "backups"
    context.backup_layout.delete_backup.assert_called_once()


@pytest.mark.parametrize("explicit_target", [False, True])
def test_delete_cloud_copy_with_default_aws_endpoint(cloud_copy, explicit_target):
    backup, meta, context, _s3, configs = cloud_copy
    backup.config["storage"]["credentials"]["endpoint_url"] = None
    if explicit_target:
        backup.config["copy"]["cloud_storage"]["target"][
            "endpoint_url"
        ] = "https://s3.example.test"
    else:
        meta.cloud_storage.source["endpoint_url"] = None
    _saved_objects(context, [])

    assert backup._delete(
        meta, {}, settings=DeleteSettings(delete_cloud_storage_data=True)
    ) == ("backup1", None)
    assert configs[0]["credentials"]["endpoint_url"] == (
        "https://s3.example.test" if explicit_target else None
    )


def test_partial_delete_shared_directory_keeps_local_parts_and_metadata(cloud_copy):
    backup, meta, context, s3, _configs = cloud_copy
    meta.cloud_storage.source.update({"bucket": "backups", "path": "ch_backup/backup1"})
    meta.add_database(Database("db", "Atomic", None, None, None))
    table = TableMetadata("db", "table", "MergeTree", None)
    table.add_part(
        PartMetadata(
            "db",
            "table",
            "all_1_1_0",
            "checksum",
            1,
            ["checksums.txt"],
            True,
            disk_name="default",
        )
    )
    meta.add_table(table)
    cloud_object = "ch_backup/backup1/xyz/data"
    archive = "ch_backup/backup1/disks/s3/db/table.tar"
    context.backup_layout.get_cloud_storage_metadata_remote_paths.return_value = [
        archive
    ]
    context.backup_layout.get_cloud_storage_metadata_files.return_value = [
        ("db/table/all_2_2_0/data.bin", b"3\n1\t5\n5\txyz/data\n1\n0\n"),
        ("db/table/frozen_metadata.txt", b"not object metadata"),
    ]
    s3.add_response(
        "delete_objects",
        {},
        {"Bucket": "backups", "Delete": {"Objects": [{"Key": cloud_object}]}},
    )
    refs: DedupReferences = defaultdict(lambda: defaultdict(set))
    refs["db"]["table"].add("all_1_1_0")

    published_sources = []
    context.backup_layout.upload_backup_metadata.side_effect = (
        lambda metadata, **_kwargs: published_sources.append(
            deepcopy(metadata.cloud_storage.source)
        )
    )

    def remove_archives(_name):
        assert published_sources[-1] is None
        context.backup_layout.get_cloud_storage_metadata_remote_paths.return_value = []

    context.backup_layout.delete_cloud_storage_data.side_effect = remove_archives
    backup._delete(meta, refs)

    deleted, _message = backup._delete(
        meta, refs, settings=DeleteSettings(delete_cloud_storage_data=True)
    )

    assert deleted is None
    assert meta.state == BackupState.PARTIALLY_DELETED
    assert len(meta.get_parts()) == 1


@pytest.mark.parametrize("unreadable", [False, True])
def test_delete_cloud_copy_can_use_light_metadata(cloud_copy, unreadable):
    backup, meta, context, s3, _configs = cloud_copy
    if unreadable:
        context.backup_layout.get_backup.side_effect = StorageError(
            "unreadable full metadata"
        )
    else:
        context.backup_layout.get_backup.return_value = None
    _saved_objects(context, ["copies/backup1/a"])
    _delete_objects(s3, ["copies/backup1/a"])

    published_sources = []
    context.backup_layout.upload_backup_metadata.side_effect = (
        lambda metadata, **_kwargs: published_sources.append(
            deepcopy(metadata.cloud_storage.source)
        )
    )

    def remove_backup(_name, unfreeze):
        assert published_sources[-1] is None
        unfreeze()

    context.backup_layout.delete_backup.side_effect = remove_backup

    assert backup._delete(
        meta, {}, settings=DeleteSettings(delete_cloud_storage_data=True)
    ) == ("backup1", None)

    assert meta.cloud_storage.source is None
    assert len(published_sources) == 2
    assert published_sources[0]["path"] == "copies/backup1"
    assert published_sources[1] is None
    context.backup_layout.delete_backup.assert_called_once()
    assert all(
        call.kwargs == {"light_only": True}
        for call in context.backup_layout.upload_backup_metadata.call_args_list
    )


def test_delete_cloud_copy_refuses_unreadable_light_metadata(cloud_copy):
    backup, _meta, context, _s3, configs = cloud_copy

    def list_backups(*, unreadable, **_kwargs):
        unreadable["backup1"] = "unreadable light metadata"
        return []

    context.backup_layout.get_backups.side_effect = list_backups

    with pytest.raises(InvalidBackupStruct, match="readable backup metadata"):
        backup.delete("backup1", DeleteSettings(delete_cloud_storage_data=True))

    assert not configs
    context.backup_layout.delete_backup.assert_not_called()


def test_delete_shared_directory_rejects_objects_outside_backup(cloud_copy):
    backup, meta, context, _s3, _configs = cloud_copy
    meta.cloud_storage.source.update({"bucket": "backups", "path": "ch_backup/backup1"})
    context.backup_layout.get_cloud_storage_metadata_remote_paths.return_value = [
        "archive"
    ]
    context.backup_layout.get_cloud_storage_metadata_files.return_value = [
        ("data.bin", b"5\n1\t5\n5\tother-backup/data\n1\n0\n\n"),
    ]

    with pytest.raises(ConfigurationError, match="outside"):
        backup._delete(meta, {}, DeleteSettings(delete_cloud_storage_data=True))

    context.backup_layout.delete_backup.assert_not_called()


@pytest.mark.parametrize(
    ("bucket", "path"),
    [("backups", "ch_backup/backup1"), ("disk-copy", "copies/backup1")],
)
def test_cleanup_requires_saved_disk_metadata(cloud_copy, bucket, path):
    backup, meta, context, _s3, _configs = cloud_copy
    meta.cloud_storage.source.update({"bucket": bucket, "path": path})
    context.backup_layout.get_cloud_storage_metadata_remote_paths.return_value = []

    with pytest.raises(InvalidBackupStruct, match="disk metadata"):
        backup._delete(meta, {}, DeleteSettings(delete_cloud_storage_data=True))

    context.backup_layout.delete_backup.assert_not_called()


@pytest.mark.parametrize("operation", ["delete", "purge"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("endpoint_url", "https://wrong.example.test"),
        ("bucket", "wrong-bucket"),
        ("path", "copies/another-backup"),
    ],
)
def test_batch_validation_precedes_all_changes(cloud_copy, operation, field, value):
    backup, meta, context, _s3, _configs = cloud_copy
    meta.state = BackupState.CREATED
    other = deepcopy(meta)
    other.name = "backup2"
    other.state = BackupState.FAILED
    other.cloud_storage.source["path"] = "copies/backup2"
    full_other = deepcopy(other)
    full_other.cloud_storage.source[field] = value
    backup.config["copy"]["cloud_storage"]["target"]["bucket"] = "disk-copy"
    backup.config["backup"].update({"retain_count": 0, "retain_time": {}})
    context.backup_layout.get_backups.return_value = [meta, other]
    context.backup_layout.get_backup.side_effect = {
        meta.name: meta,
        other.name: full_other,
    }.get
    _saved_objects(context, [])

    with pytest.raises(ConfigurationError):
        if operation == "delete":
            backup.delete(
                meta.name,
                DeleteSettings(purge_partial=True, delete_cloud_storage_data=True),
            )
        else:
            backup.purge(DeleteSettings(delete_cloud_storage_data=True))

    context.backup_layout.upload_backup_metadata.assert_not_called()
    context.backup_layout.delete_backup.assert_not_called()
    assert meta.state == BackupState.CREATED
    assert other.state == full_other.state == BackupState.FAILED


@pytest.mark.parametrize("operation", ["delete", "purge"])
def test_batch_missing_disk_metadata_precedes_all_changes(cloud_copy, operation):
    backup, meta, context, _s3, _configs = cloud_copy
    meta.state = BackupState.CREATED
    other = deepcopy(meta)
    other.name = "backup2"
    other.state = BackupState.FAILED
    other.cloud_storage.source["path"] = "copies/backup2"
    backup.config["backup"].update({"retain_count": 0, "retain_time": {}})
    context.backup_layout.get_backups.return_value = [meta, other]
    context.backup_layout.get_backup.side_effect = {
        meta.name: meta,
        other.name: other,
    }.get
    _saved_objects(context, [])
    context.backup_layout.get_cloud_storage_metadata_remote_paths.side_effect = (
        lambda name, *_args: (["archive"] if name == meta.name else [])
    )

    with pytest.raises(InvalidBackupStruct, match="disk metadata"):
        if operation == "delete":
            backup.delete(
                meta.name,
                DeleteSettings(purge_partial=True, delete_cloud_storage_data=True),
            )
        else:
            backup.purge(DeleteSettings(delete_cloud_storage_data=True))

    context.backup_layout.upload_backup_metadata.assert_not_called()
    context.backup_layout.delete_backup.assert_not_called()
    assert meta.state == BackupState.CREATED
    assert other.state == BackupState.FAILED
