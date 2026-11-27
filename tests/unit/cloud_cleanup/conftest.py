"""Scoped fixtures for cloud cleanup layout and deletion tests."""

# pylint: disable=protected-access

import json
import shutil
from copy import deepcopy
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest

from ch_backup.backup.copy_context import write_copy_manifest
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupState,
    TableMetadata,
)
from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.clickhouse.models import Database
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.storage.metadata_archive import spool_metadata
from ch_backup.storage.object_copy import CopyEntry
from tests.unit.test_backup_copy import _object_storage_metadata
from tests.unit.test_ch_backup import _backup_metadata
from tests.unit.test_layout import make_layout


@pytest.fixture(name="make_cloud_backup")
def fixture_make_cloud_backup():
    """Build independent copied-backup metadata for owners and their children."""

    def make():
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

    return make


@pytest.fixture(name="cloud_layout")
def fixture_cloud_layout(monkeypatch, make_cloud_backup):
    """Model archive publication and payload deletion without manager state."""
    config = deepcopy(DEFAULT_CONFIG)
    config["storage"]["credentials"].update(
        endpoint_url="https://s3.amazonaws.com", bucket="backups"
    )
    config["storage"]["bulk_delete_chunk_size"] = 2
    config["backup"].update(retain_count=0, retain_time={}, path_root="ch_backup")
    meta = make_cloud_backup()
    meta.state = BackupState.CREATED
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
    monkeypatch.setattr(
        layout,
        "download_cloud_storage_metadata_archive",
        MagicMock(side_effect=lambda _backup, path: spool_metadata(archives[path])),
    )
    engine_factory = MagicMock()
    engine_factory.return_value.delete_files.side_effect = objects.difference_update
    monkeypatch.setattr("ch_backup.ch_backup.get_storage_engine", engine_factory)
    return SimpleNamespace(
        meta=meta,
        config=config,
        layout=layout,
        loader=loader,
        archives=archives,
        objects=objects,
        engine=engine_factory.return_value,
        engine_factory=engine_factory,
    )


@pytest.fixture(name="cleanup")
def fixture_cleanup(monkeypatch, cloud_layout):
    """Add persisted backup records and manager operations for deletion recovery."""
    c = cloud_layout
    config, meta, layout = c.config, c.meta, c.layout
    records = {meta.name: meta.dump_json()}
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
    context = MagicMock(backup_layout=layout)
    context.config = config["backup"]
    manager = ClickhouseBackup(config)  # type: ignore[arg-type]
    manager.__dict__["_context"] = context
    c.records, c.context, c.manager = records, context, manager
    return c


@pytest.fixture
def seed_manifests(tmp_path):
    """Install standard transfer manifests and their destination list in the fixture."""

    def seed(c, *groups):
        root = "ch_backup/backup1/copy_context/cleanup"
        files = {}
        for index, (location, keys) in enumerate(groups):
            path = tmp_path / f"{index}.jsonl.gz"
            storage = {"type": "s3", "credentials": location}
            write_copy_manifest(
                str(path),
                (CopyEntry(storage, storage, "source", key, 1, 1) for key in keys),
            )
            files[f"{root}/{path.name}"] = path
        c.loader.path_exists.side_effect = (
            lambda key: key == f"{root}/locations.json" or key in c.archives
        )
        c.loader.download_data.return_value = json.dumps([loc for loc, _ in groups])
        c.loader.download_file.side_effect = lambda remote, local, **_: shutil.copyfile(
            files[remote], local
        )

    return seed
