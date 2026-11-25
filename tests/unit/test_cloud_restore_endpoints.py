"""Restore endpoint trust, AWS defaults, and external-copy ownership."""

# pylint: disable=protected-access

from unittest.mock import MagicMock, patch

import pytest

from ch_backup.backup.sources import BackupSources
from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.clickhouse.disks import (
    ClickHouseDisksException,
    ClickHouseTemporaryDisks,
)
from ch_backup.config import DEFAULT_CONFIG
from tests.unit.test_ch_backup import _backup_metadata


def _restore_disk_config(stored_endpoint: str | None) -> dict:
    meta = _backup_metadata(
        cloud_storage_source={
            "endpoint_url": stored_endpoint,
            "bucket": "copied-disk",
            "path": "copies/backup1",
        }
    )
    context = MagicMock()
    context.config = {"force_non_replicated": False}
    context.backup_layout.get_backup.return_value = meta
    backup = ClickhouseBackup.__new__(ClickhouseBackup)
    backup.__dict__["_context"] = context
    with (
        patch.object(backup, "_check_zookeeper_for_restore"),
        patch.object(backup, "_restore") as restore,
    ):
        backup.restore(BackupSources(), "backup1", [], [])
    options = restore.call_args.kwargs
    disks = ClickHouseTemporaryDisks(
        MagicMock(),
        MagicMock(),
        DEFAULT_CONFIG,  # type: ignore[arg-type]
        meta,
        options["cloud_storage_source_bucket"],
        options["cloud_storage_source_path"],
        options["cloud_storage_source_endpoint"],
        MagicMock(),
    )
    disks._disks = {
        "s3": {
            "endpoint": "https://configured.example/original/data/",
            "access_key_id": "dummy-test-key",
            "secret_access_key": "dummy-test-secret",
        }
    }
    return disks._source_disk_config(meta, "s3")


def test_restore_rejects_unconfigured_metadata_endpoint() -> None:
    with pytest.raises(ClickHouseDisksException, match="endpoint"):
        _restore_disk_config("https://unconfigured.example")


def test_restore_null_endpoint_selects_aws_default() -> None:
    assert (
        _restore_disk_config(None)["endpoint"]
        == "https://s3.amazonaws.com/copied-disk/copies/backup1/"
    )


def test_inplace_restore_rejects_external_copy_even_when_endpoints_match() -> None:
    meta = _backup_metadata(
        cloud_storage_source={
            "endpoint_url": "https://configured.example",
            "bucket": "copied-disk",
            "path": "copies/backup1",
        }
    )
    assert not meta.cloud_storage.data_copied
    disks = ClickHouseTemporaryDisks.__new__(ClickHouseTemporaryDisks)
    disks._use_local_copy = True
    disks._source_bucket = "copied-disk"
    disks._source_path = "copies/backup1"
    disks._source_endpoint = "https://configured.example"
    disks._disks = {
        "s3": {"endpoint": "https://configured.example/copied-disk/copies/backup1/"}
    }
    disks._backup_layout = MagicMock()
    with (
        patch.object(disks, "_register_disk"),
        pytest.raises(ClickHouseDisksException, match="inplace"),
    ):
        disks._create_temporary_disk(meta, "s3")
