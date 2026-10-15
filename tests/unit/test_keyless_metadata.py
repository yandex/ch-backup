"""Keyless metadata reads, legacy encryption and protection of inline data."""

# The storage loader is mocked; Pylint infers the real loader's method types.
# pylint: disable=no-member

import copy
import json
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.clickhouse.models import Database, Disk
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.encryption import get_encryption
from ch_backup.exceptions import ConfigurationError, StorageError

KEY = "a" * 32


def make_layout(key: str | None = None) -> BackupLayout:
    """Use real crypto settings with an in-memory storage loader."""
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["backup"]["path_root"] = "backups"
    config["encryption"].update(key=key, chunk_size=64)
    with patch("ch_backup.backup.layout.StorageLoader"):
        return BackupLayout(config)  # type: ignore[arg-type]


def make_backup() -> BackupMetadata:
    return BackupMetadata("test", "2", "26.3", DEFAULT_CONFIG["backup"]["time_format"])


def get_loader(layout: BackupLayout) -> MagicMock:
    return cast(MagicMock, getattr(layout, "_storage_loader"))


def test_read_plaintext_without_initializing_crypto():
    layout = make_layout()
    backup = make_backup()
    loader = get_loader(layout)
    loader.download_data.return_value = backup.dump_json().encode()
    with patch("ch_backup.backup.layout.get_encryption", side_effect=AssertionError):
        loaded = layout.get_backup(backup.name)
    assert loaded is not None
    assert loaded.encrypted is True
    assert loaded.name == backup.name
    loader.download_data.assert_called_once()
    assert loader.download_data.call_args.kwargs == {"encoding": None}


def test_upload_public_metadata_preserves_data_encryption():
    layout = make_layout()
    layout.upload_backup_metadata(make_backup())
    calls = get_loader(layout).upload_data.call_args_list
    assert len(calls) == 2
    for uploaded in calls:
        assert not uploaded.kwargs.get("encryption", False)
        meta = json.loads(uploaded.args[0])["meta"]
        assert meta["encrypted"] is True
        assert meta["metadata_encrypted"] is False


@pytest.mark.parametrize("legacy_format", [None, True])
def test_light_only_update_preserves_full_metadata_encryption(legacy_format):
    layout = make_layout()
    backup = make_backup()
    backup.metadata_encrypted = legacy_format
    layout.upload_backup_metadata(backup, light_only=True)
    uploaded = get_loader(layout).upload_data.call_args
    assert uploaded.kwargs["remote_path"].endswith("backup_light_struct.json")
    assert (
        json.loads(uploaded.args[0])["meta"].get("metadata_encrypted") is legacy_format
    )


def test_failed_full_metadata_upload_preserves_legacy_format():
    layout = make_layout()
    backup = make_backup()
    backup.metadata_encrypted = True
    get_loader(layout).upload_data.side_effect = StorageError("upload failed")
    with pytest.raises(StorageError):
        layout.upload_backup_metadata(backup)
    assert backup.metadata_encrypted is True
    get_loader(layout).upload_data.assert_called_once()


@pytest.mark.parametrize(
    "encrypted, key", [(False, None), (True, KEY), (True, None), (True, "b" * 32)]
)
def test_read_legacy_metadata_preserves_encryption_defaults(encrypted, key):
    layout = make_layout(key)
    backup = make_backup()
    backup.add_database(Database("db", "Atomic", None, None, None))
    table = TableMetadata("db", "table", "MergeTree", None)
    table.add_part(
        PartMetadata("db", "table", "all_1_1_0", "checksum", 10, ["data.bin"], True)
    )
    backup.add_table(table)
    backup.cloud_storage.add_disk("s3")
    legacy = backup.dump()
    legacy["meta"].pop("metadata_encrypted")
    legacy["meta"].pop("encrypted")
    legacy["cloud_storage"].pop("encryption")
    legacy["databases"]["db"]["tables"]["table"]["parts"]["all_1_1_0"].pop("encrypted")
    raw = json.dumps(legacy).encode()
    if encrypted:
        crypto = get_encryption("nacl", {"key": KEY})
        raw = b"".join(
            crypto.encrypt(raw[pos : pos + 64]) for pos in range(0, len(raw), 64)
        )
    get_loader(layout).download_data.return_value = raw

    if not encrypted or key == KEY:
        loaded = layout.get_backup(backup.name)
        assert loaded is not None
        assert loaded.metadata_encrypted is encrypted
        assert loaded.encrypted is True
        assert loaded.cloud_storage.encrypted is True
        assert loaded.get_parts()[0].encrypted is True
    elif key is None:
        with pytest.raises(StorageError, match=r"encryption.key is required") as error:
            layout.get_backup(backup.name)
        assert isinstance(error.value.__cause__, ConfigurationError)
    else:
        with pytest.raises(StorageError, match="incorrect encryption key"):
            layout.get_backup(backup.name)
    get_loader(layout).download_data.assert_called_once()


@pytest.mark.parametrize(
    "data, encrypted, safe",
    [
        (b"5\n1\t3\n3\tdata/key\n0\n0\n\n", False, True),
        (b"5\n0\t0\n0\n0\nINLINE_SECRET\n", False, False),
        (b"6\n0\t0\n0\n0\nUNKNOWN_CONTENT\n", False, False),
        (b"not metadata", False, False),
        (b"5\n0\t0\n0\n0\nINLINE_SECRET\n", True, True),
    ],
)
def test_rewritten_cloud_metadata_is_checked_before_upload(data, safe, encrypted):
    layout = make_layout()
    backup = make_backup()
    backup.cloud_storage.encrypted = encrypted
    files = [("store/part/data.bin", data)]
    if safe or encrypted:
        layout.upload_cloud_storage_metadata_files(backup, "disks/table.tar", files)
        get_loader(layout).upload_data_tarball.assert_called_once()
    else:
        with pytest.raises(StorageError, match="Cannot publish unencrypted") as error:
            layout.upload_cloud_storage_metadata_files(backup, "disks/table.tar", files)
        assert "INLINE_SECRET" not in str(error.value)
        get_loader(layout).upload_data_tarball.assert_not_called()


def test_frozen_inline_data_is_rejected_before_upload(tmp_path: Path) -> None:
    layout = make_layout()
    backup = make_backup()
    backup.cloud_storage.encrypted = False
    (tmp_path / "part").mkdir()
    (tmp_path / "part" / "data.bin").write_bytes(b"5\n1\t3\n3\tdata/key\n0\n0\n\n")
    (tmp_path / "frozen_metadata.txt").write_text("service metadata", encoding="utf-8")
    (tmp_path / "part" / "inline.bin").write_bytes(b"4\n0\t0\n0\n0\nSECRET\n")
    table = MagicMock(path_on_disk="store/uuid", database="db", name="table")
    table.name = "table"
    disk = Disk("s3", str(tmp_path), "s3")
    with patch(
        "ch_backup.backup.layout._table_shadow_path", return_value=str(tmp_path)
    ):
        with pytest.raises(StorageError, match="inline file data"):
            layout.upload_cloud_storage_metadata(backup, disk, table)
        get_loader(layout).upload_files_tarball_scan.assert_not_called()
