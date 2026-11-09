import copy
from unittest.mock import MagicMock, call, patch

import pytest

from ch_backup.backup.cloud_storage_decryption import (
    CloudStorageDecryption,
    resolve_decryption_staging,
    resolve_restore_decryption_locations,
)
from ch_backup.backup.metadata import BackupMetadata
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.encryption import get_cloud_storage_data_encryption_metadata
from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.object_copy import make_storage_config
from tests.unit.test_backup_copy import _object_storage_metadata


def test_decryption_keep_going_and_drain() -> None:
    # pylint: disable=protected-access
    with patch(
        "ch_backup.backup.cloud_storage_decryption.StorageLoader"
    ) as loader_class:
        decryption = CloudStorageDecryption(DEFAULT_CONFIG, MagicMock())
        loader = loader_class.return_value

        def fail_async(_entry, *, on_error, **_kwargs):
            on_error(RuntimeError("failed"))

        loader.copy_object.side_effect = fail_async
        decryption._decrypt_entries({"object": MagicMock()}, "s3", keep_going=True)
        loader.wait.assert_called_once_with(keep_going=True)

        loader.reset_mock()
        loader.copy_object.side_effect = [None, RuntimeError("submission failed")]
        with pytest.raises(RuntimeError, match="submission failed"):
            decryption._decrypt_entries(
                {"first": MagicMock(), "second": MagicMock()},
                "s3",
                keep_going=False,
            )
        assert loader.wait.call_args_list == [call(keep_going=True)]


@pytest.mark.parametrize("staging_path", ["copies/backup", "copies/backup/plaintext"])
def test_decryption_staging_rejects_overlapping_prefix(staging_path: str) -> None:
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["restore"]["cloud_storage"]["decryption_staging"]["path"] = staging_path
    source = make_storage_config(
        config["storage"], "https://s3.example", "bucket", "access", "secret"
    )
    disk = {
        "endpoint": "https://s3.example/bucket/copies/backup/",
        "access_key_id": "access",
        "secret_access_key": "secret",
    }

    with pytest.raises(ConfigurationError, match="must not overlap"):
        resolve_decryption_staging(
            config, source, "copies/backup", disk, owner_name="backup", disk_name="s3"
        )


@pytest.mark.parametrize(
    "staging, message",
    [
        ({"access_key_id": "access"}, "must be set together"),
        ({"secret_access_key": "secret"}, "must be set together"),
        ({"bucket": "other"}, "credentials are required"),
    ],
)
def test_decryption_staging_validates_credentials(staging: dict, message: str) -> None:
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["restore"]["cloud_storage"]["decryption_staging"].update(staging)
    source = make_storage_config(
        config["storage"], "https://s3.example", "bucket", "access", "secret"
    )
    disk = {
        "endpoint": "https://s3.example/bucket/source/",
        "access_key_id": "access",
        "secret_access_key": "secret",
    }

    with pytest.raises(ConfigurationError, match=message):
        resolve_decryption_staging(
            config, source, "source", disk, owner_name="backup", disk_name="s3"
        )


def test_aws_default_endpoint_cannot_hide_staging_source_overlap():
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["restore"]["cloud_storage"]["decryption_staging"]["path"] = "copies/backup"
    source = make_storage_config(config["storage"], None, "bucket", "access", "secret")
    disk = {
        "endpoint": "https://s3.amazonaws.com/bucket/data/",
        "access_key_id": "access",
        "secret_access_key": "secret",
    }
    with pytest.raises(ConfigurationError, match="must not overlap"):
        resolve_decryption_staging(
            config, source, "copies/backup", disk, owner_name="backup", disk_name="s3"
        )


@pytest.mark.parametrize("root", [None, "restore-temp"])
def test_decryption_separates_owners_and_disks_and_cleans_only_allocations(root):
    """Equal relative keys must coexist; cleanup must leave the staging root's other data."""
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["encryption"]["key"] = "a" * 32
    config["restore"]["cloud_storage"]["decryption_staging"]["path"] = root
    encryption = get_cloud_storage_data_encryption_metadata(config["encryption"])
    disk = {"endpoint": "https://s3.example/staging/data/"}
    sources = []
    for name, disk_name in [
        ("owner/a", "s3"),
        ("owner_a", "s3"),
        ("owner/a", "s3-other"),
    ]:
        owner = BackupMetadata(name, "2", "26.3", "%Y-%m-%d")
        owner.cloud_storage.data_encryption = encryption
        sources.append(
            (
                owner,
                disk_name,
                {"endpoint": f"https://s3.example/source/{name}/{disk_name}/"},
                disk,
            )
        )
    locations = resolve_restore_decryption_locations(
        config, MagicMock(), sources[0][0], sources
    )
    assert len({location.staging_path for location in locations.values()}) == 3
    assert locations == resolve_restore_decryption_locations(
        config, MagicMock(), sources[0][0], sources
    )
    if root:
        assert all(
            location.staging_path.startswith(root + "/")
            for location in locations.values()
        )

    objects = {f"{root or 'owner'}/unrelated": b"keep"}
    untouched = dict(objects)
    layout = MagicMock()
    layout.list_cloud_storage_metadata_archives.return_value = ["archive"]
    layout.read_cloud_storage_metadata_archive.return_value = [
        ("all_1_1_0/data.bin", _object_storage_metadata(("object", 1)))
    ]
    engine = MagicMock()
    engine.list_dir.side_effect = lambda prefix, **_: [
        key for key in objects if key.startswith(prefix + "/")
    ]
    engine.delete_files.side_effect = lambda keys: [objects.pop(key) for key in keys]
    with (
        patch("ch_backup.backup.cloud_storage_decryption.StorageLoader") as loader,
        patch(
            "ch_backup.backup.cloud_storage_decryption.get_storage_engine",
            return_value=engine,
        ),
    ):
        loader.return_value.copy_object.side_effect = lambda entry, **_: objects.update(
            {entry.dst_path: entry.src_path.encode()}
        )
        decryption = CloudStorageDecryption(config, layout)
        expected = dict(untouched)
        for owner, disk_name, _, _ in sources:
            location = locations[owner.name, disk_name]
            expected[f"{location.staging_path}/object"] = (
                f"{location.source_path}/object".encode()
            )
            decryption.decrypt(
                owner,
                disk_name,
                "all",
                location,
            )
            assert objects == expected
        decryption.cleanup()
        assert objects == untouched
