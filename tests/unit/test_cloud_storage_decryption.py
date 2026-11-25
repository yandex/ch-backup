import copy
from unittest.mock import MagicMock, call, patch

import pytest

from ch_backup.backup.cloud_storage_decryption import (
    CloudStorageDecryption,
    resolve_decryption_staging,
    validate_staging_sources,
)
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.object_copy import make_storage_config


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


@pytest.mark.parametrize(
    "staging_path", ["copies", "copies/backup", "copies/backup/plaintext"]
)
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
        resolve_decryption_staging(config, source, "copies/backup", disk)


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
        resolve_decryption_staging(config, source, "source", disk)


def test_default_port_keeps_restore_staging_outside_source() -> None:
    """Equivalent endpoints must select a separate plaintext staging prefix."""
    source = make_storage_config(
        DEFAULT_CONFIG["storage"], "https://s3.example", "bucket", "a", "b"
    )
    disk = {
        "endpoint": "https://s3.example:443/bucket/live/",
        "access_key_id": "a",
        "secret_access_key": "b",
    }
    storage, path = resolve_decryption_staging(
        DEFAULT_CONFIG, source, "backups/export", disk
    )
    assert path == "backups/export_decrypted"
    with (
        patch("ch_backup.backup.cloud_storage_decryption.StorageLoader"),
        patch("ch_backup.backup.cloud_storage_decryption.get_storage_engine") as engine,
    ):
        decryption = CloudStorageDecryption(DEFAULT_CONFIG, MagicMock())
        with pytest.raises(ConfigurationError, match="overlaps"):
            decryption.decrypt(
                MagicMock(),
                "s3",
                "all",
                source,
                "backups/export",
                storage,
                "backups/export",
            )
        engine.assert_not_called()


def test_staging_cannot_cover_another_source_disk() -> None:
    """A sibling disk and a linked owner remain protected across the whole restore."""
    source = make_storage_config(
        DEFAULT_CONFIG["storage"], "https://s3.example", "bucket", "a", "b"
    )
    staging = make_storage_config(
        DEFAULT_CONFIG["storage"], "https://s3.example:443", "bucket", "a", "b"
    )
    with pytest.raises(ConfigurationError, match="overlaps"):
        validate_staging_sources(
            staging, "owner/s3b", [(source, "backup/s3a"), (source, "owner/s3b")]
        )
