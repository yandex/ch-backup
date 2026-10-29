import copy
from unittest.mock import MagicMock, patch

import pytest

from ch_backup.backup.cloud_storage_decryption import (
    CloudStorageDecryption,
    resolve_decryption_staging,
)
from ch_backup.config import DEFAULT_CONFIG


@pytest.mark.parametrize(
    ("destination_endpoint", "expected_path"),
    [
        ("https://dst/destination/data/", "copies/backup"),
        (
            "https://source/encrypted/data/",
            "copies/backup_decrypted",
        ),
    ],
)
def test_resolve_decryption_staging(
    destination_endpoint: str, expected_path: str
) -> None:
    config = copy.deepcopy(DEFAULT_CONFIG)
    source = copy.deepcopy(config["storage"])
    source["credentials"].update(endpoint_url="https://source", bucket="encrypted")
    destination_disk = {
        "endpoint": destination_endpoint,
        "access_key_id": "destination-key",
        "secret_access_key": "destination-secret",
    }

    storage, path = resolve_decryption_staging(
        config, source, "copies/backup", destination_disk
    )

    assert storage["credentials"] == {
        "endpoint_url": destination_endpoint.rsplit("/", 3)[0],
        "bucket": destination_endpoint.rsplit("/", 3)[1],
        "access_key_id": "destination-key",
        "secret_access_key": "destination-secret",
    }
    assert path == expected_path


def test_decrypt_cloud_storage_data() -> None:
    config = copy.deepcopy(DEFAULT_CONFIG)
    layout = MagicMock()
    backup = MagicMock()
    backup.get_sanitized_name.return_value = "backup"
    backup.cloud_storage.compressed = True
    layout.get_cloud_storage_metadata_remote_paths.return_value = ["metadata.tar.gz"]
    metadata = b"5\n1\t4\n4\tcopies/backup/object\n1\n0\n\n"
    layout.get_cloud_storage_metadata_files.return_value = [
        ("part/data.bin", metadata),
        ("part/copy.bin", metadata),
    ]
    source = copy.deepcopy(config["storage"])
    source["credentials"].update(endpoint_url="https://source", bucket="encrypted")
    staging = copy.deepcopy(config["storage"])
    staging["credentials"].update(endpoint_url="https://target", bucket="plaintext")

    with patch(
        "ch_backup.backup.cloud_storage_decryption.StorageLoader"
    ) as loader_class:
        decryption = CloudStorageDecryption(config, layout)
        decryption.decrypt(
            backup,
            "s3",
            "all",
            source,
            "copies/backup",
            staging,
            "copies/backup",
        )

        loader = loader_class.return_value
        loader.copy_object.assert_called_once()
        entry = loader.copy_object.call_args.args[0]
        assert (entry.src_path, entry.dst_path, entry.size, entry.dst_size) == (
            "copies/backup/object",
            "copies/backup/object",
            4,
            4,
        )
        assert entry.decrypt
        loader.wait.assert_called_once_with(keep_going=False)

        loader.copy_object.reset_mock()
        loader.wait.reset_mock()

        def fail_copy(_entry, *, on_error, **_kwargs):
            on_error(RuntimeError("failed"))

        loader.copy_object.side_effect = fail_copy
        decryption.decrypt(
            backup,
            "s3",
            "all",
            source,
            "copies/backup",
            staging,
            "copies/backup",
            keep_going=True,
        )
        loader.wait.assert_called_once_with(keep_going=True)

        loader.copy_object.side_effect = None
        loader.copy_object.reset_mock()
        loader.wait.reset_mock()
        loader.wait.side_effect = [RuntimeError("failed"), None]
        with pytest.raises(RuntimeError, match="failed"):
            decryption.decrypt(
                backup,
                "s3",
                "all",
                source,
                "copies/backup",
                staging,
                "copies/backup",
            )
        assert [call.kwargs for call in loader.wait.call_args_list] == [
            {"keep_going": False},
            {"keep_going": True},
        ]
