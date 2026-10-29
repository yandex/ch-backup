import copy

import pytest

from ch_backup.backup.cloud_storage_decryption import resolve_decryption_staging
from ch_backup.config import DEFAULT_CONFIG


@pytest.mark.parametrize(
    ("destination_endpoint", "expected_path"),
    [
        ("https://dst/destination/data/", "copies/backup"),
        (
            "https://source/encrypted/data/",
            "copies/backup/.ch-backup-decrypted",
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
