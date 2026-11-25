import copy
from unittest.mock import MagicMock, patch

import pytest
from nacl.exceptions import CryptoError

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.encryption import (
    get_cloud_storage_data_encryption_config,
    get_cloud_storage_data_encryption_metadata,
    get_encryption,
    validate_cloud_storage_data_encryption_config,
)
from ch_backup.encryption.nacl import NaClEncryption
from ch_backup.encryption.noop import NoopEncryption
from ch_backup.exceptions import (
    ConfigurationError,
    StorageError,
    UnknownEncryptionError,
)
from ch_backup.storage.async_pipeline.pipelines import copy_object_pipeline
from ch_backup.storage.object_copy import CopyEntry


def test_encryption_noop():
    assert isinstance(get_encryption("noop", {}), NoopEncryption)


def test_encryption_nacl():
    assert isinstance(get_encryption("nacl", {"key": "a" * 32}), NaClEncryption)


def test_get_encryption_nacl_without_key() -> None:
    with pytest.raises(ConfigurationError, match=r"encryption.key is required"):
        get_encryption("nacl", {})


def test_get_encryption_unknown_type() -> None:
    with pytest.raises(UnknownEncryptionError):
        get_encryption("unknown", {})


def test_cloud_storage_data_encryption_inherits_key() -> None:
    config = {
        "encryption": {"type": "nacl", "key": "main-key", "chunk_size": 1024},
        "cloud_storage": {"data_encryption": {"chunk_size": 2048}},
    }

    assert get_cloud_storage_data_encryption_config(config) == {
        "type": "nacl",
        "key": "main-key",
        "chunk_size": 2048,
    }


def test_validate_cloud_storage_data_encryption_config() -> None:
    config: dict = {
        "encryption": {"type": "nacl", "key": "k" * 32, "chunk_size": 1024},
        "cloud_storage": {"data_encryption": {}},
    }
    metadata = get_cloud_storage_data_encryption_metadata(config["encryption"])

    assert (
        validate_cloud_storage_data_encryption_config(config, metadata)
        == config["encryption"]
    )

    config["cloud_storage"]["data_encryption"] = {"key": "x" * 32}
    with pytest.raises(ConfigurationError, match="key does not match"):
        validate_cloud_storage_data_encryption_config(config, metadata)


@pytest.mark.parametrize("length", [0, 1, 63, 64, 65, 193])
@pytest.mark.parametrize("encrypted", [False, True])
def test_copy_pipeline_exports_bytes_under_new_key(
    length: int, encrypted: bool
) -> None:
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["encryption"].update(key="b" * 32, chunk_size=64)
    source_config = {**config["encryption"], "key": "a" * 32}
    source = get_encryption("nacl", source_config)
    target = get_encryption("nacl", config["encryption"])
    plaintext = bytes(range(length))
    original = (
        b"".join(
            source.encrypt(plaintext[offset : offset + 64])
            for offset in range(0, length, 64)
        )
        if encrypted
        else plaintext
    )
    source_storage = config["storage"]
    target_storage = copy.deepcopy(source_storage)
    if length == 193:
        target_storage["chunk_size"] = 128
    dst_size = length + ((length + 63) // 64) * target.metadata_size()
    entry = CopyEntry(
        source_storage,
        target_storage,
        "source",
        "target",
        len(original),
        dst_size,
        encrypt=True,
        decrypt=encrypted,
        backup_encryption=True,
    )
    reader, writer = MagicMock(), MagicMock()
    reader.download_part.side_effect = [original, b""]
    reader.get_object_size.return_value = len(original)

    def get_engine(storage: dict) -> MagicMock:
        return reader if storage is source_storage else writer

    with (
        patch(
            "ch_backup.storage.async_pipeline.pipeline_builder.get_storage_engine",
            side_effect=get_engine,
        ),
        patch(
            "ch_backup.storage.async_pipeline.pipelines.get_storage_engine",
            side_effect=get_engine,
        ),
    ):
        copy_object_pipeline(config, entry, source_config)
        if length == 193:
            writer.complete_multipart_upload.assert_called_once()
            blocks = {
                call.args[3]: call.args[0] for call in writer.upload_part.call_args_list
            }
            stored = b"".join(blocks[i] for i in sorted(blocks))
        else:
            stored = writer.upload_data.call_args.args[0]
        assert len(stored) == dst_size
        chunk = 64 + target.metadata_size()
        assert (
            b"".join(
                target.decrypt(stored[i : i + chunk])
                for i in range(0, len(stored), chunk)
            )
            == plaintext
        )
        if stored:
            with pytest.raises(CryptoError):
                source.decrypt(stored[:chunk])
        if encrypted and length > 64:
            reader.download_part.side_effect = [original[:chunk], b""]
            writer.reset_mock()
            with pytest.raises(StorageError, match="changed size"):
                copy_object_pipeline(config, entry, source_config)
            writer.upload_data.assert_not_called()
            writer.complete_multipart_upload.assert_not_called()
