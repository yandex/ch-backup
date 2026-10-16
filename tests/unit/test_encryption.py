import pytest

from ch_backup.encryption import get_encryption
from ch_backup.encryption.nacl import NaClEncryption
from ch_backup.encryption.noop import NoopEncryption
from ch_backup.exceptions import ConfigurationError, UnknownEncryptionError


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
