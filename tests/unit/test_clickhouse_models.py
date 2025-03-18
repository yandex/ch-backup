"""
Unit tests clickhouse models.
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from ch_backup.clickhouse.models import EncryptedFile, EncryptedFileHeader

PIPELINE_TEST_COUNT = 100


@settings(max_examples=PIPELINE_TEST_COUNT, deadline=None)
@given(ba=st.binary(min_size=64, max_size=1024))
def test_encrypted_file_header_constructor(ba: bytes) -> None:
    iv = ba[23:39]

    efh = EncryptedFileHeader(ba)

    assert efh.iv_hex == iv.hex()


@settings(max_examples=PIPELINE_TEST_COUNT, deadline=None)
@given(ba=st.binary(min_size=128, max_size=1024))
def test_encrypted_file_constructor(ba: bytes) -> None:
    ef = EncryptedFile(ba)

    efh = EncryptedFileHeader(ba)

    assert ef.header == efh
    assert ef.data_hex == ba[64:].hex()
