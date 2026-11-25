"""Bounded sorted reads from the shared temporary disk index."""

import pytest

from ch_backup.storage.temporary_disk_index import TemporaryDiskIndex


def test_sorted_index_batches_preserve_empty_keys_and_reject_invalid_limits():
    """A None cursor starts before the empty key; a returned key resumes after it."""
    index = TemporaryDiskIndex()
    for key in ("z", "", "a"):
        index.put(key, key.encode())
    assert index.read_sorted_batch(None, 2) == [("", b""), ("a", b"a")]
    assert index.read_sorted_batch("", 1) == [("a", b"a")]
    assert index.read_sorted_batch("a", 2) == [("z", b"z")]
    assert index.read_sorted_batch("z", 2) == []
    with pytest.raises(ValueError, match="positive"):
        index.read_sorted_batch(None, 0)
