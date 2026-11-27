"""Bounded object selection for cloud cleanup."""

from ch_backup.backup.cloud_cleanup import CloudObjectDeletionIndex


def test_deletion_index_deduplicates_keys_across_batches_and_restarts():
    """Retries read the same unique keys, including keys on later index pages."""
    index = CloudObjectDeletionIndex()
    index.update(f"{number:04d}" for number in reversed(range(3000)))
    index.update(f"{number:04d}" for number in range(2500))
    assert len(index) == 3000
    expected = [f"{number:04d}" for number in range(3000)]
    assert list(index) == expected
    assert list(index) == expected
