"""Bounded object selection for cloud cleanup."""

from ch_backup.backup.cloud_cleanup import CloudObjectDeletionIndex


def test_deletion_index_deduplicates_across_batches_and_restarts():
    """Repeated keys appear once and retries can restart iteration."""
    index = CloudObjectDeletionIndex()
    index.update(f"{number:04d}" for number in reversed(range(3000)))
    index.update(f"{number:04d}" for number in range(2500))
    expected = [f"{number:04d}" for number in range(3000)]
    assert list(index) == expected
    assert list(index) == expected
