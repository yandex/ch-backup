"""Disk-backed object selection for prepared cloud cleanup."""

from collections.abc import Iterable, Iterator

from ch_backup.storage.temporary_disk_index import TemporaryDiskIndex


class CloudObjectDeletionIndex:
    """Keep unique payload keys on disk for whole-owner deletion.

    Reuse the copy index's bounded SQLite cache and temporary-file ownership.
    The prepared cleanup operation keeps this index alive until execution ends.
    """

    def __init__(self) -> None:
        self._index = TemporaryDiskIndex()

    def update(self, keys: Iterable[str]) -> None:
        """Record each key once."""
        for key in keys:
            self._index.put_if_absent(key, b"")

    def __len__(self) -> int:
        return len(self._index)

    def __iter__(self) -> Iterator[str]:
        """Read deletable keys in bounded batches; allow retries to start again."""
        after = None
        while rows := self._index.read_sorted_batch(after, 1024):
            after = rows[-1][0]
            for key, _ in rows:
                yield key
