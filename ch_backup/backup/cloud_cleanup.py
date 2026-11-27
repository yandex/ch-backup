"""Disk-backed object selection for prepared cloud cleanup."""

import json
import os
from collections.abc import Iterable, Iterator
from itertools import chain
from tempfile import NamedTemporaryFile, TemporaryDirectory

from ch_backup.backup.copy_context import (
    COPY_CONTEXT_DIR,
    read_copy_manifest,
    write_copy_manifest,
)
from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.engine.base import StorageEngine
from ch_backup.storage.loader import StorageLoader
from ch_backup.storage.object_copy import CopyEntry, storage_id
from ch_backup.storage.temporary_disk_index import TemporaryDiskIndex


class CloudObjectDeletionIndex:
    """Keep unique candidate keys on disk.

    Reuse the copy index's bounded SQLite cache and temporary-file ownership.
    The prepared cleanup operation keeps this index alive until execution ends.
    """

    def __init__(self) -> None:
        self._index = TemporaryDiskIndex()

    def update(self, keys: Iterable[str]) -> None:
        """Add unique object keys to the deletion plan."""
        for key in keys:
            self._index.put_if_absent(key, b"")

    def __len__(self) -> int:
        return len(self._index)

    def __iter__(self) -> Iterator[str]:
        """Read deletable keys in bounded batches; allow retries to start again."""
        after = None
        while rows := self._index.read_sorted_batch(after, 1024):
            after = rows[-1][0]
            for key, _value in rows:
                yield key


class CloudCleanupManifests:
    """Retain one merged copy manifest per external destination across retries."""

    def __init__(
        self, storage: StorageEngine | StorageLoader, backup_path: str
    ) -> None:
        self.storage = storage
        self.directory = os.path.join(backup_path, COPY_CONTEXT_DIR, "cleanup")
        self.locations_path = os.path.join(self.directory, "locations.json")
        self.locations = (
            json.loads(storage.download_data(self.locations_path))
            if storage.path_exists(self.locations_path)
            else []
        )
        name = os.path.basename(backup_path)
        if not isinstance(self.locations, list):
            raise ConfigurationError("Invalid cloud cleanup destinations")
        for location in self.locations:
            if not (
                isinstance(location, dict)
                and isinstance(location.get("path"), str)
                and not location["path"].startswith("/")
                and location["path"].split("/")[-1] == name
                and not {".", ".."}.intersection(location["path"].split("/"))
                and isinstance(location.get("bucket"), str)
                and location["bucket"]
                and "endpoint_url" in location
                and isinstance(location["endpoint_url"], (str, type(None)))
            ):
                raise ConfigurationError("Invalid cloud cleanup destination")

    def _path(self, index: int) -> str:
        return os.path.join(self.directory, f"{index}.jsonl.gz")

    def _entries(self, index: int) -> Iterator[CopyEntry]:
        location = self.locations[index]
        with TemporaryDirectory() as directory:
            manifest = os.path.join(directory, "manifest.gz")
            self.storage.download_file(self._path(index), manifest)
            for entry in read_copy_manifest(manifest):
                key = entry.dst_path
                if (
                    storage_id(entry.dst_storage)
                    != ("s3", location["endpoint_url"], location["bucket"])
                    or not isinstance(key, str)
                    or not key.startswith(location["path"] + "/")
                    or key == location["path"] + "/"
                    or {".", ".."}.intersection(key.split("/"))
                ):
                    raise ConfigurationError("Key is outside the copy destination")
                yield entry

    def keys(self) -> list[tuple[dict, CloudObjectDeletionIndex]]:
        """Validate each recorded target before returning keys for deletion."""
        result = []
        for index, location in enumerate(self.locations):
            keys = CloudObjectDeletionIndex()
            keys.update(entry.dst_path for entry in self._entries(index))
            result.append((location, keys))
        return result

    def merge(self, location: dict, entries: Iterable[CopyEntry]) -> None:
        """Publish planned transfers before payload writes, retaining earlier keys."""
        existing = location in self.locations
        index = self.locations.index(location) if existing else len(self.locations)
        previous = self._entries(index) if existing else ()
        seen = TemporaryDiskIndex()
        unique = (
            entry
            for entry in chain(previous, entries)
            if seen.put_if_absent(entry.dst_path, b"") is None
        )
        with NamedTemporaryFile() as manifest:
            write_copy_manifest(manifest.name, unique)
            self.storage.upload_file(manifest.name, self._path(index))
        if not existing:
            # Publish the location only after its manifest exists. Payload transfer
            # starts after both writes, so an interrupted registration can be retried.
            self.storage.upload_data(
                json.dumps([*self.locations, location]).encode(), self.locations_path
            )
            self.locations.append(location)
