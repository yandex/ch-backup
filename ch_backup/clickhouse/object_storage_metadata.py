"""
Parser of ClickHouse object storage (S3 disk) metadata files.

Each file of a part stored on an object storage disk is represented on the local
filesystem by a small metadata file that references the remote objects holding
the file data. Format (see DiskObjectStorageMetadata.cpp in ClickHouse sources):

    <version>\\n
    <objects_count>\\t<total_size>\\n
    <object_size>\\t<escaped object key>\\n    (repeated objects_count times)
    <ref_count>\\n
    <read_only>\\n                             (version >= 3)
    <escaped inline data>\\n                   (version >= 4)

In versions 2-4 object keys are relative to the key prefix of the disk (path part of
the disk endpoint), in version 5 object keys are full keys of objects in the bucket.
"""

from typing import NamedTuple, Sequence

from ch_backup.exceptions import ClickhouseBackupError

VERSION_ABSOLUTE_PATHS = 1
VERSION_RELATIVE_PATHS = 2
VERSION_READ_ONLY_FLAG = 3
VERSION_INLINE_DATA = 4
VERSION_FULL_OBJECT_KEY = 5
FROZEN_METADATA_FNAME = "frozen_metadata.txt"

_ESCAPE_SEQUENCES = {
    "a": "\a",
    "b": "\b",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "v": "\v",
    "0": "\0",
}
_ESCAPED_CHARS = {
    **{char: f"\\{escaped}" for escaped, char in _ESCAPE_SEQUENCES.items()},
    "\\": "\\\\",
    "'": "\\'",
}


class ObjectStorageMetadataError(ClickhouseBackupError):
    """
    Invalid or unsupported object storage metadata file.
    """


class StorageObject(NamedTuple):
    """
    Remote object referenced from object storage metadata file.
    """

    key: str
    size: int


class ObjectStorageMetadata(NamedTuple):
    """
    Parsed object storage metadata file.
    """

    version: int
    objects: list[StorageObject]
    ref_count: int
    read_only: bool
    inline_data: str

    @property
    def full_object_keys(self) -> bool:
        """
        Return True if object keys are full keys in the bucket, False if they're relative to the disk key prefix.
        """
        return self.version >= VERSION_FULL_OBJECT_KEY


def parse_object_storage_metadata(data: bytes) -> ObjectStorageMetadata:
    """
    Parse object storage metadata file.

    Version 1 is not supported as it stores absolute paths that are bound to the disk key prefix.
    """
    lines = data.decode("utf-8", errors="surrogateescape").split("\n")
    try:
        version = int(lines[0])
        if version == VERSION_ABSOLUTE_PATHS:
            raise ObjectStorageMetadataError(
                f"Object storage metadata version {version} is not supported"
            )
        if version < VERSION_ABSOLUTE_PATHS or version > VERSION_FULL_OBJECT_KEY:
            raise ObjectStorageMetadataError(
                f"Unknown object storage metadata version {version}"
            )

        objects_count, _total_size = lines[1].split("\t")
        objects = []
        for line in lines[2 : 2 + int(objects_count)]:
            size, key = line.split("\t", 1)
            objects.append(StorageObject(key=_unescape(key), size=int(size)))
        if len(objects) != int(objects_count):
            raise ObjectStorageMetadataError("Unexpected end of file")

        pos = 2 + len(objects)
        ref_count = int(lines[pos])
        pos += 1

        read_only = False
        if version >= VERSION_READ_ONLY_FLAG:
            read_only = _parse_bool(lines[pos])
            pos += 1

        inline_data = ""
        if version >= VERSION_INLINE_DATA:
            inline_data = _unescape(lines[pos])
            pos += 1
        if any(lines[pos:]):
            raise ObjectStorageMetadataError("Unexpected trailing content")
    except (IndexError, ValueError) as e:
        # Conversion errors can contain raw file data. Keep the exception type
        # and traceback for diagnostics, but do not expose the rejected value.
        e.args = ("Invalid object storage metadata field",)
        raise ObjectStorageMetadataError("Malformed object storage metadata") from e

    return ObjectStorageMetadata(
        version=version,
        objects=objects,
        ref_count=ref_count,
        read_only=read_only,
        inline_data=inline_data,
    )


def replace_object_keys(data: bytes, keys: Sequence[str]) -> bytes:
    """
    Replace keys of objects referenced from object storage metadata file. Other content is kept as is.
    """
    lines = data.decode("utf-8", errors="surrogateescape").split("\n")
    objects_count = int(lines[1].split("\t")[0])
    if objects_count != len(keys):
        raise ObjectStorageMetadataError(
            f"Metadata file references {objects_count} objects, but {len(keys)} keys are specified"
        )

    for i, key in enumerate(keys):
        size, _ = lines[2 + i].split("\t", 1)
        lines[2 + i] = f"{size}\t{_escape(key)}"

    return "\n".join(lines).encode("utf-8", errors="surrogateescape")


def _parse_bool(value: str) -> bool:
    if value in ("1", "true"):
        return True
    if value in ("0", "false"):
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def _escape(value: str) -> str:
    """
    Escape string as ClickHouse writeEscapedString() does.
    """
    return "".join(_ESCAPED_CHARS.get(char, char) for char in value)


def _unescape(value: str) -> str:
    """
    Unescape string written by ClickHouse writeEscapedString().
    """
    if "\\" not in value:
        return value

    result = []
    i = 0
    while i < len(value):
        char = value[i]
        if char != "\\" or i + 1 == len(value):
            result.append(char)
            i += 1
            continue

        escaped = value[i + 1]
        if escaped == "x" and i + 3 < len(value):
            result.append(chr(int(value[i + 2 : i + 4], 16)))
            i += 4
            continue

        result.append(_ESCAPE_SEQUENCES.get(escaped, escaped))
        i += 2

    return "".join(result)
