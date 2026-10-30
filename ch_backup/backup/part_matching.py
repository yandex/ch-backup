"""Pure matching rules for backup reuse and copy dependencies."""

from dataclasses import dataclass
from hashlib import blake2b

from ch_backup.backup.metadata.part_metadata import PartIdentity, PartMetadata
from ch_backup.backup.metadata.table_metadata import split_part_name

# Partition ID, minimum block, maximum block, and merge level (no mutation number).
PartNameKey = tuple[str, int, int, int]


def part_name_without_mutation(name: str) -> PartNameKey:
    """Invalid names raise ValueError."""
    return split_part_name(name)[:4]


def backup_part_names_match(current_name: str, stored_name: str) -> bool:
    """Part names match when equal as is or when equal without a mutation number"""
    return current_name == stored_name or part_name_without_mutation(
        current_name
    ) == part_name_without_mutation(stored_name)


@dataclass(frozen=True, slots=True)
class LocalCopyKey:
    """Identify compatible local parts independently of their source owner."""

    database: str
    table: str
    name: PartNameKey | str
    checksum: str
    disk_name: str
    size: int
    tarball: bool
    encrypted: bool
    files_digest: bytes


def local_copy_key(part: PartMetadata) -> LocalCopyKey:
    """Build a local reuse key, retaining exact names when parsing fails."""
    name: PartNameKey | str
    try:
        name = part_name_without_mutation(part.name)
    except ValueError:
        name = part.name
    return LocalCopyKey(
        database=part.database,
        table=part.table,
        name=name,
        checksum=part.checksum,
        disk_name=part.disk_name,
        size=part.size,
        tarball=part.tarball,
        encrypted=part.encrypted,
        # File names cannot contain NUL. The digest preserves file-list matching
        # without retaining every name after candidate metadata is released.
        files_digest=blake2b(
            "\0".join(sorted(part.files)).encode(), digest_size=16
        ).digest(),
    )


@dataclass(frozen=True, slots=True)
class MaterializedPartKey:
    """Identify a source dependency and its required storage format."""

    source_backup: str | None
    identity: PartIdentity
    checksum: str
    disk_name: str
    tarball: bool
    encrypted: bool


def materialized_part_key(part: PartMetadata) -> MaterializedPartKey:
    """Use the physical stored name so logical renames share a dependency."""
    return MaterializedPartKey(
        source_backup=part.link,
        identity=part.stored_identity,
        checksum=part.checksum,
        disk_name=part.disk_name,
        tarball=part.tarball,
        encrypted=part.encrypted,
    )


def linked_part_matches(source: PartMetadata, stored: PartMetadata) -> bool:
    """Match the stored address and checksum after the caller resolves the owner backup.

    Existing links do not compare disk, archive format, or encryption settings.
    """
    return (
        stored.identity == source.stored_identity
        and not stored.link
        and stored.checksum == source.checksum
    )


def materialized_part_matches(source: PartMetadata, stored: PartMetadata) -> bool:
    """Match a completed copy under the source's logical name and storage settings."""
    return (
        stored.identity == source.identity
        and not stored.link
        and stored.checksum == source.checksum
        and stored.disk_name == source.disk_name
        and stored.tarball == source.tarball
        and stored.encrypted == source.encrypted
    )
