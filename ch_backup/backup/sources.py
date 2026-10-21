"""
Management of partial backup or restore sources.
"""

from dataclasses import dataclass, fields

from ch_backup import logging
from ch_backup.exceptions import ConfigurationError


@dataclass(frozen=True)
class BackupSources:
    """
    Class responsible for management of backup/restore data sources.
    """

    # Perform operation for access control entities
    access: bool = True
    # Perform operation for tables data
    data: bool = True
    # Perform operation for databases and tables schemas
    schema: bool = True
    # Perform operation for user defined functions
    udf: bool = True
    # Perform operation for named collections
    named_collections: bool = True
    # Perform operation for workload entities (WORKLOADs and RESOURCEs)
    workload_entities: bool = True

    @classmethod
    # pylint: disable=too-many-positional-arguments
    def for_backup(
        cls,
        access: bool = False,
        data: bool = False,
        schema: bool = False,
        udf: bool = False,
        named_collections: bool = False,
        workload_entities: bool = False,
        schema_only: bool = False,
    ) -> "BackupSources":
        """
        Setting up sources ready for backup.

        @todo: `schema_only` is deprecated and will be replaced soon.
        """
        if any([access, data, schema, udf, named_collections, workload_entities]):
            schema = data or schema
        else:
            access, schema, udf, named_collections, workload_entities = (
                True,
                True,
                True,
                True,
                True,
            )
            data = not schema_only

        return cls(
            access=access,
            data=data,
            schema=schema,
            udf=udf,
            named_collections=named_collections,
            workload_entities=workload_entities,
        )

    @classmethod
    def from_config(cls, config: dict | None) -> "BackupSources":
        """
        Setting up sources for backup from the `backup.sources` config section.

        An unknown key, a typo or a source added in a newer version, selects all sources
        with a warning: a larger backup is safer than one that silently misses data.
        A non-boolean value of a known key, e.g. the quoted YAML string "false", fails.
        """
        config = config or {}
        known = {field.name for field in fields(cls)}
        if any(not isinstance(config[key], bool) for key in config.keys() & known):
            raise ConfigurationError(
                f"backup.sources values must be booleans: {config}"
            )
        unknown = sorted(config.keys() - known)
        if unknown:
            logging.warning(f"Unknown backup sources {unknown}, backing up all sources")
            return cls.for_backup()
        return cls.for_backup(**config)

    @classmethod
    # pylint: disable=too-many-positional-arguments
    def for_restore(
        cls,
        access: bool = False,
        data: bool = False,
        schema: bool = False,
        udf: bool = False,
        named_collections: bool = False,
        workload_entities: bool = False,
        schema_only: bool = False,
    ) -> "BackupSources":
        """
        Setting up sources ready for restore.

        @todo: method will be merged with `for_backup` when they'll have similar logic
        @todo: `schema_only` is deprecated and will be replaced soon.
        """
        if any([access, data, schema, udf, named_collections, workload_entities]):
            schema = data or schema
        else:
            access, schema, udf, named_collections, workload_entities = (
                False,
                True,
                True,
                True,
                True,
            )
            data = not schema_only

        return cls(
            access=access,
            data=data,
            schema=schema,
            udf=udf,
            named_collections=named_collections,
            workload_entities=workload_entities,
        )

    def schemas_included(self) -> bool:
        """
        Whether data or schema sources are enabled to backup or restore.
        """
        return self.data or self.schema

    @property
    def schema_only(self) -> bool:
        """
        Is the backup or restore phase of the table schema enabled.
        """
        return self.schema and not self.data
