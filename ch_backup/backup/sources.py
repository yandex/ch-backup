"""
Management of partical backup or restore sources.
"""

from dataclasses import dataclass


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

    @classmethod
    def for_backup(cls,
                   access: bool = False,
                   data: bool = False,
                   schema: bool = False,
                   udf: bool = False,
                   schema_only: bool = False) -> 'BackupSources':
        """
        Setting up sources ready for backup.

        @todo: `schema_only` is deprecated and will be replaced soon.
        """
        if any([access, data, schema, udf]):
            schema = data or schema
        else:
            access, schema, udf = True, True, True
            data = not schema_only

        return cls(access=access, data=data, schema=schema, udf=udf)

    @classmethod
    def for_restore(cls,
                    access: bool = False,
                    data: bool = False,
                    schema: bool = False,
                    udf: bool = False,
                    schema_only: bool = False) -> 'BackupSources':
        """
        Setting up sources ready for restore.

        @todo: method will be merged with `for_backup` when they'll have similar logic
        @todo: `schema_only` is deprecated and will be replaced soon.
        """
        if any([access, data, schema, udf]):
            schema = data or schema
        else:
            access, schema, udf = False, True, True
            data = not schema_only

        return cls(access=access, data=data, schema=schema, udf=udf)

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
