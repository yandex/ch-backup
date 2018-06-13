"""
Errors specific to ch-backup.
"""

# pylint: disable=missing-docstring


class ClickHouseBackupError(Exception):
    """
    Base class for ch-backup related errors.
    """


class StorageError(ClickHouseBackupError):
    pass


class ConfigurationError(ClickHouseBackupError):
    """
    Configuration errors (e.g. invalid value of configuration parameter).
    """


class InvalidBackupStruct(ClickHouseBackupError):
    pass


class StageError(ClickHouseBackupError):
    pass


class UnknownEncryptionError(ClickHouseBackupError):
    pass


class UnknownBackupStateError(ClickHouseBackupError):
    """
    Invalid state of backup
    """
