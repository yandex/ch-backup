"""
Errors specific to ch-backup.
"""


class ClickhouseBackupError(Exception):
    """
    Base class for ch-backup related errors.
    """


class StorageError(ClickhouseBackupError):
    """
    Storage layer error (e.g. save or load of data failed).
    """


class ConfigurationError(ClickhouseBackupError):
    """
    Configuration errors (e.g. invalid value of configuration parameter).
    """


class UnknownEncryptionError(ConfigurationError):
    """
    Invalid encryption type.
    """


class BackupNotFound(ClickhouseBackupError):
    """
    Backup doesn't exist.
    """

    def __init__(self, name):
        super().__init__('Backup {0} not found.'.format(name))


class InvalidBackupStruct(ClickhouseBackupError):
    """
    Invalid backup metadata.
    """


class UnknownBackupStateError(ClickhouseBackupError):
    """
    Invalid backup state.
    """
