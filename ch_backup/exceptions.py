# pylint: disable=missing-docstring


class ClickHouseBackupError(Exception):
    """
    Base exception
    """


class StorageError(ClickHouseBackupError):
    pass


class StorageUnknownError(StorageError):
    pass


class InvalidBackupStruct(ClickHouseBackupError):
    pass


class StageError(ClickHouseBackupError):
    pass


class UnknownEncryptionError(ClickHouseBackupError):
    pass
