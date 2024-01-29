"""
Common constants.
"""

from enum import Enum


class BackupStorageFormat(str, Enum):
    """
    Backup storage format.
    """

    PLAIN = "plain"
    TAR = "tar"
