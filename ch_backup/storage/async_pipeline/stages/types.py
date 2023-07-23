"""
Stage types.
"""
from enum import Enum


class StageType(str, Enum):
    """
    Stage types.
    """

    STORAGE = "storage"
    FILESYSTEM = "filesystem"
    ENCRYPTION = "encryption"
