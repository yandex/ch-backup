from ch_backup.compression.base import BaseCompression
from ch_backup.compression.zlib import ZLIBCompression


def get_compression() -> BaseCompression:
    return ZLIBCompression()
