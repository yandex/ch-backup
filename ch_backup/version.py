"""
Version module.
"""

from importlib import resources

__version__ = resources.files("ch_backup").joinpath("version.txt").read_text().strip()


def get_version() -> str:
    """
    Return ch-backup version.
    """
    return __version__
