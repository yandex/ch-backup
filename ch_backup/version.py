"""
Version module.
"""

from pkg_resources import resource_string

__version__ = resource_string(__name__, 'version.txt').decode().strip()


def get_version() -> str:
    """
    Return ch-backup version.
    """
    return __version__
