"""
ClickHouse backup tool.
"""

import warnings

# Ignore warnings from dependencies (stopit) on usage of deprecated pkg_resources
# https://setuptools.pypa.io/en/latest/pkg_resources.html#api-reference
warnings.filterwarnings("ignore", message="pkg_resources is deprecated as an API")

from .cli import cli as main
from .version import __version__
