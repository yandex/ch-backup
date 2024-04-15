"""
Formatting utilities.
"""

import humanfriendly


def format_size(value: int) -> str:
    """
    Format a value in bytes to human-friendly representation.
    """
    return humanfriendly.format_size(value, binary=True)
