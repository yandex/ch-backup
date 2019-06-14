"""
Logging module.
"""

import logging
import logging.config
import os


def configure(config: dict) -> None:
    """
    Configure logging.
    """
    for handler in config.get('handlers', {}).values():
        filename = handler.get('filename')
        if filename:
            os.makedirs(os.path.dirname(filename), exist_ok=True)

    logging.config.dictConfig(config)


def critical(msg, *args, **kwargs):
    """
    Log a message with severity 'CRITICAL'.
    """
    _get_logger().critical(msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    """
    Log a message with severity 'ERROR'.
    """
    _get_logger().error(msg, *args, **kwargs)


def exception(msg, *args, **kwargs):
    """
    Log a message with severity 'ERROR' with exception information.
    """
    _get_logger().exception(msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    """
    Log a message with severity 'WARNING'.
    """
    _get_logger().warning(msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    """
    Log a message with severity 'INFO'.
    """
    _get_logger().info(msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
    """
    Log a message with severity 'DEBUG'.
    """
    _get_logger().debug(msg, *args, **kwargs)


def _get_logger() -> logging.Logger:
    return logging.getLogger('ch-backup')
