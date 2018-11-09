"""
Logging module.
"""

import logging
import logging.config
import os

LOGGER = logging.getLogger('ch-backup')


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
    LOGGER.critical(msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    """
    Log a message with severity 'ERROR'.
    """
    LOGGER.error(msg, *args, **kwargs)


def exception(msg, *args, **kwargs):
    """
    Log a message with severity 'ERROR' with exception information.
    """
    LOGGER.exception(msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    """
    Log a message with severity 'WARNING'.
    """
    LOGGER.warning(msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    """
    Log a message with severity 'INFO'.
    """
    LOGGER.info(msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
    """
    Log a message with severity 'DEBUG'.
    """
    LOGGER.debug(msg, *args, **kwargs)
