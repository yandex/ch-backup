"""Shared unit-test fixtures."""

import pytest

from ch_backup import logging


@pytest.fixture(name="messages")
def info_messages():
    records = []
    sink = logging.logger.add(
        lambda message: records.append(message.record), level="INFO"
    )
    try:
        yield records
    finally:
        logging.logger.remove(sink)
