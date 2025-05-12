"""
Unit test for RateLimiter.
"""

from typing import List

import pytest

from ch_backup.storage.async_pipeline.base_pipeline.rate_limiter import RateLimiter
from tests.unit.time_mocker import TimeMocker


@pytest.mark.parametrize(
    "data_size, rate, expected_time",
    [
        # expected_time = divide with round up(data_size,rate) - 1
        (0, 0, 0),
        (0, 10000, 0),
        (1000, 0, 0),
        (10, 1, 9),
        (1, 10, 0),
        (10, 10, 0),
        (10, 4, 2),
        (123456, 5321, 23),
    ],
)
def test_rate_limiter_extract(data_size: int, rate: int, expected_time: int) -> None:
    timer = TimeMocker()
    data = bytes("a" * data_size, encoding="utf-8")
    rate_limiter = RateLimiter(rate, timer.time)

    while len(data) > 0:
        available = rate_limiter.extract_tokens(len(data))
        data = data[available:]
        if len(data) > 0:
            timer.sleep(1)

    assert timer.time() == expected_time


@pytest.mark.parametrize(
    "chunks_sizes, rate, expected_time",
    [
        ([100, 123, 531, 1], 0, 0),
        ([1], 1, 0),
        ([1, 1, 1], 2, 1),
        ([1, 2, 2, 1], 2, 3),
        ([10, 1, 9, 2, 11], 11, 2),
        ([1, 2, 1, 2, 3, 1, 1, 1], 3, 3),
    ],
)
def test_rate_limiter_grand(
    chunks_sizes: List[int], rate: int, expected_time: int
) -> None:
    timer = TimeMocker()

    rate_limiter = RateLimiter(rate, timer.time)
    for chunk_size in chunks_sizes:
        while not rate_limiter.grant(chunk_size):
            timer.sleep(1)
    assert timer.time() == expected_time
