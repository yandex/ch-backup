"""
Unit test for RateLimiter.
"""

from typing import List

import pytest

from ch_backup.storage.engine.s3.s3_retry import retry
from tests.unit.time_mocker import TimeMocker


@pytest.mark.parametrize(
    "max_attempts, max_interval, multiplier, expected_sleep_time",
    [
        (
            6,
            8,
            1,
            [
                (0, 1),
                (0, 2),
                (0, 4),
                (0, 8),
                (0, 8),
            ],
        ),
        (
            6,
            1,
            1,
            [
                (0, 1),
                (0, 1),
                (0, 1),
                (0, 1),
                (0, 1),
            ],
        ),
    ],
)
def test_rate_limiter_extract(
    max_attempts: int, max_interval: float, multiplier: float, expected_sleep_time: List
) -> None:
    timer = TimeMocker()
    execution_count = 0
    last_execution_time = 0.0

    @retry(
        Exception,
        max_attempts,
        max_interval,
        multiplier,
        timer.sleep,
        False,
    )
    def some_s3_function():
        nonlocal execution_count, last_execution_time
        execution_count += 1
        current_time = timer.time()
        delta = current_time - last_execution_time

        if execution_count > 0:
            # Because the retry_exponential generates random values, we should check the interval.
            assert (
                expected_sleep_time[execution_count - 1][0]
                <= delta
                <= expected_sleep_time[execution_count - 1][1]
            )

        last_execution_time = timer.time()

    some_s3_function()
