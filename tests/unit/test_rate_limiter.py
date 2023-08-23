"""
Unit test for RateLimiter.
"""

from hypothesis import example, given
from hypothesis import strategies as st

from ch_backup.storage.async_pipeline.base_pipeline.rate_limiter import RateLimiter


class TimeMocker:
    def __init__(self) -> None:
        self._timer = 0.0

    def time(self):
        return self._timer

    def sleep(self, sleep_time: float) -> None:
        self._timer = self._timer + sleep_time


@given(data_size=st.integers(1, 10000), rate=st.integers(1, 10000))
@example(
    data_size=10000,
    rate=0,
)
def test_rate_limiting(data_size, rate):
    timer = TimeMocker()
    data = bytes("a" * data_size, encoding="utf-8")
    rate_limiter = RateLimiter(rate, timer.time)

    while len(data) > 0:
        available = rate_limiter.extract_available_tokens(len(data))
        data = data[available:]
        if len(data) > 0:
            timer.sleep(1)

    if rate != 0:
        assert timer.time() == (data_size + rate - 1) // rate - 1
    else:
        # If rate equals 0, then it is unlimited.
        assert timer.time() == 0
