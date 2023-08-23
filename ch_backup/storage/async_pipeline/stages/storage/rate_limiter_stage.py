"""
Data rate limiting stage.
"""

import time
from typing import Iterator

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.base_pipeline.rate_limiter import RateLimiter
from ch_backup.storage.async_pipeline.stages.types import StageType


class RateLimiterStage(Handler):
    """
    A bottleneck for controlling the number of data to prevent excessive loading.
    Based on tocken bucket algorithm.
    """

    stype = StageType.STORAGE

    def __init__(
        self,
        traffic_limit_per_sec: int,
        update_interval: float = 0.01,
    ) -> None:
        self._update_interval = update_interval
        self._rate_limiter = RateLimiter(
            limit_per_sec=traffic_limit_per_sec,
        )

    def __call__(self, value: bytes, index: int) -> Iterator[bytes]:
        while len(value) > 0:
            available_tokens = self._rate_limiter.extract_available_tokens(len(value))

            pass_bytes = min(available_tokens, len(value))
            data_to_pass = value[:pass_bytes]
            value = value[pass_bytes:]

            yield data_to_pass

            if len(value) > 0:
                remaining_time = self._update_interval - (
                    time.time() - self._rate_limiter.bucket_last_update
                )
                if remaining_time > 0:
                    time.sleep(remaining_time)
