"""
Data rate limiting stage.
"""

import time
from typing import Iterator, Tuple

from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.base_pipeline.rate_limiter import RateLimiter
from ch_backup.storage.async_pipeline.stages.backup.stage_communication import (
    PartPipelineInfo,
)
from ch_backup.storage.async_pipeline.stages.types import StageType


class RateLimiterStage(Handler):
    """
    A bottleneck for controlling the number of data to prevent excessive loading.
    Based on token bucket algorithm.
    """

    stype = StageType.STORAGE

    def __init__(
        self, traffic_limit_per_sec: int, retry_interval: float = 0.01
    ) -> None:
        self._retry_interval = retry_interval
        self._rate_limiter = RateLimiter(limit_per_sec=traffic_limit_per_sec)

    def __call__(self, value: bytes, index: int) -> Iterator[bytes]:
        while len(value) > 0:
            available_tokens = self._rate_limiter.extract_tokens(len(value))

            pass_bytes = min(available_tokens, len(value))

            yield value[:pass_bytes]

            value = value[pass_bytes:]
            if len(value) > 0:
                time.sleep(self._retry_interval)


class RateLimiterPartStage(Handler):
    """
    A bottleneck for controlling the number of data to prevent excessive loading.
    Based on token bucket algorithm.
    """

    stype = StageType.STORAGE

    def __init__(
        self, traffic_limit_per_sec: int, retry_interval: float = 0.01
    ) -> None:
        self._retry_interval = retry_interval
        self._rate_limiter = RateLimiter(limit_per_sec=traffic_limit_per_sec)

    def __call__(
        self, value: Tuple[bytes, PartPipelineInfo], index: int
    ) -> Iterator[Tuple[bytes, PartPipelineInfo]]:
        data, part_info = value
        while len(data) > 0:
            available_tokens = self._rate_limiter.extract_tokens(len(data))

            pass_bytes = min(available_tokens, len(data))

            yield (data[:pass_bytes], part_info)

            data = data[pass_bytes:]
            if len(data) > 0:
                time.sleep(self._retry_interval)
