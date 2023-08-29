"""
Rate limiter module.
"""
import time
from typing import Callable


class RateLimiter:
    """
    Rate limiter based on token bucket algorithm without a separate replenishment process.
    """

    def __init__(self, limit_per_sec: int, get_time_func: Callable = time.time):
        self._limit_per_sec = limit_per_sec
        self._get_time_func = get_time_func
        self._bucket_tokens = self._limit_per_sec
        self._bucket_last_update = self._get_time_func()

    def _replenish_bucket(self):
        """
        Replenish the bucket with tokens depending on the time of the last update.
        """
        current_time = self._get_time_func()
        lapse = current_time - self._bucket_last_update
        self._bucket_tokens = min(
            self._limit_per_sec, self._bucket_tokens + int(lapse * self._limit_per_sec)
        )
        self._bucket_last_update = current_time

    def extract_tokens(self, desired_quantity):
        """
        Extract minimum from available in bucket and wanted number of tokens from the bucket.
        """
        if self._limit_per_sec == 0:
            return desired_quantity

        self._replenish_bucket()
        extracted = min(desired_quantity, self._bucket_tokens)

        self._bucket_tokens -= extracted
        return extracted

    def grant(self, tokens=1):
        """
        If there's enough tokens in a bucket to grant
        requested number of tokens extract them and return True. Otherwise return False.
        """

        if self._limit_per_sec == 0:
            return True
        self._replenish_bucket()

        if self._bucket_tokens >= tokens:
            self._bucket_tokens -= tokens
            return True

        return False
