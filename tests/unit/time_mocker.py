"""
Time mocker
"""


class TimeMocker:
    def __init__(self) -> None:
        self._timer = 0.0

    def time(self):
        return self._timer

    def sleep(self, sleep_time: float) -> None:
        self._timer = self._timer + sleep_time
