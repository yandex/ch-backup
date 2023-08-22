from functools import reduce
from typing import List

from hypothesis import given
from hypothesis import strategies as st
from pypeln.thread.api.from_iterable import from_iterable

from ch_backup.storage.async_pipeline import thread_flat_map
from ch_backup.storage.async_pipeline.base_pipeline.handler import Handler
from ch_backup.storage.async_pipeline.base_pipeline.map import thread_map
from ch_backup.storage.async_pipeline.pipeline_builder import PypelnStage
from ch_backup.storage.async_pipeline.pipelines import run
from ch_backup.storage.async_pipeline.stages.filesystem.chunking_stage import (
    ChunkingStage,
)
from ch_backup.storage.async_pipeline.stages.storage.multipart_storage_uploading_stage import (
    TrafficLimitingStage,
    UploadingPart,
)
from ch_backup.storage.async_pipeline.stages.types import StageType


class TimeWrapper:
    def __init__(self) -> None:
        self._timer = 0.0

    def time(self):
        return self._timer

    def sleep(self, sleep_time: float) -> None:
        self._timer = self._timer + sleep_time


class DataPreparingStage(Handler):
    """
    Convert the data into UploadingPart for compatibility with TrafficLimitingStage.
    """

    stype = StageType.STORAGE

    def __init__(self) -> None:
        pass

    def __call__(self, data: bytes, index: int) -> UploadingPart:
        return UploadingPart(data=data, upload_id=None)


class SummarizeStage(Handler):
    """
    Finishing stub for total processing data counting.
    """

    stype = StageType.STORAGE

    def __init__(self) -> None:
        self._total_size = 0

    def __call__(self, part: UploadingPart, index: int) -> None:
        self._total_size += len(part.data)

    def on_done(self) -> None:
        pass

    @property
    def total_size(self):
        return self._total_size


def build_pl(timer, finish_stage, data_size, chunk_size, buffer_size, traffic_rate):
    stages: List[PypelnStage]
    content = bytes("a" * data_size, encoding="utf-8")

    stages = [
        from_iterable([content]),
        thread_flat_map(ChunkingStage(chunk_size, buffer_size)),
        thread_map(DataPreparingStage()),
        thread_map(
            TrafficLimitingStage(traffic_rate, 1.0, timer.time, timer.sleep),
        ),
        thread_map(finish_stage),
    ]

    return reduce(lambda pipeline, stage: pipeline | stage, stages)


@given(
    content_size=st.integers(16, 10000),
    chunk_size=st.just(16),
    buffer_size=st.just(128),
)
def test_traffic_rate_limiting(content_size, chunk_size, buffer_size):
    timer = TimeWrapper()
    finish_stage = SummarizeStage()
    run(
        build_pl(
            timer=timer,
            finish_stage=finish_stage,
            data_size=content_size,
            chunk_size=chunk_size,
            buffer_size=buffer_size,
            traffic_rate=chunk_size,
        )
    )

    assert finish_stage.total_size == content_size
    assert timer.time() == ((content_size + chunk_size - 1) // chunk_size - 1)


@given(
    content_size=st.integers(16, 10000),
    chunk_size=st.just(16),
    buffer_size=st.just(128),
)
def test_traffic_rate_unlimited(content_size, chunk_size, buffer_size):
    timer = TimeWrapper()
    finish_stage = SummarizeStage()
    run(
        build_pl(
            timer=timer,
            finish_stage=finish_stage,
            data_size=content_size,
            chunk_size=chunk_size,
            buffer_size=buffer_size,
            traffic_rate=0,
        )
    )

    assert finish_stage.total_size == content_size
    assert timer.time() == 0
