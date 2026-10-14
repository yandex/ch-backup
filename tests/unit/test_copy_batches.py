"""Copy submission bounds completed and pending futures, including failures."""

# pylint: disable=protected-access

from unittest.mock import MagicMock

import pytest

from ch_backup.backup.layout import BackupLayout
from ch_backup.exceptions import StorageError
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ThreadExecPool
from ch_backup.storage.object_copy import CopyEntry


@pytest.mark.parametrize("keep_going", [False, True])
@pytest.mark.parametrize("failing", [False, True])
def test_copy_batches_drain_futures_and_report_each_result(
    keep_going: bool, failing: bool
) -> None:
    layout = BackupLayout.__new__(BackupLayout)
    loader = MagicMock()
    layout._storage_loader = loader
    completed: list[CopyEntry] = []
    errors: list[CopyEntry] = []
    entries = [CopyEntry({}, {}, str(i), f"target/{i}", 1) for i in range(1025)]
    with ThreadExecPool(2) as pool:

        def copy(entry, *, is_async, callback, on_error):
            assert is_async
            assert len(pool._future_to_job) < 256
            pool.submit(
                entry.src_path,
                int,
                "bad" if failing else "1",
                callback=callback,
                on_error=on_error,
            )

        loader.copy_object.side_effect = copy
        loader.wait.side_effect = pool.wait_all
        if failing and not keep_going:
            with pytest.raises(StorageError):
                layout.copy_objects(
                    entries,
                    callback=completed.append,
                    on_error=lambda e, _: errors.append(e),
                )
            pool.wait_all(keep_going=True)
            assert len(errors) == 256
        else:
            layout.copy_objects(
                entries,
                keep_going=keep_going,
                callback=completed.append,
                on_error=lambda e, _: errors.append(e),
            )
            assert not pool._future_to_job
            assert len(errors if failing else completed) == len(entries)
            assert loader.wait.call_count == 5
