from unittest.mock import MagicMock, call, patch

import pytest

from ch_backup.backup.cloud_storage_decryption import CloudStorageDecryption
from ch_backup.config import DEFAULT_CONFIG


def test_decryption_keep_going_and_drain() -> None:
    # pylint: disable=protected-access
    with patch(
        "ch_backup.backup.cloud_storage_decryption.StorageLoader"
    ) as loader_class:
        decryption = CloudStorageDecryption(DEFAULT_CONFIG, MagicMock())
        loader = loader_class.return_value

        def fail_async(_entry, *, on_error, **_kwargs):
            on_error(RuntimeError("failed"))

        loader.copy_object.side_effect = fail_async
        decryption._decrypt_entries({"object": MagicMock()}, "s3", keep_going=True)
        loader.wait.assert_called_once_with(keep_going=True)

        loader.reset_mock()
        loader.copy_object.side_effect = [None, RuntimeError("submission failed")]
        with pytest.raises(RuntimeError, match="submission failed"):
            decryption._decrypt_entries(
                {"first": MagicMock(), "second": MagicMock()},
                "s3",
                keep_going=False,
            )
        assert loader.wait.call_args_list == [call(keep_going=True)]
