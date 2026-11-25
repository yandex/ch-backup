"""Check journal batch ownership, acknowledgment and retries."""

from io import BytesIO
from unittest.mock import Mock

import pytest

from ch_backup.backup.copy_journal_buffer import PendingJournalRecords


def test_pending_acknowledgment_preserves_later_records() -> None:
    """Consume each prepared batch once without dropping later appends."""
    pending = PendingJournalRecords()
    pending.append("first")
    output = BytesIO()
    pending.write_pending_batch(output, 1)
    pending.append("second")
    pending.acknowledge_uploaded()
    assert len(pending) == 1
    with pytest.raises(RuntimeError):
        pending.acknowledge_uploaded()
    output = BytesIO()
    pending.write_pending_batch(output, 1)
    assert output.getvalue() == b"second\n"
    pending.acknowledge_uploaded()
    assert not pending


def test_failed_write_and_upload_leave_records_pending() -> None:
    """Clear prepared state on failed writing and retry unacknowledged uploads."""
    pending = PendingJournalRecords()
    pending.append("record")
    pending.write_pending_batch(BytesIO(), 1)
    broken = Mock(write=Mock(side_effect=OSError("write failed")))
    with pytest.raises(OSError):
        pending.write_pending_batch(broken, 1)
    with pytest.raises(RuntimeError):
        pending.acknowledge_uploaded()
    for _ in range(2):
        output = BytesIO()
        pending.write_pending_batch(output, 1)
        assert output.getvalue() == b"record\n"
        assert len(pending) == 1
    pending.acknowledge_uploaded()
    assert not pending
