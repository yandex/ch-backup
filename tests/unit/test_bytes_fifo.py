from typing import ContextManager, Optional

import pytest

from ch_backup.storage.async_pipeline.base_pipeline.bytes_fifo import BytesFIFO


def generate_bytes(size: int) -> bytes:
    """
    Generate cyclically increasing bytes sample.
    """
    return bytes(i % 256 for i in range(size))


@pytest.mark.parametrize('init_size', [0, 100])
def test_empty(init_size: int) -> None:
    fifo = BytesFIFO(init_size)

    assert fifo.capacity() == init_size
    assert fifo.empty()
    assert len(fifo) == 0


@pytest.mark.parametrize(
    'init_size, write_size, expected_written',
    (
        [0, 0, 0],  # no capacity
        [0, 1, 0],
        [100, 0, 0],  # write zero
        [100, 50, 50],  # not full
        [100, 100, 100],  # up to full
        [100, 200, 100],  # not enough space
    ),
)
def test_write_to_empty_fifo(init_size: int, write_size: int, expected_written: int) -> None:
    fifo = BytesFIFO(init_size)

    written = fifo.write(generate_bytes(write_size))

    assert written == expected_written
    assert len(fifo) == min(init_size, write_size)


@pytest.mark.parametrize(
    'init_size, prefill_size, write_size, expected_written',
    (
        [0, 0, 0, 0],  # no capacity
        [100, 0, 100, 100],  # empty
        [100, 50, 100, 50],  # half full, not enough space
        [100, 50, 50, 50],  # half full, enough space
        [100, 100, 1, 0],  # full
    ),
)
def test_write_to_prefilled_fifo(init_size: int, prefill_size: int, write_size: int, expected_written: int) -> None:
    fifo = BytesFIFO(init_size)
    fifo.write(generate_bytes(prefill_size))

    written = fifo.write(generate_bytes(write_size))

    assert written == expected_written
    assert len(fifo) == min(init_size, prefill_size + write_size)


@pytest.mark.parametrize(
    'init_size, prefill_size, read_size, expected_read',
    (
        [0, 0, 0, 0],  # no capacity
        [0, 0, 1, 0],
        [100, 0, 1, 0],  # empty
        [100, 50, 25, 25],  # read not all
        [100, 50, 50, 50],  # read all
        [100, 50, 100, 50],  # read more than all
    ),
)
def test_read(init_size: int, prefill_size: int, read_size: int, expected_read: int) -> None:
    fifo = BytesFIFO(init_size)

    prefill_data = generate_bytes(prefill_size)
    fifo.write(prefill_data)

    read_bytes = fifo.read(read_size)

    assert len(read_bytes) == expected_read
    assert read_bytes == prefill_data[:read_size]
    if read_size < prefill_size:
        assert len(fifo) == prefill_size - read_size
    else:
        assert len(fifo) == 0


@pytest.mark.parametrize(
    'init_size, prefill_size, read_size, expected_read, write_size, expected_written',
    (
        [0, 0, 0, 0, 0, 0],
        [100, 0, 0, 0, 100, 100],
        [100, 100, 0, 0, 1, 0],
        [100, 70, 70, 70, 50, 50],
        [100, 100, 70, 70, 50, 50],
        [100, 100, 70, 70, 100, 70],
        [100, 99, 98, 98, 99, 99],
        [100, 99, 98, 98, 100, 99],
    ),
)
def test_write_after_read(init_size: int, prefill_size: int, read_size: int, write_size: int, expected_read: int,
                          expected_written: int) -> None:
    fifo = BytesFIFO(init_size)
    prefill_data = generate_bytes(prefill_size)
    write_data = generate_bytes(write_size)
    data = prefill_data + write_data

    fifo.write(prefill_data)
    read_bytes = fifo.read(read_size)

    assert len(read_bytes) == expected_read

    written = fifo.write(write_data)
    assert written == expected_written

    expected_len = prefill_size + expected_written - read_size
    assert len(fifo) == expected_len
    assert fifo.read() == data[read_size:prefill_size + expected_written]


@pytest.mark.parametrize('init_size, write_size', [(0, 0), (100, 100)])
def test_flush(init_size: int, write_size: int) -> None:
    fifo = BytesFIFO(init_size)

    fifo.write(generate_bytes(write_size))
    fifo.flush()

    assert fifo.empty()
    assert len(fifo) == 0


@pytest.mark.parametrize(
    'init_size, write_size, is_full',
    [
        (0, 0, True),
        (100, 0, False),
        (100, 1, False),
        (100, 99, False),
        (100, 100, True),
    ],
)
def test_full(init_size: int, write_size: int, is_full: bool) -> None:
    fifo = BytesFIFO(init_size)

    fifo.write(generate_bytes(write_size))
    assert fifo.full() == is_full


@pytest.mark.parametrize(
    'init_size, write_size, read_size, expected_free',
    [
        (0, 0, 0, 0),
        (100, 0, 0, 100),
        (100, 1, 0, 99),
        (100, 99, 0, 1),
        (100, 100, 1, 1),
        (100, 100, 99, 99),
        (100, 100, 100, 100),
    ],
)
def test_free(init_size: int, write_size: int, read_size: int, expected_free: int) -> None:
    fifo = BytesFIFO(init_size)

    fifo.write(generate_bytes(write_size))
    fifo.read(read_size)

    assert fifo.free() == expected_free


@pytest.mark.parametrize(
    'init_size, prefill_size, new_size, exception',
    [
        (0, 0, 0, pytest.raises(ValueError, match='Cannot resize to zero')),
        (100, 0, 0, pytest.raises(ValueError, match='Cannot resize to zero')),
        (100, 0, 1, None),
        (100, 0, 99, None),
        (100, 50, 50, None),
        (100, 100, 99, pytest.raises(ValueError, match='Cannot contract FIFO to less than')),
    ],
)
def test_resize(init_size: int, prefill_size: int, new_size: int, exception: Optional[ContextManager]) -> None:
    fifo = BytesFIFO(init_size)

    data = generate_bytes(prefill_size)
    fifo.write(data)

    if exception is not None:
        with exception:
            fifo.resize(new_size)
    else:
        fifo.resize(new_size)
        assert fifo.capacity() == new_size

    assert fifo.read() == data
