import pytest
import random
from datetime import datetime, timedelta
from primitives.window_batch import WindowBatch

WINDOW_START = 1700000000
JITTER = 1000
WINDOW_END = WINDOW_START + JITTER
SCHEMA = ('user_id', 'timestamp', 'value')

RANGE = 10

def make_rows(window_start=WINDOW_START, jitter=JITTER, range_=RANGE):
    return [
        {
            'user_id': f'u{i}',
            'timestamp':  window_start + random.randint(1,jitter-1),
            'value': random.randint(1, 100)
        }
        for i in range(range_)
    ]


def make_dt_rows(window_start=WINDOW_START, jitter=JITTER, range_=RANGE):
    window_start_dt = datetime.fromtimestamp(window_start)
    return [
        {
            'user_id': f'u{i}',
            'timestamp': window_start_dt + timedelta(seconds=random.randint(1,jitter-1)),
            'value': random.randint(1, 100)
        }
        for i in range(RANGE)
    ]


def make_batch(rows=None, schema=SCHEMA, window_start=WINDOW_START, window_end=WINDOW_END):
    if rows is None:
        rows = make_rows()
    return WindowBatch(
        rows=rows,
        window_start=window_start,
        window_end=window_end,
        schema=schema,
        strict_order=True,
    )


def test_init_accepts_valid_int_window_and_rows():
    batch = make_batch()
    assert len(batch) == len(make_rows())
    assert batch.window_range == (WINDOW_START, WINDOW_END)
    assert type(batch.rows) == tuple

    for row in batch.rows:
        assert type(row) == dict


def test_init_accepts_valid_datetime_window_and_rows():
    rows=make_dt_rows()
    batch = make_batch(rows=rows)
    window_start, window_end = batch.window_range
    assert type(window_start) == int
    assert type(window_end) == int

    for row in batch.rows:
        assert type(row['timestamp']) == int


def test_init_allows_empty_rows():
    batch = make_batch(rows=[])

    assert len(batch) == 0
    assert batch.rows == ()
    assert batch.window_range == (WINDOW_START, WINDOW_END)
