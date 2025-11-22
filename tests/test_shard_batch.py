from __future__ import annotations
import pytest
from primitives.shard_batch import ShardBatch

def _make_rows(shard_id=3, n=5):
    """Helper to create n fake events for a given shard_id."""

    return [
        {
            "shard_id": shard_id,
            "timestamp": 1732100000 + i,
            "user_id": i + 1,
            "event_type": "click" if i % 2 == 0 else "view"
        }
        for i in range(n)
    ]


# ------ construction ----------

def test_empty_batch_has_no_rows_and_none_shard():
    batch = ShardBatch([])
    assert len(batch) == 0
    assert batch._schema == ()
    assert batch._shard_id is None


def test_non_empty_batch_infers_schema_and_shard():
    rows = _make_rows(shard_id=1, n=2)
    batch = ShardBatch(rows)

    assert len(batch) == 2
    assert batch._shard_id == 1
    # schema comes from the keys of the first row in order
    assert batch._schema == tuple(rows[0].keys())


def test_init_raises_on_schema_mismatch():
    same_schema_rows = _make_rows()
    mismatch_row = {"shard_id": 3, "timestamp": 123, "user_id": 1}

    with pytest.raises(ValueError):
        ShardBatch(same_schema_rows + [mismatch_row])

def test_init_raises_on_shard_mismatch():
    rows  = _make_rows(shard_id=1, n=2) + _make_rows(shard_id=2, n=2)

    with pytest.raises(ValueError):
        ShardBatch(rows) 

def test_init_raises_on_missing_shard_id():
    rows = [{"timestamp": 123, "user_id": 1, "event_type": "click"}, {"timestamp": 1234, "user_id": 2, "event_type": "click"}]

    with pytest.raises(ValueError):
        ShardBatch(rows)


# repr

def test_repr_contains_shard_id_and_columns():
    rows = _make_rows()
    batch = ShardBatch(rows)
    s = repr(batch)
    assert "ShardBatch(" in s
    assert "shard_id=3" in s
    for col in ("shard_id", "timestamp", "user_id", "event_type"):
        assert col in s

def test_repr_truncates_rows_when_more_than_two():
    rows = _make_rows(shard_id=1, n=10)
    batch = ShardBatch(rows)

    s = repr(batch)
    assert "rows=10" in s
    assert "... (8 more rows)" in s


# len and iter

def test_len_and_iter():
    rows = _make_rows(n=2)
    batch = ShardBatch(rows)

    assert len(batch) == 2
    iter_rows = list(iter(batch))
    assert rows == iter_rows

# getitem

def test_getitem_int_returns_row():
    rows = _make_rows(shard_id=3, n=3)
    batch = ShardBatch(rows)

    assert batch[0] == rows[0]
    assert batch[1] == rows[1]
    assert batch[-1] == rows[-1]

def test_getitem_slice_returns_new_shardbatch():
    rows = _make_rows(shard_id=3, n=5)
    batch = ShardBatch(rows)

    sliced = batch[1:4]
    assert isinstance(sliced, ShardBatch)
    assert len(sliced) == 3
    assert list(sliced) == rows[1:4]
    # shard + schema should be preserved
    assert sliced._shard_id == batch._shard_id
    assert sliced._schema == batch._schema

def test_getitem_empty_slice_preserves_shard_schema():
    rows = _make_rows()
    batch = ShardBatch(rows)

    empty_slice = batch[:0]

    assert isinstance(empty_slice, ShardBatch)
    assert len(empty_slice) == 0
    # KEY INVARIANTS

    assert batch._shard_id == empty_slice._shard_id
    assert batch._schema == empty_slice._schema

def test_getitem_returns_column_values():
    rows = _make_rows()
    batch = ShardBatch(rows)

    user_ids = batch['user_id']
    assert user_ids == [each['user_id'] for each in rows]

def test_getitem_wrong_type_raises_typeerror():
    rows = _make_rows()
    batch = ShardBatch(rows)

    with pytest.raises(TypeError):
        _ = batch[1.2]


# contains

def test_contains_checks_columns():
    rows = _make_rows()
    batch = ShardBatch(rows)

    assert 'user_id' in batch
    assert 'tengu' not in batch

# add

def test_add_two_batches_same_shard_and_schema():
    rows1 = _make_rows(shard_id=2, n=2)
    rows2 = _make_rows(shard_id=2, n=3)

    batch1 = ShardBatch(rows1)
    batch2 = ShardBatch(rows2)

    combined = batch1 + batch2

    assert isinstance(combined, ShardBatch)
    assert len(combined) == len(rows1) + len(rows2)
    assert combined._shard_id == 2

    assert combined._schema == batch1._schema == batch2._schema

def test_add_raises_on_shard_mismatch():
    rows1 = _make_rows(shard_id=2, n=2)
    rows2 = _make_rows(shard_id=3, n=3)

    batch_shard2 = ShardBatch(rows1)
    batch_shard3 = ShardBatch(rows2)

    with pytest.raises(ValueError):
        _ = batch_shard2 + batch_shard3


def test_add_raises_on_schema_mismatch():
    rows1 = _make_rows(shard_id=3, n=2)
    rows2 = [
        {"shard_id": 3, "timestamp": 1, "user_id": 1},  # missing event_type
    ]

    batch1 = ShardBatch(rows1)
    batch2 = ShardBatch(rows2)

    with pytest.raises(ValueError):
        _ = batch1 + batch2

def test_add_with_compatible_list_of_rows():
    base_rows = _make_rows(shard_id=3, n=2)
    extra_rows = _make_rows(shard_id=3, n=1)

    batch1 = ShardBatch(base_rows)

    combined = batch1 + extra_rows

    assert isinstance(combined, ShardBatch)
    assert len(combined) == len(base_rows) + len(extra_rows)
    assert list(combined) == base_rows + extra_rows
    assert combined._shard_id == 3
    assert combined._schema == batch1._schema


def test_add_with_unsupported_type_returns_nonimplemented():
    batch1 = ShardBatch(_make_rows())

    result = ShardBatch.__add__(batch1, 'hello')
    assert result is NotImplemented