from collections.abc import Iterator
from typing import Any


class ShardBatch:
    """
    Immutable-like batch of rows that all belong to the same shard.

    Invariants:
    - `rows` is a sequence of dict-like records.
    - Every row has a 'shard_id' key.
    - All rows share exactly the same set and order of keys (schema).
    - All rows have the same 'shard_id' value.

    Attributes
    ----------
    _rows : list[dict[str, Any]]
        A defensive copy of the input rows.
    _schema : tuple[str, ...]
        Column names in a fixed order, taken from the first row.
    _shard_id : int | str | None
        Shard identifier, taken from the 'shard_id' field of the first row.
    """

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        # Empty batch: keep a valid, but "uninitialised" shard with no schema.
        if not rows:
            self._rows: list[dict[str, Any]] = []
            self._schema: tuple[str, ...] = ()
            self._shard_id: int | str | None = None
            return

        # Derive schema and shard_id from first row.
        schema: tuple[str, ...] = tuple(rows[0].keys())
        shard_id = rows[0].get("shard_id")

        # Validate all rows against schema and shard_id.
        for idx, row in enumerate(rows):
            schema_each = tuple(row.keys())
            shard_id_each = row.get("shard_id")

            if "shard_id" not in schema_each:
                raise ValueError(
                    f"Row {idx} is missing required 'shard_id' column. "
                    "All rows in ShardBatch must include a 'shard_id' key."
                )

            if schema_each != schema:
                raise ValueError(
                    "Schema mismatch in ShardBatch initialisation: "
                    f"row {idx} has schema {schema_each}, "
                    f"but expected schema {schema} for all rows."
                )

            if shard_id_each != shard_id:
                raise ValueError(
                    "Shard ID mismatch in ShardBatch initialisation: "
                    f"row {idx} has shard_id={shard_id_each!r}, "
                    f"but expected shard_id={shard_id!r} for all rows."
                )

        # Store a defensive copy of rows so the caller can't mutate internal state.
        self._rows: list[dict[str, Any]] = [dict(row) for row in rows]
        self._schema: tuple[str, ...] = schema
        self._shard_id: int | str | None = shard_id

    def __repr__(self) -> str:
        """
        Return a concise preview, showing:
        - shard_id
        - number of rows
        - columns
        - first few rows (up to 2)
        """
        total_rows = len(self._rows)
        cols = self._schema

        display_length = 2
        default_display_rows = self._rows[:display_length]

        lines = [f"{row}," for row in default_display_rows]
        if total_rows > display_length:
            lines.append(f"... ({total_rows - display_length} more rows)")

        preview = "\n  " + "\n  ".join(lines) if lines else ""
        if lines:
            preview += "\n"

        return (
            f"ShardBatch(shard_id={self._shard_id}, "
            f"rows={total_rows}, cols=[{', '.join(cols)}])"
            f"[{preview}]"
        )

    def __len__(self) -> int:
        """Return the number of rows in the batch."""
        return len(self._rows)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Iterate over rows as dictionaries."""
        return iter(self._rows)

    def __getitem__(self, index):
        """
        Support:
        - int  -> a single row dict
        - slice -> a new ShardBatch with sliced rows (same schema & shard_id)
        - str -> a list of values for that column across all rows
        """
        if isinstance(index, int):
            # Normal list-style indexing: may still raise IndexError naturally.
            return self._rows[index]

        elif isinstance(index, slice):
            # Slice of rows -> new ShardBatch.
            new_batch = type(self)(self._rows[index])

            # Preserve shard_id and schema even for empty slices so invariants hold.
            new_batch._shard_id = self._shard_id
            new_batch._schema = self._schema

            return new_batch

        elif isinstance(index, str):
            # Column access by name.
            if index not in self._schema:
                raise KeyError(
                    f"Column {index!r} does not exist in ShardBatch schema "
                    f"{self._schema}."
                )
            return [row[index] for row in self._rows]

        # Any other index type is not supported.
        raise TypeError(
            "Invalid index type for ShardBatch. "
            f"Expected int, slice, or str, got {type(index).__name__}."
        )

    def __contains__(self, value: object) -> bool:
        """
        Membership test is defined on column names:
        `'column_name' in shard_batch` -> True if column exists in schema.
        """
        if not isinstance(value, str):
            return False
        return value in self._schema

    def __add__(self, other):
        """
        Concatenate rows while preserving shard invariants.

        Supported:
        - ShardBatch + ShardBatch  (same schema and shard_id)
        - ShardBatch + list/tuple of row dicts
          (all rows must match schema and shard_id)

        Returns a new ShardBatch.

        Raises:
        - ValueError if shard_id or schema do not match.
        - TypeError if 'other' is of an unsupported type.
        """
        if isinstance(other, type(self)):
            # Adding two ShardBatch instances.
            if self._shard_id != other._shard_id:
                raise ValueError(
                    "Cannot add ShardBatch instances with different shard IDs: "
                    f"{self._shard_id!r} != {other._shard_id!r}."
                )
            if self._schema != other._schema:
                raise ValueError(
                    "Cannot add ShardBatch instances with different schemas: "
                    f"{self._schema} != {other._schema}."
                )
            return type(self)(self._rows + other._rows)

        elif isinstance(other, (list, tuple)):
            # Adding a sequence of row dicts.
            other_rows = list(other)

            for idx, row in enumerate(other_rows):
                if not isinstance(row, dict):
                    raise TypeError(
                        "When adding a list/tuple to ShardBatch, all elements "
                        f"must be dicts. Element at index {idx} is of type "
                        f"{type(row).__name__}."
                    )

                other_schema = tuple(row.keys())
                other_shard_id = row.get("shard_id")

                if other_schema != self._schema:
                    raise ValueError(
                        "Cannot add rows with different schema to ShardBatch: "
                        f"row {idx} has schema {other_schema}, "
                        f"expected {self._schema}."
                    )

                if other_shard_id != self._shard_id:
                    raise ValueError(
                        "Cannot add rows with different shard_id to ShardBatch: "
                        f"row {idx} has shard_id={other_shard_id!r}, "
                        f"expected {self._shard_id!r}."
                    )

            combined = self._rows + other_rows
            return type(self)(combined)

        # Defer to Python's numeric protocol for unsupported types.
        return NotImplemented
    
