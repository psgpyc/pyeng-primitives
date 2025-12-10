from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from datetime import datetime
from typing import Any

from primitives.schema_validator import SchemaValidator
from primitives.time_utils import normalise_to_unix_ts


class WindowBatch:
    """
    An immutable container representing a batch of timestamped rows restricted to a
    half-open time window:  window_start ≤ timestamp < window_end.

    This primitive enforces:
        - schema validity (via SchemaValidator)
        - timestamp normalisation (datetime → UNIX seconds)
        - strict window boundaries
        - defensive copying and immutability

    WindowBatch is the event-time analogue of ShardBatch and is a foundational
    building block for windowed ETL pipelines, tumbling windows, sliding windows,
    watermarking mechanisms, and streaming semantics.

    Parameters
    ----------
    rows : Iterable[Mapping[str, Any]]
        Input record sequence (dict-like mappings). Each row must contain at least
        a 'timestamp' field. Rows are defensively copied and normalised.
    window_start : int | datetime
        Inclusive window start boundary. Must be same type as `window_end`.
    window_end : int | datetime
        Exclusive window end boundary. Must be strictly greater than `window_start`.
    schema : Iterable[str]
        Expected row schema. Must include 'timestamp'.
    strict_order : bool, default True
        Passed to SchemaValidator. If True, row keys must appear in identical order
        to the provided schema.

    Invariants
    ----------
    • Schema must include 'timestamp'
    • window_start < window_end
    • All rows must satisfy window_start ≤ ts < window_end
    • All rows must satisfy SchemaValidator
    • Rows are stored as an immutable tuple of dicts
    • All timestamps normalised to UNIX seconds (int)
    """

    def __init__(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        window_start: int | datetime,
        window_end: int | datetime,
        schema: Iterable[str],
        strict_order: bool = True,
    ) -> None:

        # 1) Schema + validator
        validator, schema_tuple = self._init_validator(schema=schema, strict_order=strict_order)
        self._validator: SchemaValidator = validator
        self._schema: tuple[str, ...] = schema_tuple

        # 2) Window bounds (always stored as integers representing UNIX timestamps)
        start_norm, end_norm = self._init_window_bounds(window_start, window_end)
        self._window_start: int = start_norm
        self._window_end: int = end_norm

        # 3) Rows — normalised, validated, immutable
        self._rows: tuple[dict[str, Any], ...] = self._init_rows(rows)

    # INITIALISATION HELPERS (private)

    def _init_validator(
        self,
        schema: Iterable[str],
        strict_order: bool,
    ) -> tuple[SchemaValidator, tuple[str, ...]]:
        """
        Validate and normalise the schema, construct SchemaValidator.

        Returns
        -------
        (validator, schema_tuple)
        """
        schema_tuple = tuple(schema)

        if "timestamp" not in schema_tuple:
            raise ValueError("WindowBatch: schema must include 'timestamp' column")

        validator = SchemaValidator(schema=schema_tuple, strict_order=strict_order)
        return validator, validator.schema

    def _init_window_bounds(
        self,
        window_start: int | datetime,
        window_end: int | datetime,
    ) -> tuple[int, int]:
        """
        Validate type compatibility of window bounds and normalise both to UNIX timestamps.

        Raises
        ------
        TypeError
            If window_start and window_end are of different types.
        ValueError
            If window_start >= window_end.
        """
        if type(window_start) is not type(window_end):
            raise TypeError(
                "WindowBatch: window_start and window_end must be of same type."
            )

        start_norm = normalise_to_unix_ts(window_start)
        end_norm = normalise_to_unix_ts(window_end)

        if start_norm >= end_norm:
            raise ValueError(
                "WindowBatch: 'window_start' must be strictly less than 'window_end'."
            )

        return start_norm, end_norm

    def _init_rows(
        self,
        rows: Iterable[Mapping[str, Any]],
    ) -> tuple[dict[str, Any], ...]:
        """
        Validate each row:
            • must be Mapping
            • must include 'timestamp'
            • timestamp normalised
            • timestamp inside window
            • schema validated via SchemaValidator

        Returns an immutable tuple of defensively-copied rows.
        """
        if not isinstance(rows, Iterable):
            raise TypeError("WindowBatch: rows must be an iterable of mappings.")

        normalised: list[dict[str, Any]] = []

        for row in rows:
            if not isinstance(row, Mapping):
                raise TypeError("WindowBatch: each row must be a mapping (dict-like).")

            if "timestamp" not in row:
                raise ValueError("WindowBatch: row missing 'timestamp' column.")

            ts = normalise_to_unix_ts(row["timestamp"])

            if not (self._window_start <= ts < self._window_end):
                raise ValueError("WindowBatch: row timestamp outside window bounds.")

            row_copy: dict[str, Any] = dict(row)
            row_copy["timestamp"] = ts

            self._validator.validate_row(row_copy)
            normalised.append(row_copy)

        return tuple(normalised)

    # PUBLIC API

    @property
    def window_range(self) -> tuple[int, int]:
        """
        Returns the inclusive/exclusive event-time window.

        Example
        -------
        (window_start, window_end)
        """
        return (self._window_start, self._window_end)

    @property
    def rows(self) -> tuple[dict[str, Any], ...]:
        """
        Expose immutable, validated, timestamp-normalised rows.
        """
        return self._rows

    # PYTHON DATA MODEL

    def __len__(self) -> int:
        """Return number of rows in the batch."""
        return len(self._rows)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Iterate over rows."""
        return iter(self._rows)

    def __repr__(self) -> str:
        """
        Truncated textual representation of the batch, showing:
            • window start/end
            • number of rows
            • schema
            • first 1–2 rows

        Mirrors common behaviour in tabular libraries (pandas, Spark).
        """
        total = len(self._rows)
        preview_n = 2
        columns = ", ".join(self._schema)
        preview = "\n  " + "\n  ".join(str(r) for r in self._rows[:preview_n])

        rep = (
            f"WindowBatch(start={self._window_start}, end={self._window_end}, "
            f"rows={total}, cols=[{columns}])[{preview}"
        )

        if total > preview_n:
            rep += f"\n  ... ({total-preview_n} more rows)\n]"
        else:
            rep += "\n]"

        return rep

    def __getitem__(self, key: int | slice | str) -> dict[str, Any] | WindowBatch | list[Any]:
        """
        Supports:
            • int     → return single row (defensive copy)
            • slice   → return new WindowBatch with subset of rows
            • str     → return list of values for that column
        """
        if isinstance(key, int):
            return dict(self._rows[key])  # defensive

        if isinstance(key, slice):
            sliced = list(self._rows[key])
            return type(self)(
                rows=sliced,
                window_start=self._window_start,
                window_end=self._window_end,
                schema=self._schema,
                strict_order=self._validator.strict_order,
            )

        if isinstance(key, str):
            if key not in self._schema:
                raise KeyError(f"Column {key!r} not in schema {self._schema}.")
            return [row[key] for row in self._rows]

        raise TypeError(
            f"WindowBatch indices must be int, slice, or str, not {type(key).__name__}"
        )

    def __contains__(self, key: Any) -> bool:
        """
        Membership operator:
            'col' in batch  → True if column exists in schema.
        """
        return isinstance(key, str) and key in self._schema

    def __add__(self, other: Any) -> WindowBatch:
        """
        Concatenate two WindowBatch objects **only if**:
            • schema matches
            • window_start/window_end match
            • strict_order config matches

        Returns a NEW WindowBatch (never mutates self or other).

        Returns
        -------
        WindowBatch
            concatenated batch

        Raises
        ------
        ValueError
            if schema or window ranges mismatch.
        TypeError
            if `other` is not a WindowBatch.
        """
        if not isinstance(other, type(self)):
            return NotImplemented

        if self._schema != other._schema:
            raise ValueError("WindowBatch add: schema mismatch")

        if self.window_range != other.window_range:
            raise ValueError("WindowBatch add: window_range mismatch")

        if self._validator.strict_order != other._validator.strict_order:
            raise ValueError("WindowBatch add: strict_order mismatch")

        combined = list(self._rows) + list(other._rows)

        return type(self)(
            rows=combined,
            window_start=self._window_start,
            window_end=self._window_end,
            schema=self._schema,
            strict_order=self._validator.strict_order,
        )
    

schema = ("timestamp", "user_id", "value")

rows = [
    {"timestamp": 1700000010, "user_id": "u1", "value": 10},
    {"timestamp": 1700000200, "user_id": "u2", "value": 15},
    {"timestamp": 1700000300, "user_id": "u3", "value": 20},
]

wb = WindowBatch(
    rows=rows,
    window_start=1700000000,
    window_end=1700001000,
    schema=schema,
    strict_order=True,
)

