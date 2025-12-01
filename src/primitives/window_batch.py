from primitives.schema_validator import SchemaValidator
from primitives.time_utils import normalise_to_unix_ts
from collections.abc import Iterable, Iterator, Mapping
from datetime import datetime

from typing import Any

class WindowBatch():
    def __init__(
        self, 
        rows: list[Mapping[str, Any]], 
        *, 
        window_start: int | datetime, 
        window_end: int | datetime, 
        schema: Iterable[str],
        strict_order: bool = True
    ) -> None:
        
        # schema + validator
        validator_, schema_ = self._init_validator(schema=schema, strict_order=strict_order)
        self._validator: SchemaValidator = validator_
        self._schema: tuple[str, ...] = schema_

        # window bounds (always stored as unix timestamp)
        start_norm, end_norm = self._init_window_bounds(window_start=window_start, window_end=window_end)
        self._window_start: int = start_norm
        self._window_end: int = end_norm

        # Normalised, validated, immutable rows
        self._rows: tuple[dict[str, Any], ...] = self._init_rows(rows=rows)
  
    
    def _init_validator(
        self, 
        schema: Iterable[str], 
        strict_order: bool
    ) -> tuple[SchemaValidator, tuple[str, ...]]:

        schema = tuple(schema)
        if 'timestamp' not in schema:
            raise ValueError("WindowBatch: schema must include 'timestamp' column")
        
        validator = SchemaValidator(schema=schema, strict_order=strict_order)
        return validator, validator.schema
    
    def _init_window_bounds(
        self, 
        window_start: int | datetime, 
        window_end: int | datetime
    ) -> tuple[int, int]:
        if type(window_start) is not type(window_end):
            raise TypeError("WindowBatch: window_start and window_end must be of same type.")

        start_norm: int = normalise_to_unix_ts(window_start)
        end_norm: int = normalise_to_unix_ts(window_end)
    
        if start_norm >= end_norm:
            raise ValueError("WindowBatch: 'window_start' must be strictly less than 'window_end'.")
        
        return start_norm, end_norm

    # ---------------------------- PRIVATE INIT HELPERS ----------------------------------

    def _init_rows(
        self, 
        rows: Iterable[Mapping[str, Any]]
    ) -> tuple[dict[str, Any], ...]:

        norm_rows: list[dict[str, Any]] = []

        if not isinstance(rows, Iterable):
            raise ValueError("WindowBatch: rows must be an iterable.")

        for row in rows:

            if not isinstance(row, Mapping):
                raise TypeError("WindowBatch: each row must be a mapping (dict-like).")
            
            if "timestamp" not in row:
                raise ValueError("WindowBatch: row missing 'timestamp' column.")
            
            ts =  normalise_to_unix_ts(row['timestamp'])

            if not self._window_start <= ts < self._window_end:
                raise ValueError('WindowBatch: row timestamp outside window bounds.')
            
            row_copy = dict(row)
            row_copy['timestamp'] = ts

            self._validator.validate_row(row_copy)
            norm_rows.append(row_copy)
            
        return tuple(norm_rows)

    # ---------------------------- PUBLIC API ----------------------------------
    @property
    def window_range(self) -> tuple[int, int]:
        return (self._window_start, self._window_end)
    
    @property
    def rows(self) -> tuple[dict[str, Any], ...]:
        return self._rows

            
    def __len__(self) -> int:
        return len(self._rows)
    
    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self._rows)
    
    # ---------------------------- Dunder Methods ----------------------------------
    
    def __repr__(self) -> str:
        total_rows = len(self._rows)
        display_rows = 2

        columns_ = ", ".join([each for each in self._schema])
        rows_ = "\n  " + "\n  ".join([str(each) for each in self._rows[:display_rows]])

        display_text = f"WindowBatch(start={self._window_start}, end={self._window_end}, rows={total_rows} ,cols=[{columns_}])[{rows_}"

        if total_rows > display_rows:
            trailing_text = '\n' + f'  ... ({total_rows-display_rows} more rows)' + '\n]'
            display_text += trailing_text
        else:
            display_text += '\n]'
        return display_text
        

schema = ("timestamp", "user_id", "value")

rows = [
    {"timestamp": 1700000010, "user_id": "u1", "value": 10},
    {"timestamp": 1700000100, "user_id": "u2", "value": 15},
    {"timestamp": 1700000100, "user_id": "u2", "value": 15}
]

wb = WindowBatch(
    rows=rows,
    window_start=1700000000,
    window_end=1700000600,
    schema=schema,
    strict_order=True
)

print(wb)

