from collections.abc import Iterable
from typing import Any

class SchemaValidator:
    def __init__(self, schema: Iterable[str], * , strict_order: bool = True) -> None:
        schema = tuple(schema)
        if not schema:
            raise ValueError("SchemaValidator: schema cannot be empty.")

        self._schema = schema
        self._strict_order = strict_order

    def validate_row(self, row:dict[str, Any]) -> None:
        if not isinstance(row, dict):
            raise TypeError("SchemaValidator: row must be of type Dict.")
        keys_ = tuple(row.keys())
        if self._strict_order:
            if not self._schema == keys_:
                raise ValueError("SchemaValidator: Schema Mismatch.")
        else:
            if not set(self._schema) == set(keys_):
                raise ValueError("SchemaValidator: Key missing.")
            
    def validate_rows(self, rows:Iterable[dict[str, Any]]) -> None:
        pass


        



    