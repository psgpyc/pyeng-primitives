from collections.abc import Iterable, Mapping
from typing import Any


class SchemaValidator:
    """
    A reusable schema enforcement utility for validating dictionaries
    (row objects) against a fixed, expected schema.

    This class is intended to be used by higher-level data pipeline
    primitives such as ShardBatch, InMemoryBatch, WindowBatch, etc.

    The validator enforces:
      - schema must be non-empty
      - each row must contain exactly the expected keys
      - optional strict ordering of keys
      - iterable validation (propagating errors from validate_row)

    Parameters
    ----------
    schema : Iterable[str]
        Iterable of column names defining the schema. Must be non-empty.
        Any iterable is accepted (list, tuple, generator, dict_keys, etc.).
        Internally stored as a tuple to ensure immutability and stable order.

    strict_order : bool, optional
        If True (default), validate_row compares the order of row keys
        exactly against the schema. If False, row key sets must match
        but ordering does not matter.

    Raises
    ------
    ValueError
        If schema is empty.
    """

    def __init__(self, schema: Iterable[str], *, strict_order: bool = True) -> None:
        schema = tuple(schema)

        if not schema:
            raise ValueError("SchemaValidator: schema cannot be empty.")

        # Internal canonical representation of the schema.
        self._schema: tuple[str, ...] = schema
        self._strict_order: bool = strict_order


    @property
    def schema(self) -> tuple[str, ...]:
        """
        Returns the stored schema as a tuple of strings.

        This is the canonical representation of the schema.
        """
        return self._schema

    @property
    def strict_order(self) -> bool:
        """
        Whether strict ordering is enforced during validation.
        """
        return self._strict_order



    def validate_row(self, row: Mapping[str, Any]) -> None:
        """
        Validate a single row (mapping) against the schema.

        Parameters
        ----------
        row : Mapping[str, Any]
            A dict-like object whose keys must match the schema.

        Raises
        ------
        TypeError
            If `row` is not a mapping (dict-like).
        ValueError
            If row keys do not match schema (missing, extra, or wrong order).
        """
        if not isinstance(row, Mapping):
            raise TypeError("SchemaValidator: row must be a mapping(dict-like).")

        keys_ = tuple(row.keys())

        if self._strict_order:
            # Must match exactly in length + order
            if self._schema != keys_:
                raise ValueError(
                    "SchemaValidator: strict order mismatch. "
                    f"Expected {self._schema} got {keys_}"
                )
        else:
            # Only the set of keys must match (order ignored)
            if set(self._schema) != set(keys_):
                raise ValueError(
                    "SchemaValidator: schema mismatch (missing or extra keys)"
                )

    def validate_rows(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """
        Validate an iterable of row mappings.

        Parameters
        ----------
        rows : Iterable[Mapping[str, Any]]
            Iterable of dict-like rows.

        Raises
        ------
        TypeError
            If rows is not iterable.
        ValueError / TypeError
            Propagated from validate_row() when a row is invalid.
        """
        if not isinstance(rows, Iterable):
            raise TypeError("SchemaValidator: rows must be an iterable of mappings")

        # Propagate any error raised by validate_row()
        for row in rows:
            self.validate_row(row)


    def is_valid_row(self, row: Mapping[str, Any]) -> bool:
        """
        Boolean version of validate_row().

        Returns False instead of raising when row is invalid.

        Parameters
        ----------
        row : Mapping[str, Any]

        Returns
        -------
        bool
            True if the row is valid; False otherwise.
        """
        try:
            self.validate_row(row)
            return True
        except (TypeError, ValueError):
            return False

    def is_valid_rows(self, rows: Iterable[Mapping[str, Any]]) -> bool:
        """
        Boolean version of validate_rows().

        Returns False instead of raising on first invalid row.

        Parameters
        ----------
        rows : Iterable[Mapping[str, Any]]

        Returns
        -------
        bool
            True if all rows are valid; False otherwise.
        """
        try:
            self.validate_rows(rows)
            return True
        except (TypeError, ValueError):
            return False


    @property
    def columns(self) -> tuple[str, ...]:
        """
        Alias for `schema` for compatibility with other pipeline primitives.

        Returns
        -------
        tuple[str, ...]
        """
        return self._schema