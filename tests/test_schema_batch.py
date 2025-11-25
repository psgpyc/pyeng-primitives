import pytest
from primitives.schema_validator import SchemaValidator


def _make_validator(schema=None, strict_order=True) -> SchemaValidator:
    """
    Small helper to construct a default validator for tests.
    """
    if schema is None:
        schema = ["a", "b", "c", "d"]
    return SchemaValidator(schema=schema, strict_order=strict_order)



def test_init_stores_schema_and_strict_order() -> None:
    validator = _make_validator()
    assert validator.schema == ("a", "b", "c", "d")
    assert validator.strict_order is True


def test_init_rejects_empty_schema() -> None:
    with pytest.raises(ValueError):
        SchemaValidator(schema=[])

    with pytest.raises(ValueError):
        SchemaValidator(schema=())


def test_init_defensive_copy_of_schema() -> None:
    schema = ["x", "y"]
    validator = _make_validator(schema=schema)

    # mutate original list after construction
    schema.append("z")

    # internal schema must not change
    assert validator.schema == ("x", "y")





def test_validate_row_strict_valid_row() -> None:
    validator = _make_validator()
    row = {"a": 1, "b": 2, "c": 3, "d": 4}
    # Should not raise
    validator.validate_row(row=row)


def test_validate_row_strict_rejects_wrong_order() -> None:
    validator = _make_validator()
    row = {"b": 1, "a": 2, "c": 3, "d": 4}

    with pytest.raises(ValueError):
        validator.validate_row(row)


@pytest.mark.parametrize(
    "row",
    [
        {"b": 1, "c": 3, "d": 4},  # missing 'a'
        {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},  # extra 'e'
    ],
)
def test_validate_row_strict_missing_or_extra_key_raises_value_error(row) -> None:
    validator = _make_validator()

    with pytest.raises(ValueError):
        validator.validate_row(row)


def test_validate_row_strict_non_mapping_raises_type_error() -> None:
    validator = _make_validator()
    row = [("a", 1), ("b", 2), ("c", 3), ("d", 4)]  # list of tuples, not a Mapping

    with pytest.raises(TypeError):
        validator.validate_row(row)



def test_validate_row_non_strict_allows_permuted_keys() -> None:
    validator = _make_validator(strict_order=False)
    row = {"b": 1, "a": 2, "d": 4, "c": 3}

    # Order is permuted but set of keys matches schema
    validator.validate_row(row)


@pytest.mark.parametrize(
    "row",
    [
        {"b": 1, "c": 3, "d": 4},  # missing 'a'
        {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},  # extra 'e'
    ],
)
def test_validate_row_non_strict_missing_or_extra_key_raises_value_error(row) -> None:
    validator = _make_validator(strict_order=False)

    with pytest.raises(ValueError):
        validator.validate_row(row)




def test_validate_rows_all_valid_strict() -> None:
    validator = _make_validator()
    rows = [
        {"a": 1, "b": 2, "c": 3, "d": 4},
        {"a": 11, "b": 21, "c": 31, "d": 41},
    ]

    # Should not raise
    validator.validate_rows(rows)


def test_validate_rows_propagates_value_error_on_first_invalid() -> None:
    validator = _make_validator()
    rows = [
        {"a": 1, "b": 2, "c": 3, "d": 4},  # valid
        {"a": 11, "b": 21, "c": 31},  # invalid: missing 'd'
        {"a": 99, "b": 99, "c": 99, "d": 99},  # should never be validated
    ]

    with pytest.raises(ValueError):
        validator.validate_rows(rows)


def test_validate_rows_propagates_type_error_for_non_mappings() -> None:
    validator = _make_validator()
    rows = [
        {"a": 1, "b": 2, "c": 3, "d": 4},
        ("a", 1, "b", 2),  # not a Mapping
    ]

    with pytest.raises(TypeError):
        validator.validate_rows(rows)


def test_validate_rows_non_iterable_raises_type_error() -> None:
    validator = _make_validator()

    with pytest.raises(TypeError):
        validator.validate_rows(None)  # type: ignore[arg-type]

    with pytest.raises(TypeError):
        validator.validate_rows(123)  # type: ignore[arg-type]



def test_is_valid_row_true_on_valid() -> None:
    validator = _make_validator()
    row = {"a": 1, "b": 2, "c": 3, "d": 4}

    assert validator.is_valid_row(row=row) is True


def test_is_valid_row_false_on_invalid_schema() -> None:
    validator = _make_validator()
    row = {"b": 1, "a": 2, "c": 3, "d": 4}  # wrong order for strict mode

    assert validator.is_valid_row(row) is False


def test_is_valid_row_false_on_non_mapping() -> None:
    validator = _make_validator()
    row = [("a", 1), ("b", 2), ("c", 3), ("d", 4)]

    assert validator.is_valid_row(row) is False



def test_is_valid_rows_true_when_all_valid() -> None:
    validator = _make_validator()
    rows = [
        {"a": 1, "b": 2, "c": 3, "d": 4},
        {"a": 11, "b": 21, "c": 31, "d": 41},
    ]

    assert validator.is_valid_rows(rows) is True


def test_is_valid_rows_false_when_any_row_invalid() -> None:
    validator = _make_validator()
    rows = [
        {"a": 1, "b": 2, "c": 3, "d": 4},
        {"b": 22, "a": 21, "c": 23, "d": 24},  # wrong order (strict → invalid)
        {"a": 11, "b": 21, "c": 31, "d": 41},
    ]

    assert validator.is_valid_rows(rows) is False


def test_is_valid_rows_false_when_non_iterable_passed() -> None:
    validator = _make_validator()

    assert validator.is_valid_rows(None) is False  # type: ignore[arg-type]


def test_columns_property_returns_schema_tuple() -> None:
    schema = ["a", "b"]
    validator = _make_validator(schema=schema)

    assert validator.columns == ("a", "b")