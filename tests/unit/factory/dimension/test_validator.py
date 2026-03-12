"""Unit tests for cave.factory.dimension.validator."""

import pytest
from sqlalchemy import Column, Integer, String

from cave.factory.dimension.utils import CaveValidationError
from cave.factory.dimension.validator import (
    is_schema_item_not_primary_key,
    validate_schema_items,
)


class TestIsSchemaItemNotPrimaryKey:
    def test_non_pk_column_returns_true(self):
        col = Column("name", String)
        assert is_schema_item_not_primary_key(col) is True

    def test_pk_column_returns_false(self):
        col = Column("id", Integer, primary_key=True)
        assert is_schema_item_not_primary_key(col) is False

    def test_explicit_non_pk_column_returns_true(self):
        col = Column("email", String, primary_key=False)
        assert is_schema_item_not_primary_key(col) is True

    def test_nullable_column_returns_true(self):
        col = Column("value", String, nullable=True)
        assert is_schema_item_not_primary_key(col) is True

    def test_object_without_primary_key_attr_returns_true(self):
        """Arbitrary objects without ``primary_key`` default to not-PK."""

        class FakeItem:
            pass

        assert is_schema_item_not_primary_key(FakeItem()) is True


class TestValidateSchemaItems:
    def test_valid_items_returns_true(self):
        items = [Column("name", String), Column("value", Integer)]
        result = validate_schema_items(items)
        assert result is True

    def test_empty_list_returns_true(self):
        assert validate_schema_items([]) is True

    def test_pk_column_raises_validation_error(self):
        items = [
            Column("id", Integer, primary_key=True),
            Column("name", String),
        ]
        with pytest.raises(CaveValidationError):
            validate_schema_items(items)

    def test_single_pk_column_raises(self):
        items = [Column("id", Integer, primary_key=True)]
        with pytest.raises(CaveValidationError):
            validate_schema_items(items)

    def test_custom_validator_used_when_provided(self):
        """A custom validator returning False causes an error."""

        def always_false(_item: object) -> bool:
            return False

        items = [Column("name", String)]
        with pytest.raises(CaveValidationError):
            validate_schema_items(items, validators=[always_false])

    def test_custom_always_true_validator(self):
        """A PK column passes if the custom validator ignores PK."""

        def always_true(_item: object) -> bool:
            return True

        items = [Column("id", Integer, primary_key=True)]
        result = validate_schema_items(items, validators=[always_true])
        assert result is True
