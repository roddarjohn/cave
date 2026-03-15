"""Unit tests for pgcraft.validation module."""

import pytest

from pgcraft.errors import PGCraftValidationError
from pgcraft.validation import validate_column_references


class TestValidateColumnReferences:
    def test_valid_columns_pass(self):
        validate_column_references(
            "test",
            ["price", "qty"],
            {"price", "qty", "name"},
        )

    def test_unknown_column_raises(self):
        with pytest.raises(PGCraftValidationError, match="bogus"):
            validate_column_references(
                "test label",
                ["bogus"],
                {"price", "qty"},
            )

    def test_error_message_includes_label(self):
        with pytest.raises(PGCraftValidationError, match="my label"):
            validate_column_references(
                "my label",
                ["missing"],
                {"a", "b"},
            )

    def test_error_message_includes_known_columns(self):
        with pytest.raises(PGCraftValidationError, match="'x'"):
            validate_column_references(
                "test",
                ["missing"],
                {"x"},
            )

    def test_empty_columns_pass(self):
        validate_column_references("test", [], {"a", "b"})

    def test_empty_known_columns_raises(self):
        with pytest.raises(PGCraftValidationError):
            validate_column_references("test", ["a"], set())
