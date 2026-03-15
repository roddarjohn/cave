"""Unit tests for pgcraft.validation module."""

import pytest

from pgcraft.errors import PGCraftValidationError
from pgcraft.validation import (
    extract_column_names,
    resolve_markers,
    validate_column_references,
)


class TestExtractColumnNames:
    def test_single_marker(self):
        assert extract_column_names("{price}") == ["price"]

    def test_multiple_markers(self):
        assert extract_column_names("{a} + {b}") == ["a", "b"]

    def test_deduplicates(self):
        assert extract_column_names("{x} AND {x}") == ["x"]

    def test_no_markers(self):
        assert extract_column_names("1 = 1") == []

    def test_embedded_in_function(self):
        assert extract_column_names("lower({name})") == ["name"]


class TestResolveMarkers:
    def test_identity(self):
        assert resolve_markers("{price} > 0", lambda c: c) == "price > 0"

    def test_new_prefix(self):
        result = resolve_markers("{price} > 0", lambda c: f"NEW.{c}")
        assert result == "NEW.price > 0"

    def test_multi_marker(self):
        result = resolve_markers("{a} + {b}", lambda c: c.upper())
        assert result == "A + B"

    def test_no_markers(self):
        assert resolve_markers("1 = 1", lambda c: c) == "1 = 1"


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
