"""Unit tests for DimensionConfig and ColumnConfig validation."""

import pytest
from pydantic import ValidationError

from pgcraft.runtime.config import ColumnConfig, DimensionConfig

_PG_ID = "valid PostgreSQL identifier"


# ---------------------------------------------------------------------------
# ColumnConfig — name validation
# ---------------------------------------------------------------------------


class TestColumnConfigName:
    def test_valid_name(self):
        col = ColumnConfig(name="my_column", type="text")
        assert col.name == "my_column"

    def test_underscore_prefix_is_valid(self):
        col = ColumnConfig(name="_internal", type="text")
        assert col.name == "_internal"

    def test_name_with_digits(self):
        col = ColumnConfig(name="col1", type="text")
        assert col.name == "col1"

    def test_name_strips_whitespace(self):
        col = ColumnConfig(name="  label  ", type="text")
        assert col.name == "label"

    def test_uppercase_is_rejected(self):
        with pytest.raises(ValidationError, match=_PG_ID):
            ColumnConfig(name="MyColumn", type="text")

    def test_spaces_are_rejected(self):
        with pytest.raises(ValidationError, match=_PG_ID):
            ColumnConfig(name="my column", type="text")

    def test_digit_prefix_is_rejected(self):
        with pytest.raises(ValidationError, match=_PG_ID):
            ColumnConfig(name="1col", type="text")

    def test_hyphen_is_rejected(self):
        with pytest.raises(ValidationError, match=_PG_ID):
            ColumnConfig(name="my-col", type="text")

    def test_name_too_long_is_rejected(self):
        with pytest.raises(ValidationError, match=_PG_ID):
            ColumnConfig(name="a" * 64, type="text")

    def test_max_length_is_accepted(self):
        col = ColumnConfig(name="a" * 63, type="text")
        assert len(col.name) == 63


# ---------------------------------------------------------------------------
# ColumnConfig — type vocabulary
# ---------------------------------------------------------------------------


class TestColumnConfigType:
    @pytest.mark.parametrize(
        "type_name",
        [
            "text",
            "integer",
            "bigint",
            "boolean",
            "timestamptz",
            "date",
            "numeric",
            "uuid",
            "jsonb",
        ],
    )
    def test_all_valid_types_accepted(self, type_name: str):
        col = ColumnConfig(name="col", type=type_name)
        assert col.type == type_name

    def test_invalid_type_rejected(self):
        with pytest.raises(ValidationError):
            ColumnConfig(name="col", type="varchar")  # type: ignore[arg-type]

    def test_raw_sql_type_rejected(self):
        with pytest.raises(ValidationError):
            ColumnConfig(name="col", type="TEXT")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# ColumnConfig — nullable and default
# ---------------------------------------------------------------------------


class TestColumnConfigNullableAndDefault:
    def test_nullable_defaults_to_true(self):
        col = ColumnConfig(name="col", type="text")
        assert col.nullable is True

    def test_nullable_false(self):
        col = ColumnConfig(name="col", type="text", nullable=False)
        assert col.nullable is False

    def test_default_none(self):
        col = ColumnConfig(name="col", type="text", default=None)
        assert col.default is None

    def test_known_function_default_accepted(self):
        col = ColumnConfig(name="col", type="timestamptz", default="now()")
        assert col.default == "now()"

    def test_current_timestamp_accepted(self):
        col = ColumnConfig(
            name="col", type="timestamptz", default="current_timestamp"
        )
        assert col.default == "current_timestamp"

    def test_gen_random_uuid_accepted(self):
        col = ColumnConfig(name="col", type="uuid", default="gen_random_uuid()")
        assert col.default == "gen_random_uuid()"

    def test_boolean_literals_accepted(self):
        assert (
            ColumnConfig(name="col", type="boolean", default="true").default
            == "true"
        )
        assert (
            ColumnConfig(name="col", type="boolean", default="false").default
            == "false"
        )

    def test_null_literal_accepted(self):
        col = ColumnConfig(name="col", type="text", default="null")
        assert col.default == "null"

    def test_quoted_string_literal_accepted(self):
        col = ColumnConfig(name="col", type="text", default="'hello'")
        assert col.default == "'hello'"

    def test_integer_literal_accepted(self):
        col = ColumnConfig(name="col", type="integer", default="42")
        assert col.default == "42"

    def test_decimal_literal_accepted(self):
        col = ColumnConfig(name="col", type="numeric", default="3.14")
        assert col.default == "3.14"

    def test_arbitrary_sql_rejected(self):
        with pytest.raises(ValidationError, match="allowlist"):
            ColumnConfig(name="col", type="text", default="(SELECT 1)")

    def test_sql_injection_attempt_rejected(self):
        with pytest.raises(ValidationError, match="allowlist"):
            ColumnConfig(name="col", type="text", default="'x'; DROP TABLE t--")

    def test_default_is_stripped(self):
        col = ColumnConfig(name="col", type="timestamptz", default="  now()  ")
        assert col.default == "now()"


# ---------------------------------------------------------------------------
# DimensionConfig — table name validation
# ---------------------------------------------------------------------------


class TestDimensionConfigTableName:
    def test_valid_name(self):
        cfg = DimensionConfig(table_name="my_table", columns=[])
        assert cfg.table_name == "my_table"

    def test_name_strips_whitespace(self):
        cfg = DimensionConfig(table_name="  orders  ", columns=[])
        assert cfg.table_name == "orders"

    def test_uppercase_rejected(self):
        with pytest.raises(ValidationError, match=_PG_ID):
            DimensionConfig(table_name="MyTable", columns=[])

    def test_hyphen_rejected(self):
        with pytest.raises(ValidationError, match=_PG_ID):
            DimensionConfig(table_name="my-table", columns=[])

    def test_name_too_long_rejected(self):
        with pytest.raises(ValidationError, match=_PG_ID):
            DimensionConfig(table_name="a" * 64, columns=[])


# ---------------------------------------------------------------------------
# DimensionConfig — defaults and structure
# ---------------------------------------------------------------------------


class TestDimensionConfigDefaults:
    def test_version_defaults_to_one(self):
        cfg = DimensionConfig(table_name="t", columns=[])
        assert cfg.version == "1"

    def test_pk_defaults_to_uuidv7(self):
        cfg = DimensionConfig(table_name="t", columns=[])
        assert cfg.pk == "uuidv7"

    def test_table_type_defaults_to_simple(self):
        cfg = DimensionConfig(table_name="t", columns=[])
        assert cfg.table_type == "simple"

    def test_all_pk_options_accepted(self):
        for pk in ("serial", "uuidv4", "uuidv7"):
            cfg = DimensionConfig(table_name="t", columns=[], pk=pk)
            assert cfg.pk == pk

    def test_invalid_pk_rejected(self):
        with pytest.raises(ValidationError):
            DimensionConfig(table_name="t", columns=[], pk="uuid")  # type: ignore[arg-type]

    def test_columns_preserved_in_order(self):
        cfg = DimensionConfig(
            table_name="t",
            columns=[
                ColumnConfig(name="b", type="text"),
                ColumnConfig(name="a", type="integer"),
            ],
        )
        assert [c.name for c in cfg.columns] == ["b", "a"]

    def test_roundtrip_json(self):
        cfg = DimensionConfig(
            table_name="product",
            columns=[ColumnConfig(name="label", type="text")],
        )
        restored = DimensionConfig.model_validate_json(cfg.model_dump_json())
        assert restored == cfg
