"""Unit tests for PGCraftEAV and EAV helper functions."""

import pytest
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Text,
)
from sqlalchemy import (
    types as sa_types,
)
from sqlalchemy.sql.expression import Label

from pgcraft.errors import PGCraftValidationError
from pgcraft.extensions.postgrest import PostgRESTView
from pgcraft.extensions.postgrest.plugin import PostgRESTPlugin
from pgcraft.factory.dimension.eav import PGCraftEAV
from pgcraft.plugins.check import TriggerCheckPlugin
from pgcraft.plugins.eav import (
    _EAVMapping,
    _needed_value_columns,
    _pivot_aggregate,
    _resolve_value_column,
)

# -----------------------------------------------------------
# _resolve_value_column
# -----------------------------------------------------------


class TestResolveValueColumn:
    def test_string_column_name(self):
        col = Column("label", String)
        name, col_type = _resolve_value_column(col)
        assert name == "string_value"

    def test_string_column_type(self):
        col = Column("label", String)
        _, col_type = _resolve_value_column(col)
        assert isinstance(col_type, String)

    def test_integer_column_name(self):
        col = Column("count", Integer)
        name, _ = _resolve_value_column(col)
        assert name == "integer_value"

    def test_boolean_column_name(self):
        col = Column("active", Boolean)
        name, _ = _resolve_value_column(col)
        assert name == "boolean_value"

    def test_float_column_name(self):
        col = Column("score", Float)
        name, _ = _resolve_value_column(col)
        assert name == "float_value"

    def test_text_column_name(self):
        col = Column("description", Text)
        name, _ = _resolve_value_column(col)
        assert name == "text_value"

    def test_returns_tuple(self):
        col = Column("x", String)
        result = _resolve_value_column(col)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_name_is_lowercase(self):
        col = Column("x", String)
        name, _ = _resolve_value_column(col)
        assert name == name.lower()


# -----------------------------------------------------------
# _needed_value_columns
# -----------------------------------------------------------


class TestNeededValueColumns:
    def test_single_mapping_returns_one_entry(self):
        mappings = [_EAVMapping("label", "string_value", String())]
        result = _needed_value_columns(mappings)
        assert "string_value" in result

    def test_deduplicates_same_column(self):
        mappings = [
            _EAVMapping("a", "string_value", String()),
            _EAVMapping("b", "string_value", String()),
        ]
        result = _needed_value_columns(mappings)
        assert len(result) == 1
        assert "string_value" in result

    def test_different_columns_both_included(self):
        mappings = [
            _EAVMapping("a", "string_value", String()),
            _EAVMapping("b", "integer_value", Integer()),
        ]
        result = _needed_value_columns(mappings)
        assert "string_value" in result
        assert "integer_value" in result
        assert len(result) == 2

    def test_empty_mappings_returns_empty(self):
        assert _needed_value_columns([]) == {}

    def test_returns_dict(self):
        mappings = [_EAVMapping("x", "string_value", String())]
        assert isinstance(_needed_value_columns(mappings), dict)

    def test_first_type_wins_for_duplicate(self):
        """First occurrence determines the stored type."""
        string_type = String()
        text_type = Text()
        mappings = [
            _EAVMapping("a", "text_value", string_type),
            _EAVMapping("b", "text_value", text_type),
        ]
        result = _needed_value_columns(mappings)
        assert result["text_value"] is string_type


# -----------------------------------------------------------
# _pivot_aggregate
# -----------------------------------------------------------


class TestPivotAggregate:
    def _make_subquery(self, col_name: str, col_type: sa_types.TypeEngine):
        from sqlalchemy import Column, MetaData, Table

        md = MetaData()
        t = Table(
            "attr",
            md,
            Column("entity_id", Integer),
            Column("attribute_name", String),
            Column(col_name, col_type),
            schema="s",
        )
        return t.alias("cur")

    def test_non_boolean_returns_label(self):
        subq = self._make_subquery("string_value", String())
        mapping = _EAVMapping("label", "string_value", String())
        result = _pivot_aggregate(subq, mapping)
        assert isinstance(result, Label)

    def test_non_boolean_label_name(self):
        subq = self._make_subquery("string_value", String())
        mapping = _EAVMapping("label", "string_value", String())
        result = _pivot_aggregate(subq, mapping)
        assert result.key == "label"

    def test_boolean_returns_label(self):
        subq = self._make_subquery("boolean_value", Boolean())
        mapping = _EAVMapping("active", "boolean_value", Boolean())
        result = _pivot_aggregate(subq, mapping)
        assert isinstance(result, Label)

    def test_boolean_label_name(self):
        subq = self._make_subquery("boolean_value", Boolean())
        mapping = _EAVMapping("active", "boolean_value", Boolean())
        result = _pivot_aggregate(subq, mapping)
        assert result.key == "active"

    def test_boolean_involves_cast(self):
        """Boolean aggregation uses a CAST."""

        subq = self._make_subquery("boolean_value", Boolean())
        mapping = _EAVMapping("active", "boolean_value", Boolean())
        result = _pivot_aggregate(subq, mapping)
        sql_str = str(result.compile())
        # Boolean pivot should cast to/from integer
        assert "CAST" in sql_str.upper()


# -----------------------------------------------------------
# PGCraftEAV -- metadata-level checks
# -----------------------------------------------------------


class TestPGCraftEAVTables:
    def test_entity_table_created(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        assert "dim.product_entity" in metadata.tables

    def test_attribute_table_created(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        assert "dim.product_attribute" in metadata.tables

    def test_exactly_two_tables(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        assert len(metadata.tables) == 2

    def test_entity_table_has_pk(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        entity = metadata.tables["dim.product_entity"]
        pk_cols = [c for c in entity.columns if c.primary_key]
        assert len(pk_cols) == 1

    def test_attribute_table_has_entity_fk(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        attr = metadata.tables["dim.product_attribute"]
        fk_targets = {fk.column.table.name for fk in attr.foreign_keys}
        assert "product_entity" in fk_targets

    def test_attribute_table_has_value_column(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        attr = metadata.tables["dim.product_attribute"]
        col_names = {c.name for c in attr.columns}
        assert "string_value" in col_names

    def test_attribute_table_has_attribute_name_column(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        attr = metadata.tables["dim.product_attribute"]
        col_names = {c.name for c in attr.columns}
        assert "attribute_name" in col_names

    def test_multiple_column_types_create_value_columns(
        self,
    ):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [
                Column("name", String),
                Column("count", Integer),
            ],
        )
        attr = metadata.tables["dim.product_attribute"]
        col_names = {c.name for c in attr.columns}
        assert "string_value" in col_names
        assert "integer_value" in col_names

    def test_schema_applied(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "myschema",
            metadata,
            [Column("name", String)],
        )
        assert "myschema.product_entity" in metadata.tables
        assert "myschema.product_attribute" in metadata.tables


class TestPGCraftEAVViews:
    def test_pivot_view_registered(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        views = metadata.info.get("views")
        assert views is not None

    def test_pivot_view_in_dim_schema(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        views = metadata.info["views"].views
        dim_views = [v for v in views if v.schema == "dim"]
        assert any(v.name == "product" for v in dim_views)

    def test_api_view_in_api_schema(self):
        metadata = MetaData()
        f = PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        PostgRESTView(source=f)
        views = metadata.info["views"].views
        api_views = [v for v in views if v.schema == "api"]
        assert len(api_views) == 1
        assert api_views[0].name == "product"

    def test_factory_creates_one_view(self):
        """Factory alone creates exactly one pivot view."""
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        assert len(metadata.info["views"].views) == 1

    def test_exactly_two_views_with_postgrest_view(self):
        """Factory + PostgRESTView creates two views total."""
        metadata = MetaData()
        f = PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        PostgRESTView(source=f)
        assert len(metadata.info["views"].views) == 2

    def test_api_view_schema_respects_configuration(self):
        metadata = MetaData()
        f = PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        PostgRESTView(source=f, schema="custom_api")
        views = metadata.info["views"].views
        assert any(v.schema == "custom_api" for v in views)

    def test_pivot_view_definition_contains_entity(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
        )
        views = metadata.info["views"].views
        pivot_view = next(
            v for v in views if v.schema == "dim" and v.name == "product"
        )
        assert "product_entity" in pivot_view.definition


_CRUD_GRANTS = ["select", "insert", "update", "delete"]


class TestPGCraftEAVTriggers:
    def test_functions_registered(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
            extra_plugins=[
                PostgRESTPlugin(grants=_CRUD_GRANTS),
                TriggerCheckPlugin(table_key="api"),
            ],
        )
        functions = metadata.info.get("functions")
        assert functions is not None
        # 6 INSTEAD OF functions (2 views x 3 ops)
        # + 2 protection functions (entity + attribute)
        assert len(functions.functions) == 8

    def test_triggers_registered(self):
        metadata = MetaData()
        PGCraftEAV(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
            extra_plugins=[
                PostgRESTPlugin(grants=_CRUD_GRANTS),
                TriggerCheckPlugin(table_key="api"),
            ],
        )
        triggers = metadata.info.get("triggers")
        assert triggers is not None
        # 6 INSTEAD OF + 6 BEFORE protection (2 tables x 3)
        assert len(triggers.triggers) == 12


class TestPGCraftEAVValidation:
    def test_pk_column_raises(self):
        metadata = MetaData()
        with pytest.raises(PGCraftValidationError):
            PGCraftEAV(
                "product",
                "dim",
                metadata,
                [Column("id", Integer, primary_key=True)],
            )
