"""Unit tests for SimpleDimensionFactory (no DB required)."""

import pytest
from sqlalchemy import Column, Integer, MetaData, String
from sqlalchemy_declarative_extensions.dialects.postgresql.trigger import (
    TriggerTimes,
)

from cave.errors import CaveValidationError
from cave.factory.dimension.simple import SimpleDimensionFactory
from cave.plugins.api import APIPlugin
from cave.plugins.pk import SerialPKPlugin
from cave.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin

_CRUD_OPS = ("insert", "update", "delete")


class TestSimpleDimensionFactoryTables:
    def test_base_table_created_in_metadata(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        assert "dim.product" in metadata.tables

    def test_base_table_has_pk_column(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        table = metadata.tables["dim.product"]
        pk_cols = [c for c in table.columns if c.primary_key]
        assert len(pk_cols) == 1

    def test_base_table_pk_column_is_auto_id(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
            plugins=[
                SerialPKPlugin(column_name="my_id"),
                SimpleTablePlugin(),
                APIPlugin(),
                SimpleTriggerPlugin(),
            ],
        )
        table = metadata.tables["dim.product"]
        pk_col = next(c for c in table.columns if c.primary_key)
        assert pk_col.name == "my_id"

    def test_base_table_has_user_columns(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product",
            "dim",
            metadata,
            [Column("name", String), Column("code", String)],
        )
        table = metadata.tables["dim.product"]
        col_names = {c.name for c in table.columns}
        assert "name" in col_names
        assert "code" in col_names

    def test_schema_applied_to_table(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "myschema", metadata, [Column("name", String)]
        )
        assert "myschema.product" in metadata.tables

    def test_no_extra_tables_created(self):
        """Simple factory should create exactly one table."""
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        assert len(metadata.tables) == 1


class TestSimpleDimensionFactoryViews:
    def test_api_view_registered(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        views = metadata.info.get("views")
        assert views is not None

    def test_api_view_has_correct_name(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        views = metadata.info["views"]
        view_names = [v.name for v in views.views]
        assert "product" in view_names

    def test_api_view_schema_defaults_to_api(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        views = metadata.info["views"]
        api_view = next(v for v in views.views if v.name == "product")
        assert api_view.schema == "api"

    def test_api_view_schema_respects_configuration(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
            plugins=[
                SerialPKPlugin(),
                SimpleTablePlugin(),
                APIPlugin(schema="public_api"),
                SimpleTriggerPlugin(),
            ],
        )
        views = metadata.info["views"]
        api_view = next(v for v in views.views if v.name == "product")
        assert api_view.schema == "public_api"

    def test_view_definition_references_base_table(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        views = metadata.info["views"]
        api_view = next(v for v in views.views if v.name == "product")
        assert "dim" in api_view.definition
        assert "product" in api_view.definition

    def test_exactly_one_view_created(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        views = metadata.info["views"]
        assert len(views.views) == 1


class TestSimpleDimensionFactoryTriggers:
    def test_functions_registered(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        functions = metadata.info.get("functions")
        assert functions is not None
        assert len(functions.functions) == len(_CRUD_OPS)

    def test_triggers_registered(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        triggers = metadata.info.get("triggers")
        assert triggers is not None
        assert len(triggers.triggers) == len(_CRUD_OPS)

    def test_insert_function_exists(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        fn_names = {f.name for f in metadata.info["functions"].functions}
        assert "api_product_insert" in fn_names

    def test_update_function_exists(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        fn_names = {f.name for f in metadata.info["functions"].functions}
        assert "api_product_update" in fn_names

    def test_delete_function_exists(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        fn_names = {f.name for f in metadata.info["functions"].functions}
        assert "api_product_delete" in fn_names

    def test_insert_trigger_is_instead_of(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        triggers = metadata.info["triggers"].triggers
        insert_tr = next(t for t in triggers if "insert" in t.name)
        assert insert_tr.time == TriggerTimes.instead_of


class TestSimpleDimensionFactoryAPIResource:
    def test_api_resource_registered(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        resources = metadata.info.get("api_resources", [])
        assert len(resources) == 1

    def test_api_resource_name_is_tablename(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        resource = metadata.info["api_resources"][0]
        assert resource.name == "product"

    def test_api_resource_schema_defaults_to_api(self):
        metadata = MetaData()
        SimpleDimensionFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        resource = metadata.info["api_resources"][0]
        assert resource.schema == "api"


class TestSimpleDimensionFactoryValidation:
    def test_pk_column_raises(self):
        metadata = MetaData()
        with pytest.raises(CaveValidationError):
            SimpleDimensionFactory(
                "product",
                "dim",
                metadata,
                [Column("id", Integer, primary_key=True)],
            )
