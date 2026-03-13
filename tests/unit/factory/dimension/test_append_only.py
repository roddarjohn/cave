"""Unit tests for AppendOnlyDimensionResourceFactory (no DB required)."""

import pytest
from sqlalchemy import Column, Integer, MetaData, String

from pgcraft.errors import PGCraftValidationError
from pgcraft.factory.dimension.append_only import (
    AppendOnlyDimensionResourceFactory,
)
from pgcraft.plugins.api import APIPlugin
from pgcraft.plugins.append_only import (
    AppendOnlyTablePlugin,
    AppendOnlyTriggerPlugin,
    AppendOnlyViewPlugin,
)
from pgcraft.plugins.pk import SerialPKPlugin


class TestAppendOnlyDimensionResourceFactoryTables:
    def test_attributes_table_created(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        keys = list(metadata.tables.keys())
        assert any("attributes" in k for k in keys)

    def test_root_table_created(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        keys = list(metadata.tables.keys())
        assert any("root" in k for k in keys)

    def test_attributes_table_default_name(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        assert "dim.product_attributes" in metadata.tables

    def test_root_table_default_name(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        assert "dim.product_root" in metadata.tables

    def test_exactly_two_tables(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        assert len(metadata.tables) == 2

    def test_attributes_table_has_user_columns(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        table = metadata.tables["dim.product_attributes"]
        col_names = {c.name for c in table.columns}
        assert "name" in col_names

    def test_root_table_has_created_at(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        table = metadata.tables["dim.product_root"]
        col_names = {c.name for c in table.columns}
        assert "created_at" in col_names

    def test_root_has_fk_to_attributes(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        root = metadata.tables["dim.product_root"]
        fk_targets = {fk.column.table.name for fk in root.foreign_keys}
        assert "product_attributes" in fk_targets

    def test_schema_applied_to_tables(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "myschema", metadata, [Column("name", String)]
        )
        assert "myschema.product_attributes" in metadata.tables
        assert "myschema.product_root" in metadata.tables


class TestAppendOnlyDimensionResourceFactoryViews:
    def test_join_view_registered(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        views = metadata.info.get("views")
        assert views is not None

    def test_join_view_in_dim_schema(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        views = metadata.info["views"].views
        dim_views = [v for v in views if v.schema == "dim"]
        assert len(dim_views) >= 1
        assert any(v.name == "product" for v in dim_views)

    def test_api_view_in_api_schema(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        views = metadata.info["views"].views
        api_views = [v for v in views if v.schema == "api"]
        assert len(api_views) == 1
        assert api_views[0].name == "product"

    def test_exactly_two_views(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        assert len(metadata.info["views"].views) == 2

    def test_api_view_schema_respects_configuration(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product",
            "dim",
            metadata,
            [Column("name", String)],
            plugins=[
                SerialPKPlugin(),
                AppendOnlyTablePlugin(),
                AppendOnlyViewPlugin(),
                APIPlugin(schema="custom_api"),
                AppendOnlyTriggerPlugin(),
            ],
        )
        views = metadata.info["views"].views
        api_views = [v for v in views if v.schema == "custom_api"]
        assert len(api_views) == 1

    def test_join_view_definition_references_tables(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        views = metadata.info["views"].views
        join_view = next(
            v for v in views if v.schema == "dim" and v.name == "product"
        )
        assert "product_root" in join_view.definition
        assert "product_attributes" in join_view.definition


class TestAppendOnlyDimensionResourceFactoryTriggers:
    def test_functions_registered(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        functions = metadata.info.get("functions")
        assert functions is not None
        # Two views × three ops = 6 functions
        assert len(functions.functions) == 6

    def test_triggers_registered(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        triggers = metadata.info.get("triggers")
        assert triggers is not None
        assert len(triggers.triggers) == 6

    def test_function_names_include_op(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        fn_names = {f.name for f in metadata.info["functions"].functions}
        # Each op appears twice (once per view schema)
        insert_fns = [n for n in fn_names if "insert" in n]
        assert len(insert_fns) == 2


class TestAppendOnlyDimensionResourceFactoryAPIResource:
    def test_api_resource_registered(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        resources = metadata.info.get("api_resources", [])
        assert len(resources) == 1

    def test_api_resource_name(self):
        metadata = MetaData()
        AppendOnlyDimensionResourceFactory(
            "product", "dim", metadata, [Column("name", String)]
        )
        assert metadata.info["api_resources"][0].name == "product"


class TestAppendOnlyDimensionResourceFactoryValidation:
    def test_pk_column_raises(self):
        metadata = MetaData()
        with pytest.raises(PGCraftValidationError):
            AppendOnlyDimensionResourceFactory(
                "product",
                "dim",
                metadata,
                [Column("id", Integer, primary_key=True)],
            )
