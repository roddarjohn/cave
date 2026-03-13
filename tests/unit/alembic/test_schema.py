"""Unit tests for pgcraft.alembic.schema."""

import pytest
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy_declarative_extensions import Schemas, Views
from sqlalchemy_declarative_extensions.view.base import View

from pgcraft.alembic.schema import (
    SYSTEM_SCHEMAS,
    collect_schemas,
    register_schemas,
)


class TestSystemSchemas:
    def test_contains_public(self):
        """SYSTEM_SCHEMAS must include 'public'."""
        assert "public" in SYSTEM_SCHEMAS

    def test_contains_pg_catalog(self):
        assert "pg_catalog" in SYSTEM_SCHEMAS

    def test_contains_information_schema(self):
        assert "information_schema" in SYSTEM_SCHEMAS

    def test_contains_pg_toast(self):
        assert "pg_toast" in SYSTEM_SCHEMAS

    def test_is_frozenset(self):
        assert isinstance(SYSTEM_SCHEMAS, frozenset)


class TestCollectSchemas:
    def test_empty_metadata_returns_empty_set(self):
        """No tables → no schemas discovered."""
        metadata = MetaData()
        assert collect_schemas(metadata) == set()

    def test_table_without_schema_not_collected(self):
        """Tables without an explicit schema have schema=None."""
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer))
        assert collect_schemas(metadata) == set()

    def test_table_schema_is_collected(self):
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer), schema="myschema")
        assert collect_schemas(metadata) == {"myschema"}

    def test_multiple_tables_multiple_schemas(self):
        metadata = MetaData()
        Table("a", metadata, Column("id", Integer), schema="s1")
        Table("b", metadata, Column("id", Integer), schema="s2")
        assert collect_schemas(metadata) == {"s1", "s2"}

    def test_system_schemas_excluded(self):
        """Tables in public/pg_catalog/etc. must be filtered out."""
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer), schema="public")
        assert collect_schemas(metadata) == set()

    def test_view_schema_is_collected(self):
        metadata = MetaData()
        views = Views().are(View("myview", "SELECT 1", schema="viewschema"))
        metadata.info["views"] = views
        assert collect_schemas(metadata) == {"viewschema"}

    def test_view_without_schema_not_collected(self):
        metadata = MetaData()
        views = Views().are(View("myview", "SELECT 1"))
        metadata.info["views"] = views
        assert collect_schemas(metadata) == set()

    def test_table_and_view_schemas_combined(self):
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer), schema="tschema")
        views = Views().are(View("v", "SELECT 1", schema="vschema"))
        metadata.info["views"] = views
        result = collect_schemas(metadata)
        assert result == {"tschema", "vschema"}

    def test_system_schema_in_views_excluded(self):
        metadata = MetaData()
        views = Views().are(View("v", "SELECT 1", schema="public"))
        metadata.info["views"] = views
        assert collect_schemas(metadata) == set()

    def test_duplicate_schemas_deduplicated(self):
        metadata = MetaData()
        Table("a", metadata, Column("id", Integer), schema="same")
        Table("b", metadata, Column("id", Integer), schema="same")
        assert collect_schemas(metadata) == {"same"}


class TestRegisterSchemas:
    def test_no_schemas_no_op(self):
        """When there are no non-system schemas, metadata.info is unchanged."""
        metadata = MetaData()
        register_schemas(metadata)
        assert "schemas" not in metadata.info

    def test_new_schema_is_registered(self):
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer), schema="myschema")
        register_schemas(metadata)
        schemas = metadata.info.get("schemas")
        assert schemas is not None
        names = {s.name for s in schemas.schemas}
        assert "myschema" in names

    def test_ignore_unspecified_set_on_new_schemas(self):
        """Auto-discovered schemas use ``ignore_unspecified=True``."""
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer), schema="s")
        register_schemas(metadata)
        assert metadata.info["schemas"].ignore_unspecified is True

    def test_existing_schemas_not_re_added(self):
        """Schemas already registered are not added a second time."""
        metadata = MetaData()
        existing = Schemas(ignore_unspecified=True).are("existing")
        metadata.info["schemas"] = existing
        Table("t", metadata, Column("id", Integer), schema="existing")
        register_schemas(metadata)
        # The schema was already registered; no new schemas discovered.
        names = [s.name for s in metadata.info["schemas"].schemas]
        assert names.count("existing") == 1

    def test_already_registered_schema_not_duplicated(self):
        """Calling register_schemas twice should not add duplicates."""
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer), schema="s")
        register_schemas(metadata)
        register_schemas(metadata)
        names = [s.name for s in metadata.info["schemas"].schemas]
        assert names.count("s") == 1

    def test_only_system_schemas_no_op(self):
        """Only system-schema tables → metadata.info unchanged."""
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer), schema="public")
        register_schemas(metadata)
        assert "schemas" not in metadata.info

    def test_multiple_schemas_all_registered(self):
        metadata = MetaData()
        Table("a", metadata, Column("id", Integer), schema="s1")
        Table("b", metadata, Column("id", Integer), schema="s2")
        register_schemas(metadata)
        names = {s.name for s in metadata.info["schemas"].schemas}
        assert "s1" in names
        assert "s2" in names

    def test_view_schema_registered(self):
        metadata = MetaData()
        views = Views().are(View("v", "SELECT 1", schema="vschema"))
        metadata.info["views"] = views
        register_schemas(metadata)
        names = {s.name for s in metadata.info["schemas"].schemas}
        assert "vschema" in names


@pytest.mark.parametrize(
    "schema",
    ["public", "pg_catalog", "information_schema", "pg_toast"],
)
def test_system_schemas_all_excluded(schema: str) -> None:
    """Each system schema is individually excluded by collect_schemas."""
    metadata = MetaData()
    Table("t", metadata, Column("id", Integer), schema=schema)
    assert collect_schemas(metadata) == set()
