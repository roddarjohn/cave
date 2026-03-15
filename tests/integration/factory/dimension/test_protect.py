"""Integration tests for RawTableProtectionPlugin.

Verifies that BEFORE triggers block direct DML on raw backing tables
while still allowing mutations that arrive through INSTEAD OF triggers
on the API view (pg_trigger_depth() > 0).
"""

import pytest
from sqlalchemy import Column, MetaData, String, text
from sqlalchemy.exc import ProgrammingError

from pgcraft.config import PGCraftConfig
from pgcraft.extensions.postgrest import PostgRESTExtension, PostgRESTView
from pgcraft.factory.dimension.simple import PGCraftSimple
from tests.integration.conftest import create_all_from_metadata


@pytest.fixture
def protected_dim(db_conn, db_schema):
    """Set up a protected 'items' dimension in the test schema."""
    config = PGCraftConfig(auto_discover=False)
    config.use(PostgRESTExtension())

    metadata = MetaData()
    factory = PGCraftSimple(
        "items",
        db_schema,
        metadata,
        schema_items=[Column("name", String, nullable=False)],
        config=config,
    )
    PostgRESTView(
        source=factory,
        grants=["select", "insert", "update", "delete"],
    )
    create_all_from_metadata(db_conn, metadata)
    return db_conn, db_schema


class TestDirectDmlBlocked:
    """Direct DML on the raw backing table must raise an exception."""

    def test_direct_insert_raises(self, protected_dim):
        conn, schema = protected_dim
        with pytest.raises(ProgrammingError, match="not allowed"):
            conn.execute(
                text(f"INSERT INTO {schema}.items (name) VALUES ('x')")
            )

    def test_direct_update_raises(self, protected_dim):
        conn, schema = protected_dim
        conn.execute(text("INSERT INTO api.items (name) VALUES ('original')"))
        with pytest.raises(ProgrammingError, match="not allowed"):
            conn.execute(
                text(
                    f"UPDATE {schema}.items SET name = 'changed'"
                    f" WHERE name = 'original'"
                )
            )

    def test_direct_delete_raises(self, protected_dim):
        conn, schema = protected_dim
        conn.execute(text("INSERT INTO api.items (name) VALUES ('to_delete')"))
        with pytest.raises(ProgrammingError, match="not allowed"):
            conn.execute(
                text(f"DELETE FROM {schema}.items WHERE name = 'to_delete'")
            )


class TestApiViewDmlAllowed:
    """DML through the API view must succeed (pg_trigger_depth() > 0)."""

    def test_insert_via_api_view_succeeds(self, protected_dim):
        conn, _ = protected_dim
        conn.execute(text("INSERT INTO api.items (name) VALUES ('gadget')"))
        row = conn.execute(text("SELECT name FROM api.items")).fetchone()
        assert row is not None
        assert row[0] == "gadget"

    def test_update_via_api_view_succeeds(self, protected_dim):
        conn, _ = protected_dim
        conn.execute(text("INSERT INTO api.items (name) VALUES ('old')"))
        conn.execute(
            text("UPDATE api.items SET name = 'new' WHERE name = 'old'")
        )
        row = conn.execute(text("SELECT name FROM api.items")).fetchone()
        assert row is not None
        assert row[0] == "new"

    def test_delete_via_api_view_succeeds(self, protected_dim):
        conn, _ = protected_dim
        conn.execute(text("INSERT INTO api.items (name) VALUES ('gone')"))
        conn.execute(text("DELETE FROM api.items WHERE name = 'gone'"))
        count = conn.execute(text("SELECT COUNT(*) FROM api.items")).scalar()
        assert count == 0

    def test_error_message_names_table(self, protected_dim):
        """Exception message must name the schema and table."""
        conn, schema = protected_dim
        with pytest.raises(ProgrammingError) as exc_info:
            conn.execute(
                text(f"INSERT INTO {schema}.items (name) VALUES ('x')")
            )
        msg = str(exc_info.value)
        assert "items" in msg
