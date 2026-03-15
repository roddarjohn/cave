"""Integration tests for SimpleDimensionFactory.

Creates real database objects and verifies CRUD operations through
the API view and its INSTEAD OF triggers.
"""

import pytest
from sqlalchemy import Column, MetaData, String, text

from pgcraft.config import PGCraftConfig
from pgcraft.extensions.postgrest import PostgRESTExtension, PostgRESTView
from pgcraft.factory.dimension.simple import PGCraftSimple
from tests.integration.conftest import create_all_from_metadata


@pytest.fixture
def simple_dim(db_conn, db_schema):
    """Set up a simple 'widgets' dimension in the test schema."""
    config = PGCraftConfig(auto_discover=False)
    config.use(PostgRESTExtension())

    metadata = MetaData()
    factory = PGCraftSimple(
        "widgets",
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


class TestSimpleDimensionInsert:
    def test_insert_via_api_view_succeeds(self, simple_dim):
        conn, _ = simple_dim
        conn.execute(text("INSERT INTO api.widgets (name) VALUES ('Gadget')"))

    def test_inserted_row_visible_in_api_view(self, simple_dim):
        conn, _ = simple_dim
        conn.execute(text("INSERT INTO api.widgets (name) VALUES ('Gadget')"))
        row = conn.execute(text("SELECT name FROM api.widgets")).fetchone()
        assert row is not None
        assert row[0] == "Gadget"

    def test_multiple_inserts(self, simple_dim):
        conn, _ = simple_dim
        conn.execute(
            text("INSERT INTO api.widgets (name) VALUES ('A'), ('B'), ('C')")
        )
        count = conn.execute(text("SELECT COUNT(*) FROM api.widgets")).scalar()
        assert count == 3


class TestSimpleDimensionUpdate:
    @pytest.fixture(autouse=True)
    def _seed(self, simple_dim):
        conn, schema = simple_dim
        conn.execute(text("INSERT INTO api.widgets (name) VALUES ('Original')"))
        self.conn = conn
        self.schema = schema

    def test_update_via_api_view(self):
        self.conn.execute(
            text(
                "UPDATE api.widgets SET name = 'Updated'"
                " WHERE name = 'Original'"
            )
        )
        row = self.conn.execute(text("SELECT name FROM api.widgets")).fetchone()
        assert row is not None
        assert row[0] == "Updated"


class TestSimpleDimensionDelete:
    @pytest.fixture(autouse=True)
    def _seed(self, simple_dim):
        conn, schema = simple_dim
        conn.execute(text("INSERT INTO api.widgets (name) VALUES ('ToDelete')"))
        self.conn = conn
        self.schema = schema

    def test_delete_via_api_view(self):
        self.conn.execute(
            text("DELETE FROM api.widgets WHERE name = 'ToDelete'")
        )
        count = self.conn.execute(
            text("SELECT COUNT(*) FROM api.widgets")
        ).scalar()
        assert count == 0
