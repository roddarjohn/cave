"""Integration tests for EAV nullable attribute enforcement.

Uses pgcraft's EAV factory to create real database objects and
verifies that:

- Non-nullable attributes raise an exception on INSERT/UPDATE when
  the value is omitted (NULL).
- Nullable attributes silently allow NULL (no row is written to the
  attribute table).
- Updates that do not change a value write no new attribute row.
"""

import pytest
from sqlalchemy import Column, MetaData, String, text

from pgcraft.config import PGCraftConfig
from pgcraft.extensions.postgrest import PostgRESTExtension, PostgRESTPlugin
from pgcraft.factory.dimension.eav import PGCraftEAV
from tests.integration.conftest import create_all_from_metadata


@pytest.fixture
def eav_nullable(db_conn, db_schema):
    """EAV setup with one required (sku) and one optional (color) attribute."""
    config = PGCraftConfig(auto_discover=False)
    config.use(PostgRESTExtension())

    metadata = MetaData()
    PGCraftEAV(
        "things",
        db_schema,
        metadata,
        schema_items=[
            Column("sku", String, nullable=False),
            Column("color", String, nullable=True),
        ],
        config=config,
        extra_plugins=[
            PostgRESTPlugin(
                grants=["select", "insert", "update"],
            ),
        ],
    )
    create_all_from_metadata(db_conn, metadata)
    return db_conn, db_schema


class TestInsertEnforcement:
    def test_insert_with_required_attribute_succeeds(self, eav_nullable):
        conn, _ = eav_nullable
        conn.execute(text("INSERT INTO api.things (sku) VALUES ('ABC-1')"))
        row = conn.execute(text("SELECT sku FROM api.things")).fetchone()
        assert row is not None
        assert row[0] == "ABC-1"

    def test_insert_missing_required_attribute_raises(self, eav_nullable):
        conn, _ = eav_nullable
        with pytest.raises(Exception, match="sku"):
            conn.execute(text("INSERT INTO api.things (color) VALUES ('red')"))

    def test_insert_with_null_optional_attribute_succeeds(self, eav_nullable):
        conn, _ = eav_nullable
        conn.execute(
            text("INSERT INTO api.things (sku, color) VALUES ('ABC-2', NULL)")
        )
        row = conn.execute(text("SELECT sku, color FROM api.things")).fetchone()
        assert row is not None
        assert row[0] == "ABC-2"
        assert row[1] is None

    def test_insert_omitting_optional_attribute_succeeds(self, eav_nullable):
        conn, _ = eav_nullable
        conn.execute(text("INSERT INTO api.things (sku) VALUES ('ABC-3')"))
        row = conn.execute(text("SELECT color FROM api.things")).fetchone()
        assert row is not None
        assert row[0] is None


class TestUpdateEnforcement:
    @pytest.fixture(autouse=True)
    def _seed(self, eav_nullable):
        conn, schema = eav_nullable
        conn.execute(
            text("INSERT INTO api.things (sku, color) VALUES ('ABC-10', 'red')")
        )
        self.conn = conn
        self.schema = schema

    def test_update_required_attribute_to_null_raises(self):
        with pytest.raises(Exception, match="sku"):
            self.conn.execute(
                text("UPDATE api.things SET sku = NULL WHERE sku = 'ABC-10'")
            )

    def test_update_with_same_value_writes_no_new_row(self):
        before = self.conn.execute(
            text(
                f"SELECT COUNT(*) FROM {self.schema}.things_attribute"
                f" WHERE attribute_name = 'color'"
            )
        ).scalar()
        self.conn.execute(
            text("UPDATE api.things SET color = 'red' WHERE sku = 'ABC-10'")
        )
        after = self.conn.execute(
            text(
                f"SELECT COUNT(*) FROM {self.schema}.things_attribute"
                f" WHERE attribute_name = 'color'"
            )
        ).scalar()
        assert before == after

    def test_update_with_changed_value_writes_new_row(self):
        before = self.conn.execute(
            text(
                f"SELECT COUNT(*) FROM {self.schema}.things_attribute"
                f" WHERE attribute_name = 'color'"
            )
        ).scalar()
        self.conn.execute(
            text("UPDATE api.things SET color = 'blue' WHERE sku = 'ABC-10'")
        )
        after = self.conn.execute(
            text(
                f"SELECT COUNT(*) FROM {self.schema}.things_attribute"
                f" WHERE attribute_name = 'color'"
            )
        ).scalar()
        assert after == before + 1
