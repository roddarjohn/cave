"""Integration tests for EAV check constraints via TriggerCheckPlugin.

Uses pgcraft's EAV factory with PGCraftCheck items to create real
database objects, then verifies that:

- Valid inserts succeed.
- Inserts violating a single-column check are rejected.
- Inserts violating a multi-column check are rejected.
- Updates that violate a check are rejected.
- Updates that satisfy the check succeed.
"""

import pytest
from sqlalchemy import Column, Integer, MetaData, text

from pgcraft.check import PGCraftCheck
from pgcraft.config import PGCraftConfig
from pgcraft.extensions.postgrest import PostgRESTExtension, PostgRESTPlugin
from pgcraft.factory.dimension.eav import PGCraftEAV
from tests.integration.conftest import create_all_from_metadata


@pytest.fixture
def eav_with_checks(db_conn, db_schema):
    """EAV with price (>0) and qty (>=0) columns plus a cross-column check."""
    config = PGCraftConfig(auto_discover=False)
    config.use(PostgRESTExtension())

    metadata = MetaData()
    PGCraftEAV(
        "products",
        db_schema,
        metadata,
        schema_items=[
            Column("price", Integer, nullable=False),
            Column("qty", Integer, nullable=False),
            PGCraftCheck("{price} > 0", name="positive_price"),
            PGCraftCheck("{qty} >= 0", name="nonneg_qty"),
            PGCraftCheck(
                "{price} * {qty} <= 1000000",
                name="max_total",
            ),
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


class TestEAVCheckInsert:
    def test_valid_insert_succeeds(self, eav_with_checks):
        conn, _ = eav_with_checks
        conn.execute(
            text("INSERT INTO api.products (price, qty) VALUES (10, 5)")
        )
        row = conn.execute(
            text("SELECT price, qty FROM api.products")
        ).fetchone()
        assert row is not None
        assert row[0] == 10
        assert row[1] == 5

    def test_negative_price_rejected(self, eav_with_checks):
        conn, _ = eav_with_checks
        with pytest.raises(Exception, match="positive_price"):
            conn.execute(
                text("INSERT INTO api.products (price, qty) VALUES (-1, 5)")
            )

    def test_zero_price_rejected(self, eav_with_checks):
        conn, _ = eav_with_checks
        with pytest.raises(Exception, match="positive_price"):
            conn.execute(
                text("INSERT INTO api.products (price, qty) VALUES (0, 5)")
            )

    def test_negative_qty_rejected(self, eav_with_checks):
        conn, _ = eav_with_checks
        with pytest.raises(Exception, match="nonneg_qty"):
            conn.execute(
                text("INSERT INTO api.products (price, qty) VALUES (10, -1)")
            )

    def test_cross_column_check_rejected(self, eav_with_checks):
        conn, _ = eav_with_checks
        with pytest.raises(Exception, match="max_total"):
            conn.execute(
                text(
                    "INSERT INTO api.products (price, qty) VALUES (1001, 1000)"
                )
            )

    def test_cross_column_check_at_boundary_succeeds(self, eav_with_checks):
        conn, _ = eav_with_checks
        conn.execute(
            text("INSERT INTO api.products (price, qty) VALUES (1000, 1000)")
        )
        row = conn.execute(
            text("SELECT price, qty FROM api.products")
        ).fetchone()
        assert row[0] == 1000
        assert row[1] == 1000


class TestEAVCheckUpdate:
    @pytest.fixture(autouse=True)
    def _seed(self, eav_with_checks):
        conn, schema = eav_with_checks
        conn.execute(
            text("INSERT INTO api.products (price, qty) VALUES (10, 5)")
        )
        self.conn = conn
        self.schema = schema

    def test_update_to_invalid_price_rejected(self):
        with pytest.raises(Exception, match="positive_price"):
            self.conn.execute(text("UPDATE api.products SET price = -1"))

    def test_update_to_valid_price_succeeds(self):
        self.conn.execute(text("UPDATE api.products SET price = 20"))
        row = self.conn.execute(
            text("SELECT price FROM api.products")
        ).fetchone()
        assert row[0] == 20

    def test_update_violating_cross_column_check_rejected(self):
        with pytest.raises(Exception, match="max_total"):
            self.conn.execute(
                text("UPDATE api.products SET price = 1001, qty = 1000")
            )
