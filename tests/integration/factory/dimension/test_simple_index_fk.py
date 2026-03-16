"""Integration tests for index and FK enforcement on simple dimensions.

Uses pgcraft factories with PGCraftIndex and PGCraftFK schema items
and verifies that unique indexes reject duplicates and foreign keys
enforce referential integrity.
"""

import pytest
from sqlalchemy import Column, Integer, MetaData, String, text

from pgcraft.config import PGCraftConfig
from pgcraft.extensions.postgrest import PostgRESTExtension, PostgRESTPlugin
from pgcraft.factory.dimension.simple import PGCraftSimple
from pgcraft.fk import PGCraftFK
from pgcraft.index import PGCraftIndex
from tests.integration.conftest import create_all_from_metadata


class TestUniqueIndex:
    @pytest.fixture(autouse=True)
    def _setup(self, db_conn, db_schema):
        config = PGCraftConfig(auto_discover=False)
        config.use(PostgRESTExtension())

        metadata = MetaData()
        PGCraftSimple(
            "products",
            db_schema,
            metadata,
            schema_items=[
                Column("code", String, nullable=False),
                PGCraftIndex("uq_code", "{code}", unique=True),
            ],
            config=config,
            extra_plugins=[
                PostgRESTPlugin(grants=["select", "insert"]),
            ],
        )
        create_all_from_metadata(db_conn, metadata)
        self.conn = db_conn
        self.schema = db_schema

    def test_unique_index_allows_distinct(self):
        self.conn.execute(text("INSERT INTO api.products (code) VALUES ('A')"))
        self.conn.execute(text("INSERT INTO api.products (code) VALUES ('B')"))
        count = self.conn.execute(
            text("SELECT COUNT(*) FROM api.products")
        ).scalar()
        assert count == 2

    def test_unique_index_rejects_duplicate(self):
        self.conn.execute(text("INSERT INTO api.products (code) VALUES ('A')"))
        with pytest.raises(Exception, match="uq_code"):
            self.conn.execute(
                text("INSERT INTO api.products (code) VALUES ('A')")
            )


class TestForeignKey:
    @pytest.fixture(autouse=True)
    def _setup(self, db_conn, db_schema):
        config = PGCraftConfig(auto_discover=False)
        config.use(PostgRESTExtension())

        metadata = MetaData()

        PGCraftSimple(
            "orgs",
            db_schema,
            metadata,
            schema_items=[
                Column("name", String, nullable=False),
            ],
            config=config,
            extra_plugins=[
                PostgRESTPlugin(grants=["select", "insert"]),
            ],
        )

        PGCraftSimple(
            "members",
            db_schema,
            metadata,
            schema_items=[
                Column("org_id", Integer, nullable=False),
                Column("name", String, nullable=False),
                PGCraftFK(
                    raw_references={
                        "{org_id}": f"{db_schema}.orgs.id",
                    },
                    name="fk_org",
                ),
            ],
            config=config,
            extra_plugins=[
                PostgRESTPlugin(grants=["select", "insert"]),
            ],
        )
        create_all_from_metadata(db_conn, metadata)

        db_conn.execute(text("INSERT INTO api.orgs (name) VALUES ('Acme')"))
        self.conn = db_conn
        self.schema = db_schema

    def test_fk_allows_valid_reference(self):
        self.conn.execute(
            text("INSERT INTO api.members (org_id, name) VALUES (1, 'Alice')")
        )
        count = self.conn.execute(
            text("SELECT COUNT(*) FROM api.members")
        ).scalar()
        assert count == 1

    def test_fk_rejects_invalid_reference(self):
        with pytest.raises(Exception, match="fk_org"):
            self.conn.execute(
                text(
                    "INSERT INTO api.members"
                    " (org_id, name) VALUES (9999, 'Ghost')"
                )
            )
