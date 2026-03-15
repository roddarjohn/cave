"""Integration tests for index and FK enforcement on simple dimensions.

Creates real database objects and verifies that unique indexes
reject duplicates and foreign keys enforce referential integrity.
"""

import pytest
from sqlalchemy import text


@pytest.fixture
def ref_table(db_conn, db_schema):
    """Create a reference table for FK tests."""
    db_conn.execute(
        text(
            f"CREATE TABLE {db_schema}.orgs "
            f"(id SERIAL PRIMARY KEY, name TEXT NOT NULL)"
        )
    )
    db_conn.execute(
        text(f"INSERT INTO {db_schema}.orgs (name) VALUES ('Acme')")
    )
    return db_conn, db_schema


class TestUniqueIndex:
    @pytest.fixture(autouse=True)
    def _setup(self, db_conn, db_schema):
        db_conn.execute(
            text(
                f"CREATE TABLE {db_schema}.products "
                f"(id SERIAL PRIMARY KEY, "
                f"code TEXT NOT NULL)"
            )
        )
        db_conn.execute(
            text(f"CREATE UNIQUE INDEX uq_code ON {db_schema}.products (code)")
        )
        self.conn = db_conn
        self.schema = db_schema

    def test_unique_index_allows_distinct(self):
        self.conn.execute(
            text(f"INSERT INTO {self.schema}.products (code) VALUES ('A')")
        )
        self.conn.execute(
            text(f"INSERT INTO {self.schema}.products (code) VALUES ('B')")
        )
        count = self.conn.execute(
            text(f"SELECT COUNT(*) FROM {self.schema}.products")
        ).scalar()
        assert count == 2

    def test_unique_index_rejects_duplicate(self):
        self.conn.execute(
            text(f"INSERT INTO {self.schema}.products (code) VALUES ('A')")
        )
        with pytest.raises(Exception, match="uq_code"):
            self.conn.execute(
                text(f"INSERT INTO {self.schema}.products (code) VALUES ('A')")
            )


class TestForeignKey:
    @pytest.fixture(autouse=True)
    def _setup(self, ref_table):
        conn, schema = ref_table
        conn.execute(
            text(
                f"CREATE TABLE {schema}.members "
                f"(id SERIAL PRIMARY KEY, "
                f"org_id INTEGER NOT NULL, "
                f"name TEXT NOT NULL)"
            )
        )
        conn.execute(
            text(
                f"ALTER TABLE {schema}.members "
                f"ADD CONSTRAINT fk_org "
                f"FOREIGN KEY (org_id) "
                f"REFERENCES {schema}.orgs (id)"
            )
        )
        self.conn = conn
        self.schema = schema

    def test_fk_allows_valid_reference(self):
        self.conn.execute(
            text(
                f"INSERT INTO {self.schema}.members "
                f"(org_id, name) VALUES (1, 'Alice')"
            )
        )
        count = self.conn.execute(
            text(f"SELECT COUNT(*) FROM {self.schema}.members")
        ).scalar()
        assert count == 1

    def test_fk_rejects_invalid_reference(self):
        with pytest.raises(Exception, match="fk_org"):
            self.conn.execute(
                text(
                    f"INSERT INTO "
                    f"{self.schema}.members "
                    f"(org_id, name) "
                    f"VALUES (9999, 'Ghost')"
                )
            )
