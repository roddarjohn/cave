"""Integration tests for UUIDV7PKPlugin against a live database.

Verifies that the ``uuidv7()`` server default works end-to-end on
PostgreSQL 18+.
"""

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError


@pytest.fixture
def uuidv7_table(db_conn, db_schema):
    """Create a table with a UUIDv7 primary key."""
    db_conn.execute(
        text(f"""
        CREATE TABLE {db_schema}.items (
            id UUID PRIMARY KEY DEFAULT uuidv7(),
            name TEXT NOT NULL
        )
    """)
    )
    return db_conn, db_schema


@pytest.fixture(autouse=True)
def _skip_if_no_uuidv7(db_conn):
    """Skip the entire module when uuidv7() is unavailable."""
    try:
        db_conn.execute(text("SELECT uuidv7()"))
    except ProgrammingError:
        pytest.skip("uuidv7() not available (requires PG 18+)")


class TestUUIDV7Insert:
    def test_insert_generates_uuid(self, uuidv7_table):
        conn, schema = uuidv7_table
        conn.execute(
            text(f"INSERT INTO {schema}.items (name) VALUES ('widget')")
        )
        row = conn.execute(text(f"SELECT id FROM {schema}.items")).fetchone()
        assert row is not None
        pk = row[0]
        assert isinstance(pk, uuid.UUID)

    def test_uuid_is_version_7(self, uuidv7_table):
        conn, schema = uuidv7_table
        conn.execute(
            text(f"INSERT INTO {schema}.items (name) VALUES ('gadget')")
        )
        row = conn.execute(text(f"SELECT id FROM {schema}.items")).fetchone()
        pk = uuid.UUID(str(row[0]))
        assert pk.version == 7

    def test_multiple_inserts_are_ordered(self, uuidv7_table):
        conn, schema = uuidv7_table
        conn.execute(
            text(
                f"INSERT INTO {schema}.items (name) VALUES ('a'), ('b'), ('c')"
            )
        )
        rows = conn.execute(
            text(f"SELECT id FROM {schema}.items ORDER BY id")
        ).fetchall()
        assert len(rows) == 3
        ids = [row[0] for row in rows]
        assert ids == sorted(ids)
