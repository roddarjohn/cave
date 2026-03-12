import os

import pytest
from sqlalchemy import create_engine, text


@pytest.fixture(scope="session")
def db_engine():
    """Engine for raw integration tests (no Alembic).

    Skips the test session if DATABASE_URL is not set.
    """
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not set")
    return create_engine(database_url)


@pytest.fixture
def db_conn(db_engine):
    """Transactional connection that rolls back after each test.

    Each test gets a clean slate: all DDL and DML executed inside
    the test is rolled back automatically, so tests are isolated
    without needing teardown logic.
    """
    with db_engine.connect() as conn, conn.begin():
        yield conn
        conn.rollback()


@pytest.fixture
def db_schema(db_conn):
    """Temporary schema for tests that need a dedicated namespace.

    Creates a schema named ``cave_test`` inside the transactional
    connection provided by ``db_conn``.  No explicit teardown is
    needed: PostgreSQL DDL is transactional, so the ``ROLLBACK`` in
    ``db_conn`` undoes the ``CREATE SCHEMA`` automatically.
    """
    db_conn.execute(text("CREATE SCHEMA IF NOT EXISTS cave_test"))
    return "cave_test"
