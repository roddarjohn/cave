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
    db_conn.execute(text("CREATE SCHEMA IF NOT EXISTS api"))
    return "cave_test"


def create_all_from_metadata(conn, metadata):
    """Create tables, views, functions, and triggers from *metadata*.

    Mirrors what Alembic does at migration time: tables via
    SQLAlchemy's ``create_all``, then views, functions, and triggers
    from the ``sqlalchemy_declarative_extensions`` registrations.

    Does **not** commit — the caller controls transaction boundaries.
    Integration tests rely on a transactional rollback for isolation,
    so committing here would break that pattern.
    """
    metadata.create_all(conn)

    funcs_obj = metadata.info.get("functions")
    if funcs_obj:
        for fn in funcs_obj.functions:
            for stmt in fn.to_sql_create(replace=True):
                conn.execute(text(stmt))

    views_obj = metadata.info.get("views")
    if views_obj:
        for view in views_obj:
            schema = view.schema or "public"
            defn = view.compile_definition()
            fqn = f"{schema}.{view.name}"
            conn.execute(text(f"CREATE OR REPLACE VIEW {fqn} AS {defn}"))

    triggers_obj = metadata.info.get("triggers")
    if triggers_obj:
        for trg in triggers_obj.triggers:
            conn.execute(text(trg.to_sql_create()))
