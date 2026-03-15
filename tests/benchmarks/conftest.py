"""Shared fixtures for performance benchmarks.

All benchmarks run against a real PostgreSQL instance.  DDL is
committed in a dedicated ``bench_test`` schema so benchmarks
measure steady-state performance, not transactional overhead.
The schema is dropped at session teardown.
"""

import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from pgcraft.utils.template import load_template

SCHEMA = "bench_test"
_TEMPLATES = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "pgcraft"
    / "plugins"
    / "templates"
)


# -- engine / connection ---------------------------------------------------


@pytest.fixture(scope="session")
def db_engine():
    """Session-scoped engine; skips if DATABASE_URL is unset."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set")
    return create_engine(url)


@pytest.fixture(scope="session")
def bench_schema(db_engine):
    """Create a dedicated schema for benchmarks; drop on teardown."""
    with db_engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {SCHEMA}"))
        conn.commit()
    yield SCHEMA
    with db_engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        conn.commit()


@pytest.fixture
def bench_conn(db_engine, bench_schema):  # noqa: ARG001
    """Per-test connection that depends on bench_schema for setup."""
    with db_engine.connect() as conn:
        yield conn


# -- simple dimension helpers ----------------------------------------------


def setup_simple_dimension(conn, schema, tablename, col_defs):
    """Create a simple dimension table and a read-only view."""
    base = f"{schema}.{tablename}"
    view = f"{schema}.{tablename}_view"

    conn.execute(
        text(
            f"CREATE TABLE IF NOT EXISTS {base}"
            f" (id SERIAL PRIMARY KEY, {col_defs})"
        )
    )
    conn.execute(
        text(f"CREATE OR REPLACE VIEW {view} AS SELECT id, name FROM {base}")
    )
    conn.commit()


# -- append-only dimension helpers -----------------------------------------


def setup_append_only_dimension(conn, schema, tablename, attr_cols):
    """Create an append-only dimension (root + attributes + view)."""
    root = f"{schema}.{tablename}_root"
    attr = f"{schema}.{tablename}_attributes"
    view = f"{schema}.{tablename}_view"

    conn.execute(
        text(
            f"CREATE TABLE IF NOT EXISTS {root}"
            f" (id SERIAL PRIMARY KEY,"
            f" latest_attr_id INTEGER,"
            f" created_at TIMESTAMPTZ DEFAULT now())"
        )
    )
    conn.execute(
        text(
            f"CREATE TABLE IF NOT EXISTS {attr}"
            f" (id SERIAL PRIMARY KEY,"
            f" {attr_cols},"
            f" created_at TIMESTAMPTZ DEFAULT now())"
        )
    )
    conn.execute(
        text(
            f"ALTER TABLE {root}"
            f" DROP CONSTRAINT IF EXISTS"
            f" {tablename}_root_latest_fk"
        )
    )
    conn.execute(
        text(
            f"ALTER TABLE {root}"
            f" ADD CONSTRAINT {tablename}_root_latest_fk"
            f" FOREIGN KEY (latest_attr_id)"
            f" REFERENCES {attr}(id)"
        )
    )

    conn.execute(
        text(
            f"CREATE OR REPLACE VIEW {view} AS"
            f" SELECT r.id, a.name, a.created_at"
            f" FROM {root} r"
            f" JOIN {attr} a ON a.id = r.latest_attr_id"
        )
    )

    tpl_dir = _TEMPLATES / "append_only"

    # INSERT trigger
    tpl = load_template(tpl_dir / "insert.plpgsql.mako")
    body = tpl.render(
        root_table=root,
        attr_table=attr,
        attr_cols="name",
        new_cols="NEW.name",
        attr_fk_col="latest_attr_id",
    )
    conn.execute(
        text(
            f"CREATE OR REPLACE FUNCTION"
            f" {schema}.{tablename}_insert()"
            f" RETURNS trigger LANGUAGE plpgsql"
            f" AS $$ {body} $$"
        )
    )
    conn.execute(text(f"DROP TRIGGER IF EXISTS {tablename}_insert ON {view}"))
    conn.execute(
        text(
            f"CREATE TRIGGER {tablename}_insert"
            f" INSTEAD OF INSERT ON {view}"
            f" FOR EACH ROW"
            f" EXECUTE FUNCTION"
            f" {schema}.{tablename}_insert()"
        )
    )

    # UPDATE trigger
    tpl = load_template(tpl_dir / "update.plpgsql.mako")
    body = tpl.render(
        root_table=root,
        attr_table=attr,
        attr_cols="name",
        new_cols="NEW.name",
        attr_fk_col="latest_attr_id",
    )
    conn.execute(
        text(
            f"CREATE OR REPLACE FUNCTION"
            f" {schema}.{tablename}_update()"
            f" RETURNS trigger LANGUAGE plpgsql"
            f" AS $$ {body} $$"
        )
    )
    conn.execute(text(f"DROP TRIGGER IF EXISTS {tablename}_update ON {view}"))
    conn.execute(
        text(
            f"CREATE TRIGGER {tablename}_update"
            f" INSTEAD OF UPDATE ON {view}"
            f" FOR EACH ROW"
            f" EXECUTE FUNCTION"
            f" {schema}.{tablename}_update()"
        )
    )

    # DELETE trigger
    tpl = load_template(tpl_dir / "delete.plpgsql.mako")
    body = tpl.render(root_table=root)
    conn.execute(
        text(
            f"CREATE OR REPLACE FUNCTION"
            f" {schema}.{tablename}_delete()"
            f" RETURNS trigger LANGUAGE plpgsql"
            f" AS $$ {body} $$"
        )
    )
    conn.execute(text(f"DROP TRIGGER IF EXISTS {tablename}_delete ON {view}"))
    conn.execute(
        text(
            f"CREATE TRIGGER {tablename}_delete"
            f" INSTEAD OF DELETE ON {view}"
            f" FOR EACH ROW"
            f" EXECUTE FUNCTION"
            f" {schema}.{tablename}_delete()"
        )
    )

    conn.commit()


# -- EAV dimension helpers -------------------------------------------------


def setup_eav_dimension(conn, schema, tablename, mappings):
    """Create an EAV dimension (entity + attribute + pivot view).

    *mappings* is a list of ``(attr_name, value_column)`` tuples.
    All attributes use ``text_value`` in this benchmark harness.
    """
    entity = f"{schema}.{tablename}_entity"
    attr = f"{schema}.{tablename}_attribute"
    view = f"{schema}.{tablename}_view"

    conn.execute(
        text(
            f"CREATE TABLE IF NOT EXISTS {entity}"
            f" (id SERIAL PRIMARY KEY,"
            f" created_at TIMESTAMPTZ DEFAULT now())"
        )
    )
    conn.execute(
        text(
            f"CREATE TABLE IF NOT EXISTS {attr}"
            f" (id SERIAL PRIMARY KEY,"
            f" entity_id INTEGER NOT NULL"
            f" REFERENCES {entity}(id) ON DELETE CASCADE,"
            f" attribute_name TEXT NOT NULL,"
            f" text_value TEXT,"
            f" created_at TIMESTAMPTZ DEFAULT now())"
        )
    )

    pivot_cols = ",\n".join(
        f"MAX(a.text_value)"
        f" FILTER (WHERE a.attribute_name = '{name}')"
        f" AS {name}"
        for name, _ in mappings
    )
    conn.execute(
        text(
            f"CREATE OR REPLACE VIEW {view} AS"
            f" SELECT e.id, e.created_at, {pivot_cols}"
            f" FROM {entity} e"
            f" LEFT JOIN ("
            f"   SELECT DISTINCT ON"
            f"     (entity_id, attribute_name) *"
            f"   FROM {attr}"
            f"   ORDER BY entity_id, attribute_name,"
            f"     created_at DESC, id DESC"
            f" ) a ON a.entity_id = e.id"
            f" GROUP BY e.id, e.created_at"
        )
    )

    tpl_dir = _TEMPLATES / "eav"

    # INSERT trigger
    tpl = load_template(tpl_dir / "insert.plpgsql.mako")
    body = tpl.render(
        entity_table=entity,
        attr_table=attr,
        mappings=[(name, col, False) for name, col in mappings],
    )
    conn.execute(
        text(
            f"CREATE OR REPLACE FUNCTION"
            f" {schema}.{tablename}_insert()"
            f" RETURNS trigger LANGUAGE plpgsql"
            f" AS $$ {body} $$"
        )
    )
    conn.execute(text(f"DROP TRIGGER IF EXISTS {tablename}_insert ON {view}"))
    conn.execute(
        text(
            f"CREATE TRIGGER {tablename}_insert"
            f" INSTEAD OF INSERT ON {view}"
            f" FOR EACH ROW"
            f" EXECUTE FUNCTION"
            f" {schema}.{tablename}_insert()"
        )
    )

    # UPDATE trigger
    tpl = load_template(tpl_dir / "update.plpgsql.mako")
    body = tpl.render(
        attr_table=attr,
        mappings=[(name, col, False) for name, col in mappings],
    )
    conn.execute(
        text(
            f"CREATE OR REPLACE FUNCTION"
            f" {schema}.{tablename}_update()"
            f" RETURNS trigger LANGUAGE plpgsql"
            f" AS $$ {body} $$"
        )
    )
    conn.execute(text(f"DROP TRIGGER IF EXISTS {tablename}_update ON {view}"))
    conn.execute(
        text(
            f"CREATE TRIGGER {tablename}_update"
            f" INSTEAD OF UPDATE ON {view}"
            f" FOR EACH ROW"
            f" EXECUTE FUNCTION"
            f" {schema}.{tablename}_update()"
        )
    )

    conn.commit()


# -- ledger helpers --------------------------------------------------------


def setup_ledger(conn, schema, tablename, extra_cols=""):
    """Create a ledger table and a read-only view."""
    base = f"{schema}.{tablename}"
    view = f"{schema}.{tablename}_view"

    extra = f", {extra_cols}" if extra_cols else ""
    conn.execute(
        text(
            f"CREATE TABLE IF NOT EXISTS {base}"
            f" (id SERIAL PRIMARY KEY,"
            f" entry_id UUID NOT NULL DEFAULT gen_random_uuid(),"
            f" created_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
            f" value INTEGER NOT NULL"
            f" {extra})"
        )
    )

    cols_select = "id, entry_id, created_at, value"
    if extra_cols:
        col_names = [c.strip().split()[0] for c in extra_cols.split(",")]
        cols_select += ", " + ", ".join(col_names)

    conn.execute(
        text(
            f"CREATE OR REPLACE VIEW {view} AS SELECT {cols_select} FROM {base}"
        )
    )
    conn.commit()


# -- seed helpers ----------------------------------------------------------


def seed_simple_rows(conn, schema, tablename, n):
    """Insert *n* rows directly into a simple dimension table."""
    table = f"{schema}.{tablename}"
    values = ", ".join(f"('item_{i}')" for i in range(n))
    conn.execute(text(f"INSERT INTO {table} (name) VALUES {values}"))
    conn.commit()


def seed_eav_rows(conn, schema, tablename, mappings, n):
    """Insert *n* entities into an EAV dimension's view."""
    view = f"{schema}.{tablename}_view"
    cols = ", ".join(name for name, _ in mappings)
    values = ", ".join(
        "(" + ", ".join(f"'val_{i}_{name}'" for name, _ in mappings) + ")"
        for i in range(n)
    )
    conn.execute(text(f"INSERT INTO {view} ({cols}) VALUES {values}"))
    conn.commit()


def seed_ledger_rows(conn, schema, tablename, n):
    """Insert *n* entries directly into a ledger table."""
    table = f"{schema}.{tablename}"
    values = ", ".join(f"({i}, 'cat_{i % 5}')" for i in range(1, n + 1))
    conn.execute(text(f"INSERT INTO {table} (value, category) VALUES {values}"))
    conn.commit()


def seed_append_only_rows(conn, schema, tablename, n):
    """Insert *n* rows into an append-only dimension's view."""
    view = f"{schema}.{tablename}_view"
    values = ", ".join(f"('item_{i}')" for i in range(n))
    conn.execute(text(f"INSERT INTO {view} (name) VALUES {values}"))
    conn.commit()
