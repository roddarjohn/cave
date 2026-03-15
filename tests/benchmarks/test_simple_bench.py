"""Benchmarks for simple dimension CRUD via API view triggers."""

import pytest
from sqlalchemy import text

from tests.benchmarks.conftest import (
    SCHEMA,
    seed_simple_rows,
    setup_simple_dimension,
)


@pytest.fixture(scope="module")
def _simple_tables(db_engine, bench_schema):  # noqa: ARG001
    with db_engine.connect() as conn:
        setup_simple_dimension(
            conn, SCHEMA, "bench_simple", "name TEXT NOT NULL"
        )


@pytest.fixture(scope="module")
def _seeded_tables(db_engine, _simple_tables):
    with db_engine.connect() as conn:
        setup_simple_dimension(
            conn,
            SCHEMA,
            "bench_simple_seeded",
            "name TEXT NOT NULL",
        )
        seed_simple_rows(conn, SCHEMA, "bench_simple_seeded", 10_000)


# -- single-row operations ------------------------------------------------


@pytest.mark.usefixtures("_simple_tables")
class TestSimpleSingleRow:
    def test_insert_single(self, benchmark, bench_conn):
        schema = SCHEMA
        counter = {"i": 0}

        def do_insert():
            counter["i"] += 1
            bench_conn.execute(
                text(
                    f"INSERT INTO {schema}.api_bench_simple"
                    f" (name) VALUES ('item_{counter['i']}')"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_insert, rounds=10_000)

    def test_update_single(self, benchmark, bench_conn):
        schema = SCHEMA
        bench_conn.execute(
            text(
                f"INSERT INTO {schema}.api_bench_simple"
                f" (name) VALUES ('to_update')"
            )
        )
        bench_conn.commit()
        row_id = bench_conn.execute(
            text(
                f"SELECT id FROM {schema}.bench_simple WHERE name = 'to_update'"
            )
        ).scalar()

        counter = {"i": 0}

        def do_update():
            counter["i"] += 1
            bench_conn.execute(
                text(
                    f"UPDATE {schema}.api_bench_simple"
                    f" SET name = 'upd_{counter['i']}'"
                    f" WHERE id = {row_id}"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_update, rounds=10_000)

    def test_delete_single(self, benchmark, bench_conn):
        schema = SCHEMA

        def do_delete():
            bench_conn.execute(
                text(
                    f"INSERT INTO {schema}.api_bench_simple"
                    f" (name) VALUES ('del_me')"
                )
            )
            bench_conn.commit()
            bench_conn.execute(
                text(
                    f"DELETE FROM {schema}.api_bench_simple"
                    f" WHERE name = 'del_me'"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_delete, rounds=10_000)


# -- batch operations ------------------------------------------------------


@pytest.mark.usefixtures("_simple_tables")
class TestSimpleBatch:
    def test_insert_batch_100(self, benchmark, bench_conn):
        schema = SCHEMA
        counter = {"i": 0}

        def do_batch():
            counter["i"] += 1
            base = counter["i"] * 100
            values = ", ".join(f"('b100_{base + j}')" for j in range(100))
            bench_conn.execute(
                text(
                    f"INSERT INTO {schema}.api_bench_simple"
                    f" (name) VALUES {values}"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_batch, rounds=100)

    def test_insert_batch_1000(self, benchmark, bench_conn):
        schema = SCHEMA
        counter = {"i": 0}

        def do_batch():
            counter["i"] += 1
            base = counter["i"] * 1000
            values = ", ".join(f"('b1k_{base + j}')" for j in range(1000))
            bench_conn.execute(
                text(
                    f"INSERT INTO {schema}.api_bench_simple"
                    f" (name) VALUES {values}"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_batch, rounds=100)


# -- SELECT operations ----------------------------------------------------


@pytest.mark.usefixtures("_seeded_tables")
class TestSimpleSelect:
    def test_select_all(self, benchmark, bench_conn):
        schema = SCHEMA

        def do_select():
            bench_conn.execute(
                text(f"SELECT * FROM {schema}.api_bench_simple_seeded")
            ).fetchall()

        benchmark.pedantic(do_select, rounds=1_000)

    def test_select_filtered(self, benchmark, bench_conn):
        schema = SCHEMA

        def do_select():
            bench_conn.execute(
                text(
                    f"SELECT * FROM"
                    f" {schema}.api_bench_simple_seeded"
                    f" WHERE name = 'item_500'"
                )
            ).fetchall()

        benchmark.pedantic(do_select, rounds=1_000)
