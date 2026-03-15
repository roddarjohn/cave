"""Benchmarks for append-only dimension via API view triggers."""

import pytest
from sqlalchemy import text

from tests.benchmarks.conftest import (
    SCHEMA,
    seed_append_only_rows,
    setup_append_only_dimension,
)


@pytest.fixture(scope="module")
def _ao_tables(db_engine, bench_schema):  # noqa: ARG001
    with db_engine.connect() as conn:
        setup_append_only_dimension(
            conn, SCHEMA, "bench_ao", "name TEXT NOT NULL"
        )


@pytest.fixture(scope="module")
def _seeded_ao(db_engine, _ao_tables):
    with db_engine.connect() as conn:
        setup_append_only_dimension(
            conn,
            SCHEMA,
            "bench_ao_seeded",
            "name TEXT NOT NULL",
        )
        seed_append_only_rows(conn, SCHEMA, "bench_ao_seeded", 10_000)


# -- single-row operations ------------------------------------------------


@pytest.mark.usefixtures("_ao_tables")
class TestAppendOnlySingleRow:
    def test_insert_single(self, benchmark, bench_conn):
        schema = SCHEMA
        counter = {"i": 0}

        def do_insert():
            counter["i"] += 1
            bench_conn.execute(
                text(
                    f"INSERT INTO {schema}.api_bench_ao"
                    f" (name) VALUES ('item_{counter['i']}')"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_insert, rounds=10_000)

    def test_update_single(self, benchmark, bench_conn):
        schema = SCHEMA
        bench_conn.execute(
            text(
                f"INSERT INTO {schema}.api_bench_ao (name) VALUES ('to_update')"
            )
        )
        bench_conn.commit()
        row_id = bench_conn.execute(
            text(
                f"SELECT id FROM {schema}.api_bench_ao"
                f" WHERE name = 'to_update' LIMIT 1"
            )
        ).scalar()

        counter = {"i": 0}

        def do_update():
            counter["i"] += 1
            bench_conn.execute(
                text(
                    f"UPDATE {schema}.api_bench_ao"
                    f" SET name = 'upd_{counter['i']}'"
                    f" WHERE id = {row_id}"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_update, rounds=10_000)


# -- batch operations ------------------------------------------------------


@pytest.mark.usefixtures("_ao_tables")
class TestAppendOnlyBatch:
    def test_insert_batch_100(self, benchmark, bench_conn):
        schema = SCHEMA
        counter = {"i": 0}

        def do_batch():
            counter["i"] += 1
            base = counter["i"] * 100
            values = ", ".join(f"('b100_{base + j}')" for j in range(100))
            bench_conn.execute(
                text(
                    f"INSERT INTO {schema}.api_bench_ao (name) VALUES {values}"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_batch, rounds=100)


# -- SELECT operations ----------------------------------------------------


@pytest.mark.usefixtures("_seeded_ao")
class TestAppendOnlySelect:
    def test_select_latest(self, benchmark, bench_conn):
        schema = SCHEMA

        def do_select():
            bench_conn.execute(
                text(f"SELECT * FROM {schema}.api_bench_ao_seeded")
            ).fetchall()

        benchmark.pedantic(do_select, rounds=1_000)

    def test_select_after_many_updates(self, db_engine, benchmark, bench_conn):
        schema = SCHEMA
        # Create a single entity and update it 100 times
        with db_engine.connect() as setup_conn:
            setup_append_only_dimension(
                setup_conn,
                SCHEMA,
                "bench_ao_revisions",
                "name TEXT NOT NULL",
            )
            setup_conn.execute(
                text(
                    f"INSERT INTO"
                    f" {schema}.api_bench_ao_revisions"
                    f" (name) VALUES ('rev_entity')"
                )
            )
            setup_conn.commit()
            row_id = setup_conn.execute(
                text(f"SELECT id FROM {schema}.api_bench_ao_revisions LIMIT 1")
            ).scalar()
            for i in range(100):
                setup_conn.execute(
                    text(
                        f"UPDATE"
                        f" {schema}.api_bench_ao_revisions"
                        f" SET name = 'rev_{i}'"
                        f" WHERE id = {row_id}"
                    )
                )
            setup_conn.commit()

        def do_select():
            bench_conn.execute(
                text(f"SELECT * FROM {schema}.api_bench_ao_revisions")
            ).fetchall()

        benchmark.pedantic(do_select, rounds=1_000)
