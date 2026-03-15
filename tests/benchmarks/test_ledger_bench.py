"""Benchmarks for ledger dimension operations."""

import pytest
from sqlalchemy import text

from tests.benchmarks.conftest import (
    SCHEMA,
    seed_ledger_rows,
    setup_ledger,
)


@pytest.fixture(scope="module")
def _ledger_tables(db_engine, bench_schema):  # noqa: ARG001
    with db_engine.connect() as conn:
        setup_ledger(
            conn,
            SCHEMA,
            "bm_ledger",
            extra_cols="category TEXT NOT NULL",
        )


@pytest.fixture(scope="module")
def _seeded_ledger(db_engine, _ledger_tables):
    with db_engine.connect() as conn:
        setup_ledger(
            conn,
            SCHEMA,
            "bm_ledger_big",
            extra_cols="category TEXT NOT NULL",
        )
        seed_ledger_rows(conn, SCHEMA, "bm_ledger_big", 10_000)
        conn.execute(
            text(
                f"CREATE OR REPLACE VIEW"
                f" {SCHEMA}.bm_ledger_big_balances AS"
                f" SELECT category, SUM(value) AS balance"
                f" FROM {SCHEMA}.bm_ledger_big"
                f" GROUP BY category"
            )
        )
        conn.commit()


# -- single-row operations ------------------------------------------------


@pytest.mark.usefixtures("_ledger_tables")
class TestLedgerSingleRow:
    def test_insert_single(self, benchmark, bench_conn):
        schema = SCHEMA
        counter = {"i": 0}

        def do_insert():
            counter["i"] += 1
            bench_conn.execute(
                text(
                    f"INSERT INTO {schema}.bm_ledger"
                    f" (value, category)"
                    f" VALUES ({counter['i']}, 'sales')"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_insert, rounds=10_000)


# -- batch operations ------------------------------------------------------


@pytest.mark.usefixtures("_ledger_tables")
class TestLedgerBatch:
    def test_insert_batch_100(self, benchmark, bench_conn):
        schema = SCHEMA
        counter = {"i": 0}

        def do_batch():
            counter["i"] += 1
            base = counter["i"] * 100
            values = ", ".join(
                f"({base + j}, 'cat_{j % 5}')" for j in range(100)
            )
            bench_conn.execute(
                text(
                    f"INSERT INTO {schema}.bm_ledger"
                    f" (value, category) VALUES {values}"
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
            values = ", ".join(
                f"({base + j}, 'cat_{j % 5}')" for j in range(1000)
            )
            bench_conn.execute(
                text(
                    f"INSERT INTO {schema}.bm_ledger"
                    f" (value, category) VALUES {values}"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_batch, rounds=100)


# -- SELECT operations ----------------------------------------------------


@pytest.mark.usefixtures("_seeded_ledger")
class TestLedgerSelect:
    def test_select_balance(self, benchmark, bench_conn):
        schema = SCHEMA

        def do_select():
            bench_conn.execute(
                text(f"SELECT * FROM {schema}.bm_ledger_big_balances")
            ).fetchall()

        benchmark.pedantic(do_select, rounds=1_000)
