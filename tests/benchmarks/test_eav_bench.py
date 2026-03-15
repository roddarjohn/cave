"""Benchmarks for EAV dimension operations."""

import pytest
from sqlalchemy import text

from tests.benchmarks.conftest import (
    SCHEMA,
    seed_eav_rows,
    setup_eav_dimension,
)

EAV_MAPPINGS = [
    ("sku", "text_value"),
    ("color", "text_value"),
]


@pytest.fixture(scope="module")
def _eav_tables(db_engine, bench_schema):  # noqa: ARG001
    with db_engine.connect() as conn:
        setup_eav_dimension(conn, SCHEMA, "bm_eav", EAV_MAPPINGS)


@pytest.fixture(scope="module")
def _seeded_eav(db_engine, _eav_tables):
    with db_engine.connect() as conn:
        setup_eav_dimension(conn, SCHEMA, "bm_eav_big", EAV_MAPPINGS)
        seed_eav_rows(
            conn,
            SCHEMA,
            "bm_eav_big",
            EAV_MAPPINGS,
            10_000,
        )


# -- single-row operations ------------------------------------------------


@pytest.mark.usefixtures("_eav_tables")
class TestEAVSingleRow:
    def test_insert_single(self, benchmark, bench_conn):
        schema = SCHEMA
        counter = {"i": 0}

        def do_insert():
            counter["i"] += 1
            bench_conn.execute(
                text(
                    f"INSERT INTO {schema}.bm_eav_view"
                    f" (sku, color)"
                    f" VALUES"
                    f" ('SKU-{counter['i']}',"
                    f" 'red')"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_insert, rounds=10_000)

    def test_update_single_attribute(self, benchmark, bench_conn):
        schema = SCHEMA
        bench_conn.execute(
            text(
                f"INSERT INTO {schema}.bm_eav_view"
                f" (sku, color)"
                f" VALUES ('UPD-SKU', 'blue')"
            )
        )
        bench_conn.commit()
        row_id = bench_conn.execute(
            text(
                f"SELECT id FROM {schema}.bm_eav_view"
                f" WHERE sku = 'UPD-SKU' LIMIT 1"
            )
        ).scalar()

        counter = {"i": 0}

        def do_update():
            counter["i"] += 1
            bench_conn.execute(
                text(
                    f"UPDATE {schema}.bm_eav_view"
                    f" SET color = 'color_{counter['i']}'"
                    f" WHERE id = {row_id}"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_update, rounds=10_000)


# -- batch operations ------------------------------------------------------


@pytest.mark.usefixtures("_eav_tables")
class TestEAVBatch:
    def test_insert_batch_100(self, benchmark, bench_conn):
        schema = SCHEMA
        counter = {"i": 0}

        def do_batch():
            counter["i"] += 1
            base = counter["i"] * 100
            values = ", ".join(
                f"('SKU-B-{base + j}', 'clr_{base + j}')" for j in range(100)
            )
            bench_conn.execute(
                text(
                    f"INSERT INTO {schema}.bm_eav_view"
                    f" (sku, color) VALUES {values}"
                )
            )
            bench_conn.commit()

        benchmark.pedantic(do_batch, rounds=100)


# -- SELECT operations ----------------------------------------------------


@pytest.mark.usefixtures("_seeded_eav")
class TestEAVSelect:
    def test_select_pivot(self, benchmark, bench_conn):
        schema = SCHEMA

        def do_select():
            bench_conn.execute(
                text(f"SELECT * FROM {schema}.bm_eav_big_view")
            ).fetchall()

        benchmark.pedantic(do_select, rounds=1_000)

    def test_select_pivot_filtered(self, benchmark, bench_conn):
        schema = SCHEMA

        def do_select():
            bench_conn.execute(
                text(
                    f"SELECT * FROM {schema}.bm_eav_big_view"
                    f" WHERE sku = 'val_500_sku'"
                )
            ).fetchall()

        benchmark.pedantic(do_select, rounds=1_000)
