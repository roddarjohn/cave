"""Integration tests for PGCraftLedger.

Creates real database objects via pgcraft factories and verifies
insert-only operations through the API view and its INSTEAD OF
trigger.
"""

import pytest
from sqlalchemy import Column, MetaData, String, text
from sqlalchemy.exc import ProgrammingError

from pgcraft.config import PGCraftConfig
from pgcraft.extensions.postgrest import PostgRESTExtension, PostgRESTView
from pgcraft.factory.ledger import PGCraftLedger
from pgcraft.plugins.ledger import DoubleEntryPlugin, DoubleEntryTriggerPlugin
from tests.integration.conftest import create_all_from_metadata


def _make_ledger(  # noqa: PLR0913
    schema,
    metadata,
    config,
    tablename="transactions",
    extra_items=None,
    extra_plugins=None,
):
    factory = PGCraftLedger(
        tablename,
        schema,
        metadata,
        schema_items=list(extra_items or []),
        config=config,
        extra_plugins=extra_plugins,
    )
    PostgRESTView(
        source=factory,
        grants=["select", "insert", "update", "delete"],
    )
    return factory


@pytest.fixture
def ledger(db_conn, db_schema):
    """Set up an integer ledger in the test schema."""
    config = PGCraftConfig(auto_discover=False)
    config.use(PostgRESTExtension())
    metadata = MetaData()
    _make_ledger(
        db_schema,
        metadata,
        config,
        extra_items=[Column("category", String, nullable=False)],
    )
    create_all_from_metadata(db_conn, metadata)
    return db_conn, db_schema


@pytest.fixture
def numeric_ledger(db_conn, db_schema):
    """Set up a numeric ledger in the test schema."""
    config = PGCraftConfig(auto_discover=False)
    config.use(PostgRESTExtension())
    metadata = MetaData()
    _make_ledger(db_schema, metadata, config, tablename="payments")
    create_all_from_metadata(db_conn, metadata)
    return db_conn, db_schema


class TestLedgerInsert:
    def test_insert_via_api_view_succeeds(self, ledger):
        conn, _ = ledger
        conn.execute(
            text(
                "INSERT INTO api.transactions (value, category)"
                " VALUES (100, 'sales')"
            )
        )

    def test_value_and_created_at_populated(self, ledger):
        conn, _ = ledger
        conn.execute(
            text(
                "INSERT INTO api.transactions (value, category)"
                " VALUES (42, 'refund')"
            )
        )
        row = conn.execute(
            text(
                "SELECT value, created_at, category, entry_id"
                " FROM api.transactions"
            )
        ).fetchone()
        assert row is not None
        assert row[0] == 42
        assert row[1] is not None
        assert row[2] == "refund"
        assert row[3] is not None

    def test_explicit_entry_id_correlates_rows(self, ledger):
        conn, _ = ledger
        eid = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        conn.execute(
            text(
                "INSERT INTO api.transactions (entry_id, value, category)"
                f" VALUES ('{eid}', 10, 'a'), ('{eid}', 20, 'b')"
            )
        )
        rows = conn.execute(
            text("SELECT entry_id FROM api.transactions ORDER BY value")
        ).fetchall()
        assert len(rows) == 2
        assert str(rows[0][0]) == eid
        assert str(rows[1][0]) == eid

    def test_multiple_inserts(self, ledger):
        conn, _ = ledger
        conn.execute(
            text(
                "INSERT INTO api.transactions (value, category)"
                " VALUES (10, 'a'), (20, 'b'), (30, 'c')"
            )
        )
        count = conn.execute(
            text("SELECT COUNT(*) FROM api.transactions")
        ).scalar()
        assert count == 3


class TestLedgerImmutability:
    @pytest.fixture(autouse=True)
    def _seed(self, ledger):
        conn, schema = ledger
        conn.execute(
            text(
                "INSERT INTO api.transactions (value, category)"
                " VALUES (100, 'sales')"
            )
        )
        self.conn = conn
        self.schema = schema

    def test_update_raises(self):
        with pytest.raises(ProgrammingError, match="cannot update"):
            self.conn.execute(text("UPDATE api.transactions SET value = 200"))

    def test_delete_raises(self):
        with pytest.raises(ProgrammingError, match="cannot delete"):
            self.conn.execute(text("DELETE FROM api.transactions"))


class TestLedgerNumeric:
    def test_numeric_value_with_decimals(self, numeric_ledger):
        conn, _ = numeric_ledger
        conn.execute(text("INSERT INTO api.payments (value) VALUES (99.95)"))
        row = conn.execute(text("SELECT value FROM api.payments")).fetchone()
        assert row is not None
        assert float(row[0]) == pytest.approx(99.95)


# -- Balance view tests ------------------------------------------------


@pytest.fixture
def balance_ledger(db_conn, db_schema):
    """Set up a ledger with a balance view grouped by category."""
    config = PGCraftConfig(auto_discover=False)
    config.use(PostgRESTExtension())
    metadata = MetaData()
    factory = PGCraftLedger(
        "bal_ledger",
        db_schema,
        metadata,
        schema_items=[Column("category", String, nullable=False)],
        config=config,
    )
    PostgRESTView(
        source=factory,
        grants=["select", "insert"],
    )
    create_all_from_metadata(db_conn, metadata)
    db_conn.execute(
        text(
            f"CREATE VIEW {db_schema}.bal_ledger_balances"
            f" AS SELECT category, SUM(value) AS balance"
            f" FROM {db_schema}.bal_ledger"
            f" GROUP BY category"
        )
    )
    return db_conn, db_schema


class TestLedgerBalanceView:
    def test_balance_sums_correctly(self, balance_ledger):
        conn, schema = balance_ledger
        conn.execute(
            text(
                "INSERT INTO api.bal_ledger (value, category)"
                " VALUES (10, 'sales'), (20, 'sales'), (5, 'refund')"
            )
        )
        rows = conn.execute(
            text(
                f"SELECT category, balance FROM {schema}.bal_ledger_balances"
                f" ORDER BY category"
            )
        ).fetchall()
        balances = {r[0]: r[1] for r in rows}
        assert balances["sales"] == 30
        assert balances["refund"] == 5

    def test_empty_ledger_returns_no_rows(self, balance_ledger):
        conn, schema = balance_ledger
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {schema}.bal_ledger_balances")
        ).scalar()
        assert count == 0


# -- Double-entry tests -------------------------------------------------


@pytest.fixture
def double_entry_ledger(db_conn, db_schema):
    """Set up a double-entry ledger in the test schema."""
    config = PGCraftConfig(auto_discover=False)
    config.use(PostgRESTExtension())
    metadata = MetaData()
    factory = PGCraftLedger(
        "journal",
        db_schema,
        metadata,
        schema_items=[Column("account", String, nullable=False)],
        config=config,
        extra_plugins=[DoubleEntryPlugin(), DoubleEntryTriggerPlugin()],
    )
    PostgRESTView(
        source=factory,
        grants=["select", "insert"],
    )
    create_all_from_metadata(db_conn, metadata)
    return db_conn, db_schema


class TestDoubleEntryLedger:
    def test_balanced_entry_succeeds(self, double_entry_ledger):
        conn, schema = double_entry_ledger
        eid = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        conn.execute(
            text(
                "INSERT INTO api.journal"
                " (entry_id, value, direction, account)"
                f" VALUES ('{eid}', 100, 'debit', 'cash'),"
                f" ('{eid}', 100, 'credit', 'revenue')"
            )
        )
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {schema}.journal")
        ).scalar()
        assert count == 2

    def test_unbalanced_entry_raises(self, double_entry_ledger):
        conn, _ = double_entry_ledger
        eid = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
        with pytest.raises(ProgrammingError, match="double-entry violation"):
            conn.execute(
                text(
                    "INSERT INTO api.journal"
                    " (entry_id, value, direction, account)"
                    f" VALUES ('{eid}', 100, 'debit', 'cash'),"
                    f" ('{eid}', 50, 'credit', 'revenue')"
                )
            )

    def test_single_debit_without_credit_raises(self, double_entry_ledger):
        conn, _ = double_entry_ledger
        eid = "cccccccc-dddd-4eee-8fff-aaaaaaaaaaaa"
        with pytest.raises(ProgrammingError, match="double-entry violation"):
            conn.execute(
                text(
                    "INSERT INTO api.journal"
                    " (entry_id, value, direction, account)"
                    f" VALUES ('{eid}', 100, 'debit', 'cash')"
                )
            )
