from sqlalchemy import (
    Boolean,
    Column,
    Computed,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import ARRAY

from pgcraft.check import PGCraftCheck
from pgcraft.declarative import register
from pgcraft.factory.dimension import (
    AppendOnlyDimensionResourceFactory,
    EAVDimensionResourceFactory,
    SimpleDimensionResourceFactory,
)
from pgcraft.factory.ledger import LedgerResourceFactory
from pgcraft.ledger.events import LedgerEvent, ledger_balances
from pgcraft.plugins.api import APIPlugin
from pgcraft.plugins.check import (
    TableCheckPlugin,
    TriggerCheckPlugin,
)
from pgcraft.plugins.ledger import (
    DoubleEntryPlugin,
    DoubleEntryTriggerPlugin,
    LedgerBalanceCheckPlugin,
    LedgerBalanceViewPlugin,
    LedgerLatestViewPlugin,
)
from pgcraft.plugins.pk import SerialPKPlugin
from pgcraft.plugins.simple import (
    SimpleTablePlugin,
    SimpleTriggerPlugin,
)
from pgcraft.statistics import PGCraftStatisticsView
from pgcraft.utils.naming_convention import build_naming_convention

metadata = MetaData(
    naming_convention=build_naming_convention(),
)

# -- Factory-based models -----------------------------------------------

SimpleDimensionResourceFactory(
    tablename="users",
    schemaname="public",
    metadata=metadata,
    schema_items=[
        Column("name", String),
        Column("price", Integer),
        Column("qty", Integer),
        Column("total", Integer, Computed("price * qty")),
        PGCraftCheck("{price} > 0", name="positive_price"),
        PGCraftCheck("{qty} >= 0", name="nonneg_qty"),
    ],
    plugins=[
        SerialPKPlugin(),
        SimpleTablePlugin(),
        TableCheckPlugin(),
        APIPlugin(
            grants=["select", "insert", "update", "delete"]
        ),
        SimpleTriggerPlugin(),
    ],
)

AppendOnlyDimensionResourceFactory(
    tablename="students",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("name", String),
        Column("user_id", ForeignKey("public.users.id")),
    ],
)

# -- Invoices (EAV dimension) ------------------------------------------

EAVDimensionResourceFactory(
    tablename="invoices",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("customer_id", Integer),
        Column("amount", Numeric(10, 2)),
        Column("paid", Boolean),
        Column("description", String),
    ],
    extra_plugins=[
        TriggerCheckPlugin(),
        TriggerCheckPlugin(view_key="api"),
    ],
)

# -- Products (EAV dimension) ------------------------------------------

EAVDimensionResourceFactory(
    tablename="products",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("color", String),
        Column("weight", Float),
        Column("is_active", Boolean),
        Column("price", Integer),
        PGCraftCheck(
            "{price} > 0", name="positive_product_price"
        ),
    ],
    extra_plugins=[
        TriggerCheckPlugin(),
        TriggerCheckPlugin(view_key="api"),
    ],
)

# -- Reference tables for statistics queries ----------------------------
# These reflect tables that already exist in the database.
# They are not managed by pgcraft — they only provide column
# metadata so SQLAlchemy can compile the statistics queries.

_orders_ref = Table(
    "orders",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("customer_id", Integer),
    Column("total", Numeric(10, 2)),
    schema="public",
    extend_existing=True,
)

_invoices_ref = Table(
    "invoices_entity",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("customer_id", Integer),
    Column("amount", Numeric(10, 2)),
    Column("paid", Boolean),
    schema="private",
    extend_existing=True,
)

# -- Customers with statistics ------------------------------------------

_order_stats = select(
    _orders_ref.c.customer_id,
    func.count().label("order_count"),
    func.sum(_orders_ref.c.total).label("order_total"),
).group_by(_orders_ref.c.customer_id)

_invoice_stats = select(
    _invoices_ref.c.customer_id,
    func.count().label("invoice_count"),
    func.sum(_invoices_ref.c.amount).label("invoiced_total"),
).group_by(_invoices_ref.c.customer_id)

SimpleDimensionResourceFactory(
    tablename="customers",
    schemaname="public",
    metadata=metadata,
    schema_items=[
        Column("name", String, nullable=False),
        Column("email", String),
        PGCraftStatisticsView(
            name="orders",
            query=_order_stats,
            join_key="customer_id",
        ),
        PGCraftStatisticsView(
            name="invoices",
            query=_invoice_stats,
            join_key="customer_id",
        ),
    ],
)

# -- Ledger models ------------------------------------------------------

LedgerResourceFactory(
    tablename="order_events",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("order_id", String, nullable=False),
        Column("status", String, nullable=False),
    ],
    extra_plugins=[
        LedgerLatestViewPlugin(dimensions=["order_id"]),
    ],
)

LedgerResourceFactory(
    tablename="stock_movements",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("warehouse", String, nullable=False),
        Column("sku", String, nullable=False),
    ],
    extra_plugins=[
        LedgerBalanceViewPlugin(dimensions=["warehouse", "sku"]),
        LedgerBalanceCheckPlugin(
            dimensions=["warehouse", "sku"],
        ),
    ],
)

# -- Inventory ledger with events -----------------------------------
# Simple event: record an ad-hoc stock adjustment (fire-and-forget).

_inv_adjust = LedgerEvent(
    name="adjust",
    input=lambda p: select(
        p("warehouse", String).label("warehouse"),
        p("sku", String).label("sku"),
        p("value", Integer).label("value"),
        p("reason", String).label("reason"),
    ),
)

LedgerResourceFactory(
    tablename="inventory",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("warehouse", String, nullable=False),
        Column("sku", String, nullable=False),
        Column("reason", String, nullable=True),
    ],
    extra_plugins=[
        LedgerBalanceViewPlugin(dimensions=["warehouse", "sku"]),
    ],
    events=[_inv_adjust],
)

# -- Invoice lines (source table for revenue reconciliation) --------

_invoice_lines = Table(
    "invoice_lines",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("invoice_id", Integer, nullable=False),
    Column("account", String, nullable=False),
    Column("amount", Integer, nullable=False),
    schema="private",
)

# -- Revenue ledger with transactional reconciliation ---------------
# The reconcile event accepts invoice IDs, joins against the
# invoice_lines table to produce the desired state, and diffs
# against existing balances per (invoice_id, account).

_rev_recognize = LedgerEvent(
    name="recognize",
    input=lambda p: select(
        func.unnest(p("invoice_ids", ARRAY(Integer)))
        .label("invoice_id"),
    ),
    desired=lambda pginput: select(
        _invoice_lines.c.invoice_id,
        _invoice_lines.c.account,
        _invoice_lines.c.amount.label("value"),
    ).where(
        _invoice_lines.c.invoice_id.in_(
            select(pginput.c.invoice_id)
        )
    ),
    existing=ledger_balances("invoice_id", "account"),
    diff_keys=["invoice_id", "account"],
)

LedgerResourceFactory(
    tablename="revenue",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("invoice_id", Integer, nullable=False),
        Column("account", String, nullable=False),
    ],
    extra_plugins=[
        LedgerBalanceViewPlugin(
            dimensions=["invoice_id", "account"]
        ),
    ],
    events=[_rev_recognize],
)

SimpleDimensionResourceFactory(
    tablename="accounts",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("name", String, nullable=False),
        Column("category", String, nullable=False),
    ],
)

LedgerResourceFactory(
    tablename="journal",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column(
            "account_id",
            ForeignKey("private.accounts.id"),
            nullable=False,
        ),
    ],
    extra_plugins=[
        DoubleEntryPlugin(),
        DoubleEntryTriggerPlugin(),
    ],
)

# -- Declarative models -------------------------------------------------


@register(
    metadata=metadata,
    plugins=[
        SerialPKPlugin(),
        SimpleTablePlugin(),
        TableCheckPlugin(),
        APIPlugin(grants=["select", "insert", "update"]),
        SimpleTriggerPlugin(),
    ],
)
class Locations:
    __tablename__ = "locations"
    __table_args__ = {"schema": "public"}

    name = Column(String, nullable=False)
    city = Column(String)
    country = Column(String)
    display = Column(
        String,
        Computed("name || ', ' || city"),
        nullable=True,
    )
    name_not_empty = PGCraftCheck(
        "length({name}) > 0", name="locations_name_not_empty"
    )
