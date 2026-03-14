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
    func,
    literal_column,
    select,
    union_all,
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
    LedgerBalanceViewPlugin,
)
from pgcraft.plugins.pk import SerialPKPlugin
from pgcraft.plugins.simple import (
    SimpleTablePlugin,
    SimpleTriggerPlugin,
)
from pgcraft.statistics import PGCraftStatisticsView
from pgcraft import pgcraft_build_naming_conventions

metadata = MetaData(
    naming_convention=pgcraft_build_naming_conventions(),
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

# -- Invoices (append-only dimension) ----------------------------------

Invoices = AppendOnlyDimensionResourceFactory(
    tablename="invoices",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("customer_id", Integer, nullable=False),
        Column("amount", Numeric(10, 2), nullable=False),
    ],
)

# -- Invoice lines (EAV dimension, FK to invoices) --------------------

InvoiceLines = EAVDimensionResourceFactory(
    tablename="invoice_lines",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column(
            "invoice_id",
            Integer,
            ForeignKey("private.invoices_root.id"),
            nullable=False,
        ),
        Column("department", String, nullable=False),
        Column("amount", Integer, nullable=False),
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

# -- Orders (simple dimension) -----------------------------------------

Orders = SimpleDimensionResourceFactory(
    tablename="orders",
    schemaname="public",
    metadata=metadata,
    schema_items=[
        Column("customer_id", Integer, nullable=False),
        Column("total", Numeric(10, 2), nullable=False),
    ],
)

# -- Customers with statistics ------------------------------------------

_order_stats = select(
    Orders.table.c.customer_id,
    func.count().label("order_count"),
    func.sum(Orders.table.c.total).label("order_total"),
).group_by(Orders.table.c.customer_id)

_invoice_stats = select(
    Invoices.table.c.customer_id,
    func.count().label("invoice_count"),
    func.sum(Invoices.table.c.amount).label("invoiced_total"),
).group_by(Invoices.table.c.customer_id)

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

# -- Inventory ledger (simple event) -----------------------------------

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

# -- General ledger (double-entry reconciliation) ---------------------
# Each invoice line produces two balanced ledger entries:
#   - debit  to "accounts_receivable"
#   - credit to "revenue"

_il = InvoiceLines.table

_post_invoices = LedgerEvent(
    name="post",
    input=lambda p: select(
        func.unnest(p("invoice_ids", ARRAY(Integer)))
        .label("invoice_id"),
    ),
    desired=lambda pginput: union_all(
        select(
            _il.c.invoice_id,
            _il.c.department,
            literal_column("'accounts_receivable'")
            .label("account"),
            literal_column("'debit'").label("direction"),
            _il.c.amount.label("value"),
        ).where(
            _il.c.invoice_id.in_(
                select(pginput.c.invoice_id)
            )
        ),
        select(
            _il.c.invoice_id,
            _il.c.department,
            literal_column("'revenue'").label("account"),
            literal_column("'credit'").label("direction"),
            _il.c.amount.label("value"),
        ).where(
            _il.c.invoice_id.in_(
                select(pginput.c.invoice_id)
            )
        ),
    ),
    existing=ledger_balances(
        "invoice_id", "department", "account", "direction"
    ),
    diff_keys=[
        "invoice_id", "department", "account", "direction"
    ],
)

LedgerResourceFactory(
    tablename="ledger",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("invoice_id", Integer, nullable=False),
        Column("department", String, nullable=False),
        Column("account", String, nullable=False),
    ],
    extra_plugins=[
        DoubleEntryPlugin(),
        DoubleEntryTriggerPlugin(),
        LedgerBalanceViewPlugin(
            dimensions=[
                "invoice_id",
                "department",
                "account",
            ]
        ),
    ],
    events=[_post_invoices],
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
