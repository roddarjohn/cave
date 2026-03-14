from sqlalchemy import (
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

from pgcraft import (
    LedgerEvent,
    ledger_balances,
    pgcraft_build_naming_conventions,
)
from pgcraft.check import PGCraftCheck
from pgcraft.declarative import register
from pgcraft.factory.dimension.append_only import PGCraftAppendOnly
from pgcraft.factory.dimension.eav import PGCraftEAV
from pgcraft.factory.dimension.simple import PGCraftSimple
from pgcraft.factory.ledger import PGCraftLedger
from pgcraft.plugins.ledger import (
    DoubleEntryPlugin,
    DoubleEntryTriggerPlugin,
)
from pgcraft.views.actions import LedgerActions
from pgcraft.views.api import APIView
from pgcraft.views.balance import BalanceView
from pgcraft.views.view import PGCraftView

metadata = MetaData(
    naming_convention=pgcraft_build_naming_conventions(),
)

# -- Table factories: create the data model -------------------------

users = PGCraftSimple(
    "users",
    "public",
    metadata,
    schema_items=[
        Column("name", String),
        Column("price", Integer),
        Column("qty", Integer),
        Column("total", Integer, Computed("price * qty")),
        PGCraftCheck("{price} > 0", name="positive_price"),
        PGCraftCheck("{qty} >= 0", name="nonneg_qty"),
    ],
)

students = PGCraftAppendOnly(
    "students",
    "private",
    metadata,
    schema_items=[
        Column("name", String),
        Column(
            "user_id", ForeignKey("public.users.id")
        ),
    ],
)

Invoices = PGCraftAppendOnly(
    "invoices",
    "private",
    metadata,
    schema_items=[
        Column("customer_id", Integer, nullable=False),
        Column("amount", Numeric(10, 2), nullable=False),
    ],
)

InvoiceLines = PGCraftEAV(
    "invoice_lines",
    "private",
    metadata,
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
)

PGCraftEAV(
    "products",
    "private",
    metadata,
    schema_items=[
        Column("color", String),
        Column("weight", Float),
        Column("price", Integer),
        PGCraftCheck(
            "{price} > 0", name="positive_product_price"
        ),
    ],
)

Orders = PGCraftSimple(
    "orders",
    "public",
    metadata,
    schema_items=[
        Column("customer_id", Integer, nullable=False),
        Column("total", Numeric(10, 2), nullable=False),
    ],
)

customers = PGCraftSimple(
    "customers",
    "public",
    metadata,
    schema_items=[
        Column("name", String, nullable=False),
        Column("email", String),
    ],
)

# -- View factories: create derived output --------------------------

APIView(
    source=users,
    grants=["select", "insert", "update", "delete"],
)
APIView(source=students)
APIView(source=Invoices)
APIView(source=InvoiceLines)
APIView(source=Orders)

# -- Statistics via PGCraftView + APIView query lambda --

_order_stats = PGCraftView(
    "customers_orders_stats",
    "public",
    metadata,
    query=select(
        Orders.table.c.customer_id,
        func.count().label("order_count"),
        func.sum(Orders.table.c.total).label("order_total"),
    ).group_by(Orders.table.c.customer_id),
)

_invoice_stats = PGCraftView(
    "customers_invoices_stats",
    "public",
    metadata,
    query=select(
        Invoices.table.c.customer_id,
        func.count().label("invoice_count"),
        func.sum(Invoices.table.c.amount).label(
            "invoiced_total"
        ),
    ).group_by(Invoices.table.c.customer_id),
)

APIView(source=customers)

# -- Inventory ledger (simple event) --------------------------------

_inv_adjust = LedgerEvent(
    name="adjust",
    input=lambda p: select(
        p("warehouse", String).label("warehouse"),
        p("sku", String).label("sku"),
        p("value", Integer).label("value"),
        p("reason", String).label("reason"),
    ),
)

inventory = PGCraftLedger(
    "inventory",
    "private",
    metadata,
    schema_items=[
        Column("warehouse", String, nullable=False),
        Column("sku", String, nullable=False),
        Column("reason", String, nullable=True),
    ],
)

APIView(
    source=inventory,
    grants=["select", "insert"],
)
BalanceView(
    source=inventory, dimensions=["warehouse", "sku"]
)
LedgerActions(source=inventory, events=[_inv_adjust])

# -- General ledger (double-entry reconciliation) -------------------

_il = InvoiceLines.table

_post_invoices = LedgerEvent(
    name="post",
    input=lambda p: select(
        func.unnest(
            p("invoice_ids", ARRAY(Integer))
        ).label("invoice_id"),
    ),
    desired=lambda pginput: union_all(
        select(
            _il.c.invoice_id,
            _il.c.department,
            literal_column("'accounts_receivable'").label(
                "account"
            ),
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
        "invoice_id",
        "department",
        "account",
        "direction",
    ),
    diff_keys=[
        "invoice_id",
        "department",
        "account",
        "direction",
    ],
)

ledger = PGCraftLedger(
    "ledger",
    "private",
    metadata,
    schema_items=[
        Column("invoice_id", Integer, nullable=False),
        Column("department", String, nullable=False),
        Column("account", String, nullable=False),
    ],
    extra_plugins=[
        DoubleEntryPlugin(),
        DoubleEntryTriggerPlugin(),
    ],
)

APIView(
    source=ledger,
    grants=["select", "insert"],
)
BalanceView(
    source=ledger,
    dimensions=[
        "invoice_id",
        "department",
        "account",
    ],
)
LedgerActions(source=ledger, events=[_post_invoices])

# -- Declarative models --------------------------------------------


@register(
    metadata=metadata,
    api={"grants": ["select", "insert", "update"]},
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
        "length({name}) > 0",
        name="locations_name_not_empty",
    )
