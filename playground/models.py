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

from pgcraft.check import PGCraftCheck
from pgcraft.declarative import register
from pgcraft.factory.dimension import (
    AppendOnlyDimensionResourceFactory,
    EAVDimensionResourceFactory,
    SimpleDimensionResourceFactory,
)
from pgcraft.plugins.api import APIPlugin
from pgcraft.plugins.check import (
    TableCheckPlugin,
    TriggerCheckPlugin,
)
from pgcraft.plugins.pk import SerialPKPlugin
from pgcraft.plugins.simple import (
    SimpleTablePlugin,
    SimpleTriggerPlugin,
)
from pgcraft.plugins.statistics import StatisticsViewPlugin
from pgcraft.statistics import PGCraftStatistics
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
        PGCraftStatistics(
            name="orders",
            query=_order_stats,
            join_key="customer_id",
        ),
        PGCraftStatistics(
            name="invoices",
            query=_invoice_stats,
            join_key="customer_id",
        ),
    ],
    plugins=[
        SerialPKPlugin(),
        SimpleTablePlugin(),
        StatisticsViewPlugin(),
        APIPlugin(stats_key="statistics_views"),
        SimpleTriggerPlugin(),
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
