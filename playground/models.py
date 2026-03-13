from sqlalchemy import (
    Boolean,
    Column,
    Computed,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
)

from pgcraft.check import PGCraftCheck
from pgcraft.declarative import register
from pgcraft.factory.dimension import (
    AppendOnlyDimensionResourceFactory,
    EAVDimensionResourceFactory,
    SimpleDimensionResourceFactory,
)
from pgcraft.factory.ledger import LedgerResourceFactory
from pgcraft.plugins.api import APIPlugin
from pgcraft.plugins.check import TableCheckPlugin, TriggerCheckPlugin
from pgcraft.plugins.ledger import (
    DoubleEntryPlugin,
    DoubleEntryTriggerPlugin,
    LedgerBalanceViewPlugin,
)
from pgcraft.plugins.pk import SerialPKPlugin
from pgcraft.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin
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
        APIPlugin(grants=["select", "insert", "update", "delete"]),
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

EAVDimensionResourceFactory(
    tablename="products",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("color", String),
        Column("weight", Float),
        Column("is_active", Boolean),
        Column("price", Integer),
        PGCraftCheck("{price} > 0", name="positive_product_price"),
    ],
    extra_plugins=[
        TriggerCheckPlugin(),
        TriggerCheckPlugin(view_key="api"),
    ],
)

# -- Ledger models ------------------------------------------------------

LedgerResourceFactory(
    tablename="transactions",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("account", String, nullable=False),
        Column("category", String),
    ],
    extra_plugins=[
        LedgerBalanceViewPlugin(dimensions=["account"]),
    ],
)

LedgerResourceFactory(
    tablename="journal",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("account", String, nullable=False),
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
        String, Computed("name || ', ' || city"), nullable=True
    )
    name_not_empty = PGCraftCheck(
        "length({name}) > 0", name="locations_name_not_empty"
    )
