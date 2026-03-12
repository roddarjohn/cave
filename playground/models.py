from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
)

from cave.factory.dimension import (
    AppendOnlyDimensionFactory,
    EAVDimensionFactory,
    SimpleDimensionFactory,
)
from cave.plugins.api import APIPlugin
from cave.plugins.pk import SerialPKPlugin
from cave.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin
from cave.utils.naming_convention import build_naming_convention

metadata = MetaData(
    naming_convention=build_naming_convention(),
)

SimpleDimensionFactory(
    tablename="users",
    schemaname="public",
    metadata=metadata,
    dimensions=[
        Column("name", Integer),
    ],
    plugins=[
        SerialPKPlugin(),
        SimpleTablePlugin(),
        APIPlugin(grants=["select", "insert", "update", "delete"]),
        SimpleTriggerPlugin(),
    ],
)

AppendOnlyDimensionFactory(
    tablename="students",
    schemaname="private",
    metadata=metadata,
    dimensions=[
        Column("name", String),
        Column("user_id", ForeignKey("public.users.id")),
    ],
)

EAVDimensionFactory(
    tablename="products",
    schemaname="private",
    metadata=metadata,
    dimensions=[
        Column("color", String),
        Column("weight", Float),
        Column("is_active", Boolean),
        Column("price", Integer),
    ],
)
