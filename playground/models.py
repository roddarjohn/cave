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
    AppendOnlyDimensionResourceFactory,
    EAVDimensionResourceFactory,
    SimpleDimensionResourceFactory,
)
from cave.plugins.api import APIPlugin
from cave.plugins.pk import SerialPKPlugin
from cave.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin
from cave.utils.naming_convention import build_naming_convention

metadata = MetaData(
    naming_convention=build_naming_convention(),
)

SimpleDimensionResourceFactory(
    tablename="users",
    schemaname="public",
    metadata=metadata,
    schema_items=[
        Column("name", Integer),
    ],
    plugins=[
        SerialPKPlugin(),
        SimpleTablePlugin(),
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
    ],
)
