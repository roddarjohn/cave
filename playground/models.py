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
    APIResourceConfiguration,
    AppendOnlyDimensionFactory,
    EAVDimensionFactory,
    SimpleDimensionFactory,
)
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
    api_configuration=APIResourceConfiguration(
        grants=["select", "insert", "update", "delete"],
    ),
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
