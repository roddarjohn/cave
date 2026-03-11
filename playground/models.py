from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
)

from cave.factory.dimension.append_only import (
    append_only_log_dimension_factory,
)
from cave.factory.dimension.eav import eav_dimension_factory
from cave.factory.dimension.simple import (
    simple_dimension_factory,
)
from cave.utils.naming_convention import build_naming_convention

metadata = MetaData(
    naming_convention=build_naming_convention(),
)

simple_dimension_factory(
    tablename="users",
    schemaname="public",
    metadata=metadata,
    dimensions=[
        Column("name", Integer),
    ],
    grants=["select", "insert", "update", "delete"],
)

append_only_log_dimension_factory(
    tablename="students",
    schemaname="private",
    metadata=metadata,
    dimensions=[
        Column("name", String),
        Column("user_id", ForeignKey("public.users.id")),
    ],
)

eav_dimension_factory(
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
