from cave.utils.naming_convention import build_naming_convention
from cave.factory.dimension.append_only import append_only_log_dimension_factory
from cave.factory.dimension.simple import (
    simple_dimension_factory,
)
from sqlalchemy import Integer, String, MetaData, Column, Date, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

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
