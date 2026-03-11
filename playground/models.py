from cave.factory.dimension.append_only import append_only_log_dimension_factory
from cave.factory.dimension.simple import (
    simple_dimension_factory,
)
from sqlalchemy import Integer, String, MetaData, Column
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

metadata = MetaData()


simple_dimension_factory(
    tablename="users",
    schemaname="public",
    metadata=metadata,
    dimensions=[
        Column("name", Integer),
    ],
)

append_only_log_dimension_factory(
    tablename="students",
    schemaname="private",
    metadata=metadata,
    dimensions=[
        Column("name", Integer),
    ],
)
