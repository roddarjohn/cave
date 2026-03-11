from sqlalchemy import Select
from sqlalchemy.dialects import postgresql


def compile_query(query: Select) -> str:
    """Compile a SQLAlchemy query to a PostgreSQL SQL string."""
    return str(
        query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
