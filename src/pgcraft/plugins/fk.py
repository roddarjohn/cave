"""Foreign key plugin for pgcraft dimensions.

:class:`TableFKPlugin` converts :class:`~pgcraft.fk.PGCraftFK`
items into real SQLAlchemy ``ForeignKeyConstraint`` objects on a
table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKeyConstraint

from pgcraft.fk import collect_fks
from pgcraft.plugin import Dynamic, Plugin, requires
from pgcraft.validation import validate_column_references

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext


@requires(Dynamic("table_key"))
class TableFKPlugin(Plugin):
    """Materialize :class:`~pgcraft.fk.PGCraftFK` as FK constraints.

    Reads ``PGCraftFK`` items from ``ctx.schema_items``, validates
    local column names, and creates ``ForeignKeyConstraint`` objects
    on the target table.

    Args:
        table_key: Key in ``ctx`` for the target table
            (default ``"primary"``).

    """

    def __init__(self, table_key: str = "primary") -> None:
        """Store the context key."""
        self.table_key = table_key

    def run(self, ctx: FactoryContext) -> None:
        """Collect, validate, and create foreign key constraints."""
        fks = collect_fks(ctx.schema_items)
        if not fks:
            return
        table = ctx[self.table_key]
        col_names = {c.name for c in table.columns}
        for fk in fks:
            validate_column_references(
                f"PGCraftFK {fk.name!r}",
                fk.column_names(),
                col_names,
            )
            resolved_columns = fk.resolve(lambda c: c)
            constraint = ForeignKeyConstraint(
                resolved_columns,
                fk.references,
                name=fk.name,
                ondelete=fk.ondelete,
                onupdate=fk.onupdate,
            )
            table.append_constraint(constraint)
