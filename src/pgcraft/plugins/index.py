"""Index plugin for pgcraft dimensions.

:class:`TableIndexPlugin` converts :class:`~pgcraft.index.PGCraftIndex`
items into real SQLAlchemy ``Index`` objects on a table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Index, text

from pgcraft.index import collect_indices
from pgcraft.plugin import Dynamic, Plugin, requires
from pgcraft.validation import validate_column_references

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext


@requires(Dynamic("table_key"))
class TableIndexPlugin(Plugin):
    """Materialize :class:`~pgcraft.index.PGCraftIndex` as table indexes.

    Reads ``PGCraftIndex`` items from ``ctx.schema_items``, validates
    column names, and creates ``Index`` objects on the target table.

    Args:
        table_key: Key in ``ctx`` for the target table
            (default ``"primary"``).

    """

    def __init__(self, table_key: str = "primary") -> None:
        """Store the context key."""
        self.table_key = table_key

    def run(self, ctx: FactoryContext) -> None:
        """Collect, validate, and create indexes."""
        indices = collect_indices(ctx.schema_items)
        if not indices:
            return
        table = ctx[self.table_key]
        col_names = {c.name for c in table.columns}
        for idx in indices:
            validate_column_references(
                f"PGCraftIndex {idx.name!r}",
                idx.column_names(),
                col_names,
            )
            resolved = idx.resolve(lambda c: c)
            has_expressions = any(expr not in col_names for expr in resolved)
            cols = [
                table.c[expr] if expr in col_names else text(expr)
                for expr in resolved
            ]
            sa_index = Index(
                idx.name,
                *cols,
                unique=idx.unique,
            )
            if has_expressions:
                table.append_constraint(sa_index)
