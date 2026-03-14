"""LatestView: latest ledger entry per dimension group."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy_declarative_extensions import View, register_view

from pgcraft.errors import PGCraftValidationError
from pgcraft.utils.naming import resolve_name
from pgcraft.utils.query import compile_query

if TYPE_CHECKING:
    from pgcraft.factory.ledger import PGCraftLedger

_NAMING_DEFAULTS = {
    "ledger_latest_view": "%(table_name)s_latest",
}


class LatestView:
    """Create a view showing the most recent row per dimension.

    Uses ``DISTINCT ON`` ordered by ``created_at DESC``.

    Args:
        source: A :class:`~pgcraft.factory.ledger.PGCraftLedger`
            instance.
        dimensions: Column names to partition by.  Must be a
            non-empty list.

    Raises:
        PGCraftValidationError: If *dimensions* is empty.

    """

    def __init__(
        self,
        source: PGCraftLedger,
        dimensions: list[str],
    ) -> None:
        """Create and register the latest view."""
        if not dimensions:
            msg = "dimensions must be a non-empty list"
            raise PGCraftValidationError(msg)

        ctx = source.ctx
        table = ctx["primary"]
        created_at_col = ctx["created_at_column"]
        dim_columns = [table.c[d] for d in dimensions]

        query = (
            select(table)
            .distinct(*dim_columns)
            .order_by(
                *dim_columns,
                table.c[created_at_col].desc(),
            )
        )

        view_name = resolve_name(
            ctx.metadata,
            "ledger_latest_view",
            {
                "table_name": ctx.tablename,
                "schema": ctx.schemaname,
            },
            _NAMING_DEFAULTS,
        )

        self.view = View(
            view_name,
            compile_query(query),
            schema=ctx.schemaname,
        )
        register_view(ctx.metadata, self.view)
        self.name = view_name
