"""BalanceView: ledger balance aggregation view."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import func, select
from sqlalchemy_declarative_extensions import View, register_view

from pgcraft.errors import PGCraftValidationError
from pgcraft.utils.naming import resolve_name
from pgcraft.utils.query import compile_query

if TYPE_CHECKING:
    from pgcraft.factory.ledger import PGCraftLedger

_NAMING_DEFAULTS = {
    "ledger_balance_view": "%(table_name)s_balances",
}


class BalanceView:
    """Create a view showing current balances per dimension group.

    Generates ``SELECT dim_cols, SUM(value) AS balance
    FROM ledger GROUP BY dim_cols``.

    Args:
        source: A :class:`~pgcraft.factory.ledger.PGCraftLedger`
            instance.
        dimensions: Column names to group by.  Must be a
            non-empty list.

    Raises:
        PGCraftValidationError: If *dimensions* is empty.

    """

    def __init__(
        self,
        source: PGCraftLedger,
        dimensions: list[str],
    ) -> None:
        """Create and register the balance view."""
        if not dimensions:
            msg = "dimensions must be a non-empty list"
            raise PGCraftValidationError(msg)

        ctx = source.ctx
        table = ctx["primary"]
        dim_columns = [table.c[d] for d in dimensions]

        query = (
            select(
                *[c.label(c.key) for c in dim_columns],
                func.sum(table.c["value"]).label("balance"),
            )
            .select_from(table)
            .group_by(*dim_columns)
        )

        view_name = resolve_name(
            ctx.metadata,
            "ledger_balance_view",
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
