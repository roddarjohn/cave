"""LedgerEvent configuration and helpers.

A :class:`LedgerEvent` declares a named operation on a ledger.  The
user provides lambdas that produce SQLAlchemy selects; the plugin
compiles them into a single PostgreSQL function per event.

Two modes are supported:

- **Simple mode** (``input`` only): the input select is inserted
  directly into the ledger via the API view.
- **Diff mode** (``input`` + ``desired`` + ``existing``): the desired
  state is diffed against the existing state and only the correcting
  deltas are inserted.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import column, func, literal_column, select, table
from sqlalchemy.dialects import postgresql as pg_dialect
from sqlalchemy.sql.expression import tuple_

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import FromClause, Table
    from sqlalchemy.sql.expression import Select
    from sqlalchemy.sql.sqltypes import TypeEngine
    from sqlalchemy_declarative_extensions.dialects.postgresql import (
        FunctionParam,
    )


class ParamCollector:
    """Collect SQL function parameters during lambda evaluation.

    Usage inside an ``input`` lambda::

        lambda p: select(
            p("warehouse", String).label("warehouse"),
            p("sku", String).label("sku"),
        )

    Each call to ``p(name, sa_type)`` records the parameter and
    returns a :func:`~sqlalchemy.sql.expression.literal_column`
    reference (``p_name``) suitable for embedding in a select.

    Args:
        None.

    """

    def __init__(self) -> None:
        """Initialise with empty parameter list."""
        self._params: list[tuple[str, TypeEngine]] = []

    def __call__(
        self,
        name: str,
        sa_type: type[TypeEngine] | TypeEngine,
    ) -> literal_column:
        """Register a parameter and return a column reference.

        Args:
            name: Parameter name (without ``p_`` prefix).
            sa_type: SQLAlchemy type (class or instance).

        Returns:
            A ``literal_column("p_name")`` for use in selects.

        """
        if isinstance(sa_type, type):
            sa_type = sa_type()
        self._params.append((name, sa_type))
        return literal_column(f"p_{name}")

    @property
    def function_params(self) -> list[FunctionParam]:
        """Build the ``FunctionParam`` list for function registration.

        Returns:
            List of ``FunctionParam.input(...)`` entries.

        """
        from sqlalchemy_declarative_extensions.dialects.postgresql import (  # noqa: PLC0415
            FunctionParam,
        )

        return [
            FunctionParam.input(
                f"p_{name}",
                sa_type.compile(dialect=pg_dialect.dialect()),
            )
            for name, sa_type in self._params
        ]


@dataclass
class LedgerEvent:
    """Declare a named ledger operation.

    Each event compiles into a single PostgreSQL function that
    inserts rows into the ledger API view and returns the inserted
    rows via ``RETURNING *``.

    **Simple mode** — provide only ``input``.  The input select's
    columns are inserted directly.

    **Diff mode** — provide ``input``, ``desired``, ``existing``,
    and ``diff_keys``.  The desired and existing selects are unioned
    and only non-zero deltas are inserted.

    Args:
        name: Unique event name within the ledger.
        input: Lambda ``(p) -> Select`` that builds the input CTE.
            ``p`` is a :class:`ParamCollector`.
        desired: Lambda ``(pginput) -> Select`` that builds the
            desired-state CTE from the input CTE reference.
        existing: Lambda ``(table, desired) -> Select`` that builds
            the existing-state CTE.  Use :func:`ledger_balances`
            for the common pattern.
        diff_keys: Column names used for grouping in diff mode.
            Required when ``desired`` is set.

    """

    name: str
    input: Callable[[ParamCollector], Select]
    desired: Callable[[FromClause], Select] | None = None
    existing: Callable[[Table, FromClause], Select] | None = None
    diff_keys: list[str] = field(default_factory=list)


def ledger_balances(
    *keys: str,
) -> Callable[[Table, FromClause], Select]:
    """Return an ``existing`` callable for common balance lookup.

    Produces a select that negates the current balances for each
    diff-key group present in the desired CTE::

        SELECT key1, key2, SUM(value) * -1 AS value
        FROM ledger_table
        WHERE (key1, key2) IN (SELECT key1, key2 FROM desired)
        GROUP BY key1, key2

    Args:
        *keys: Dimension column names to group by.

    Returns:
        A callable suitable for ``LedgerEvent(existing=...)``.

    """

    def _existing(
        root: Table,
        desired: FromClause,
    ) -> Select:
        key_cols = [root.c[k] for k in keys]
        desired_key_cols = [desired.c[k] for k in keys]
        return (
            select(
                *key_cols,
                (func.sum(root.c.value) * -1).label("value"),
            )
            .where(tuple_(*key_cols).in_(select(*desired_key_cols)))
            .group_by(*key_cols)
        )

    return _existing


def _input_table_ref(
    input_select: Select,
) -> FromClause:
    """Build a synthetic ``table("input", ...)`` from the input select.

    Args:
        input_select: The compiled input select.

    Returns:
        A :func:`~sqlalchemy.table` reference with matching columns.

    """
    cols = [column(col.name) for col in input_select.selected_columns]
    return table("input", *cols)


def _desired_table_ref(
    desired_select: Select,
) -> FromClause:
    """Build a synthetic ``table("desired", ...)`` from desired select.

    Args:
        desired_select: The compiled desired select.

    Returns:
        A :func:`~sqlalchemy.table` reference with matching columns.

    """
    cols = [column(col.name) for col in desired_select.selected_columns]
    return table("desired", *cols)
