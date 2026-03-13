"""Shared helpers for plugin unit tests."""

from __future__ import annotations

from sqlalchemy import Column, Integer, MetaData, String
from sqlalchemy_declarative_extensions import View

from pgcraft.columns import PrimaryKeyColumns
from pgcraft.factory.context import FactoryContext


def make_ctx(
    tablename: str = "product",
    schemaname: str = "dim",
    schema_items: list | None = None,
    pk_col_name: str = "id",
    store: dict | None = None,
) -> FactoryContext:
    """Return a FactoryContext suitable for testing a plugin.

    Args:
        tablename: Table name.
        schemaname: Schema name.
        schema_items: Schema item columns (defaults to a single
            String column).
        pk_col_name: Name for the pre-populated pk column.
        store: Extra keys to pre-populate in the ctx store.

    Returns:
        A FactoryContext with pk_columns in the store.

    """
    if schema_items is None:
        schema_items = [Column("name", String)]
    ctx = FactoryContext(
        tablename=tablename,
        schemaname=schemaname,
        metadata=MetaData(),
        schema_items=list(schema_items),
        plugins=[],
    )
    ctx["pk_columns"] = PrimaryKeyColumns(
        [Column(pk_col_name, Integer, primary_key=True)]
    )
    for k, v in (store or {}).items():
        ctx[k] = v
    return ctx


def make_view(name: str, schema: str, definition: str = "SELECT 1") -> View:
    """Return a minimal View object for ctx store use."""
    return View(name, definition, schema=schema)
