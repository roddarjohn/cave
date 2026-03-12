"""Shared helpers for plugin unit tests."""

from __future__ import annotations

from sqlalchemy import Column, Integer, MetaData, String
from sqlalchemy_declarative_extensions import View

from cave.factory.context import FactoryContext


def make_ctx(
    tablename: str = "product",
    schemaname: str = "dim",
    dimensions: list | None = None,
    pk_col_name: str = "id",
    store: dict | None = None,
) -> FactoryContext:
    """Return a FactoryContext suitable for testing an individual plugin.

    Args:
        tablename: Table name.
        schemaname: Schema name.
        dimensions: Dimension columns (defaults to a single String column).
        pk_col_name: Name for the pre-populated pk column.
        store: Extra keys to pre-populate in the ctx store.

    Returns:
        A FactoryContext with pk_columns and extra_columns already set.

    """
    if dimensions is None:
        dimensions = [Column("name", String)]
    ctx = FactoryContext(
        tablename=tablename,
        schemaname=schemaname,
        metadata=MetaData(),
        dimensions=list(dimensions),
        plugins=[],
        pk_columns=[Column(pk_col_name, Integer, primary_key=True)],
        extra_columns=[],
    )
    for k, v in (store or {}).items():
        ctx[k] = v
    return ctx


def make_view(name: str, schema: str, definition: str = "SELECT 1") -> View:
    """Return a minimal View object for use as a ctx store value."""
    return View(name, definition, schema=schema)
