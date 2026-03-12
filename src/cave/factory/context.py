"""Factory context dataclass."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy import Column, MetaData, Table
    from sqlalchemy.schema import SchemaItem
    from sqlalchemy_declarative_extensions import View

    from cave.plugin import Plugin


@dataclass
class FactoryContext:
    """Carries inputs and accumulates outputs across plugin phases.

    Plugins read and write to this context throughout the factory
    lifecycle.  The ``tables`` and ``views`` dicts are populated
    by plugins during their respective phases.

    Convention:
        - ``tables["primary"]``: Set by the storage plugin in
          ``create_tables`` (simple) or ``create_views``
          (append-only, EAV).  ``APIPlugin`` reads it to build the
          API view.
        - ``views["api"]``: Set by ``APIPlugin.create_views``.
          Trigger plugins read it in ``create_triggers``.
        - ``state``: Arbitrary inter-phase plugin state (e.g. EAV
          attribute mappings computed in ``create_tables`` and
          consumed in ``create_views`` / ``create_triggers``).
    """

    tablename: str
    schemaname: str
    metadata: MetaData
    dimensions: list[SchemaItem]
    plugins: list[Plugin]

    # Resolved by DimensionFactory before create_tables is called.
    pk_columns: list[Column] = field(default_factory=list)
    extra_columns: list[Column] = field(default_factory=list)

    tables: dict[str, Table] = field(default_factory=dict)
    views: dict[str, View] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)
