"""Decorator for applying cave plugins to plain classes.

Table creation is fully deferred to cave's plugin pipeline — the
decorated class does NOT inherit from a SQLAlchemy declarative base
and no ``__table__`` is created until plugins run.

After decoration the class is imperatively mapped so that
``select(User)`` and other ORM operations work normally.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column

from cave.errors import CaveValidationError
from cave.factory.base import (
    _resolve_plugins,
    _run_plugin_validators,
    _sort_plugins,
)
from cave.factory.context import FactoryContext

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import MetaData
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.schema import SchemaItem

    from cave.plugin import Plugin


def _parse_table_args(
    cls: type,
) -> tuple[str | None, list]:
    """Extract schema and extra constraints from __table_args__.

    Returns:
        ``(schema, extra_constraints)`` where *schema* may be
        ``None`` and *extra_constraints* is a list of table-level
        objects (``UniqueConstraint``, ``Index``, etc.).

    """
    raw = getattr(cls, "__table_args__", None)
    if raw is None:
        return None, []
    if isinstance(raw, dict):
        return raw.get("schema"), []
    # Tuple form: (constraint, ..., {kw_dict})
    if isinstance(raw, tuple) and raw:
        last = raw[-1]
        if isinstance(last, dict):
            return last.get("schema"), list(raw[:-1])
        return None, list(raw)
    return None, []


def _collect_columns(cls: type) -> list[Column]:
    """Read ``Column`` objects from the class dict, inferring names.

    Column objects whose ``name`` is ``None`` get the attribute
    name assigned automatically (matching declarative behaviour).
    """
    columns: list[Column] = []
    for attr_name, value in list(cls.__dict__.items()):
        if isinstance(value, Column):
            if value.name is None:
                value.name = attr_name
            if value.key is None:
                value.key = attr_name
            columns.append(value)
    return columns


def _validate_class(
    cls: type,
    schema_override: str | None,
) -> tuple[str, str]:
    """Validate *cls* and return ``(tablename, schema)``.

    Raises:
        CaveValidationError: On any validation failure.

    """
    if hasattr(cls, "__table__"):
        msg = (
            f"{cls.__name__} already has a __table__ "
            f"(likely inherits from a DeclarativeBase). "
            f"@register requires a plain class — cave "
            f"handles table creation via plugins."
        )
        raise CaveValidationError(msg)

    tablename: str | None = getattr(cls, "__tablename__", None)
    if tablename is None:
        msg = f"{cls.__name__} must define __tablename__."
        raise CaveValidationError(msg)

    schema = schema_override
    if schema is None:
        schema, _ = _parse_table_args(cls)
    if schema is None:
        msg = (
            f"{cls.__name__} must specify a schema via "
            f"__table_args__ = {{'schema': '...'}} "
            f"or the schema= parameter on @register."
        )
        raise CaveValidationError(msg)

    return tablename, schema


def register[T](  # noqa: PLR0913
    *,
    plugins: list[Plugin],
    base: type[DeclarativeBase] | None = None,
    metadata: MetaData | None = None,
    schema: str | None = None,
    cave: object | None = None,
    extra_plugins: list[Plugin] | None = None,
) -> Callable[[type[T]], type[T]]:
    """Apply cave plugins to a plain class and optionally ORM-map it.

    The class declares columns as plain ``Column`` attributes.
    Cave's plugin pipeline handles table creation, PK generation,
    API views, triggers, and any other registered work.  After
    plugins run, the class is imperatively mapped (when *base* is
    provided) so that ``select(cls)`` works.

    Usage::

        @register(
            base=Base,
            plugins=[
                SerialPKPlugin(),
                SimpleTablePlugin(),
                APIPlugin(),
                SimpleTriggerPlugin(),
            ],
        )
        class User:
            __tablename__ = "users"
            __table_args__ = {"schema": "public"}

            name = Column(String)
            email = Column(String, nullable=True)

    After decoration:

    - ``User.__table__`` is the cave-created table.
    - ``select(User)`` works (when *base* is given).
    - Views, triggers, and API resources are registered on
      *metadata*.

    Args:
        plugins: Plugins to run.  Include table-creating plugins
            (``SerialPKPlugin``, ``SimpleTablePlugin``, etc.) —
            there is no auto-created table to build on.
        base: A SQLAlchemy :class:`~sqlalchemy.orm.DeclarativeBase`
            subclass.  When provided, the class is imperatively
            mapped via ``base.registry`` and ``base.metadata`` is
            used as the target metadata.
        metadata: Explicit ``MetaData`` instance.  Required when
            *base* is not given.  Ignored when *base* is provided.
        schema: PostgreSQL schema name.  Overrides
            ``__table_args__["schema"]`` when given.
        cave: Optional :class:`~cave.config.CaveConfig` providing
            global plugins.
        extra_plugins: Appended to the resolved plugin list.

    Returns:
        A class decorator.

    Raises:
        CaveValidationError: If required attributes are missing
            or plugin validation fails.

    """

    def decorator(cls: type[T]) -> type[T]:
        tablename, resolved_schema = _validate_class(cls, schema)

        # -- resolve metadata -----------------------------------------
        md: MetaData | None = None
        if base is not None:
            md = base.metadata
        elif metadata is not None:
            md = metadata
        if md is None:
            msg = (
                "register() requires either base= or "
                "metadata= so that cave knows where to "
                "create tables."
            )
            raise CaveValidationError(msg)

        # -- collect columns and run plugins --------------------------
        schema_items: list[SchemaItem] = list(_collect_columns(cls))

        resolved = _resolve_plugins(cave, plugins, extra_plugins, [])
        _run_plugin_validators(resolved)

        ctx = FactoryContext(
            tablename=tablename,
            schemaname=resolved_schema,
            metadata=md,
            schema_items=schema_items,
            plugins=resolved,
        )

        for p in _sort_plugins(resolved):
            p.run(ctx)

        # -- bind __table__ -------------------------------------------
        if "__root__" not in ctx:
            msg = (
                "No plugin produced '__root__'. "
                "Include a table-creating plugin such as "
                "SimpleTablePlugin in the plugin list."
            )
            raise CaveValidationError(msg)

        root = ctx["__root__"]
        cls.__table__ = root  # type: ignore[attr-defined]

        # -- ORM mapping ----------------------------------------------
        if base is not None:
            base.registry.map_imperatively(cls, root)

        return cls

    return decorator
