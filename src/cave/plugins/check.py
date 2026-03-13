"""Check constraint plugins for cave dimensions.

:class:`TableCheckPlugin` converts :class:`~cave.check.CaveCheck`
items into real SQLAlchemy ``CheckConstraint`` objects on a table
(for simple and append-only dimensions).

:class:`TriggerCheckPlugin` generates INSTEAD OF trigger functions
that enforce checks before the main dimension triggers (for EAV
dimensions where columns are virtual).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import CheckConstraint

from cave.check import CaveCheck, collect_checks
from cave.errors import CaveValidationError
from cave.plugin import Dynamic, Plugin, requires
from cave.utils.template import load_template
from cave.utils.trigger import register_view_triggers

if TYPE_CHECKING:
    from cave.factory.context import FactoryContext

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "check"

_NAMING_DEFAULTS = {
    "check_function": ("_check_%(schema)s_%(table_name)s_%(op)s"),
    "check_trigger": ("_check_%(schema)s_%(table_name)s_%(op)s"),
}


@requires(Dynamic("table_key"))
class TableCheckPlugin(Plugin):
    """Materialize :class:`~cave.check.CaveCheck` as table constraints.

    Reads ``CaveCheck`` items from ``ctx.schema_items``, resolves
    ``{col}`` markers to plain column names (identity), and appends
    real ``CheckConstraint`` objects to the target table.

    Args:
        table_key: Key in ``ctx`` for the target table
            (default ``"primary"``).

    """

    def __init__(self, table_key: str = "primary") -> None:
        """Store the context key."""
        self.table_key = table_key

    def run(self, ctx: FactoryContext) -> None:
        """Append check constraints to the target table."""
        checks = collect_checks(ctx.schema_items)
        if not checks:
            return

        table = ctx[self.table_key]
        col_names = {c.name for c in table.columns}

        for cave_check in checks:
            _validate_columns(cave_check, col_names)
            expr = cave_check.resolve(lambda c: c)
            constraint = CheckConstraint(expr, name=cave_check.name)
            table.append_constraint(constraint)


@requires(Dynamic("view_key"))
class TriggerCheckPlugin(Plugin):
    """Enforce checks via INSTEAD OF triggers (EAV dimensions).

    Generates a single trigger function per view per operation
    (INSERT/UPDATE) that validates all :class:`~cave.check.CaveCheck`
    items.  Triggers use a ``_check_`` prefix to fire before the
    main dimension triggers (Postgres fires multiple INSTEAD OF
    triggers in alphabetical order by name).

    Args:
        view_key: Key in ``ctx`` for the trigger target view
            (default ``"primary"``).

    """

    def __init__(self, view_key: str = "primary") -> None:
        """Store the context key."""
        self.view_key = view_key

    def run(self, ctx: FactoryContext) -> None:
        """Register check-enforcement triggers."""
        checks = collect_checks(ctx.schema_items)
        if not checks:
            return

        col_names = {col.key for col in ctx.columns}
        for cave_check in checks:
            _validate_columns(cave_check, col_names)

        resolved_checks = [
            (
                cave_check.resolve(lambda c: f"NEW.{c}"),
                cave_check.name,
            )
            for cave_check in checks
        ]

        template = load_template(_TEMPLATES / "validate.mako")
        template_vars = {"checks": resolved_checks}

        views_to_trigger: list[tuple[str, str]] = []
        if self.view_key in ctx:
            view = ctx[self.view_key]
            view_schema = view.schema or ctx.schemaname
            view_fullname = f"{view_schema}.{ctx.tablename}"
            views_to_trigger.append((view_schema, view_fullname))

        for view_schema, view_fullname in views_to_trigger:
            register_view_triggers(
                metadata=ctx.metadata,
                view_schema=view_schema,
                view_fullname=view_fullname,
                tablename=ctx.tablename,
                template_vars=template_vars,
                ops=[
                    ("insert", template),
                    ("update", template),
                ],
                naming_defaults=_NAMING_DEFAULTS,
                function_key="check_function",
                trigger_key="check_trigger",
            )

            _validate_trigger_ordering(ctx, view_fullname)


def _validate_columns(
    cave_check: CaveCheck,
    known_columns: set[str],
) -> None:
    """Raise if a check references unknown columns.

    Args:
        cave_check: The check to validate.
        known_columns: Set of known column names.

    Raises:
        CaveValidationError: If a referenced column is not
            in *known_columns*.

    """
    for col in cave_check.column_names():
        if col not in known_columns:
            msg = (
                f"CaveCheck {cave_check.name!r} references "
                f"unknown column {col!r}. "
                f"Known columns: "
                f"{sorted(known_columns)}"
            )
            raise CaveValidationError(msg)


def _validate_trigger_ordering(
    ctx: FactoryContext,
    view_fullname: str,
) -> None:
    """Validate check triggers fire before existing triggers.

    Postgres fires multiple INSTEAD OF triggers on the same view
    in alphabetical order by trigger name.  Check triggers must
    sort before the main dimension triggers so that constraint
    violations are caught before any data is modified.

    Args:
        ctx: Factory context (for metadata access).
        view_fullname: Fully qualified view name to check.

    Raises:
        CaveValidationError: If a check trigger name sorts
            after an existing non-check trigger on the same
            view.

    """
    triggers_info = ctx.metadata.info.get("triggers")
    if triggers_info is None:
        return

    check_triggers: list[str] = []
    other_triggers: list[str] = []
    for t in triggers_info.triggers:
        if t.on != view_fullname:
            continue
        name = t.name
        if name.startswith("_check_"):
            check_triggers.append(name)
        else:
            other_triggers.append(name)

    for ct in check_triggers:
        for ot in other_triggers:
            if ct > ot:
                msg = (
                    f"Check trigger {ct!r} sorts after "
                    f"existing trigger {ot!r} on "
                    f"{view_fullname}. Postgres fires "
                    f"multiple INSTEAD OF triggers in "
                    f"alphabetical order by name, so "
                    f"check triggers must sort before "
                    f"the main dimension triggers to "
                    f"enforce constraints before data "
                    f"is modified. Consider renaming "
                    f"the check or using a naming "
                    f"convention that ensures the "
                    f"check trigger sorts first "
                    f"(e.g. a '_check_' prefix)."
                )
                raise CaveValidationError(msg)
