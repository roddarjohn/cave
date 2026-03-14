"""Ledger action configuration objects.

Actions define named, typed operations on a ledger.  Each action
generates one or more PostgreSQL functions when passed to
:class:`~pgcraft.plugins.ledger_actions.LedgerActionsPlugin`.

Two action types are provided:

- :class:`StateAction` -- declarative reconciliation: the caller
  describes desired state and the system computes and inserts the
  correcting deltas automatically.
- :class:`EventAction` -- explicit delta insert: a thin typed
  wrapper around a direct INSERT into the ledger view.
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy import Table


@dataclass
class Action(ABC):
    """Base class for ledger actions.

    Args:
        name: Unique action name within the ledger.  Used as a
            suffix in generated PostgreSQL function names.
        write_only_keys: Column names that are written on every
            generated row but are not used for diff/identity
            matching.  Each becomes an optional SQL parameter
            (``DEFAULT NULL``) on the generated function.

    """

    name: str
    write_only_keys: list[str] = field(default_factory=list)


@dataclass
class StateAction(Action):
    """Declarative reconciliation action.

    The caller populates a staging temp table with desired state
    rows (diff key combinations + target value), then calls the
    generated ``_apply`` function.  The system computes the delta
    between the desired state and the current ledger balance for
    each diff-key group and inserts only the correcting entries.

    Two PostgreSQL functions are generated:

    - ``{schema}.{table}_{name}_begin()`` -- creates (or re-uses)
      the staging temp table and truncates it.
    - ``{schema}.{table}_{name}_apply(write_only_params...)``
      -- computes deltas, inserts correcting rows, truncates
      staging, and returns the number of rows inserted.

    After :class:`~pgcraft.plugins.ledger_actions.LedgerActionsPlugin`
    runs, the following private attributes are populated and used
    by :class:`~pgcraft.runtime.ledger.LedgerStateRecorder`:

    - ``_staging_table``: SQLAlchemy :class:`~sqlalchemy.Table`
      for the temp table (isolated MetaData, not migrated).
    - ``_begin_fn``: Fully-qualified begin function name.
    - ``_apply_fn``: Fully-qualified apply function name.

    Args:
        name: Unique action name.
        diff_keys: Non-empty list of column names used to
            identify each balance group (e.g. ``["sku",
            "warehouse"]``).  Must be column names on the
            ledger table.
        partial: When ``True`` (default), only reconcile
            diff-key groups present in the staging table.
            When ``False``, also zero out groups absent from
            staging that have a non-zero balance.
        write_only_keys: Column names appended to each
            inserted row without participating in diff.

    """

    diff_keys: list[str] = field(default_factory=list)
    partial: bool = True

    # Populated by LedgerActionsPlugin.run()
    _staging_table: Table | None = field(default=None, init=False, repr=False)
    _begin_fn: str | None = field(default=None, init=False, repr=False)
    _apply_fn: str | None = field(default=None, init=False, repr=False)


@dataclass
class EventAction(Action):
    """Explicit delta-insert action.

    Generates a single PostgreSQL function that inserts one row into
    the ledger API view with all dimension and value columns as
    typed parameters.

    The dimension parameters are resolved from (in priority order):

    1. ``dim_keys`` if explicitly set on this action.
    2. ``diff_keys`` from a sibling :class:`StateAction` in the
       same ledger (if one exists).
    3. All writable (non-PK, non-computed) columns from the ledger
       schema items.

    After :class:`~pgcraft.plugins.ledger_actions.LedgerActionsPlugin`
    runs, ``_record_fn`` holds the fully-qualified function name.

    Args:
        name: Unique action name.
        dim_keys: Explicit list of dimension column names to use
            as parameters.  When ``None``, falls back to sibling
            StateAction diff_keys or all writable columns.
        write_only_keys: Optional parameter columns appended to
            the function signature with ``DEFAULT NULL``.

    """

    dim_keys: list[str] | None = field(default=None)

    # Populated by LedgerActionsPlugin.run()
    _record_fn: str | None = field(default=None, init=False, repr=False)
