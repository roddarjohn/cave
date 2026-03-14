Ledger Actions
==============

Actions are named, user-defined operations on a ledger.  Instead of
inserting raw delta rows directly, callers use typed PostgreSQL
functions generated from action declarations.

Two types are available:

- :class:`~pgcraft.ledger.actions.StateAction` -- **declarative
  reconciliation**: the caller describes what the ledger *should*
  look like; the system diffs against current balances and inserts
  only the correcting rows.
- :class:`~pgcraft.ledger.actions.EventAction` -- **explicit delta**:
  a thin typed wrapper around a direct INSERT.  Best for one-off
  adjustments where the delta is known upfront.

Both types are imported from the top-level ``pgcraft`` package::

    from pgcraft import StateAction, EventAction


Choosing an Action Type
-----------------------

Use **StateAction** when you receive a snapshot of desired state (e.g.
a nightly inventory feed from a WMS, an ERP sync) and need the ledger
to reflect that snapshot.  The system handles the arithmetic.

Use **EventAction** when a discrete event occurs (a shipment departs,
a charge is applied) and you want to record the exact delta value.

Both types can coexist on the same ledger; a common pattern is to use
``StateAction`` for bulk reconciliation and ``EventAction`` for
intraday adjustments.


StateAction
-----------

``StateAction`` generates two PostgreSQL functions:

- ``{schema}.{table}_{name}_begin()`` — creates (or re-uses) a session-
  scoped temporary staging table and truncates it.
- ``{schema}.{table}_{name}_apply(write_only_params...)`` — diffs the
  staging table against the current ledger balances, inserts correcting
  rows into the API view, truncates staging, and returns the count of
  rows inserted as ``BIGINT``.

**Configuration:**

.. literalinclude:: ../scripts/examples/ledger_state_action.py
   :language: python
   :start-after: # --- example start ---
   :end-before: # --- example end ---
   :dedent:

Key parameters:

``diff_keys``
    Non-empty list of column names that identify each balance group
    (e.g. ``["warehouse", "sku"]``).  Every column must exist on the
    ledger table.

``partial`` (default ``True``)
    When ``True``, only the diff-key groups present in the staging
    table are reconciled.  Groups absent from staging are left
    unchanged.

    When ``False``, groups absent from staging that carry a non-zero
    ledger balance are also zeroed out.  Use this for full-snapshot
    feeds where any omitted row implies a desired balance of zero.

``write_only_keys``
    Optional column names written to every inserted delta row as
    metadata.  They do not participate in the diff.  Each becomes an
    optional ``DEFAULT NULL`` parameter on ``_apply``.

**SQL usage:**

.. literalinclude:: ../scripts/examples/ledger_state_action.sql
   :language: sql

The staging table name follows the pattern ``_{table}_{name}``
(e.g. ``_inventory_reconcile``).  It is a PostgreSQL temporary table
with ``ON COMMIT DELETE ROWS``, so it lives for the duration of the
session and its rows are cleared automatically at each transaction
commit.

The apply function returns a single row with a ``delta`` column
(``BIGINT``) indicating how many correcting rows were inserted.


Python runtime helper
~~~~~~~~~~~~~~~~~~~~~

Use :class:`~pgcraft.runtime.ledger.LedgerStateRecorder` to drive the
begin/apply lifecycle from Python without writing the function calls
by hand::

    from pgcraft.runtime.ledger import LedgerStateRecorder

    with LedgerStateRecorder(session, reconcile, source="nightly") as staging:
        session.execute(
            staging.insert(),
            [
                {"warehouse": "east", "sku": "WIDGET-A", "value": 200},
                {"warehouse": "east", "sku": "WIDGET-B", "value": 50},
            ],
        )
    # apply is called automatically on clean exit

``__enter__`` calls ``_begin`` and returns the staging
:class:`~sqlalchemy.Table`.  ``__exit__`` calls ``_apply`` (with any
``write_only_keys`` passed as keyword arguments) on a clean exit;
on an exception it is skipped and the surrounding transaction rollback
handles cleanup.

For async code use :class:`~pgcraft.runtime.ledger.AsyncLedgerStateRecorder`
with ``async with``::

    from pgcraft.runtime.ledger import AsyncLedgerStateRecorder

    async with AsyncLedgerStateRecorder(session, reconcile, source="nightly") as staging:
        await session.execute(staging.insert(), rows)


EventAction
-----------

``EventAction`` generates a single PostgreSQL function:

``{schema}.{table}_{name}(p_value <type>, dim_params..., write_only_params... DEFAULT NULL)``

This is a thin typed INSERT into the ledger API view.

**Configuration:**

.. literalinclude:: ../scripts/examples/ledger_event_action.py
   :language: python
   :start-after: # --- example start ---
   :end-before: # --- example end ---
   :dedent:

Key parameters:

``dim_keys`` (optional)
    Explicit list of dimension column names to expose as required
    parameters.  When ``None`` (default), the dimension keys are
    resolved in priority order:

    1. ``diff_keys`` from a sibling :class:`~pgcraft.ledger.actions.StateAction`
       on the same ledger, if one exists.
    2. All writable (non-PK, non-computed) columns from the ledger
       schema items.

``write_only_keys``
    Same as ``StateAction``: optional metadata columns appended to
    the function signature with ``DEFAULT NULL``.

**SQL usage:**

.. literalinclude:: ../scripts/examples/ledger_event_action.sql
   :language: sql

The function accepts named parameters; all dimension parameters are
required and ``value`` is always first.


Factory integration
-------------------

Pass a list of actions to :class:`~pgcraft.factory.ledger.LedgerResourceFactory`
via the ``actions`` parameter::

    from pgcraft import StateAction, EventAction
    from pgcraft.factory.ledger import LedgerResourceFactory

    reconcile = StateAction(name="reconcile", diff_keys=["warehouse", "sku"])
    adjust    = EventAction(name="adjust", write_only_keys=["reason"])

    LedgerResourceFactory(
        tablename="inventory",
        schemaname="ops",
        metadata=metadata,
        schema_items=[
            Column("warehouse", String, nullable=False),
            Column("sku",       String, nullable=False),
            Column("reason",    String, nullable=True),
        ],
        actions=[reconcile, adjust],
    )

``LedgerActionsPlugin`` is appended to ``extra_plugins`` automatically
and runs after the table and API view have been created.

After the factory runs, the private attributes ``reconcile._begin_fn``,
``reconcile._apply_fn``, ``reconcile._staging_table``, and
``adjust._record_fn`` are populated.  These are used by the runtime
helpers and can be inspected for debugging.


Naming convention
~~~~~~~~~~~~~~~~~

Generated function names follow the pattern
``{schema}_{table}_{op}`` and are registered in the ledger's schema
(not the API schema):

=========================  =====================================
Function                   Example
=========================  =====================================
``{table}_{name}_begin``   ``ops_inventory_reconcile_begin``
``{table}_{name}_apply``   ``ops_inventory_reconcile_apply``
``{table}_{name}``         ``ops_inventory_adjust``
=========================  =====================================

Action names must be unique within a ledger.


The pgcraft utility schema
--------------------------

``StateAction`` apply functions delegate to a shared utility function
``pgcraft.ledger_apply_state``.  This function is registered on
``metadata`` by :func:`~pgcraft.utils.pgcraft_schema._ensure_pgcraft_utilities`
and included in Alembic migrations automatically.

The utility schema name defaults to ``"pgcraft"`` and can be changed
via :class:`~pgcraft.config.PGCraftConfig`::

    from pgcraft.config import PGCraftConfig
    from sqlalchemy import MetaData

    metadata = MetaData()
    metadata.info["pgcraft_config"] = PGCraftConfig(utility_schema="myapp_internal")

The ``pgcraft`` schema (or whichever name you choose) must be created
before the functions that reference it are called.  Alembic handles
this automatically because ``_ensure_pgcraft_utilities`` registers the
schema via ``sqlalchemy-declarative-extensions``.


Validation
----------

All action configuration is validated when
:class:`~pgcraft.plugins.ledger_actions.LedgerActionsPlugin` runs
(at factory construction time):

- ``StateAction.diff_keys`` must be non-empty.
- Every key in ``diff_keys`` and ``write_only_keys`` must name a
  column on the ledger table.
- ``write_only_keys`` must not overlap with ``diff_keys`` or
  contain ``"value"``.
- Action names must be unique within a ledger.

Violations raise :class:`~pgcraft.errors.PGCraftValidationError`.
