Ledger Events
=============

Events are named, user-defined operations on a ledger.  Each event
compiles into a single PostgreSQL function (``LANGUAGE sql``) that
inserts rows into the ledger API view and returns the inserted rows
via ``RETURNING *``.

Two modes are available:

- **Simple mode** — provide only ``input``.  The input select's
  columns are inserted directly into the ledger.
- **Diff mode** — provide ``input``, ``desired``, ``existing``, and
  ``diff_keys``.  The desired and existing selects are unioned and
  only non-zero deltas are inserted.

Import from the top-level ``pgcraft`` package::

    from pgcraft import LedgerEvent, ledger_balances


Choosing a Mode
---------------

Use **simple mode** when a discrete event occurs (a shipment departs,
a charge is applied) and you want to record the exact delta value.

Use **diff mode** when you receive a snapshot of desired state (e.g.
a nightly inventory feed from a WMS, an ERP sync) and need the ledger
to reflect that snapshot.  The system handles the arithmetic.

Both modes can coexist on the same ledger; a common pattern is to use
diff mode for bulk reconciliation and simple mode for intraday
adjustments.


Simple Event
------------

A simple event takes parameters via a ``ParamCollector`` and inserts
them directly::

    from pgcraft import LedgerEvent

    adjust = LedgerEvent(
        name="adjust",
        input=lambda p: select(
            p("warehouse", String).label("warehouse"),
            p("sku", String).label("sku"),
            p("value", Integer).label("value"),
            p("reason", String).label("reason"),
        ),
    )

The generated function accepts the declared parameters and returns
``SETOF api_view`` (the inserted rows).


Diff Event (Reconciliation)
---------------------------

A diff event compares desired state against existing ledger balances
and inserts only the correcting deltas::

    from pgcraft import LedgerEvent, ledger_balances

    reconcile = LedgerEvent(
        name="reconcile",
        input=lambda p: select(
            p("warehouse", String).label("warehouse"),
            p("sku", String).label("sku"),
            p("value", Integer).label("value"),
            p("source", String).label("source"),
        ),
        desired=lambda pginput: select(
            pginput.c.warehouse, pginput.c.sku,
            pginput.c.value, pginput.c.source,
        ),
        existing=ledger_balances("warehouse", "sku"),
        diff_keys=["warehouse", "sku"],
    )

Key parameters:

``diff_keys``
    Column names used for grouping.  Required when ``desired`` is set.

``existing``
    A callable ``(table, desired) -> Select`` that returns the negated
    current balances.  Use :func:`~pgcraft.ledger.events.ledger_balances`
    for the common pattern.

``desired``
    A callable ``(input) -> Select`` that builds the desired-state CTE
    from the input CTE reference.

The generated SQL uses CTEs: ``input``, ``desired``, ``existing``, and
``deltas`` (the union of desired + existing, filtered to non-zero
values).  This is a single ``INSERT ... RETURNING *`` statement using
``LANGUAGE sql``.

The ``ledger_balances`` helper produces an ``existing`` callable that
groups ``SUM(value) * -1`` by the specified keys, filtered to only
groups present in the desired CTE.


Factory Integration
-------------------

Pass a list of events to :class:`~pgcraft.factory.ledger.LedgerResourceFactory`
via the ``events`` parameter::

    from pgcraft import LedgerEvent, ledger_balances
    from pgcraft.factory.ledger import LedgerResourceFactory

    reconcile = LedgerEvent(
        name="reconcile",
        input=lambda p: select(
            p("warehouse", String).label("warehouse"),
            p("sku", String).label("sku"),
            p("value", Integer).label("value"),
        ),
        desired=lambda pginput: select(
            pginput.c.warehouse, pginput.c.sku, pginput.c.value,
        ),
        existing=ledger_balances("warehouse", "sku"),
        diff_keys=["warehouse", "sku"],
    )

    adjust = LedgerEvent(
        name="adjust",
        input=lambda p: select(
            p("warehouse", String).label("warehouse"),
            p("sku", String).label("sku"),
            p("value", Integer).label("value"),
            p("reason", String).label("reason"),
        ),
    )

    LedgerResourceFactory(
        tablename="inventory",
        schemaname="ops",
        metadata=metadata,
        schema_items=[
            Column("warehouse", String, nullable=False),
            Column("sku",       String, nullable=False),
            Column("reason",    String, nullable=True),
        ],
        events=[reconcile, adjust],
    )

``LedgerActionsPlugin`` is appended to ``extra_plugins`` automatically
and runs after the table and API view have been created.


Naming Convention
~~~~~~~~~~~~~~~~~

Generated function names follow the pattern
``{schema}_{table}_{name}`` and are registered in the ledger's schema:

=========================  =====================================
Function                   Example
=========================  =====================================
``{table}_{name}``         ``ops_inventory_reconcile``
``{table}_{name}``         ``ops_inventory_adjust``
=========================  =====================================

Event names must be unique within a ledger.


Validation
----------

All event configuration is validated when
:class:`~pgcraft.plugins.ledger_actions.LedgerActionsPlugin` runs
(at factory construction time):

- Event names must be unique within a ledger.
- ``diff_keys`` is required when ``desired`` is set.
- ``existing`` requires ``desired`` to be set.
- Every key in ``diff_keys`` must name a column on the ledger table.

Violations raise :class:`~pgcraft.errors.PGCraftValidationError`.
