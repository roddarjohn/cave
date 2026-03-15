Ledger events
=============

Events are named, user-defined operations on a ledger.  Each event
compiles into a single PostgreSQL function (``LANGUAGE sql``) that
inserts rows into the ledger API view and returns the inserted rows
via ``RETURNING *``.

Import from the top-level ``pgcraft`` package::

    from pgcraft import LedgerEvent, ledger_balances


Reconciliation (diff mode)
--------------------------

The primary pattern is **reconciliation**: you have a transactional
object (invoices, shipments, work orders) with line items, and you
want the ledger to reflect the current state of those line items.
The system handles the arithmetic — it diffs the desired state against
existing balances and inserts only the correcting deltas.

This makes the operation **idempotent**: calling the same event twice
with the same input produces zero new rows the second time.  Amending
the source and re-calling inserts exactly the correcting entries.

**Example** — revenue recognition from invoice line items::

    from sqlalchemy import Integer, String, func, select
    from sqlalchemy.dialects.postgresql import ARRAY

    from pgcraft import LedgerEvent, ledger_balances

    # Assume `invoice_lines` is a SQLAlchemy Table with columns:
    #   id, invoice_id, account, amount

    recognize = LedgerEvent(
        name="recognize",
        input=lambda p: select(
            func.unnest(p("invoice_ids", ARRAY(Integer)))
            .label("invoice_id"),
        ),
        desired=lambda pginput: select(
            invoice_lines.c.invoice_id,
            invoice_lines.c.account,
            invoice_lines.c.amount.label("value"),
        ).where(
            invoice_lines.c.invoice_id.in_(
                select(pginput.c.invoice_id)
            )
        ),
        existing=ledger_balances("invoice_id", "account"),
        diff_keys=["invoice_id", "account"],
    )

The generated SQL uses CTEs: ``input`` (the function parameters),
``desired`` (the target state from the source table), ``existing``
(negated current balances), and ``deltas`` (the union of desired +
existing, filtered to non-zero values).  This is a single
``INSERT ... RETURNING *`` statement.

Key parameters:

``input``
    Lambda ``(p) -> Select`` that builds the input CTE.  ``p`` is a
    :class:`~pgcraft.ledger.events.ParamCollector` — each call to
    ``p("name", Type)`` records a function parameter and returns a
    ``literal_column("p_name")`` reference.

``desired``
    Lambda ``(pginput) -> Select`` that builds the desired-state CTE.
    ``pginput`` is a synthetic table reference to the ``input`` CTE.
    Typically joins against a source table (e.g. ``invoice_lines``)
    to produce the ledger entries.

``existing``
    Lambda ``(table, desired) -> Select`` that returns the negated
    current balances.  Use :func:`~pgcraft.ledger.events.ledger_balances`
    for the common pattern.

``diff_keys``
    Column names used for grouping.  Required when ``desired`` is set.

The ``ledger_balances`` helper produces an ``existing`` callable that
groups ``SUM(value) * -1`` by the specified keys, filtered to only
groups present in the desired CTE.


Usage
~~~~~

.. code-block:: sql

   -- Recognize revenue for two invoices:
   SELECT * FROM ops.ops_revenue_recognize(
       p_invoice_ids => ARRAY[1001, 1002]
   );

   -- Call again — idempotent, returns 0 rows:
   SELECT * FROM ops.ops_revenue_recognize(
       p_invoice_ids => ARRAY[1001, 1002]
   );

   -- Amend invoice 1001's line items in the source table,
   -- then re-reconcile — only correcting deltas are inserted:
   SELECT * FROM ops.ops_revenue_recognize(
       p_invoice_ids => ARRAY[1001]
   );


Simple event (input only)
-------------------------

A simple event is a special case — it provides only ``input`` and
inserts directly, without diffing.  This is what happens when you
omit ``desired``, ``existing``, and ``diff_keys``: there are no
existing entries to diff against, so every call inserts new rows.

Use this for fire-and-forget deltas where idempotency is not needed
(stock adjustments, manual corrections, one-off charges)::

    adjust = LedgerEvent(
        name="adjust",
        input=lambda p: select(
            p("warehouse", String).label("warehouse"),
            p("sku", String).label("sku"),
            p("value", Integer).label("value"),
            p("reason", String).label("reason"),
        ),
    )

.. tip::

   If your simple event maps one-to-one with an external identifier
   (e.g. an ``invoice_id``), prefer diff mode instead.  Diffing over
   that identifier makes the operation idempotent — calling the same
   event twice with the same input produces zero new rows the second
   time.


Factory integration
-------------------

Create the factory, then pass events to
:class:`~pgcraft.views.actions.LedgerActions`::

    from pgcraft import LedgerEvent, ledger_balances
    from pgcraft.factory import PGCraftLedger
    from pgcraft.views import APIView, LedgerActions

    recognize = LedgerEvent(
        name="recognize",
        input=lambda p: select(
            func.unnest(p("invoice_ids", ARRAY(Integer)))
            .label("invoice_id"),
        ),
        desired=lambda pginput: select(
            invoice_lines.c.invoice_id,
            invoice_lines.c.account,
            invoice_lines.c.amount.label("value"),
        ).where(
            invoice_lines.c.invoice_id.in_(
                select(pginput.c.invoice_id)
            )
        ),
        existing=ledger_balances("invoice_id", "account"),
        diff_keys=["invoice_id", "account"],
    )

    revenue = PGCraftLedger(
        tablename="revenue",
        schemaname="ops",
        metadata=metadata,
        schema_items=[
            Column("invoice_id", Integer, nullable=False),
            Column("account",    String, nullable=False),
        ],
    )

    APIView(
        source=revenue,
        grants=["select", "insert"],
    )
    LedgerActions(source=revenue, events=[recognize])


Declarative style
~~~~~~~~~~~~~~~~~

The ``@register`` decorator works with ledger events too.  Create
an ``APIView`` via the ``api=`` kwarg and attach views separately::

    from pgcraft import LedgerEvent, ledger_balances
    from pgcraft.declarative import register
    from pgcraft.views import BalanceView, LedgerActions

    recognize = LedgerEvent(
        name="recognize",
        input=lambda p: select(
            func.unnest(p("invoice_ids", ARRAY(Integer)))
            .label("invoice_id"),
        ),
        desired=lambda pginput: select(
            invoice_lines.c.invoice_id,
            invoice_lines.c.account,
            invoice_lines.c.amount.label("value"),
        ).where(
            invoice_lines.c.invoice_id.in_(
                select(pginput.c.invoice_id)
            )
        ),
        existing=ledger_balances("invoice_id", "account"),
        diff_keys=["invoice_id", "account"],
    )

    @register(
        metadata=metadata,
        api={"grants": ["select", "insert"]},
    )
    class Revenue:
        __tablename__ = "revenue"
        __table_args__ = {"schema": "ops"}

        invoice_id = Column(Integer, nullable=False)
        account = Column(String, nullable=False)


Naming convention
~~~~~~~~~~~~~~~~~

Generated function names follow the pattern
``{schema}_{table}_{name}`` and are registered in the ledger's schema:

=========================  =====================================
Function                   Example
=========================  =====================================
``{table}_{name}``         ``ops_revenue_recognize``
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
