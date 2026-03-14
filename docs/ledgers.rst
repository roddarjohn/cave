Ledger Tables
=============

Ledger tables are append-only tables designed for recording immutable
events such as status transitions, resource consumption, or financial
transactions.  Every row has a ``value`` column, an ``entry_id`` UUID
for correlating related entries, a ``created_at`` timestamp, and
consumer-provided dimension columns.

Unlike dimensions, ledger entries are never updated or deleted.  The
API view only allows ``SELECT`` and ``INSERT``.

Choose the variant that matches your data:

- **Basic ledger** -- a single append-only table with a value column.
  Best for event logs, status tracking, or metric observations.
- **Double-entry ledger** -- adds a ``direction`` column
  (``'debit'``/``'credit'``) and a constraint trigger that validates
  debits equal credits per ``entry_id``.  Best for financial journals.


Basic Ledger
------------

A single append-only table with an API view.  Insert-only: UPDATE and
DELETE on the API view raise a PostgreSQL error.

**Example configuration:**

.. literalinclude:: ../scripts/examples/ledger.py
   :language: python
   :start-after: # --- example start ---
   :end-before: # --- example end ---
   :dedent:

**Usage** -- all operations go through the API view:

.. literalinclude:: ../scripts/examples/ledger.sql
   :language: sql

.. include:: _generated/dim_ledger.rst


Latest View
~~~~~~~~~~~

Use :class:`~pgcraft.plugins.ledger.LedgerLatestViewPlugin` to create
a view that shows the most recent row per dimension group.  This is
useful for status-tracking ledgers where you care about current state
rather than historical sums:

.. code-block:: python

   from pgcraft.factory.ledger import LedgerResourceFactory
   from pgcraft.plugins.ledger import LedgerLatestViewPlugin

   LedgerResourceFactory(
       tablename="order_events",
       schemaname="ops",
       metadata=metadata,
       schema_items=[
           Column("order_id", String, nullable=False),
           Column("status", String, nullable=False),
       ],
       extra_plugins=[
           LedgerLatestViewPlugin(dimensions=["order_id"]),
       ],
   )

This registers an ``order_events_latest`` view using PostgreSQL's
``DISTINCT ON``:

.. code-block:: sql

   -- Current status per order:
   SELECT * FROM ops.order_events_latest;

The view name follows the naming convention and can be customised via
``metadata.naming_convention["ledger_latest_view"]``.


Balance Views
~~~~~~~~~~~~~

Use :class:`~pgcraft.plugins.ledger.LedgerBalanceViewPlugin` to create
a view that shows current balances (``SUM(value)``) per dimension group.
Best for ledgers where the running total is meaningful (inventory,
resource quotas, point systems):

.. code-block:: python

   from pgcraft.factory.ledger import LedgerResourceFactory
   from pgcraft.plugins.ledger import LedgerBalanceViewPlugin

   LedgerResourceFactory(
       tablename="stock_movements",
       schemaname="inventory",
       metadata=metadata,
       schema_items=[
           Column("warehouse", String, nullable=False),
           Column("sku", String, nullable=False),
       ],
       extra_plugins=[
           LedgerBalanceViewPlugin(dimensions=["warehouse", "sku"]),
       ],
   )

This registers a ``stock_movements_balances`` view:

.. code-block:: sql

   SELECT warehouse, sku, balance
   FROM inventory.stock_movements_balances;

The view name follows the naming convention and can be customised via
``metadata.naming_convention["ledger_balance_view"]``.


Balance Constraints
~~~~~~~~~~~~~~~~~~~

Use :class:`~pgcraft.plugins.ledger.LedgerBalanceCheckPlugin` to enforce
that ``SUM(value)`` for a dimension group never drops below a threshold.
This is useful for preventing negative inventory, overdrafts, or
exceeding resource quotas:

.. code-block:: python

   from pgcraft.plugins.ledger import (
       LedgerBalanceCheckPlugin,
       LedgerBalanceViewPlugin,
   )

   LedgerResourceFactory(
       tablename="stock_movements",
       schemaname="inventory",
       metadata=metadata,
       schema_items=[
           Column("warehouse", String, nullable=False),
           Column("sku", String, nullable=False),
       ],
       extra_plugins=[
           LedgerBalanceViewPlugin(dimensions=["warehouse", "sku"]),
           LedgerBalanceCheckPlugin(
               dimensions=["warehouse", "sku"],
               min_balance=0,   # default; cannot go negative
           ),
       ],
   )

The trigger fires ``AFTER INSERT FOR EACH STATEMENT`` and checks only
the dimension groups affected by the new rows.  If any group's balance
falls below ``min_balance``, the entire statement is rejected:

.. code-block:: sql

   -- Succeeds (balance stays >= 0):
   INSERT INTO inventory.stock_movements (value, warehouse, sku)
   VALUES (100, 'east', 'WIDGET-A');

   -- Fails (balance would go to -50):
   INSERT INTO inventory.stock_movements (value, warehouse, sku)
   VALUES (-150, 'east', 'WIDGET-A');
   -- ERROR: ledger balance violation ...

Set ``min_balance`` to a different value for other use cases:

.. code-block:: python

   # Allow overdraft up to -1000:
   LedgerBalanceCheckPlugin(
       dimensions=["account"],
       min_balance=-1000,
   )


Double-Entry Ledger
-------------------

A double-entry ledger extends the basic ledger with debit/credit
semantics.  Two additional plugins are required:

- :class:`~pgcraft.plugins.ledger.DoubleEntryPlugin` -- adds the
  ``direction`` column to the table.
- :class:`~pgcraft.plugins.ledger.DoubleEntryTriggerPlugin` -- registers
  an ``AFTER INSERT FOR EACH STATEMENT`` constraint trigger that
  validates debits equal credits for every ``entry_id`` in the batch.

Dimension columns like ``category`` belong on a separate dimension
table (e.g. ``accounts``), not on the journal itself.  The journal
references the dimension via a foreign key:

**Example configuration:**

.. literalinclude:: ../scripts/examples/double_entry.py
   :language: python
   :start-after: # --- example start ---
   :end-before: # --- example end ---
   :dedent:

**Usage:**

.. literalinclude:: ../scripts/examples/double_entry.sql
   :language: sql

.. include:: _generated/dim_double_entry.rst


How the constraint trigger works
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The trigger fires ``AFTER INSERT FOR EACH STATEMENT`` using a
``REFERENCING NEW TABLE AS new_entries`` transition table.  This means:

1. All rows in a single ``INSERT`` statement are visible to the trigger.
2. The trigger groups by ``entry_id`` and checks that ``SUM(value)``
   where ``direction = 'debit'`` equals ``SUM(value)`` where
   ``direction = 'credit'``.
3. If any ``entry_id`` is unbalanced, the entire statement is rejected.

This approach allows multi-row inserts (debit + credit in one
``INSERT``) to succeed, while single-sided inserts are correctly
rejected.

.. note::

   The constraint trigger fires on the **backing table**, not on the
   API view.  When inserting through the API view, the INSTEAD OF
   INSERT trigger routes each row to the backing table.  Because
   INSTEAD OF triggers fire row-by-row, each row is a separate
   statement from the backing table's perspective.  This means that
   when inserting through the API view, inserts must go directly to
   the backing table to benefit from statement-level batching.


Customising the Value Type
--------------------------

The default value type is ``INTEGER``.  To use ``NUMERIC`` for decimal
precision, pass ``value_type="numeric"`` to
:class:`~pgcraft.plugins.ledger.LedgerTablePlugin`:

.. code-block:: python

   from pgcraft.factory.ledger import LedgerResourceFactory
   from pgcraft.plugins.ledger import LedgerTablePlugin
   from pgcraft.plugins.api import APIPlugin
   from pgcraft.plugins.created_at import CreatedAtPlugin
   from pgcraft.plugins.entry_id import UUIDEntryIDPlugin
   from pgcraft.plugins.pk import SerialPKPlugin

   LedgerResourceFactory(
       tablename="payments",
       schemaname="finance",
       metadata=metadata,
       schema_items=[
           Column("account_id", Integer, nullable=False),
       ],
       plugins=[
           SerialPKPlugin(),
           UUIDEntryIDPlugin(),
           CreatedAtPlugin(),
           LedgerTablePlugin(value_type="numeric"),
           APIPlugin(grants=["select", "insert"]),
       ],
   )


Using a UUID Primary Key
------------------------

Swap :class:`~pgcraft.plugins.pk.SerialPKPlugin` for
:class:`~pgcraft.plugins.pk.UUIDV4PKPlugin` to use a UUIDv4 primary
key with ``gen_random_uuid()`` as the server default:

.. code-block:: python

   from pgcraft.plugins.pk import UUIDV4PKPlugin

   LedgerResourceFactory(
       tablename="events",
       schemaname="analytics",
       metadata=metadata,
       schema_items=[Column("event_type", String, nullable=False)],
       plugins=[
           UUIDV4PKPlugin(),
           UUIDEntryIDPlugin(),
           CreatedAtPlugin(),
           LedgerTablePlugin(),
           APIPlugin(grants=["select", "insert"]),
       ],
   )


Ledger Actions
--------------

Use :doc:`ledger_actions` to attach named PostgreSQL functions to a
ledger.  Two types are provided:

- :class:`~pgcraft.ledger.actions.StateAction` -- declarative
  reconciliation from a desired-state snapshot.
- :class:`~pgcraft.ledger.actions.EventAction` -- explicit typed
  delta insert.

See the :doc:`ledger_actions` page for full documentation.


Plugin Reference
----------------

All ledger plugins are documented in the :doc:`api` reference.  The
key context keys are:

``SerialPKPlugin`` / ``UUIDV4PKPlugin``
    Writes ``"pk_columns"``.

``UUIDEntryIDPlugin``
    Writes ``"entry_id_column"`` (a ``Column`` object) and appends the
    column to ``ctx.injected_columns``.

``CreatedAtPlugin``
    Writes ``"created_at_column"`` (the column name string) and appends
    a ``DateTime`` column to ``ctx.injected_columns``.

``LedgerTablePlugin``
    Reads ``"pk_columns"`` and spreads ``ctx.injected_columns`` into
    the table.  Requires ``"entry_id_column"`` and
    ``"created_at_column"`` for plugin ordering.  Writes ``"primary"``
    (the table) and ``"__root__"``.

``APIPlugin``
    Reads ``"primary"``.  Writes ``"api"`` (the view).

``LedgerTriggerPlugin``
    Reads ``"primary"``, ``"api"``, ``"entry_id_column"``.

``LedgerLatestViewPlugin``
    Reads ``"primary"`` and ``"created_at_column"``.  Writes
    ``"latest_view"`` (the view name).

``LedgerBalanceViewPlugin``
    Reads ``"primary"``.  Writes ``"balance_view"`` (the view name).

``LedgerBalanceCheckPlugin``
    Reads ``"primary"``.  Registers an AFTER INSERT trigger enforcing
    ``SUM(value) >= min_balance`` per dimension group.

``DoubleEntryPlugin``
    Writes ``"double_entry_columns"`` (the direction column name) and
    appends the direction column to ``ctx.injected_columns``.

``DoubleEntryTriggerPlugin``
    Reads ``"primary"``, ``"double_entry_columns"``,
    ``"entry_id_column"``.
