Ledger Tables
=============

Ledger tables are append-only tables designed for recording immutable
events such as financial transactions, audit logs, or metric
observations.  Every row has a ``value`` column, an ``entry_id`` UUID
for correlating related entries, a ``created_at`` timestamp, and
consumer-provided dimension columns.

Unlike dimensions, ledger entries are never updated or deleted.  The
API view only allows ``SELECT`` and ``INSERT``.

Choose the variant that matches your data:

- **Basic ledger** -- a single append-only table with a value column.
  Best for simple event logs, metrics, or single-entry accounting.
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


Balance Views
~~~~~~~~~~~~~

Use :class:`~pgcraft.plugins.ledger.LedgerBalanceViewPlugin` to create
a view that shows current balances per dimension group:

.. code-block:: python

   from pgcraft.factory.ledger import LedgerResourceFactory
   from pgcraft.plugins.ledger import LedgerBalanceViewPlugin

   LedgerResourceFactory(
       tablename="transactions",
       schemaname="finance",
       metadata=metadata,
       schema_items=[
           Column("account", String, nullable=False),
           Column("category", String),
       ],
       extra_plugins=[
           LedgerBalanceViewPlugin(dimensions=["account"]),
       ],
   )

This registers a ``transactions_balances`` view:

.. code-block:: sql

   SELECT account, balance
   FROM finance.transactions_balances;

The view name follows the naming convention and can be customised via
``metadata.naming_convention["ledger_balance_view"]``.


Double-Entry Ledger
-------------------

A double-entry ledger extends the basic ledger with debit/credit
semantics.  Two additional plugins are required:

- :class:`~pgcraft.plugins.ledger.DoubleEntryPlugin` -- adds the
  ``direction`` column to the table.
- :class:`~pgcraft.plugins.ledger.DoubleEntryTriggerPlugin` -- registers
  an ``AFTER INSERT FOR EACH STATEMENT`` constraint trigger that
  validates debits equal credits for every ``entry_id`` in the batch.

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
           Column("account", String, nullable=False),
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

``DoubleEntryPlugin``
    Writes ``"double_entry_columns"`` (the direction column name) and
    appends the direction column to ``ctx.injected_columns``.

``DoubleEntryTriggerPlugin``
    Reads ``"primary"``, ``"double_entry_columns"``,
    ``"entry_id_column"``.

``LedgerBalanceViewPlugin``
    Reads ``"primary"``.  Writes ``"balance_view"`` (the view name).
