Naming conventions
==================

pgcraft generates database objects automatically — tables, views, functions,
and triggers — and uses a naming convention system to derive their names
deterministically.  Conventions are stored in the SQLAlchemy
:class:`~sqlalchemy.schema.MetaData` ``naming_convention`` dict, giving you
one place to control every generated name in your schema.

This builds directly on SQLAlchemy's built-in `constraint naming conventions`_
feature.  If you are not already familiar with it, read that guide first —
pgcraft extends the same mechanism rather than inventing a parallel system.

.. _constraint naming conventions: https://docs.sqlalchemy.org/en/20/core/constraints.html#configuring-constraint-naming-conventions

Setting up naming conventions
------------------------------

Pass :func:`~pgcraft.pg_build_naming_conventions` to your
:class:`~sqlalchemy.schema.MetaData` when you create it:

.. code-block:: python

   from sqlalchemy import MetaData
   from pgcraft import pg_build_naming_conventions

   metadata = MetaData(naming_convention=pg_build_naming_conventions())

This populates the standard SQLAlchemy constraint keys (``pk``, ``fk``,
``uq``, ``ix``, ``ck``) with length-safe token callables, and also seeds the
pgcraft-specific keys that plugins use for views, functions, and triggers.
Without it, SQLAlchemy leaves constraint names as ``None`` and Alembic
autogenerate cannot track them reliably.

The ``max_length`` parameter (default ``63``) controls the maximum character
length of generated constraint names.  PostgreSQL's identifier limit is 63
bytes, so the default matches that.  Names that would exceed this limit are
truncated and an 8-character MD5 digest is appended to avoid collisions:

.. code-block:: python

   metadata = MetaData(naming_convention=pg_build_naming_conventions(max_length=63))


Overriding a specific convention
---------------------------------

Every naming convention key is just a string in the ``naming_convention`` dict.
Set it *after* calling :func:`~pgcraft.pg_build_naming_conventions` to override
only that key while keeping all others:

.. code-block:: python

   metadata = MetaData(naming_convention=pg_build_naming_conventions())

   # Give the "prices" append-only dimension custom table names.
   metadata.naming_convention["append_only_root"] = "%(table_name)s_ids"
   metadata.naming_convention["append_only_attributes"] = "%(table_name)s_log"

Overrides are global — they apply to every factory that uses that key.  To
target a specific dimension only, embed the table name directly in the template:

.. code-block:: python

   # Only renames the root table for "prices"; other dimensions are unaffected.
   metadata.naming_convention["append_only_root"] = "price_ids"

Templates use Python ``%``-style interpolation.  The available substitution
variables for each key are listed in the reference below.


Convention reference
---------------------

Constraint names
~~~~~~~~~~~~~~~~

These keys are provided by :func:`~pgcraft.pg_build_naming_conventions` and
govern how SQLAlchemy names database constraints and indexes.  They follow the
same ``%(token)s`` interpolation format described in the SQLAlchemy
`constraint naming conventions`_ guide.

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Key
     - Default template
     - Example output
   * - ``pk``
     - ``%(pk_name)s``
     - ``pk__products__id``
   * - ``fk``
     - ``%(fk_name)s``
     - ``fk__orders__product_id__products``
   * - ``uq``
     - ``%(uq_name)s``
     - ``uq__products__sku``
   * - ``ix``
     - ``%(ix_name)s``
     - ``ix__products__name``
   * - ``ck``
     - ``%(ck_name)s``
     - ``ck__orders__status``

The name tokens (``pk_name``, ``fk_name``, etc.) are callable objects that
build the full name from the table and column names.  SQLAlchemy's naming
convention system supports both string templates and callables as values — the
callables here are an example of that.  You should not override them directly;
override the corresponding template key (``pk``, ``fk``, etc.) instead if you
need a different format.

Simple dimension
~~~~~~~~~~~~~~~~

Used by :class:`~pgcraft.plugins.simple.SimpleTriggerPlugin` when creating
``INSTEAD OF`` triggers on the API view.

Available substitutions: ``%(table_name)s``, ``%(schema)s``, ``%(op)s``
(``insert``, ``update``, or ``delete``).

.. list-table::
   :header-rows: 1
   :widths: 30 45 25

   * - Key
     - Default template
     - Names
   * - ``simple_function``
     - ``%(schema)s_%(table_name)s_%(op)s``
     - INSTEAD OF trigger function
   * - ``simple_trigger``
     - ``%(schema)s_%(table_name)s_%(op)s``
     - INSTEAD OF trigger

Append-only dimension
~~~~~~~~~~~~~~~~~~~~~~

Used by :class:`~pgcraft.plugins.append_only.AppendOnlyTablePlugin` for the
two backing tables, and by
:class:`~pgcraft.plugins.append_only.AppendOnlyTriggerPlugin` for INSTEAD OF
triggers on the API view.

Available substitutions: ``%(table_name)s``, ``%(schema)s``, ``%(op)s``.

.. list-table::
   :header-rows: 1
   :widths: 30 45 25

   * - Key
     - Default template
     - Names
   * - ``append_only_root``
     - ``%(table_name)s_root``
     - Entity (root) table
   * - ``append_only_attributes``
     - ``%(table_name)s_attributes``
     - Attributes log table
   * - ``append_only_function``
     - ``%(schema)s_%(table_name)s_%(op)s``
     - INSTEAD OF trigger function
   * - ``append_only_trigger``
     - ``%(schema)s_%(table_name)s_%(op)s``
     - INSTEAD OF trigger

EAV dimension
~~~~~~~~~~~~~~

Used by :class:`~pgcraft.plugins.eav.EAVTablePlugin` for the two backing
tables, and by :class:`~pgcraft.plugins.eav.EAVTriggerPlugin` for INSTEAD OF
triggers on the API view.

Available substitutions: ``%(table_name)s``, ``%(schema)s``, ``%(op)s``.

.. list-table::
   :header-rows: 1
   :widths: 30 45 25

   * - Key
     - Default template
     - Names
   * - ``eav_entity``
     - ``%(table_name)s_entity``
     - Entity (root) table
   * - ``eav_attribute``
     - ``%(table_name)s_attribute``
     - Attribute log table
   * - ``eav_function``
     - ``%(schema)s_%(table_name)s_%(op)s``
     - INSTEAD OF trigger function
   * - ``eav_trigger``
     - ``%(schema)s_%(table_name)s_%(op)s``
     - INSTEAD OF trigger

Check plugin
~~~~~~~~~~~~~

Used by :class:`~pgcraft.plugins.check.TriggerCheckPlugin` when adding
validation triggers to EAV dimensions.

Available substitutions: ``%(table_name)s``, ``%(schema)s``, ``%(op)s``.

.. list-table::
   :header-rows: 1
   :widths: 30 45 25

   * - Key
     - Default template
     - Names
   * - ``check_function``
     - ``_check_%(schema)s_%(table_name)s_%(op)s``
     - Validation trigger function
   * - ``check_trigger``
     - ``_check_%(schema)s_%(table_name)s_%(op)s``
     - Validation trigger

Ledger
~~~~~~~

Used by the ledger factory plugins.  See :doc:`ledgers` for full documentation
of each plugin.

Available substitutions: ``%(table_name)s``, ``%(schema)s``, ``%(op)s``.

.. list-table::
   :header-rows: 1
   :widths: 30 45 25

   * - Key
     - Default template
     - Names
   * - ``ledger_function``
     - ``%(schema)s_%(table_name)s_%(op)s``
     - INSTEAD OF trigger function
   * - ``ledger_trigger``
     - ``%(schema)s_%(table_name)s_%(op)s``
     - INSTEAD OF trigger
   * - ``ledger_balance_view``
     - ``%(table_name)s_balances``
     - Balance summary view
   * - ``ledger_latest_view``
     - ``%(table_name)s_latest``
     - Latest-row view (``DISTINCT ON``)
   * - ``balance_check_function``
     - ``%(schema)s_%(table_name)s_%(op)s``
     - Balance enforcement trigger function
   * - ``balance_check_trigger``
     - ``%(schema)s_%(table_name)s_%(op)s``
     - Balance enforcement trigger
   * - ``double_entry_function``
     - ``%(schema)s_%(table_name)s_%(op)s``
     - Double-entry validation trigger function
   * - ``double_entry_trigger``
     - ``%(schema)s_%(table_name)s_%(op)s``
     - Double-entry validation trigger
