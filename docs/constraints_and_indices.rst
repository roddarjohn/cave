Constraints and indices
======================

pgcraft dimensions are defined with ``schema_items`` — a list of
SQLAlchemy ``Column`` objects mixed with pgcraft constraint and
index definitions.  All three use ``{column_name}`` markers to
reference columns, and all three are validated against the actual
table columns at factory time.

.. list-table::
   :header-rows: 1
   :widths: 25 40 35

   * - Class
     - Purpose
     - Import
   * - :class:`~pgcraft.check.PGCraftCheck`
     - ``CHECK`` constraints
     - ``from pgcraft.check import PGCraftCheck``
   * - :class:`~pgcraft.index.PGCraftIndex`
     - Indices (btree, GIN, unique, functional, …)
     - ``from pgcraft.index import PGCraftIndex``
   * - :class:`~pgcraft.fk.PGCraftFK`
     - Foreign key constraints
     - ``from pgcraft.fk import PGCraftFK``


Column markers
--------------

All three classes reference columns with ``{column_name}``
markers.  At factory time, pgcraft validates that every
referenced column exists on the target table and substitutes the
markers with real column references.

.. code-block:: python

   # Check: the expression is a SQL predicate
   PGCraftCheck("{price} > 0", name="positive_price")

   # Index: each argument is an expression
   PGCraftIndex("idx_name", "{name}")
   PGCraftIndex("idx_lower", "lower({name})")

   # FK: dict maps {local_col} to target reference
   PGCraftFK(
       references={"{customer_id}": "customers.id"},
       name="fk_customer",
   )

If a marker names a column that does not exist on the table,
pgcraft raises ``PGCraftValidationError`` at factory time — not
at migration time or at runtime.


Check constraints
-----------------

:class:`~pgcraft.check.PGCraftCheck` defines a SQL ``CHECK``
constraint.  It takes an expression and a name.

.. code-block:: python

   PGCraftCheck("{price} > 0", name="positive_price")
   PGCraftCheck(
       "{end_date} > {start_date}",
       name="valid_date_range",
   )

For simple and append-only dimensions, this becomes a real
``CHECK`` constraint on the table.  For EAV dimensions, it
becomes a trigger function that validates ``NEW.price > 0``
before the main EAV triggers process the row.  The same
``PGCraftCheck`` definition works on all dimension types —
pgcraft picks the right enforcement strategy automatically.


Indices
-------

:class:`~pgcraft.index.PGCraftIndex` mirrors the
``sqlalchemy.Index`` constructor: name first, then column
expressions, then keyword arguments passed through to the
underlying index.

.. code-block:: python

   # Simple index
   PGCraftIndex("idx_products_sku", "{sku}")

   # Unique index
   PGCraftIndex("uq_products_name", "{name}", unique=True)

   # Functional index with dialect kwargs
   PGCraftIndex(
       "idx_lower_name", "lower({name})",
       postgresql_using="btree",
   )

   # Multi-column index
   PGCraftIndex("idx_name_price", "{name}", "{price}")

``PGCraftIndex`` supports the same keyword arguments as
``sqlalchemy.Index``:

.. list-table::
   :header-rows: 1

   * - Keyword
     - Effect
   * - ``unique=True``
     - Creates a ``UNIQUE`` index
   * - ``postgresql_using="gin"``
     - Uses the GIN index method
   * - ``postgresql_where=text("active")``
     - Partial index (``WHERE active``)
   * - ``postgresql_ops={"data": "jsonb_path_ops"}``
     - Operator class for a column


Foreign keys
------------

:class:`~pgcraft.fk.PGCraftFK` defines foreign key constraints.
Each entry in the dict maps a ``{local_col}`` marker to its
target reference — keeping the column and its target together,
like SQLAlchemy's ``ForeignKey("customers.id")``.

``references`` vs ``raw_references``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Exactly one must be provided:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Parameter
     - Format
     - When to use
   * - ``references``
     - ``{"{col}": "dimension.column"}``
     - Target is a pgcraft dimension.  Resolved via the
       dimension registry at factory time.
   * - ``raw_references``
     - ``{"{col}": "schema.table.column"}``
     - Target is outside pgcraft, or you want full control.
       Passed through to SQLAlchemy as-is.

.. code-block:: python

   # Resolved — pgcraft finds the physical table
   PGCraftFK(
       references={"{customer_id}": "customers.id"},
       name="fk_orders_customer",
       ondelete="CASCADE",
   )

   # Raw — passed through directly
   PGCraftFK(
       raw_references={"{org_id}": "tenant.orgs.id"},
       name="fk_orders_org",
   )

   # Multi-column
   PGCraftFK(
       raw_references={
           "{tenant_id}": "shared.orgs.tenant_id",
           "{org_id}": "shared.orgs.org_id",
       },
       name="fk_composite",
   )

Cascade options
^^^^^^^^^^^^^^^

Both ``ondelete`` and ``onupdate`` accept any PostgreSQL action:
``CASCADE``, ``SET NULL``, ``SET DEFAULT``, ``RESTRICT``, or
``NO ACTION`` (the default).

.. code-block:: python

   PGCraftFK(
       references={"{customer_id}": "customers.id"},
       name="fk_orders_customer",
       ondelete="CASCADE",
       onupdate="SET NULL",
   )


Simple dimension example
------------------------

.. literalinclude:: ../scripts/examples/constraints_simple.py
   :language: python
   :start-after: # --- example start ---
   :end-before: # --- example end ---
   :dedent:

.. include:: _generated/dim_constraints_simple.rst


Append-only dimension example
-----------------------------

Constraints and indices on append-only dimensions are placed on
the **attributes table**.  Foreign keys that target an
append-only dimension resolve to the **root table** — the stable
primary key.

.. literalinclude:: ../scripts/examples/constraints_append_only.py
   :language: python
   :start-after: # --- example start ---
   :end-before: # --- example end ---
   :dedent:

.. include:: _generated/dim_constraints_append_only.rst


EAV dimension example
---------------------

EAV dimensions store attributes as rows, not columns.
Table-level ``CHECK`` constraints cannot reference virtual
columns, so pgcraft enforces checks via INSTEAD OF trigger
functions instead.  The same ``PGCraftCheck`` syntax works — the
enforcement mechanism is chosen automatically.

.. literalinclude:: ../scripts/examples/constraints_eav.py
   :language: python
   :start-after: # --- example start ---
   :end-before: # --- example end ---
   :dedent:

.. include:: _generated/dim_constraints_eav.rst
