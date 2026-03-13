Built-in Dimensions
===================

pgcraft ships with three dimension types, each backed by a different
storage strategy.  All three share the same plugin-driven pipeline
and expose a unified PostgREST API view.

Choose the type that matches your data:

- **Simple** -- one table, direct CRUD.  Best for reference data.
- **Append-Only (SCD Type 2)** -- full change history via an
  append-only log.  Best for slowly changing dimensions.
- **EAV** -- sparse attributes stored as rows and pivoted back to
  columns.  Best for highly dynamic or optional fields.


Simple Dimension
----------------

A single backing table with a corresponding API view.  Suitable
for reference data and simple lookups that don't need change
history.

**Example configuration:**

.. literalinclude:: ../scripts/examples/simple.py
   :language: python
   :start-after: # --- example start ---
   :end-before: # --- example end ---
   :dedent:

**Usage** -- all operations go through the API view:

.. literalinclude:: ../scripts/examples/simple.sql
   :language: sql

.. include:: _generated/dim_simple.rst


Append-Only Dimension (SCD Type 2)
----------------------------------

Tracks full change history using an append-only attributes log.
Every update creates a new row in the attributes table; the root
table points to the latest version.  A join view presents the
current state.  Ideal for slowly changing dimensions where audit
trails matter.

**Example configuration:**

.. literalinclude:: ../scripts/examples/append_only.py
   :language: python
   :start-after: # --- example start ---
   :end-before: # --- example end ---
   :dedent:

**Usage** -- inserts and updates go through the API view; the
triggers manage the internal tables:

.. literalinclude:: ../scripts/examples/append_only.sql
   :language: sql

.. include:: _generated/dim_append_only.rst


EAV Dimension (Entity-Attribute-Value)
--------------------------------------

Stores attributes as rows rather than columns, using typed value
columns (``string_value``, ``integer_value``, etc.) with a check
constraint enforcing exactly one non-null value per row.  A pivot
view reconstructs the familiar columnar layout.  Ideal for sparse
or highly dynamic attributes where most entities only have a
subset of possible fields.

**Example configuration:**

.. literalinclude:: ../scripts/examples/eav.py
   :language: python
   :start-after: # --- example start ---
   :end-before: # --- example end ---
   :dedent:

**Usage** -- the API view looks like a normal table; the triggers
decompose columns into EAV rows behind the scenes:

.. literalinclude:: ../scripts/examples/eav.sql
   :language: sql

.. include:: _generated/dim_eav.rst
