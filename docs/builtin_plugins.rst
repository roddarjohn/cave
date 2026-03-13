Built-in plugins
================

Every part of pgcraft's dimension pipeline is a plugin.  This page
documents each built-in plugin: what it does, what context keys it
reads and writes, and how to configure it.

For the plugin architecture itself (dependency declarations,
topological sort, singletons, writing custom plugins), see
:doc:`plugins`.


SerialPKPlugin
--------------

.. module:: pgcraft.plugins.pk

Adds an auto-incrementing integer primary key column.

**Produces:** ``pk_columns``

**Singleton group:** ``__pk__``

**Parameters:**

``column_name``
    Name for the PK column (default ``"id"``).

**Example:**

.. code-block:: python

   from pgcraft.plugins.pk import SerialPKPlugin

   # Default: adds "id SERIAL PRIMARY KEY"
   SerialPKPlugin()

   # Custom column name
   SerialPKPlugin(column_name="user_id")


CreatedAtPlugin
---------------

.. module:: pgcraft.plugins.created_at

Adds a ``created_at`` timestamp column with a server-side default
of ``now()``.  Used by append-only and EAV dimension types.

**Produces:** ``created_at_column``

**Example:**

.. code-block:: python

   from pgcraft.plugins.created_at import CreatedAtPlugin

   CreatedAtPlugin()


SimpleTablePlugin
-----------------

.. module:: pgcraft.plugins.simple

Creates a single backing table by combining the PK columns and
schema items.

**Produces:** ``"primary"`` (via ``table_key``), ``"__root__"``

**Requires:** ``"pk_columns"``

**Singleton group:** ``__table__``

**Parameters:**

``table_key``
    Context key to store the table under (default ``"primary"``).

**Example:**

.. code-block:: python

   from sqlalchemy import Column, MetaData, String

   from pgcraft.factory.dimension.simple import (
       SimpleDimensionResourceFactory,
   )
   from pgcraft.plugins.pk import SerialPKPlugin
   from pgcraft.plugins.simple import SimpleTablePlugin

   metadata = MetaData()

   SimpleDimensionResourceFactory(
       tablename="users",
       schemaname="public",
       metadata=metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("email", String),
       ],
       plugins=[
           SerialPKPlugin(),
           SimpleTablePlugin(),
       ],
   )

This creates ``public.users`` with columns ``id``, ``name``,
``email``.


APIPlugin
---------

.. module:: pgcraft.plugins.api

Creates a PostgREST-facing view and registers the API resource
for role/grant generation.

**Produces:** ``"api"`` (via ``view_key``)

**Requires:** ``"primary"`` (via ``table_key``)

**Parameters:**

``schema``
    Schema for the API view (default ``"api"``).

``grants``
    PostgREST privileges (default ``["select"]``).

``table_key``
    Context key to read the source table from (default
    ``"primary"``).

``view_key``
    Context key to store the created view under (default
    ``"api"``).

``columns``
    List of column names to include in the view.  When ``None``
    (the default), all table columns are selected.

``stats_key``
    Context key holding statistics view info (default
    ``"statistics_views"``).  LEFT JOINs statistics views into
    the API view when the key exists and is non-empty.

Default behaviour — ``SELECT *``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pgcraft.plugins.api import APIPlugin

   # Exposes all columns, SELECT only
   APIPlugin()

   # Full CRUD grants
   APIPlugin(grants=["select", "insert", "update", "delete"])

Column selection
~~~~~~~~~~~~~~~~

Expose a subset of columns — useful when the backing table has
internal columns that should not be visible through the API:

.. code-block:: python

   from sqlalchemy import Column, String, Text

   schema_items = [
       Column("name", String, nullable=False),
       Column("internal_notes", Text),  # hidden from API
   ]

   APIPlugin(columns=["id", "name"])

The generated view:

.. code-block:: sql

   SELECT p.id, p.name
   FROM public.users AS p

Statistics joins
~~~~~~~~~~~~~~~~

When ``PGCraftStatisticsView`` items are present in
``schema_items``, the default ``StatisticsViewPlugin`` creates
the statistics views and ``APIPlugin`` automatically LEFT JOINs
them into the API view.  No extra configuration is needed.

See :ref:`StatisticsViewPlugin <statistics-view-plugin>` for
the full workflow.

Combined with column selection:

.. code-block:: python

   APIPlugin(columns=["id", "name"])


.. _statistics-view-plugin:

StatisticsViewPlugin
--------------------

.. module:: pgcraft.plugins.statistics

Creates statistics views from
:class:`~pgcraft.statistics.PGCraftStatisticsView` schema items and
stores info for :class:`~pgcraft.plugins.api.APIPlugin` to
consume.  Included in every dimension factory's default plugins —
a no-op when no statistics items are present.

**Produces:** ``"statistics_views"`` (via ``stats_key``)

**Requires:** ``"primary"`` (via ``table_key``), ``"pk_columns"``

**Parameters:**

``stats_key``
    Context key to store the view info dict under (default
    ``"statistics_views"``).

``table_key``
    Context key for the source table (default ``"primary"``).

How it works
~~~~~~~~~~~~

1. For each :class:`~pgcraft.statistics.PGCraftStatisticsView` in
   ``ctx.schema_items``, compiles the SQLAlchemy query to SQL and
   creates a view named ``{tablename}_{name}_statistics``.
2. Stores a dict of
   :class:`~pgcraft.statistics.StatisticsViewInfo` in
   ``ctx[stats_key]`` for downstream plugins.
3. The join key column is automatically excluded from the API
   select list — only the aggregate columns appear.
4. Each view's ``schema`` defaults to the dimension's schema but
   can be overridden per-view.

Example — order and invoice statistics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import (
       Column,
       Integer,
       MetaData,
       Numeric,
       String,
       Table,
       func,
       select,
   )

   from pgcraft.factory.dimension.simple import (
       SimpleDimensionResourceFactory,
   )
   from pgcraft.statistics import PGCraftStatisticsView
   from pgcraft.utils.naming_convention import (
       build_naming_convention,
   )

   metadata = MetaData(
       naming_convention=build_naming_convention(),
   )

   # Reference tables (already exist in the database)
   orders = Table(
       "orders", metadata,
       Column("id", Integer, primary_key=True),
       Column("customer_id", Integer),
       Column("total", Numeric(10, 2)),
       schema="public",
   )

   invoices = Table(
       "invoices", metadata,
       Column("id", Integer, primary_key=True),
       Column("customer_id", Integer),
       Column("amount", Numeric(10, 2)),
       schema="public",
   )

   # Statistics queries
   order_stats = select(
       orders.c.customer_id,
       func.count().label("order_count"),
       func.sum(orders.c.total).label("order_total"),
   ).group_by(orders.c.customer_id)

   invoice_stats = select(
       invoices.c.customer_id,
       func.count().label("invoice_count"),
       func.sum(invoices.c.amount).label("invoiced_total"),
   ).group_by(invoices.c.customer_id)

   # Dimension with statistics — just use defaults
   SimpleDimensionResourceFactory(
       tablename="customers",
       schemaname="public",
       metadata=metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("email", String),
           PGCraftStatisticsView(
               name="orders",
               query=order_stats,
               join_key="customer_id",
           ),
           PGCraftStatisticsView(
               name="invoices",
               query=invoice_stats,
               join_key="customer_id",
           ),
       ],
   )

This creates:

* ``public.customers`` — the backing table.
* ``public.customers_orders_statistics`` — order aggregation view.
* ``public.customers_invoices_statistics`` — invoice aggregation
  view.
* ``api.customers`` — API view with LEFT JOINs to both.

The generated API view:

.. code-block:: sql

   SELECT p.id, p.name, p.email,
          s.order_count, s.order_total,
          s1.invoice_count, s1.invoiced_total
   FROM public.customers AS p
   LEFT OUTER JOIN public.customers_orders_statistics AS s
     ON p.id = s.customer_id
   LEFT OUTER JOIN public.customers_invoices_statistics AS s1
     ON p.id = s1.customer_id

Materialized statistics
~~~~~~~~~~~~~~~~~~~~~~~

For expensive aggregations, set ``materialized=True``.  The view
must be refreshed manually:

.. code-block:: python

   PGCraftStatisticsView(
       name="lifetime",
       query=select(
           orders.c.customer_id,
           func.sum(orders.c.total).label("lifetime_value"),
       ).group_by(orders.c.customer_id),
       join_key="customer_id",
       materialized=True,
   )

.. code-block:: sql

   REFRESH MATERIALIZED VIEW
     public.customers_lifetime_statistics;

Custom schema
~~~~~~~~~~~~~

Place statistics views in a different schema:

.. code-block:: python

   PGCraftStatisticsView(
       name="orders",
       query=order_stats,
       join_key="customer_id",
       schema="analytics",
   )

Computed columns with statistics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Combine PostgreSQL ``Computed`` columns (native generated columns)
with statistics views.  ``Computed`` derives a column from others
in the same row; statistics aggregate data from related tables.

.. code-block:: python

   from sqlalchemy import Column, Computed, Integer, String

   from pgcraft.statistics import PGCraftStatisticsView

   schema_items = [
       Column("name", String, nullable=False),
       Column("price", Integer, nullable=False),
       Column("qty", Integer, nullable=False),
       # Computed: Postgres evaluates price * qty automatically
       Column("total", Integer, Computed("price * qty")),
       # Statistics: aggregated from a related table
       PGCraftStatisticsView(
           name="orders",
           query=order_stats,
           join_key="customer_id",
       ),
   ]

The ``total`` column lives in the backing table (PostgreSQL
generates it).  The ``order_count`` and ``order_total`` columns
are LEFT JOINed from the statistics view into the API.


SimpleTriggerPlugin
-------------------

Registers INSTEAD OF ``INSERT`` / ``UPDATE`` / ``DELETE`` triggers
on the API view, forwarding writes to the backing table.

**Requires:** ``"primary"`` (via ``table_key``),
``"api"`` (via ``view_key``)

**Parameters:**

``table_key``
    Context key for the backing table (default ``"primary"``).

``view_key``
    Context key for the API view (default ``"api"``).

**Example:**

.. code-block:: python

   from pgcraft.plugins.simple import SimpleTriggerPlugin

   SimpleTriggerPlugin()

This is typically the last plugin in a simple dimension pipeline.


TableCheckPlugin
----------------

.. module:: pgcraft.plugins.check

Resolves :class:`~pgcraft.check.PGCraftCheck` items into real
``CHECK`` constraints on the backing table.

**Requires:** ``"__root__"``

**Example:**

.. code-block:: python

   from pgcraft.check import PGCraftCheck
   from pgcraft.plugins.check import TableCheckPlugin

   schema_items = [
       Column("price", Integer),
       PGCraftCheck("{price} > 0", name="positive_price"),
   ]

   plugins = [
       SerialPKPlugin(),
       SimpleTablePlugin(),
       TableCheckPlugin(),
       APIPlugin(),
       SimpleTriggerPlugin(),
   ]


TriggerCheckPlugin
~~~~~~~~~~~~~~~~~~

Resolves :class:`~pgcraft.check.PGCraftCheck` items into trigger-
based enforcement (``RAISE EXCEPTION`` in INSTEAD OF triggers).
Used with EAV dimensions where table-level checks cannot reference
the pivot view.

**Parameters:**

``view_key``
    Which view's triggers to add checks to (default varies by
    dimension type).

**Example:**

.. code-block:: python

   from pgcraft.plugins.check import TriggerCheckPlugin

   extra_plugins = [
       TriggerCheckPlugin(),
       TriggerCheckPlugin(view_key="api"),
   ]


AppendOnlyTablePlugin
---------------------

.. module:: pgcraft.plugins.append_only

Creates the root table and attributes table for an append-only
(SCD Type 2) dimension.

**Produces:** ``"root_table"``, ``"attributes"``

**Requires:** ``"pk_columns"``

**Singleton group:** ``__table__``


AppendOnlyViewPlugin
~~~~~~~~~~~~~~~~~~~~

Creates a join view that presents the current state by joining the
root table to the latest attributes row.

**Produces:** ``"primary"``

**Requires:** ``"root_table"``, ``"attributes"``


AppendOnlyTriggerPlugin
~~~~~~~~~~~~~~~~~~~~~~~

Registers INSTEAD OF triggers that insert new attribute rows on
update (preserving history) and handle deletes.

**Requires:** ``"root_table"``, ``"attributes"``,
``"api"`` (optional)

**Example:**

.. code-block:: python

   from sqlalchemy import Column, ForeignKey, String

   from pgcraft.factory.dimension.append_only import (
       AppendOnlyDimensionResourceFactory,
   )

   AppendOnlyDimensionResourceFactory(
       tablename="students",
       schemaname="private",
       metadata=metadata,
       schema_items=[
           Column("name", String),
           Column("user_id", ForeignKey("public.users.id")),
       ],
   )

This creates ``private.students`` (root), ``private.students_log``
(attributes), a join view at ``private.students_current``, and an
API view at ``api.students``.


EAVTablePlugin
--------------

.. module:: pgcraft.plugins.eav

Creates the entity and attribute tables for an EAV dimension.
Attributes are stored as typed rows (``string_value``,
``integer_value``, etc.) with a check constraint enforcing exactly
one non-null value per row.

**Produces:** ``"entity"``, ``"attribute"``, ``"eav_mappings"``

**Requires:** ``"pk_columns"``

**Singleton group:** ``__table__``


EAVViewPlugin
~~~~~~~~~~~~~

Creates a pivot view that reconstructs the familiar columnar
layout from the EAV rows.

**Produces:** ``"primary"``

**Requires:** ``"entity"``, ``"attribute"``, ``"eav_mappings"``


EAVTriggerPlugin
~~~~~~~~~~~~~~~~

Registers INSTEAD OF triggers that decompose columnar inserts/
updates into individual EAV attribute rows.

**Requires:** ``"entity"``, ``"attribute"``, ``"eav_mappings"``,
``"api"`` (optional)

**Example:**

.. code-block:: python

   from sqlalchemy import Boolean, Column, Float, Integer, String

   from pgcraft.check import PGCraftCheck
   from pgcraft.factory.dimension.eav import (
       EAVDimensionResourceFactory,
   )
   from pgcraft.plugins.check import TriggerCheckPlugin

   EAVDimensionResourceFactory(
       tablename="products",
       schemaname="private",
       metadata=metadata,
       schema_items=[
           Column("color", String),
           Column("weight", Float),
           Column("is_active", Boolean),
           Column("price", Integer),
           PGCraftCheck(
               "{price} > 0", name="positive_price"
           ),
       ],
       extra_plugins=[
           TriggerCheckPlugin(),
           TriggerCheckPlugin(view_key="api"),
       ],
   )

This creates ``private.products_entity``,
``private.products_attribute``, a pivot view, and an API view.
Check constraints are enforced in the INSTEAD OF triggers.


Plugin execution order
----------------------

The factory topologically sorts plugins by their
:func:`~pgcraft.plugin.produces` / :func:`~pgcraft.plugin.requires`
declarations.  A typical simple dimension pipeline runs:

.. code-block:: text

   SerialPKPlugin        → pk_columns
   SimpleTablePlugin     → primary, __root__
   TableCheckPlugin      (reads __root__)
   StatisticsViewPlugin  → statistics_views  (no-op if empty)
   APIPlugin             → api
   SimpleTriggerPlugin   (reads primary, api)

All context key names are overridable via constructor arguments,
so two independent pipelines can coexist in one factory.
