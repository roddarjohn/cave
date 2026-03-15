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
   :no-index:

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


UUIDV4PKPlugin
--------------

Adds a UUIDv4 primary key column using PostgreSQL's
``gen_random_uuid()`` as the server default.

**Produces:** ``pk_columns``

**Singleton group:** ``__pk__``

**Parameters:**

``column_name``
    Name for the PK column (default ``"id"``).

**Example:**

.. code-block:: python

   from pgcraft.plugins.pk import UUIDV4PKPlugin

   # Default: adds "id UUID PRIMARY KEY DEFAULT gen_random_uuid()"
   UUIDV4PKPlugin()

   # Custom column name
   UUIDV4PKPlugin(column_name="user_id")


UUIDV7PKPlugin
--------------

Adds a UUIDv7 primary key column using PostgreSQL 18's
``uuidv7()`` as the server default.  UUIDv7 values are
time-ordered, making them friendlier to B-tree indexes than
random UUIDv4 values.

Requires PostgreSQL 18 or later (declared via
``@requires(MinPGVersion(18))``).  Use
:func:`~pgcraft.plugin.check_pg_version` to validate the
server version before applying DDL.

**Produces:** ``pk_columns``

**Requires:** ``MinPGVersion(18)``

**Singleton group:** ``__pk__``

**Parameters:**

``column_name``
    Name for the PK column (default ``"id"``).

**Example:**

.. code-block:: python

   from pgcraft.plugins.pk import UUIDV7PKPlugin

   # Default: adds "id UUID PRIMARY KEY DEFAULT uuidv7()"
   UUIDV7PKPlugin()

   # Custom column name
   UUIDV7PKPlugin(column_name="ticket_id")

To validate the server version at runtime:

.. code-block:: python

   from pgcraft.plugin import check_pg_version

   with engine.connect() as conn:
       major = conn.dialect.server_version_info[0]
       check_pg_version(major, factory.ctx.plugins)


CreatedAtPlugin
---------------

.. module:: pgcraft.plugins.created_at
   :no-index:

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
   :no-index:

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

   from pgcraft.factory import PGCraftSimple

   metadata = MetaData()

   PGCraftSimple(
       "users", "public", metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("email", String),
       ],
   )

This creates ``public.users`` with columns ``id``, ``name``,
``email``.


PostgRESTView
-------------

.. module:: pgcraft.extensions.postgrest.view
   :no-index:

.. note::

   ``PostgRESTView`` lives in the PostgREST extension package
   (``pgcraft.extensions.postgrest``).  Install the extension
   and register it on your
   :class:`~pgcraft.config.PGCraftConfig` before use.

API views are created separately from the factory using
:class:`~pgcraft.extensions.postgrest.PostgRESTView`.  This
creates a PostgREST-facing view and registers the API resource
for role/grant generation.

.. note::

   The PostgREST extension must be registered on your
   :class:`~pgcraft.config.PGCraftConfig` for roles and grants
   to be generated.  See the setup snippet in the first example
   below.

Grants drive triggers: INSTEAD OF triggers are only created for
the DML operations listed in ``grants``.  A ``["select"]``-only
view has no triggers and is read-only.

**Parameters:**

``source``
    The factory instance to create the API view for.

``schema``
    Schema for the API view (default ``"api"``).

``grants``
    PostgREST privileges (default ``["select"]``).
    Determines which INSTEAD OF triggers are created.

``query``
    Optional callable ``(query, source_table) -> Select`` for
    SQLAlchemy-style view customisation (joins, column filtering).

``columns``
    List of column names to include in the view.  When ``None``
    (the default), all table columns are selected.  Mutually
    exclusive with ``exclude_columns``.

``exclude_columns``
    List of column names to hide from the view.  Mutually
    exclusive with ``columns``.

Default behaviour — ``SELECT *``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pgcraft.config import PGCraftConfig
   from pgcraft.extensions.postgrest import (
       PostgRESTExtension,
       PostgRESTView,
   )
   from pgcraft.factory import PGCraftSimple

   config = PGCraftConfig()
   config.use(PostgRESTExtension())

   users = PGCraftSimple(
       "users", "public", metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("email", String),
       ],
       config=config,
   )

   # Exposes all columns, SELECT only (read-only, no triggers)
   PostgRESTView(source=users)

   # Full CRUD — creates INSERT, UPDATE, DELETE triggers
   PostgRESTView(
       source=users,
       grants=["select", "insert", "update", "delete"],
   )

Column selection
~~~~~~~~~~~~~~~~

Expose a subset of columns — useful when the backing table has
internal columns that should not be visible through the API:

.. code-block:: python

   PostgRESTView(source=users, columns=["id", "name"])

The generated view:

.. code-block:: sql

   SELECT p.id, p.name
   FROM public.users AS p

Excluding columns
~~~~~~~~~~~~~~~~~

Alternatively, specify which columns to hide — all others are
included automatically:

.. code-block:: python

   PostgRESTView(
       source=users,
       exclude_columns=["internal_notes"],
   )

This is often more convenient than ``columns`` when you only want
to hide one or two columns from a large table.

Query customisation
~~~~~~~~~~~~~~~~~~~

Use ``query=`` for full SQLAlchemy control — joins, column
transforms, or any valid ``Select``:

.. code-block:: python

   from pgcraft.views.view import PGCraftView

   stats = PGCraftView(
       "order_stats", "public", metadata,
       query=select(
           orders.c.customer_id,
           func.count().label("order_count"),
       ).group_by(orders.c.customer_id),
   )

   _s = stats.table

   PostgRESTView(
       source=customers,
       grants=["select", "insert", "update"],
       query=lambda q, t: (
           select(t.c.id, t.c.name, _s.c.order_count)
           .select_from(t)
           .outerjoin(_s, t.c.id == _s.c.customer_id)
       ),
   )

Triggers still operate on the base table's dimension columns;
joined columns are read-only.


.. _statistics-view-plugin:

StatisticsViewPlugin
--------------------

.. module:: pgcraft.plugins.statistics
   :no-index:

Creates statistics views from
:class:`~pgcraft.statistics.PGCraftStatisticsView` schema items and
stores join info for the API view to consume.  Included in every
dimension factory's default plugins — a no-op when no statistics
items are present.

**Produces:** ``"joins"`` (via ``joins_key``)

**Requires:** ``"primary"`` (via ``table_key``), ``"pk_columns"``

**Parameters:**

``joins_key``
    Context key to store the view info dict under (default
    ``"joins"``).

``table_key``
    Context key for the source table (default ``"primary"``).

How it works
~~~~~~~~~~~~

1. For each :class:`~pgcraft.statistics.PGCraftStatisticsView` in
   ``ctx.schema_items``, compiles the SQLAlchemy query to SQL and
   creates a view named ``{tablename}_{name}_statistics``.
2. Stores a dict of
   :class:`~pgcraft.statistics.JoinedView` in
   ``ctx[joins_key]`` for downstream plugins.
3. The join key column is automatically excluded from the API
   select list — only the aggregate columns appear.
4. Each view's ``schema`` defaults to the dimension's schema but
   can be overridden per-view.


SimpleTriggerPlugin
-------------------

Registers INSTEAD OF ``INSERT`` / ``UPDATE`` / ``DELETE`` triggers
on the API view, forwarding writes to the backing table.  This is
an internal plugin used by ``PostgRESTView`` — not typically configured
directly by users.

**Requires:** ``"primary"`` (via ``table_key``),
``"api"`` (via ``view_key``)

**Parameters:**

``columns``
    Writable column subset.  When ``None``, uses all dim columns.

``permitted_operations``
    Which DML operations get triggers (``"insert"``,
    ``"update"``, ``"delete"``).  When ``None``, creates all
    three.


RawTableProtectionPlugin
------------------------

Prevents direct DML on raw backing tables by installing BEFORE
triggers that raise an exception when called outside a trigger
context.  Included automatically in each factory's
``_INTERNAL_PLUGINS``.

**Requires:** the table keys passed to its constructor.


TableCheckPlugin
----------------

.. module:: pgcraft.plugins.check
   :no-index:

Resolves :class:`~pgcraft.check.PGCraftCheck` items into real
``CHECK`` constraints on the backing table.

**Requires:** ``"__root__"``

**Example:**

.. code-block:: python

   from pgcraft.check import PGCraftCheck
   from pgcraft.factory import PGCraftSimple

   products = PGCraftSimple(
       "products", "public", metadata,
       schema_items=[
           Column("price", Integer),
           PGCraftCheck(
               "{price} > 0", name="positive_price"
           ),
       ],
   )


TriggerCheckPlugin
~~~~~~~~~~~~~~~~~~

Resolves :class:`~pgcraft.check.PGCraftCheck` items into trigger-
based enforcement (``RAISE EXCEPTION`` in INSTEAD OF triggers).
Used with EAV dimensions where table-level checks cannot reference
the pivot view.

**Parameters:**

``table_key``
    Which view's triggers to add checks to (default varies by
    dimension type).


TableIndexPlugin
----------------

.. module:: pgcraft.plugins.index
   :no-index:

Resolves :class:`~pgcraft.index.PGCraftIndex` items into real
``sqlalchemy.Index`` objects on the backing table.  Both simple
column references (``"{col}"``) and functional expressions
(``"lower({col})"``) are supported.  Extra keyword arguments on
the ``PGCraftIndex`` are passed through to the underlying
``sqlalchemy.Index`` (e.g. ``postgresql_using``,
``postgresql_where``).

**Requires:** ``"primary"`` (via ``table_key``)

**Parameters:**

``table_key``
    Context key for the target table (default ``"primary"``).
    Append-only dimensions use ``"attributes"``.

**Example:**

.. code-block:: python

   from sqlalchemy import Column, Integer, String

   from pgcraft.factory import PGCraftSimple
   from pgcraft.index import PGCraftIndex

   products = PGCraftSimple(
       "products", "public", metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("price", Integer, nullable=False),
           PGCraftIndex("idx_products_name", "{name}"),
           PGCraftIndex(
               "idx_products_price",
               "{price}",
               unique=True,
           ),
           PGCraftIndex(
               "idx_products_lower_name",
               "lower({name})",
               postgresql_using="btree",
           ),
       ],
   )


TableFKPlugin
-------------

.. module:: pgcraft.plugins.fk
   :no-index:

Resolves :class:`~pgcraft.fk.PGCraftFK` items into
``ForeignKeyConstraint`` objects on the backing table.

Local columns use ``{column_name}`` markers.  Exactly one of
``references`` or ``raw_references`` must be provided:

- ``references`` — ``"dimension.column"`` strings resolved via
  the dimension registry.
- ``raw_references`` — ``"schema.table.column"`` strings passed
  through directly to SQLAlchemy.

The dimension registry is populated automatically when factories
run — each factory registers its FK-targetable table (the root
table for append-only dimensions, the primary table for simple
dimensions).

**Requires:** ``"primary"`` (via ``table_key``)

**Parameters:**

``table_key``
    Context key for the target table (default ``"primary"``).
    Append-only dimensions use ``"attributes"``.

**Example (resolved references):**

.. code-block:: python

   from sqlalchemy import Column, Integer, String

   from pgcraft.factory import PGCraftSimple
   from pgcraft.fk import PGCraftFK

   customers = PGCraftSimple(
       "customers", "public", metadata,
       schema_items=[
           Column("name", String, nullable=False),
       ],
   )

   orders = PGCraftSimple(
       "orders", "public", metadata,
       schema_items=[
           Column("customer_id", Integer, nullable=False),
           Column("total", Integer, nullable=False),
           PGCraftFK(
               references={
                   "{customer_id}": "customers.id"
               },
               name="fk_orders_customer",
               ondelete="CASCADE",
           ),
       ],
   )

``"customers.id"`` is resolved to the physical table via the
dimension registry.  If ``customers`` is append-only, this
resolves to the root table automatically.

**Example (raw references):**

.. code-block:: python

   PGCraftFK(
       raw_references={
           "{org_id}": "public.organizations.id"
       },
       name="fk_orders_org",
   )

Use ``raw_references`` for tables outside pgcraft or when you
want full control over the FK target.

See :doc:`constraints_and_indices` for a walkthrough of the
generated SQL.


AppendOnlyTablePlugin
---------------------

.. module:: pgcraft.plugins.append_only
   :no-index:

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

   from pgcraft.factory import PGCraftAppendOnly

   students = PGCraftAppendOnly(
       "students", "private", metadata,
       schema_items=[
           Column("name", String),
           Column(
               "user_id",
               ForeignKey("public.users.id"),
           ),
       ],
   )

This creates ``private.students`` (root), ``private.students_log``
(attributes), and a join view at ``private.students_current``.

To expose via PostgREST:

.. code-block:: python

   from pgcraft.extensions.postgrest import (
       PostgRESTView,
   )

   PostgRESTView(source=students)


EAVTablePlugin
--------------

.. module:: pgcraft.plugins.eav
   :no-index:

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

   from sqlalchemy import Column, Float, Integer, String

   from pgcraft.check import PGCraftCheck
   from pgcraft.factory import PGCraftEAV

   products = PGCraftEAV(
       "products", "private", metadata,
       schema_items=[
           Column("color", String),
           Column("weight", Float),
           Column("price", Integer),
           PGCraftCheck(
               "{price} > 0", name="positive_price"
           ),
       ],
   )

This creates ``private.products_entity``,
``private.products_attribute``, and a pivot view.
Check constraints are enforced in the INSTEAD OF triggers.

To expose via PostgREST:

.. code-block:: python

   from pgcraft.extensions.postgrest import (
       PostgRESTView,
   )

   PostgRESTView(source=products)


Plugin execution order
----------------------

The factory topologically sorts plugins by their
:func:`~pgcraft.plugin.produces` / :func:`~pgcraft.plugin.requires`
declarations.  A typical simple dimension pipeline runs:

.. code-block:: text

   SerialPKPlugin              -> pk_columns
   SimpleTablePlugin           -> primary, __root__
   TableCheckPlugin            (reads __root__)
   TableIndexPlugin            (reads primary)
   TableFKPlugin               (reads primary)
   RawTableProtectionPlugin    (reads primary)

API views and triggers are created separately via
``PostgRESTView``, which is part of the optional PostgREST
extension (``pgcraft.extensions.postgrest``).  It is not
included in the core plugin pipeline.

All context key names are overridable via constructor arguments,
so two independent pipelines can coexist in one factory.
