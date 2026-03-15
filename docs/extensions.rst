Extension system
================

While :doc:`plugins` compose behaviour *within* a single factory,
**extensions** sit one level above: they bundle plugins, metadata
hooks, Alembic hooks, and CLI commands into a single installable
unit.

Extensions make it possible for third-party packages to extend
pgcraft, and for pgcraft's own opt-in subsystems (PostgREST,
future auth/RLS) to be cleanly separated from the core.


Quick start
-----------

Register an extension on your
:class:`~pgcraft.config.PGCraftConfig`:

.. code-block:: python

   from pgcraft.config import PGCraftConfig
   from pgcraft.extensions.postgrest import (
       PostgRESTExtension,
   )

   config = PGCraftConfig()
   config.use(PostgRESTExtension())

Pass this config to your factories and to
:func:`~pgcraft.alembic.register.pgcraft_configure_metadata`:

.. code-block:: python

   from pgcraft.alembic.register import (
       pgcraft_configure_metadata,
   )

   PGCraftSimple(
       "users", "public", metadata, ...,
       config=config,
   )
   pgcraft_configure_metadata(metadata, config)


Extension hooks
---------------

:class:`~pgcraft.extension.PGCraftExtension` provides five hooks.
Override only the ones you need — every hook is a no-op by
default.

``plugins()``
    Return a list of :class:`~pgcraft.plugin.Plugin` instances
    that are prepended to every factory's plugin list.

``configure_metadata(metadata)``
    Register roles, grants, schemas, or other metadata-level
    objects.  Called by
    :func:`~pgcraft.alembic.register.pgcraft_configure_metadata`.

``configure_alembic()``
    Register custom Alembic renderers or rewriters.  Called by
    :func:`~pgcraft.alembic.register.pgcraft_alembic_hook`.

``register_cli(app)``
    Add subcommands to the ``pgcraft`` CLI.

``validate(registered_names)``
    Check that required peer extensions are present.  Called
    after all extensions are loaded.


Inter-extension dependencies
----------------------------

Declare dependencies using the ``depends_on`` class variable:

.. code-block:: python

   from dataclasses import dataclass
   from typing import ClassVar

   from pgcraft.extension import PGCraftExtension

   @dataclass
   class MyExtension(PGCraftExtension):
       name: str = "my-ext"
       depends_on: ClassVar[list[str]] = ["postgrest"]

pgcraft validates that all declared dependencies are present
when extensions are resolved.  A
:class:`~pgcraft.errors.PGCraftValidationError` is raised if
any are missing.


Entry point discovery
---------------------

Third-party packages can register extensions via the
``pgcraft.extensions`` entry point group in ``pyproject.toml``:

.. code-block:: toml

   [project.entry-points."pgcraft.extensions"]
   nanoid = "pgcraft_nanoid:NanoIDExtension"

Discovered extensions are automatically loaded unless
``auto_discover=False`` is set on the config.  Manually
registered extensions take precedence over discovered ones with
the same name.


Writing an extension
--------------------

Here are three example tiers, from simple to complex.


Column-level extension (NanoID PK)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An extension that contributes a single plugin to replace the
default serial PK with a NanoID:

.. code-block:: python

   from dataclasses import dataclass

   from pgcraft.extension import PGCraftExtension
   from pgcraft.plugin import Plugin


   class NanoIDPKPlugin(Plugin):
       """Replace serial PK with a NanoID column."""
       # ... plugin implementation ...


   @dataclass
   class NanoIDExtension(PGCraftExtension):
       name: str = "nanoid"

       def plugins(self) -> list[Plugin]:
           return [NanoIDPKPlugin()]


Composite extension (audit trail)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An extension that bundles multiple plugins — a shadow table and
a trigger that writes to it:

.. code-block:: python

   from dataclasses import dataclass

   from pgcraft.extension import PGCraftExtension
   from pgcraft.plugin import Plugin


   @dataclass
   class AuditExtension(PGCraftExtension):
       name: str = "audit"

       def plugins(self) -> list[Plugin]:
           return [
               ShadowTablePlugin(),
               ShadowTriggerPlugin(),
           ]


Full subsystem extension (PostgREST)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The built-in PostgREST extension demonstrates a full subsystem:
metadata hooks for roles/grants.

.. code-block:: python

   from dataclasses import dataclass
   from typing import TYPE_CHECKING

   from pgcraft.extension import PGCraftExtension

   if TYPE_CHECKING:
       from sqlalchemy import MetaData

   @dataclass
   class PostgRESTExtension(PGCraftExtension):
       name: str = "postgrest"
       schema: str = "api"

       def configure_metadata(
           self, metadata: MetaData,
       ) -> None:
           from pgcraft.models.roles import (
               register_roles,
           )
           register_roles(metadata)


.. _ext-postgrest:

PostgREST extension
-------------------

pgcraft can generate PostgREST-compatible API views, INSTEAD OF
triggers for write operations, and the role/grant declarations
that PostgREST expects.  Enable the
:class:`~pgcraft.extensions.postgrest.PostgRESTExtension` on
your config, create a factory with that config, then call
:class:`~pgcraft.extensions.postgrest.PostgRESTView` to expose
it.

How it works
~~~~~~~~~~~~

When :class:`~pgcraft.extensions.postgrest.PostgRESTView` is
called it:

1. Creates a view in the ``api`` schema (configurable) that
   ``SELECT *`` s from the backing table.
2. Registers an :class:`~pgcraft.resource.APIResource` on the
   metadata so that pgcraft can generate role and grant
   statements.
3. Creates INSTEAD OF ``INSERT`` / ``UPDATE`` / ``DELETE``
   triggers on the view so PostgREST clients can write through
   it.

Minimal example
~~~~~~~~~~~~~~~

.. code-block:: python

   # models.py
   from sqlalchemy import (
       Column,
       Integer,
       MetaData,
       Numeric,
       String,
   )

   from pgcraft.config import PGCraftConfig
   from pgcraft.extensions.postgrest import (
       PostgRESTExtension,
       PostgRESTView,
   )
   from pgcraft.factory import PGCraftSimple
   from pgcraft import pgcraft_build_naming_conventions

   # Enable PostgREST roles and grants
   config = PGCraftConfig()
   config.use(PostgRESTExtension())

   metadata = MetaData(
       naming_convention=pgcraft_build_naming_conventions(),
   )

   products = PGCraftSimple(
       tablename="products",
       schemaname="inventory",
       metadata=metadata,
       config=config,
       schema_items=[
           Column("name", String, nullable=False),
           Column("sku", String(32), nullable=False),
           Column(
               "price",
               Numeric(10, 2),
               nullable=False,
           ),
       ],
   )

   PostgRESTView(source=products)

Remember to pass ``config`` to
``pgcraft_configure_metadata(metadata, config)`` in your
``env.py`` — see :doc:`setup` for the full Alembic wiring.

This creates:

* ``inventory.products`` — the backing table with a serial PK.
* ``api.products`` — a view for PostgREST to expose.
* INSTEAD OF triggers so ``INSERT`` / ``UPDATE`` / ``DELETE``
  through the view are forwarded to the backing table.
* An ``anon`` role with ``SELECT`` grants on
  ``api.products``.

Customising grants
~~~~~~~~~~~~~~~~~~

By default the ``anon`` role gets only ``SELECT``. Pass a
``grants`` list to
:class:`~pgcraft.extensions.postgrest.PostgRESTView` to allow
writes:

.. code-block:: python

   # config setup as above
   PostgRESTView(
       source=products,
       grants=[
           "select", "insert", "update", "delete",
       ],
   )

Grants drive triggers: INSTEAD OF triggers are only created for
the DML operations listed in ``grants``.  A
``["select"]``-only view has no triggers and is read-only.  A
view with ``["select", "insert"]`` gets only an INSERT
trigger — no UPDATE or DELETE.

Changing the API schema
~~~~~~~~~~~~~~~~~~~~~~~

The default API schema is ``api``. Override it with the
``schema`` parameter:

.. code-block:: python

   # config setup as above
   PostgRESTView(
       source=products, schema="reporting",
   )

PostgREST setup
~~~~~~~~~~~~~~~

After generating and applying migrations, point PostgREST at
your database. A minimal ``postgrest.conf``:

.. code-block:: ini

   db-uri = "postgresql://authenticator:changeme@localhost/mydb"
   db-schemas = "api"
   db-anon-role = "anon"
   db-extra-search-path = "public, inventory"

Start the server and query the API:

.. code-block:: bash

   postgrest postgrest.conf

   # List all products
   curl -s http://localhost:3000/products \
       | python3 -m json.tool

   # Filter
   curl -s \
       "http://localhost:3000/products?name=eq.Widget"

   # Insert (requires insert grant)
   curl -s http://localhost:3000/products \
       -H "Content-Type: application/json" \
       -d '{"name": "Widget", "sku": "W-001", "price": 9.99}'

See the
`PostgREST documentation <https://docs.postgrest.org>`_
for the full query syntax and configuration reference.


.. _ext-postgrest-column-selection:

Exposing a subset of columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default
:class:`~pgcraft.extensions.postgrest.PostgRESTView` creates a
``SELECT *`` view.  Pass a ``columns`` list to expose only
specific columns through the API — useful when a table has
internal columns that should not be visible to API consumers.

.. code-block:: python

   from sqlalchemy import (
       Column, MetaData, Numeric, String, Text,
   )

   from pgcraft.config import PGCraftConfig
   from pgcraft.extensions.postgrest import (
       PostgRESTExtension,
       PostgRESTView,
   )
   from pgcraft.factory import PGCraftSimple
   from pgcraft import pgcraft_build_naming_conventions

   config = PGCraftConfig()
   config.use(PostgRESTExtension())

   metadata = MetaData(
       naming_convention=pgcraft_build_naming_conventions(),
   )

   products = PGCraftSimple(
       tablename="products",
       schemaname="inventory",
       metadata=metadata,
       config=config,
       schema_items=[
           Column("name", String, nullable=False),
           Column("sku", String(32), nullable=False),
           Column(
               "price",
               Numeric(10, 2),
               nullable=False,
           ),
           Column("internal_notes", Text),  # hidden
       ],
   )

   PostgRESTView(
       source=products,
       columns=["id", "name", "sku", "price"],
   )

The generated view selects only the listed columns:

.. code-block:: sql

   CREATE VIEW api.products AS
   SELECT p.id, p.name, p.sku, p.price
   FROM inventory.products AS p

The ``internal_notes`` column exists in the backing table but
is invisible through the PostgREST API.  Any column name not
found on the table raises ``ValueError`` at factory
construction time.

Alternatively, use ``exclude_columns`` to hide specific columns
while including everything else — often more convenient for
large tables:

.. code-block:: python

   PostgRESTView(
       source=products,
       exclude_columns=["internal_notes"],
   )


.. _ext-postgrest-statistics:

Joining aggregate views into the API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create standalone aggregate views with
:class:`~pgcraft.views.view.PGCraftView`, then join them into
an API view using the ``query=`` parameter on
:class:`~pgcraft.extensions.postgrest.PostgRESTView`.

Each ``PGCraftView`` exposes a ``.table`` property — a joinable
SQLAlchemy selectable — so you can compose joins using standard
SQLAlchemy syntax.

Multiple statistics on one dimension
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A realistic customer dimension with both order and invoice
statistics:

.. code-block:: python

   from sqlalchemy import (
       Column,
       Integer,
       MetaData,
       Numeric,
       String,
       func,
       select,
   )

   from pgcraft.config import PGCraftConfig
   from pgcraft.extensions.postgrest import (
       PostgRESTExtension,
       PostgRESTView,
   )
   from pgcraft.factory import PGCraftSimple
   from pgcraft.views.view import PGCraftView
   from pgcraft import (
       pgcraft_build_naming_conventions,
   )

   config = PGCraftConfig()
   config.use(PostgRESTExtension())

   metadata = MetaData(
       naming_convention=(
           pgcraft_build_naming_conventions()
       ),
   )

   # -- Table factories ----------------------------

   Orders = PGCraftSimple(
       "orders", "public", metadata,
       config=config,
       schema_items=[
           Column(
               "customer_id",
               Integer,
               nullable=False,
           ),
           Column(
               "total",
               Numeric(10, 2),
               nullable=False,
           ),
       ],
   )

   customers = PGCraftSimple(
       "customers", "public", metadata,
       config=config,
       schema_items=[
           Column("name", String, nullable=False),
           Column("email", String),
       ],
   )

   # -- Standalone aggregate views -----------------

   _orders_t = Orders.table
   order_stats = PGCraftView(
       "customer_order_stats", "public", metadata,
       query=select(
           _orders_t.c.customer_id,
           func.count().label("order_count"),
           func.sum(_orders_t.c.total).label(
               "order_total",
           ),
       ).group_by(_orders_t.c.customer_id),
   )

   # -- API view with joined statistics ------------
   # PGCraftView.table is a joinable SQLAlchemy Table.
   # Triggers still work -- they operate on the base
   # table columns; joined columns are read-only.

   _os = order_stats.table

   PostgRESTView(
       source=customers,
       grants=[
           "select", "insert", "update", "delete",
       ],
       query=lambda q, t: (
           select(
               t.c.id,
               t.c.name,
               t.c.email,
               _os.c.order_count,
               _os.c.order_total,
           )
           .select_from(t)
           .outerjoin(
               _os, t.c.id == _os.c.customer_id
           )
       ),
   )

This creates:

* ``public.customers`` — the backing table (``id``, ``name``,
  ``email``).
* ``public.customer_order_stats`` — a standalone aggregate
  view.
* ``api.customers`` — the API view with a LEFT JOIN to the
  statistics view.

The ``query=`` lambda receives the base ``SELECT *`` query and
the source table. Return any valid SQLAlchemy ``Select`` — add
joins, filter columns, or transform freely. INSTEAD OF triggers
are still created for the base table columns; joined columns
are read-only.

How it works
^^^^^^^^^^^^

1. ``PGCraftView`` creates a standalone view from any
   SQLAlchemy ``select()`` expression and exposes ``.table``
   for use in further joins.
2. ``PostgRESTView`` with ``query=`` uses the lambda to
   customise the view definition. Grants drive which INSTEAD OF
   triggers are created.
3. Writable columns are automatically restricted to the base
   table's dimension columns — joined columns cannot be
   written through the API view.


.. _ext-postgrest-computed:

Computed columns
~~~~~~~~~~~~~~~~

PostgreSQL computed columns (``Computed``) are derived from
other columns in the same row.  PostgreSQL evaluates them
automatically — they appear in the API view like any other
column but cannot be written to.

.. code-block:: python

   from sqlalchemy import (
       Column,
       Computed,
       Integer,
       MetaData,
       String,
   )

   from pgcraft.config import PGCraftConfig
   from pgcraft.extensions.postgrest import (
       PostgRESTExtension,
       PostgRESTView,
   )
   from pgcraft.factory import PGCraftSimple
   from pgcraft import pgcraft_build_naming_conventions

   config = PGCraftConfig()
   config.use(PostgRESTExtension())

   metadata = MetaData(
       naming_convention=(
           pgcraft_build_naming_conventions()
       ),
   )

   products = PGCraftSimple(
       "products", "inventory", metadata,
       config=config,
       schema_items=[
           Column("name", String, nullable=False),
           Column("price", Integer, nullable=False),
           Column("qty", Integer, nullable=False),
           Column(
               "total", Integer,
               Computed("price * qty"),
           ),
       ],
   )

   PostgRESTView(
       source=products,
       grants=[
           "select", "insert", "update", "delete",
       ],
   )

The ``total`` column is a generated column — it appears in the
API but is computed by PostgreSQL, not writable through the API.
