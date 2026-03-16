Setting up a new project
========================

This guide walks through integrating pgcraft into a new project that uses
Alembic for database migrations and SQLAlchemy for models.

Installation
------------

pgcraft is available on `PyPI <https://pypi.org/project/pgcraft/>`_:

.. code-block:: bash

   pip install pgcraft

Or with `uv <https://docs.astral.sh/uv/>`_:

.. code-block:: bash

   uv add pgcraft

Dependencies
------------

pgcraft installs SQLAlchemy and its `declarative extensions`_
automatically. You will also need Alembic for migrations. See the
`Alembic documentation`_ for a full project setup guide.

.. _declarative extensions: https://docs.sqlalchemy.org/en/latest/orm/extensions/declarative/index.html
.. _Alembic documentation: https://alembic.sqlalchemy.org/en/latest/tutorial.html

``alembic.ini``
---------------

In your ``alembic.ini``, add a ``[logger_pgcraft]`` section to enable pgcraft's
debug output:

.. code-block:: ini

   [loggers]
   keys = root,sqlalchemy,alembic,pgcraft

   [logger_pgcraft]
   level = DEBUG
   handlers = console
   qualname = pgcraft
   propagate = 0

``env.py``
----------

Make three pgcraft-specific additions to ``migrations/env.py``:

1. Call :func:`pgcraft.alembic.register.pgcraft_alembic_hook` before importing
   your models. This applies pgcraft's patches and registers its Alembic
   extensions.
2. Call :func:`pgcraft.alembic.register.pgcraft_configure_metadata` after loading
   your models/metadata. This registers schemas, roles, and grants.
3. Pass ``pgcraft_process_revision_directives`` to both ``context.configure()``
   calls. This enables pgcraft's autogenerate extensions, including dependency
   ordering of operations within each generated migration.

.. code-block:: python

   from pgcraft.alembic.register import (
       pgcraft_alembic_hook,
       pgcraft_configure_metadata,
       pgcraft_process_revision_directives,
   )

   pgcraft_alembic_hook()

   # ... your existing env.py setup (loading config, metadata, etc.) ...

   pgcraft_configure_metadata(target_metadata, config=config)

   def run_migrations_offline() -> None:
       context.configure(
           # ... your existing options ...
           process_revision_directives=pgcraft_process_revision_directives,
       )
       with context.begin_transaction():
           context.run_migrations()


   def run_migrations_online() -> None:
       with connectable.connect() as connection:
           context.configure(
               # ... your existing options ...
               process_revision_directives=pgcraft_process_revision_directives,
           )
           with context.begin_transaction():
               context.run_migrations()

``models.py``
-------------

Pass your ``MetaData`` instance to a pgcraft dimension factory so that
generated tables are registered for autogenerate detection.

Simple dimension (single table)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A simple dimension creates a single table in the specified
schema:

.. code-block:: python

   from sqlalchemy import Column, MetaData, String, Text
   from pgcraft.factory import PGCraftSimple

   metadata = MetaData()

   products = PGCraftSimple(
       tablename="products",
       schemaname="dim",
       metadata=metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("description", Text),
       ],
   )

This creates:

- ``dim.products`` — the dimension table

Append-only dimension (SCD Type 2)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An append-only dimension keeps a full history of attribute
changes.  The current state is always the most recent row in
the attributes log:

.. code-block:: python

   from sqlalchemy import Column, MetaData, Numeric, String
   from pgcraft.factory import PGCraftAppendOnly

   prices = PGCraftAppendOnly(
       tablename="prices",
       schemaname="dim",
       metadata=metadata,
       schema_items=[
           Column("sku", String, nullable=False),
           Column(
               "amount", Numeric(10, 2), nullable=False,
           ),
           Column("currency", String(3), nullable=False),
       ],
   )

This creates:

- ``dim.prices_root`` — entity root table with PK and
  ``created_at``
- ``dim.prices_attributes`` — append-only attributes log
- ``dim.prices`` — a view joining root and attributes to
  show the current state

Ledger (append-only value table)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A ledger stores immutable entries with a ``value`` column, an
``entry_id`` UUID for correlating related entries, and dimension
columns:

.. code-block:: python

   from sqlalchemy import Column, MetaData, String
   from pgcraft.factory import PGCraftLedger

   order_events = PGCraftLedger(
       tablename="order_events",
       schemaname="ops",
       metadata=metadata,
       schema_items=[
           Column("order_id", String, nullable=False),
           Column("status", String, nullable=False),
       ],
   )

This creates:

- ``ops.order_events`` — the append-only ledger table

See :doc:`ledgers` for balance views, double-entry enforcement,
and numeric value types.

EAV dimension (sparse / dynamic attributes)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An EAV dimension stores each attribute as a separate row,
making it efficient when rows have many nullable fields or when
attributes are added frequently:

.. code-block:: python

   from sqlalchemy import (
       Boolean, Column, Integer, MetaData, String,
   )
   from pgcraft.factory import PGCraftEAV

   features = PGCraftEAV(
       tablename="features",
       schemaname="dim",
       metadata=metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("enabled", Boolean),
           Column("max_seats", Integer),
       ],
   )

This creates:

- ``dim.features_entity`` — entity table with PK and
  ``created_at``
- ``dim.features_attribute`` — attribute key/value rows
- ``dim.features`` — a view pivoting attributes into columns

Customising factory behaviour
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Any factory argument can be changed by passing a custom plugin
list.  See :doc:`plugins` for a full explanation.

Custom PK type:

.. code-block:: python

   from pgcraft.factory import PGCraftSimple
   from pgcraft.plugins.pk import UUIDV4PKPlugin

   products = PGCraftSimple(
       "products", "dim", metadata, schema_items,
       plugins=[UUIDV4PKPlugin()],
   )

UUIDv7 primary key (PostgreSQL 18+):

.. code-block:: python

   from pgcraft.plugins.pk import UUIDV7PKPlugin

   products = PGCraftSimple(
       "products", "dim", metadata, schema_items,
       plugins=[UUIDV7PKPlugin()],
   )


Apply a custom plugin to every factory via
:class:`~pgcraft.config.PGCraftConfig`:

.. code-block:: python

   from pgcraft.config import PGCraftConfig

   pgcraft_cfg = PGCraftConfig()
   pgcraft_cfg.register(
       TimestampPlugin(), TenantPlugin(),
   )

   PGCraftSimple(
       "products", "dim", metadata, schema_items,
       config=pgcraft_cfg,
   )
   PGCraftAppendOnly(
       "orders", "dim", metadata, schema_items,
       config=pgcraft_cfg,
   )

To add PostgREST API views, see :doc:`extensions`.
