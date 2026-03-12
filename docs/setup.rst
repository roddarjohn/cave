Setting up a new project
========================

This guide walks through integrating cave into a new project that uses
Alembic for database migrations and SQLAlchemy for models.

Dependencies
------------

Add ``cave`` as a dependency alongside your existing Alembic and SQLAlchemy
dependencies. See the `Alembic documentation`_ for a full project setup guide.

.. _Alembic documentation: https://alembic.sqlalchemy.org/en/latest/tutorial.html

``alembic.ini``
---------------

In your ``alembic.ini``, add a ``[logger_cave]`` section to enable cave's
debug output:

.. code-block:: ini

   [loggers]
   keys = root,sqlalchemy,alembic,cave

   [logger_cave]
   level = DEBUG
   handlers = console
   qualname = cave
   propagate = 0

``env.py``
----------

Make two cave-specific additions to ``migrations/env.py``:

1. Call :func:`cave.alembic.register.cave_alembic_hook` before importing
   your models. This applies cave's patches and registers its Alembic
   extensions.
2. Call :func:`cave.alembic.register.cave_configure_metadata` after loading
   your models/metadata. This registers schemas, roles, and grants.
3. Pass ``cave_process_revision_directives`` to both ``context.configure()``
   calls. This enables cave's autogenerate extensions, including dependency
   ordering of operations within each generated migration.

.. code-block:: python

   from cave.alembic.register import (
       cave_alembic_hook,
       cave_configure_metadata,
       cave_process_revision_directives,
   )

   cave_alembic_hook()

   # ... your existing env.py setup (loading config, metadata, etc.) ...

   cave_configure_metadata(target_metadata)

   def run_migrations_offline() -> None:
       context.configure(
           # ... your existing options ...
           process_revision_directives=cave_process_revision_directives,
       )
       with context.begin_transaction():
           context.run_migrations()


   def run_migrations_online() -> None:
       with connectable.connect() as connection:
           context.configure(
               # ... your existing options ...
               process_revision_directives=cave_process_revision_directives,
           )
           with context.begin_transaction():
               context.run_migrations()

``models.py``
-------------

Pass your ``MetaData`` instance to a cave dimension factory so that
generated tables are registered for autogenerate detection.

Simple dimension (single table)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A simple dimension stores each row in one table and exposes it through an
``api`` schema view with INSTEAD OF triggers:

.. code-block:: python

   from sqlalchemy import Column, MetaData, String, Text
   from cave.factory.dimension.simple import SimpleDimensionFactory

   metadata = MetaData()

   SimpleDimensionFactory(
       tablename="products",
       schemaname="dim",
       metadata=metadata,
       dimensions=[
           Column("name", String, nullable=False),
           Column("description", Text),
       ],
   )

Append-only dimension (SCD Type 2)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An append-only dimension keeps a full history of attribute changes.  The
current state is always the most recent row in the attributes log:

.. code-block:: python

   from sqlalchemy import Column, MetaData, Numeric, String
   from cave.factory.dimension.append_only import AppendOnlyDimensionFactory

   AppendOnlyDimensionFactory(
       tablename="prices",
       schemaname="dim",
       metadata=metadata,
       dimensions=[
           Column("sku", String, nullable=False),
           Column("amount", Numeric(10, 2), nullable=False),
           Column("currency", String(3), nullable=False),
       ],
   )

EAV dimension (sparse / dynamic attributes)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An EAV dimension stores each attribute as a separate row, making it
efficient when rows have many nullable fields or when attributes are added
frequently:

.. code-block:: python

   from sqlalchemy import Boolean, Column, Integer, MetaData, String
   from cave.factory.dimension.eav import EAVDimensionFactory

   EAVDimensionFactory(
       tablename="features",
       schemaname="dim",
       metadata=metadata,
       dimensions=[
           Column("name", String, nullable=False),
           Column("enabled", Boolean),
           Column("max_seats", Integer),
       ],
   )

Customising factory behaviour
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Any factory argument can be changed by passing a custom plugin list.
See :doc:`plugins` for a full explanation.

Custom PK column name:

.. code-block:: python

   from cave.factory.dimension.simple import SimpleDimensionFactory
   from cave.plugins.pk import SerialPKPlugin
   from cave.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin
   from cave.plugins.api import APIPlugin

   SimpleDimensionFactory(
       "products", "dim", metadata, dimensions,
       plugins=[
           SerialPKPlugin(column_name="product_id"),
           SimpleTablePlugin(),
           APIPlugin(),
           SimpleTriggerPlugin(),
       ],
   )

Custom API schema:

.. code-block:: python

   SimpleDimensionFactory(
       "products", "dim", metadata, dimensions,
       plugins=[
           SerialPKPlugin(),
           SimpleTablePlugin(),
           APIPlugin(schema="reporting"),
           SimpleTriggerPlugin(),
       ],
   )

Apply a custom plugin to every factory via :class:`~cave.config.CaveConfig`:

.. code-block:: python

   from cave.config import CaveConfig

   cave_cfg = CaveConfig()
   cave_cfg.register(TimestampPlugin(), TenantPlugin())

   SimpleDimensionFactory(
       "products", "dim", metadata, dimensions, cave=cave_cfg
   )
   AppendOnlyDimensionFactory(
       "orders", "dim", metadata, dimensions, cave=cave_cfg
   )
