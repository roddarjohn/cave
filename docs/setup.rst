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

Pass your ``MetaData`` instance to the cave factory functions so that
generated tables are registered for autogenerate detection. See the
:doc:`api` reference for the available factory functions.

.. code-block:: python

   from sqlalchemy import MetaData

   from cave.factory.dimension.simple import simple_dimension_factory

   metadata = MetaData()

   simple_dimension_factory(
       tablename="users",
       schemaname="app",
       metadata=metadata,
       dimensions=[...],
   )
