Cookbook
=======

Practical recipes for common pgcraft use cases.

.. _cookbook-migrations-only:

Migrations only
---------------

Use pgcraft purely as a migration generator — define your schema with
pgcraft factories, produce Alembic migrations, and export them as raw
SQL. No pgcraft code runs at application time.

The default factory plugins include
:class:`~pgcraft.plugins.api.APIPlugin` (PostgREST views) and
:class:`~pgcraft.plugins.simple.SimpleTriggerPlugin` (INSTEAD OF
triggers on those views). Since this recipe does not use PostgREST,
pass an explicit ``plugins`` list that omits them.

**Project layout**::

    myproject/
    ├── alembic.ini
    ├── models.py
    └── migrations/
        ├── env.py
        └── versions/

**1. Define your schema**

.. code-block:: python

   # models.py
   from sqlalchemy import Column, MetaData, Numeric, String

   from pgcraft.factory.dimension.simple import (
       SimpleDimensionResourceFactory,
   )
   from pgcraft.plugins.pk import SerialPKPlugin
   from pgcraft.plugins.simple import SimpleTablePlugin
   from pgcraft.utils.naming_convention import build_naming_convention

   metadata = MetaData(
       naming_convention=build_naming_convention(),
   )

   SimpleDimensionResourceFactory(
       tablename="products",
       schemaname="inventory",
       metadata=metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("sku", String(32), nullable=False),
           Column("price", Numeric(10, 2), nullable=False),
       ],
       plugins=[
           SerialPKPlugin(),
           SimpleTablePlugin(),
       ],
   )

**2. Wire up Alembic**

Follow the standard :doc:`setup` instructions: call
``pgcraft_alembic_hook()`` early in ``env.py``, call
``pgcraft_configure_metadata()`` after loading your metadata, and pass
``pgcraft_process_revision_directives`` to ``context.configure()``.

**3. Generate a migration**

.. code-block:: bash

   alembic revision --autogenerate -m "add products table"

Review the generated Python file in ``migrations/versions/``.

**4. Export as raw SQL**

Alembic's ``--sql`` flag renders migrations as plain SQL instead of
executing them against a database. This is useful when you hand
migrations off to a DBA, run them via CI, or apply them with ``psql``
directly.

Generate the upgrade SQL for all pending migrations:

.. code-block:: bash

   # Upgrade from nothing to head (full schema)
   alembic upgrade head --sql > upgrade.sql

Generate SQL for a specific revision range:

.. code-block:: bash

   # From one revision to another
   alembic upgrade abc123:def456 --sql > upgrade.sql

   # Downgrade SQL
   alembic downgrade def456:abc123 --sql > downgrade.sql

The resulting ``.sql`` files are standalone — they have no dependency on
pgcraft or Python. You can commit them to your repo, review them in a
PR, or hand them to your ops team.

**5. Apply with psql**

.. code-block:: bash

   psql -d mydb -f upgrade.sql

At this point pgcraft has done its job. Your application code never
imports pgcraft — it only consumes the database schema that the
migrations created.


.. _cookbook-models-in-app:

Using pgcraft models in your application
----------------------------------------

pgcraft factories produce real SQLAlchemy tables registered on a shared
``MetaData`` instance. This means you can query and insert data using
SQLAlchemy Core or ORM directly — in Flask, FastAPI, or any other
framework.

This recipe uses the :func:`~pgcraft.declarative.register` decorator
to define models. The decorator registers the table on ``metadata`` at
import time — access it via
``metadata.tables["<schema>.<tablename>"]`` to get a standard
:class:`sqlalchemy.schema.Table` object.

Since this recipe does not use PostgREST, the plugin list omits
:class:`~pgcraft.plugins.api.APIPlugin` and
:class:`~pgcraft.plugins.simple.SimpleTriggerPlugin`.

Shared models module
~~~~~~~~~~~~~~~~~~~~

Put your pgcraft definitions in a module that both Alembic and your
application import:

.. code-block:: python

   # myapp/models.py
   from sqlalchemy import Column, Integer, MetaData, String

   from pgcraft.declarative import register
   from pgcraft.plugins.pk import SerialPKPlugin
   from pgcraft.plugins.simple import SimpleTablePlugin
   from pgcraft.utils.naming_convention import build_naming_convention

   metadata = MetaData(
       naming_convention=build_naming_convention(),
   )


   @register(
       metadata=metadata,
       plugins=[
           SerialPKPlugin(),
           SimpleTablePlugin(),
       ],
   )
   class Users:
       __tablename__ = "users"
       __table_args__ = {"schema": "public"}

       name = Column(String, nullable=False)
       email = Column(String, nullable=False)
       age = Column(Integer)

   # Grab the generated table for use in queries
   users = metadata.tables["public.users"]

FastAPI example
~~~~~~~~~~~~~~~

.. code-block:: python

   # app.py
   from fastapi import FastAPI, HTTPException
   from pydantic import BaseModel
   from sqlalchemy import create_engine, select

   from myapp.models import users

   engine = create_engine("postgresql+psycopg://localhost/mydb")
   app = FastAPI()


   class UserCreate(BaseModel):
       name: str
       email: str
       age: int | None = None


   @app.get("/users")
   def list_users():
       with engine.connect() as conn:
           rows = conn.execute(select(users)).mappings().all()
           return [dict(r) for r in rows]


   @app.get("/users/{user_id}")
   def get_user(user_id: int):
       with engine.connect() as conn:
           row = conn.execute(
               select(users).where(users.c.id == user_id)
           ).mappings().first()
           if not row:
               raise HTTPException(status_code=404)
           return dict(row)


   @app.post("/users", status_code=201)
   def create_user(body: UserCreate):
       with engine.begin() as conn:
           result = conn.execute(
               users.insert()
               .values(**body.model_dump(exclude_none=True))
               .returning(users)
           )
           return dict(result.mappings().first())

Flask example
~~~~~~~~~~~~~

.. code-block:: python

   # app.py
   from flask import Flask, jsonify, request
   from sqlalchemy import create_engine, select

   from myapp.models import users

   engine = create_engine("postgresql+psycopg://localhost/mydb")
   app = Flask(__name__)


   @app.get("/users")
   def list_users():
       with engine.connect() as conn:
           rows = conn.execute(select(users)).mappings().all()
           return jsonify([dict(r) for r in rows])


   @app.post("/users")
   def create_user():
       data = request.get_json()
       with engine.begin() as conn:
           result = conn.execute(
               users.insert().values(**data).returning(users)
           )
           return jsonify(dict(result.mappings().first())), 201


.. _cookbook-postgrest:

Adding PostgREST API views
--------------------------

pgcraft can generate PostgREST-compatible API views, INSTEAD OF
triggers for write operations, and the role/grant declarations that
PostgREST expects. This is the default behaviour — the built-in
factory plugins include
:class:`~pgcraft.plugins.api.APIPlugin` and
:class:`~pgcraft.plugins.simple.SimpleTriggerPlugin` out of the box.

How it works
~~~~~~~~~~~~

When :class:`~pgcraft.plugins.api.APIPlugin` runs it:

1. Creates a view in the ``api`` schema (configurable) that
   ``SELECT *`` s from the backing table.
2. Registers an :class:`~pgcraft.resource.APIResource` on the
   metadata so that pgcraft can generate role and grant statements.

:class:`~pgcraft.plugins.simple.SimpleTriggerPlugin` then creates
INSTEAD OF ``INSERT`` / ``UPDATE`` / ``DELETE`` triggers on that
view so PostgREST clients can write through it.

Minimal example
~~~~~~~~~~~~~~~

Use the factory defaults — no explicit plugin list needed:

.. code-block:: python

   # models.py
   from sqlalchemy import Column, Integer, MetaData, Numeric, String

   from pgcraft.factory.dimension.simple import (
       SimpleDimensionResourceFactory,
   )
   from pgcraft.utils.naming_convention import build_naming_convention

   metadata = MetaData(
       naming_convention=build_naming_convention(),
   )

   SimpleDimensionResourceFactory(
       tablename="products",
       schemaname="inventory",
       metadata=metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("sku", String(32), nullable=False),
           Column("price", Numeric(10, 2), nullable=False),
       ],
   )

This creates:

* ``inventory.products`` — the backing table with a serial PK.
* ``api.products`` — a view for PostgREST to expose.
* INSTEAD OF triggers so ``INSERT`` / ``UPDATE`` / ``DELETE`` through
  the view are forwarded to the backing table.
* An ``anon`` role with ``SELECT`` grants on ``api.products``.

Customising grants
~~~~~~~~~~~~~~~~~~

By default the ``anon`` role gets only ``SELECT``. Pass a ``grants``
list to :class:`~pgcraft.plugins.api.APIPlugin` to allow writes:

.. code-block:: python

   from pgcraft.plugins.api import APIPlugin
   from pgcraft.plugins.pk import SerialPKPlugin
   from pgcraft.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin

   SimpleDimensionResourceFactory(
       tablename="products",
       schemaname="inventory",
       metadata=metadata,
       schema_items=[
           Column("name", String, nullable=False),
           Column("sku", String(32), nullable=False),
           Column("price", Numeric(10, 2), nullable=False),
       ],
       plugins=[
           SerialPKPlugin(),
           SimpleTablePlugin(),
           APIPlugin(grants=["select", "insert", "update", "delete"]),
           SimpleTriggerPlugin(),
       ],
   )

Changing the API schema
~~~~~~~~~~~~~~~~~~~~~~~

The default API schema is ``api``. Override it with the ``schema``
parameter:

.. code-block:: python

   APIPlugin(schema="reporting")

PostgREST setup
~~~~~~~~~~~~~~~

After generating and applying migrations, point PostgREST at your
database. A minimal ``postgrest.conf``:

.. code-block:: ini

   db-uri = "postgresql://authenticator:changeme@localhost/mydb"
   db-schemas = "api"
   db-anon-role = "anon"
   db-extra-search-path = "public, inventory"

Start the server and query the API:

.. code-block:: bash

   postgrest postgrest.conf

   # List all products
   curl -s http://localhost:3000/products | python3 -m json.tool

   # Filter
   curl -s "http://localhost:3000/products?name=eq.Widget"

   # Insert (requires insert grant)
   curl -s http://localhost:3000/products \
       -H "Content-Type: application/json" \
       -d '{"name": "Widget", "sku": "W-001", "price": 9.99}'

See the `PostgREST documentation <https://docs.postgrest.org>`_ for
the full query syntax and configuration reference.
